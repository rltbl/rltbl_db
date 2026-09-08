//! Driver using deadpool-postgres (tokio-postgres).

use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use deadpool_postgres::{
    Config, Runtime,
    tokio_postgres::{
        NoTls,
        row::Row as PgRow,
        types::{FromSql, IsNull, ToSql, Type, to_sql_checked},
    },
};
use futures_util::{SinkExt, stream};
use indexmap::IndexMap;
use rust_decimal::Decimal;
use std::{
    fs::File,
    io::{BufRead, BufReader},
    pin::pin,
};

use crate::{
    Column, Error, JsonValue, Pool, Query, Row, Rows, Syntax, Transaction, Value,
    postgres::PostgresSyntax, sql_parse::validate_table_name,
};

/// Extracts the value at the given index from the given [PgRow].
fn extract_value(row: &PgRow, idx: usize) -> Result<Value, Error> {
    let column = &row.columns()[idx];
    match column.type_() {
        &Type::TEXT | &Type::VARCHAR | &Type::NAME => {
            match row.try_get::<usize, Option<&str>>(idx)? {
                Some(value) => Ok(value.into()),
                None => Ok(Value::Null),
            }
        }
        &Type::INT2 => match row.try_get::<usize, Option<i16>>(idx)? {
            Some(value) => Ok(value.into()),
            None => Ok(Value::Null),
        },
        &Type::INT4 => match row.try_get::<usize, Option<i32>>(idx)? {
            Some(value) => Ok(value.into()),
            None => Ok(Value::Null),
        },
        &Type::INT8 => match row.try_get::<usize, Option<i64>>(idx)? {
            Some(value) => Ok(value.into()),
            None => Ok(Value::Null),
        },
        &Type::BOOL => match row.try_get::<usize, Option<bool>>(idx)? {
            Some(value) => Ok(value.into()),
            None => Ok(Value::Null),
        },
        &Type::FLOAT4 => match row.try_get::<usize, Option<f32>>(idx)? {
            Some(value) => Ok(value.into()),
            None => Ok(Value::Null),
        },
        &Type::FLOAT8 => match row.try_get::<usize, Option<f64>>(idx)? {
            Some(value) => Ok(value.into()),
            None => Ok(Value::Null),
        },
        // WARN: This downcasts a Postgres NUMERIC to a 64 bit Number.
        &Type::NUMERIC => match row.try_get::<usize, Option<Decimal>>(idx)? {
            Some(value) => {
                let v = value.to_string();
                if let Ok(number) = v.parse::<u64>() {
                    Ok(number.into())
                } else if let Ok(number) = v.parse::<i64>() {
                    Ok(number.into())
                } else if let Ok(number) = v.parse::<f64>() {
                    Ok(number.into())
                } else {
                    Err(Error::DataError(format!("Not a u64, i64, or f64: {value}")))
                }
            }
            None => Ok(Value::Null),
        },
        &Type::JSON | &Type::JSONB => {
            let value = row.try_get::<usize, JsonValue>(idx)?;
            Ok(Value::Json(value))
        }
        other => {
            let value: Result<GenericPostgresType, Error> = row
                .try_get(idx)
                .map_err(|err| Error::DatatypeError(err.to_string()));
            match value {
                Ok(value) => match value.bytes {
                    Some(bytes) => {
                        let string_opt = match std::str::from_utf8(&bytes) {
                            Ok(string) => Some(string.to_string()),
                            Err(_err) => None,
                        };
                        Ok(Value::Other(other.to_string(), bytes, string_opt))
                    }
                    None => Ok(Value::Null),
                },
                Err(_) => Ok(Value::Null),
            }
        }
    }
}

/// Represents a deadool-postgres database connection pool.
#[derive(Debug)]
pub struct PostgresPool {
    syntax: PostgresSyntax,
    pool: deadpool_postgres::Pool,
}

impl PostgresPool {
    /// Connect to the PostgreSQL database at the given URL.
    pub async fn connect(url: &str) -> Result<Self, Error> {
        match url.starts_with("postgresql:///") {
            true => {
                let mut cfg = Config::new();
                let db_name = url
                    .strip_prefix("postgresql:///")
                    .ok_or(Error::ConnectError("Invalid Postgres URL".to_string()))?;
                cfg.dbname = Some(db_name.to_string());
                let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;
                Ok(Self {
                    pool: pool,
                    syntax: PostgresSyntax,
                })
            }
            false => Err(Error::ConnectError(format!(
                "Invalid Postgres URL: '{url}'"
            ))),
        }
    }
}

/// Represents a PostgreSQL datatype that is not explicitly handled in extract_value() and query().
#[derive(Clone, Debug)]
pub struct GenericPostgresType {
    // Raw representation of the value.
    bytes: Option<Vec<u8>>,
}

impl FromSql<'_> for GenericPostgresType {
    fn from_sql(
        _ty: &Type,
        raw: &[u8],
    ) -> Result<GenericPostgresType, Box<dyn std::error::Error + Sync + Send>> {
        Ok(GenericPostgresType {
            bytes: Some(raw.to_owned()),
        })
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

impl ToSql for GenericPostgresType {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        match &self.bytes {
            Some(val) => {
                out.put(&**val);
                Ok(IsNull::No)
            }
            None => Ok(IsNull::Yes),
        }
    }

    fn accepts(_ty: &Type) -> bool
    where
        Self: Sized,
    {
        true
    }

    to_sql_checked!();
}

#[async_trait]
impl Query for PostgresPool {
    /// Implements [Query::syntax()] for [PostgresPool]
    fn syntax(&self) -> &dyn Syntax {
        &self.syntax
    }

    /// Implements [Query::execute_batch()] for [PostgresPool]
    async fn execute_batch(&self, sql: &str) -> Result<(), Error> {
        let client = self.pool.get().await?;
        client.batch_execute(sql).await?;
        Ok(())
    }

    /// Implements [Query::query()] for [PostgresPool]
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Rows, Error> {
        let client = self.pool.get().await?;

        // The expected types of all of the parameters as reported by the database via prepare():
        let param_pg_types = client.prepare(sql).await?.params().to_vec();

        let mut paramses: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
        let gen_err = |param: &Value, sql_type: &str| -> String {
            format!("Param {param:?} is wrong type for {sql_type} in query: {sql}")
        };

        for (i, param) in params.iter().enumerate() {
            let pg_type = &param_pg_types[i];
            match pg_type {
                &Type::TEXT | &Type::VARCHAR | &Type::NAME => {
                    match param {
                        Value::Null => paramses.push(Box::new(None::<String>)),
                        Value::Text(text) => paramses.push(Box::new(text.to_string())),
                        _ => return Err(Error::InputError(gen_err(&param, "TEXT"))),
                    };
                }
                &Type::INT2 => {
                    match param {
                        Value::Null => paramses.push(Box::new(None::<i16>)),
                        Value::SmallInteger(num) => paramses.push(Box::new(*num)),
                        _ => return Err(Error::InputError(gen_err(&param, "INT2"))),
                    };
                }
                &Type::INT4 => {
                    match param {
                        Value::Null => paramses.push(Box::new(None::<i32>)),
                        Value::Integer(num) => paramses.push(Box::new(*num)),
                        _ => return Err(Error::InputError(gen_err(&param, "INT4"))),
                    };
                }
                &Type::INT8 => {
                    match param {
                        Value::Null => paramses.push(Box::new(None::<i64>)),
                        Value::BigInteger(num) => paramses.push(Box::new(*num)),
                        _ => return Err(Error::InputError(gen_err(&param, "INT8"))),
                    };
                }
                &Type::FLOAT4 => {
                    match param {
                        Value::Null => paramses.push(Box::new(None::<f32>)),
                        Value::Real(num) => paramses.push(Box::new(*num)),
                        _ => return Err(Error::InputError(gen_err(&param, "FLOAT4"))),
                    };
                }
                &Type::FLOAT8 => {
                    match param {
                        Value::Null => paramses.push(Box::new(None::<f64>)),
                        Value::BigReal(num) => paramses.push(Box::new(*num)),
                        _ => return Err(Error::InputError(gen_err(&param, "FLOAT8"))),
                    };
                }
                &Type::NUMERIC => {
                    match param {
                        Value::Null => paramses.push(Box::new(None::<Decimal>)),
                        Value::Numeric(num) => paramses.push(Box::new(*num)),
                        _ => return Err(Error::InputError(gen_err(&param, "NUMERIC"))),
                    };
                }
                &Type::BOOL => {
                    match param {
                        Value::Null => paramses.push(Box::new(None::<bool>)),
                        Value::Boolean(flag) => paramses.push(Box::new(*flag)),
                        _ => return Err(Error::InputError(gen_err(&param, "BOOL"))),
                    };
                }
                &Type::JSON | &Type::JSONB => match param {
                    Value::Null => paramses.push(Box::new(None::<JsonValue>)),
                    Value::Json(value) => paramses.push(Box::new(value.clone())),
                    _ => {
                        return Err(Error::InputError(gen_err(&param, &pg_type.to_string())));
                    }
                },
                other => {
                    match param {
                        Value::Null => paramses.push(Box::new(GenericPostgresType { bytes: None })),
                        Value::Other(_cname, bytes, _string_opt) => {
                            paramses.push(Box::new(GenericPostgresType {
                                bytes: Some(bytes.clone()),
                            }))
                        }
                        _ => {
                            return Err(Error::InputError(gen_err(&param, &other.to_string())));
                        }
                    };
                }
            };
        }

        // Finally, execute the query and return the results:
        let query_params: Vec<&(dyn ToSql + Sync)> = paramses
            .iter()
            .map(|p| p.as_ref() as &(dyn ToSql + Sync))
            .collect();
        let rows = client.query(sql, &query_params).await?;
        let mut db_rows = vec![];
        for row in &rows {
            let mut db_row = Row::new();
            let columns = row.columns();
            for (i, column) in columns.iter().enumerate() {
                db_row.insert(
                    column.name().to_string(),
                    match extract_value(row, i) {
                        Err(err) => {
                            eprintln!("WARNING: Got error: '{err}' while querying column.");
                            Value::Null
                        }
                        Ok(val) => val,
                    },
                );
            }
            db_rows.push(db_row);
        }

        Ok(Rows { rows: db_rows })
    }

    /// Load the given table using the data from the given file.
    async fn load_table(
        &self,
        table: &str,
        _columns: &IndexMap<String, Column>,
        filename: &str,
    ) -> Result<(), Error> {
        eprintln!("Loading table '{table}' from '{filename}' using PostgreSQL's COPY IN command.");
        if !filename.to_lowercase().ends_with("tsv") && !filename.to_lowercase().ends_with(".csv") {
            return Err(Error::InputError(format!(
                "Filename: '{filename}' must end with .tsv or .csv"
            )));
        }
        let file = File::open(filename)
            .map_err(|err| Error::InputError(format!("Unable to open '{filename}': {err}")))?;
        let buf_reader = BufReader::new(file);
        let mut stream = buf_reader.split(b'\n').map(|line| {
            let line = line.unwrap();
            let mut bytes = BytesMut::with_capacity(line.len() + 1);
            bytes.extend_from_slice(&line);
            bytes.put_u8(b'\n');
            bytes
        });

        // Send the input stream to the tokio-postgres client which is executing a copy_in():
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| Error::ConnectError(format!("Unable to get from pool: {err:?}")))?;
        let mut sink = pin!(
            client
                .copy_in(&format!(r#"COPY "{table}" FROM STDIN WITH NULL ''"#))
                .await
                .map_err(|err| {
                    Error::InputError(format!("Unable to COPY IN to '{table}': {err}"))
                })?
        );

        // Ignore the header line:
        stream
            .next()
            .ok_or(Error::InputError(format!("File '{filename}' is empty")))?;
        let mut stream =
            stream::iter(stream.map(Ok::<_, deadpool_postgres::tokio_postgres::Error>));

        sink.send_all(&mut stream)
            .await
            .map_err(|err| Error::InputError(format!("Unable to COPY IN to '{table}': {err}")))?;
        let _num_written = sink
            .finish()
            .await
            .map_err(|err| Error::InputError(format!("Unable to COPY IN to '{table}': {err}")))?;

        Ok(())
    }

    async fn drop_table(&self, table: &str) -> Result<(), Error> {
        let table = validate_table_name(table)?;

        self.execute(&format!(r#"DROP TABLE IF EXISTS "{table}" CASCADE"#), &[])
            .await?;
        Ok(())
    }

    async fn drop_view(&self, view: &str) -> Result<(), Error> {
        let view = validate_table_name(view)?;

        // Drop the view:
        self.execute(&format!(r#"DROP VIEW IF EXISTS "{view}" CASCADE"#), &[])
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Pool for PostgresPool {
    async fn transaction(&self) -> Result<Box<dyn Transaction>, Error> {
        panic!("Don't use this! Use builtin driver methods instead")
    }
}

// TODO: Move these tests to unit_tests.rs

#[cfg(test)]
mod tests {
    use crate::{AnyPool, Error, Pool, Value, tokio_postgres::PostgresPool, values};

    #[tokio::test]
    async fn test_postgres_anypool() -> Result<(), Error> {
        let url = "postgresql:///rltbl_db";
        let pool = PostgresPool::connect(url).await?;
        let pool: Box<dyn Pool> = Box::new(pool);
        let pool = AnyPool::from(pool);

        let _rows = pool.query("DROP TABLE IF EXISTS foo CASCADE", ()).await?;
        let _rows = pool
            .query("CREATE TABLE foo (bar BIGINT, gar TEXT)", ())
            .await?;
        let sql = "INSERT INTO foo VALUES ($1, $2)";
        let values = vec![Value::from(1_i64), Value::from("foo")];
        let _values = values![1_i64, "foo"];
        let _rows = pool.query(sql, &values).await?;
        let _rows = pool.query("DROP TABLE foo CASCADE", ()).await?;

        Ok(())
    }
}
