//! Driver using deadpool-sqlite (rusqlite).

use async_trait::async_trait;
use deadpool_postgres::{
    Config,
    // Pool,
    Runtime,
    tokio_postgres::{
        // Error,
        NoTls,
        row::Row as PgRow,
        types::{ToSql, Type},
    },
};

use crate::{
    Error, Pool, Query, Row, Rows, Syntax, Transaction, Value, postgresql::PostgresSyntax,
};

#[derive(Debug)]
pub struct PostgresPool {
    syntax: PostgresSyntax,
    pool: deadpool_postgres::Pool,
}

impl PostgresPool {
    pub async fn connect(url: &str) -> Result<Self, Error> {
        match url.starts_with("postgresql:///") {
            true => {
                let mut cfg = Config::new();
                let db_name = url
                    .strip_prefix("postgresql:///")
                    .ok_or(Error::ConnectError("Invalid PostgreSQL URL".to_string()))?;
                cfg.dbname = Some(db_name.to_string());
                let pool = cfg
                    .create_pool(Some(Runtime::Tokio1), NoTls)
                    .map_err(|err| Error::ConnectError(format!("Error creating pool: {err:?}")))?;
                Ok(Self {
                    pool: pool,
                    syntax: PostgresSyntax,
                })
            }
            false => Err(Error::ConnectError(format!(
                "Invalid PostgreSQL URL: '{url}'"
            ))),
        }
    }
}

/// TODO: Add docstring.
fn extract_value(row: &PgRow, idx: usize) -> Result<Value, Error> {
    let column = &row.columns()[idx];
    match column.type_() {
        &Type::TEXT | &Type::VARCHAR | &Type::NAME => match row
            .try_get::<usize, Option<&str>>(idx)
            .map_err(|err| Error::DataError(err.to_string()))?
        {
            Some(value) => Ok(value.into()),
            None => Ok(Value::Null),
        },
        // &Type::INT2 => match row
        //     .try_get::<usize, Option<i16>>(idx)
        //     .map_err(|err| Error::DataError(err.to_string()))?
        // {
        //     Some(value) => Ok(value.into()),
        //     None => Ok(Value::Null),
        // },
        // &Type::INT4 => match row
        //     .try_get::<usize, Option<i32>>(idx)
        //     .map_err(|err| Error::DataError(err.to_string()))?
        // {
        //     Some(value) => Ok(value.into()),
        //     None => Ok(Value::Null),
        // },
        &Type::INT8 => match row
            .try_get::<usize, Option<i64>>(idx)
            .map_err(|err| Error::DataError(err.to_string()))?
        {
            Some(value) => Ok(value.into()),
            None => Ok(Value::Null),
        },
        // &Type::BOOL => match row
        //     .try_get::<usize, Option<bool>>(idx)
        //     .map_err(|err| Error::DataError(err.to_string()))?
        // {
        //     Some(value) => Ok(value.into()),
        //     None => Ok(Value::Null),
        // },
        // &Type::FLOAT4 => match row
        //     .try_get::<usize, Option<f32>>(idx)
        //     .map_err(|err| Error::DataError(err.to_string()))?
        // {
        //     Some(value) => Ok(value.into()),
        //     None => Ok(Value::Null),
        // },
        // &Type::FLOAT8 => match row
        //     .try_get::<usize, Option<f64>>(idx)
        //     .map_err(|err| Error::DataError(err.to_string()))?
        // {
        //     Some(value) => Ok(value.into()),
        //     None => Ok(Value::Null),
        // },
        // // WARN: This downcasts a Postgres NUMERIC to a 64 bit Number.
        // &Type::NUMERIC => match row
        //     .try_get::<usize, Option<Decimal>>(idx)
        //     .map_err(|err| Error::DataError(err.to_string()))?
        // {
        //     Some(value) => {
        //         let v = value.to_string();
        //         if let Ok(number) = v.parse::<u64>() {
        //             Ok(number.into())
        //         } else if let Ok(number) = v.parse::<i64>() {
        //             Ok(number.into())
        //         } else if let Ok(number) = v.parse::<f64>() {
        //             Ok(number.into())
        //         } else {
        //             Err(Error::DataError(format!(
        //                 "Not a u64, i64, or f64: {value}"
        //             )))
        //         }
        //     }
        //     None => Ok(Value::Null),
        // },
        // &Type::JSON | &Type::JSONB => {
        //     let value = row
        //         .try_get::<usize, JsonValue>(idx)
        //         .map_err(|err| Error::DataError(err.to_string()))?;
        //     Ok(Value::Json(value))
        // }
        // other => {
        //     let value: Result<GenericTypeValue, Error> = row.try_get(idx).map_err(|_err| {
        //         Error::DataError(format!(
        //             "Error getting value of type '{other}' at index {idx} from row {row:?}"
        //         ))
        //     });
        //     match value {
        //         Ok(value) => match value.bytes {
        //             Some(bytes) => {
        //                 let string_opt = match std::str::from_utf8(&bytes) {
        //                     Ok(string) => Some(string.to_string()),
        //                     Err(_err) => None,
        //                 };
        //                 Ok(Value::Other(other.to_string(), bytes, string_opt))
        //             }
        //             None => Ok(Value::Null),
        //         },
        //         Err(_) => Ok(Value::Null),
        //     }
        // }
        other => panic!("Unsupported type: {other}"),
    }
}

#[async_trait]
impl Query for PostgresPool {
    /// TODO: Add docstring.
    fn syntax(&self) -> &dyn Syntax {
        &self.syntax
    }

    /// TODO: Add docstring.
    async fn query(&self, sql: &str, params: &[&Value]) -> Result<Rows, Error> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| Error::ConnectError(format!("Unable to get from pool: {err:?}")))?;

        // The expected types of all of the parameters as reported by the database via prepare():
        let param_pg_types = client
            .prepare(sql)
            .await
            .map_err(|err| Error::DatabaseError(format!("Error preparing statement: {err:?}")))?
            .params()
            .to_vec();

        let mut paramses: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
        let gen_err = |param: &Value, sql_type: &str| -> String {
            format!("DbParam {param:?} is wrong type for {sql_type} in query: {sql}")
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
                // &Type::INT2 => {
                //     match param {
                //         Value::Null => paramses.push(Box::new(None::<i16>)),
                //         Value::SmallInteger(num) => paramses.push(Box::new(*num)),
                //         _ => return Err(Error::InputError(gen_err(&param, "INT2"))),
                //     };
                // }
                // &Type::INT4 => {
                //     match param {
                //         Value::Null => paramses.push(Box::new(None::<i32>)),
                //         Value::Integer(num) => paramses.push(Box::new(*num)),
                //         _ => return Err(Error::InputError(gen_err(&param, "INT4"))),
                //     };
                // }
                &Type::INT8 => {
                    match param {
                        Value::Null => paramses.push(Box::new(None::<i64>)),
                        Value::BigInteger(num) => paramses.push(Box::new(*num)),
                        _ => return Err(Error::InputError(gen_err(&param, "INT8"))),
                    };
                }
                // &Type::FLOAT4 => {
                //     match param {
                //         Value::Null => paramses.push(Box::new(None::<f32>)),
                //         Value::Real(num) => paramses.push(Box::new(*num)),
                //         _ => return Err(Error::InputError(gen_err(&param, "FLOAT4"))),
                //     };
                // }
                &Type::FLOAT8 => {
                    match param {
                        Value::Null => paramses.push(Box::new(None::<f64>)),
                        Value::BigReal(num) => paramses.push(Box::new(*num)),
                        _ => return Err(Error::InputError(gen_err(&param, "FLOAT8"))),
                    };
                }
                // &Type::NUMERIC => {
                //     match param {
                //         Value::Null => paramses.push(Box::new(None::<Decimal>)),
                //         Value::Numeric(num) => paramses.push(Box::new(*num)),
                //         _ => return Err(Error::InputError(gen_err(&param, "NUMERIC"))),
                //     };
                // }
                &Type::BOOL => {
                    match param {
                        Value::Null => paramses.push(Box::new(None::<bool>)),
                        Value::Boolean(flag) => paramses.push(Box::new(*flag)),
                        _ => return Err(Error::InputError(gen_err(&param, "BOOL"))),
                    };
                }
                // &Type::JSON | &Type::JSONB => match param {
                //     Value::Null => paramses.push(Box::new(None::<JsonValue>)),
                //     Value::Json(value) => paramses.push(Box::new(value.clone())),
                //     _ => {
                //         return Err(Error::InputError(gen_err(
                //             &param,
                //             &pg_type.to_string(),
                //         )));
                //     }
                // },
                // other => {
                //     match param {
                //         Value::Null => {
                //             paramses.push(Box::new(GenericTypeValue { bytes: None }))
                //         }
                //         Value::Other(_cname, bytes, _string_opt) => {
                //             paramses.push(Box::new(GenericTypeValue {
                //                 bytes: Some(bytes.clone()),
                //             }))
                //         }
                //         _ => {
                //             return Err(Error::InputError(gen_err(
                //                 &param,
                //                 &other.to_string(),
                //             )));
                //         }
                //     };
                // }
                other => panic!("Unsupported type: {other}"),
            };
        }

        // Finally, execute the query and return the results:
        let query_params: Vec<&(dyn ToSql + Sync)> = paramses
            .iter()
            .map(|p| p.as_ref() as &(dyn ToSql + Sync))
            .collect();
        let rows = client
            .query(sql, &query_params)
            .await
            .map_err(|err| Error::DatabaseError(format!("Error in query(): {err:?}")))?;
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
}

#[async_trait]
impl Pool for PostgresPool {
    async fn transaction(&self) -> Result<Box<dyn Transaction>, Error> {
        panic!("Don't use this! Use builtin driver methods instead")
    }
}

#[cfg(test)]
mod tests {
    use crate::{AnyPool, Error, Pool, Value, tokio_postgres::PostgresPool, values};

    #[tokio::test]
    async fn test_postgres_anypool() -> Result<(), Error> {
        let url = "postgresql:///rltbl_db";
        let pool = PostgresPool::connect(url).await?;
        let pool: Box<dyn Pool> = Box::new(pool);
        let pool = AnyPool::from(pool);

        let _rows = pool.query("DROP TABLE IF EXISTS foo CASCADE", &[]).await?;
        let _rows = pool
            .query("CREATE TABLE foo (bar BIGINT, gar TEXT)", &[])
            .await?;
        let sql = "INSERT INTO foo VALUES ($1, $2)";
        let values = vec![Value::from(1_i64), Value::from("foo")];
        let _values = values![1_i64, "foo"];
        let _rows = pool.query(sql, &values).await?;
        let _rows = pool.query("DROP TABLE foo CASCADE", &[]).await?;

        Ok(())
    }
}
