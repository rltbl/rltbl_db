//! tokio-postgres implementation for rltbl_db.

use crate::{
    core::{CachingStrategy, DbError, DbQuery},
    db_kind::{DbKind, MAX_PARAMS_POSTGRES},
    db_value::{DbParams, DbRow, DbValue, FromDbRows, IntoDbParams, IntoDbRows},
    shared::{EditType, edit},
};

use bytes::{BufMut, BytesMut};

use deadpool_postgres::{
    Config, Pool, Runtime,
    tokio_postgres::{
        NoTls,
        row::Row,
        types::{FromSql, IsNull, ToSql, Type, to_sql_checked},
    },
};
use rust_decimal::Decimal;

/// Extracts the value at the given index from the given [Row].
fn extract_value(row: &Row, idx: usize) -> Result<DbValue, DbError> {
    let column = &row.columns()[idx];
    match column.type_() {
        &Type::TEXT | &Type::VARCHAR | &Type::NAME => match row
            .try_get::<usize, Option<&str>>(idx)
            .map_err(|err| DbError::DataError(err.to_string()))?
        {
            Some(value) => Ok(value.into()),
            None => Ok(DbValue::Null),
        },
        &Type::INT2 => match row
            .try_get::<usize, Option<i16>>(idx)
            .map_err(|err| DbError::DataError(err.to_string()))?
        {
            Some(value) => Ok(value.into()),
            None => Ok(DbValue::Null),
        },
        &Type::INT4 => match row
            .try_get::<usize, Option<i32>>(idx)
            .map_err(|err| DbError::DataError(err.to_string()))?
        {
            Some(value) => Ok(value.into()),
            None => Ok(DbValue::Null),
        },
        &Type::INT8 => match row
            .try_get::<usize, Option<i64>>(idx)
            .map_err(|err| DbError::DataError(err.to_string()))?
        {
            Some(value) => Ok(value.into()),
            None => Ok(DbValue::Null),
        },
        &Type::BOOL => match row
            .try_get::<usize, Option<bool>>(idx)
            .map_err(|err| DbError::DataError(err.to_string()))?
        {
            Some(value) => Ok(value.into()),
            None => Ok(DbValue::Null),
        },
        &Type::FLOAT4 => match row
            .try_get::<usize, Option<f32>>(idx)
            .map_err(|err| DbError::DataError(err.to_string()))?
        {
            Some(value) => Ok(value.into()),
            None => Ok(DbValue::Null),
        },
        &Type::FLOAT8 => match row
            .try_get::<usize, Option<f64>>(idx)
            .map_err(|err| DbError::DataError(err.to_string()))?
        {
            Some(value) => Ok(value.into()),
            None => Ok(DbValue::Null),
        },
        // WARN: This downcasts a Postgres NUMERIC to a 64 bit Number.
        &Type::NUMERIC => match row
            .try_get::<usize, Option<Decimal>>(idx)
            .map_err(|err| DbError::DataError(err.to_string()))?
        {
            Some(value) => {
                let v = value.to_string();
                if let Ok(number) = v.parse::<u64>() {
                    Ok(number.into())
                } else if let Ok(number) = v.parse::<i64>() {
                    Ok(number.into())
                } else if let Ok(number) = v.parse::<f64>() {
                    Ok(number.into())
                } else {
                    Err(DbError::DataError(format!(
                        "Not a u64, i64, or f64: {value}"
                    )))
                }
            }
            None => Ok(DbValue::Null),
        },
        other => {
            let value: Result<GenericPgValue, DbError> = row
                .try_get(idx)
                .map_err(|_err| DbError::DataError("add a better error message here".to_string()));
            match value {
                Ok(value) => Ok(DbValue::Other(other.to_string(), value.0.unwrap())),
                Err(_) => Ok(DbValue::Null),
            }

            //Err(DbError::DataError(format!(
            //    "Unsupported column type: {}. Supported types are: TEXT, VARCHAR, INT2, INT4, \
            //     INT8, FLOAT4, FLOAT8, NUMERIC, BOOL.",
            //    column.type_()
            //)))
        }
    }
}

// TODO: Possibly move this (and the implementation) to a better location in this file.
#[derive(Clone, Debug)]
struct GenericPgValue(Option<Vec<u8>>);

impl FromSql<'_> for GenericPgValue {
    fn from_sql(
        _: &Type,
        raw: &[u8],
    ) -> Result<GenericPgValue, Box<dyn std::error::Error + Sync + Send>> {
        //let result = std::str::from_utf8(raw).unwrap();
        let val = GenericPgValue(Some(raw.to_owned()));
        Ok(val)
    }
    fn accepts(_ty: &Type) -> bool {
        true
    }
}

impl ToSql for GenericPgValue {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        //println!("SELF: {self:?}, TYPE: {_ty:?}");
        match &self.0 {
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

/// Represents a PostgreSQL database connection pool
#[derive(Debug)]
pub struct TokioPostgresPool {
    pool: Pool,
    caching_strategy: CachingStrategy,
    cache_aware_query: bool,
}

impl TokioPostgresPool {
    /// Connect to a PostgreSQL database using the given url, which should be of the form
    /// postgresql:///DATABASE_NAME
    pub async fn connect(url: &str) -> Result<Self, DbError> {
        match url.starts_with("postgresql:///") {
            true => {
                let mut cfg = Config::new();
                let db_name = url
                    .strip_prefix("postgresql:///")
                    .ok_or(DbError::ConnectError("Invalid PostgreSQL URL".to_string()))?;
                cfg.dbname = Some(db_name.to_string());
                let pool = cfg
                    .create_pool(Some(Runtime::Tokio1), NoTls)
                    .map_err(|err| {
                        DbError::ConnectError(format!("Error creating pool: {err:?}"))
                    })?;
                Ok(Self {
                    pool: pool,
                    caching_strategy: CachingStrategy::None,
                    cache_aware_query: false,
                })
            }
            false => Err(DbError::ConnectError(format!(
                "Invalid PostgreSQL URL: '{url}'"
            ))),
        }
    }
}

impl DbQuery for TokioPostgresPool {
    /// Implements [DbQuery::kind()] for PostgreSQL.
    fn kind(&self) -> DbKind {
        DbKind::PostgreSQL
    }

    /// Implements [DbQuery::set_caching_strategy()] for PostgreSQL.
    fn set_caching_strategy(&mut self, strategy: &CachingStrategy) {
        self.caching_strategy = *strategy;
    }

    /// Implements [DbQuery::get_caching_strategy()] for PostgreSQL.
    fn get_caching_strategy(&self) -> CachingStrategy {
        self.caching_strategy
    }

    /// Implements [DbQuery::set_cache_aware_query()] for PostgreSQL.
    fn set_cache_aware_query(&mut self, flag: bool) {
        self.cache_aware_query = flag;
    }

    /// Implements [DbQuery::get_cache_aware_query()] for PostgreSQL.
    fn get_cache_aware_query(&self) -> bool {
        self.cache_aware_query
    }

    /// Implements [DbQuery::execute_batch()] for PostgreSQL
    async fn execute_batch(&self, sql: &str) -> Result<(), DbError> {
        let client =
            self.pool.get().await.map_err(|err| {
                DbError::ConnectError(format!("Unable to get from pool: {err:?}"))
            })?;
        client
            .batch_execute(sql)
            .await
            .map_err(|err| DbError::DatabaseError(format!("Error in query(): {err:?}")))?;

        self.clear_cache_for_affected_tables(sql).await?;
        Ok(())
    }

    /// Implements [DbQuery::query_no_cache()] for PostgreSQL.
    async fn query_no_cache<T: FromDbRows>(
        &self,
        sql: &str,
        into_db_params: impl IntoDbParams + Send,
    ) -> Result<T, DbError> {
        let into_db_params = into_db_params.into_db_params();
        let client =
            self.pool.get().await.map_err(|err| {
                DbError::ConnectError(format!("Unable to get from pool: {err:?}"))
            })?;

        // The expected types of all of the parameters as reported by the database via prepare():
        let param_pg_types = client
            .prepare(sql)
            .await
            .map_err(|err| DbError::DatabaseError(format!("Error preparing statement: {err:?}")))?
            .params()
            .to_vec();

        let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
        let gen_err = |param: &DbValue, sql_type: &str| -> String {
            format!("DbParam {param:?} is wrong type for {sql_type} in query: {sql}")
        };
        match into_db_params {
            DbParams::None => (),
            DbParams::Positional(plist) => {
                for (i, param) in plist.iter().enumerate() {
                    let pg_type = &param_pg_types[i];
                    match pg_type {
                        &Type::TEXT | &Type::VARCHAR | &Type::NAME => {
                            match param {
                                DbValue::Null => params.push(Box::new(None::<String>)),
                                DbValue::Text(text) => params.push(Box::new(text.to_string())),
                                _ => return Err(DbError::InputError(gen_err(&param, "TEXT"))),
                            };
                        }
                        &Type::INT2 => {
                            match param {
                                DbValue::Null => params.push(Box::new(None::<i16>)),
                                DbValue::SmallInteger(num) => params.push(Box::new(*num)),
                                _ => return Err(DbError::InputError(gen_err(&param, "INT2"))),
                            };
                        }
                        &Type::INT4 => {
                            match param {
                                DbValue::Null => params.push(Box::new(None::<i32>)),
                                DbValue::Integer(num) => params.push(Box::new(*num)),
                                _ => return Err(DbError::InputError(gen_err(&param, "INT4"))),
                            };
                        }
                        &Type::INT8 => {
                            match param {
                                DbValue::Null => params.push(Box::new(None::<i64>)),
                                DbValue::BigInteger(num) => params.push(Box::new(*num)),
                                _ => return Err(DbError::InputError(gen_err(&param, "INT8"))),
                            };
                        }
                        &Type::FLOAT4 => {
                            match param {
                                DbValue::Null => params.push(Box::new(None::<f32>)),
                                DbValue::Real(num) => params.push(Box::new(*num)),
                                _ => return Err(DbError::InputError(gen_err(&param, "FLOAT4"))),
                            };
                        }
                        &Type::FLOAT8 => {
                            match param {
                                DbValue::Null => params.push(Box::new(None::<f64>)),
                                DbValue::BigReal(num) => params.push(Box::new(*num)),
                                _ => return Err(DbError::InputError(gen_err(&param, "FLOAT8"))),
                            };
                        }
                        &Type::NUMERIC => {
                            match param {
                                DbValue::Null => params.push(Box::new(None::<Decimal>)),
                                DbValue::Numeric(num) => params.push(Box::new(*num)),
                                _ => return Err(DbError::InputError(gen_err(&param, "NUMERIC"))),
                            };
                        }
                        &Type::BOOL => {
                            match param {
                                DbValue::Null => params.push(Box::new(None::<bool>)),
                                DbValue::Boolean(flag) => params.push(Box::new(*flag)),
                                _ => return Err(DbError::InputError(gen_err(&param, "BOOL"))),
                            };
                        }
                        _ => {
                            match param {
                                DbValue::Null => {
                                    let cval = GenericPgValue(None);
                                    params.push(Box::new(cval.clone()))
                                }
                                DbValue::Other(_cname, cval) => {
                                    //let cval = std::str::from_utf8(&cval).unwrap().to_string();
                                    //let cval = json!(cval);
                                    let cval = GenericPgValue(Some(cval.clone()));
                                    params.push(Box::new(cval.clone()))
                                }
                                _ => return Err(DbError::InputError(gen_err(&param, "OTHER"))),
                            };
                        }
                    };
                }
            }
        };

        // Finally, execute the query and return the results:
        let query_params: Vec<&(dyn ToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn ToSql + Sync))
            .collect();
        let rows = client
            .query(sql, &query_params)
            .await
            .map_err(|err| DbError::DatabaseError(format!("Error in query(): {err:?}")))?;
        let mut db_rows = vec![];
        for row in &rows {
            let mut db_row = DbRow::new();
            let columns = row.columns();
            for (i, column) in columns.iter().enumerate() {
                db_row.insert(
                    column.name().to_string(),
                    match extract_value(row, i) {
                        Err(err) => {
                            // TODO: If we keep this, should it be a proper tracing::warn!()
                            // call?
                            eprintln!("WARNING: Got error: '{err}' while querying column.");
                            DbValue::Null
                        }
                        Ok(val) => val,
                    },
                );
            }
            db_rows.push(db_row);
        }

        Ok(FromDbRows::from(db_rows))
    }

    /// Implements [DbQuery::insert()] for PostgreSQL
    async fn insert(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
    ) -> Result<(), DbError> {
        let _: Vec<DbRow> = edit(
            self,
            &EditType::Insert,
            &MAX_PARAMS_POSTGRES,
            table,
            columns,
            rows,
            false,
            &[],
        )
        .await?;
        Ok(())
    }

    /// Implements [DbQuery::insert_returning()] for PostgreSQL
    async fn insert_returning<T: FromDbRows>(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
        returning: &[&str],
    ) -> Result<T, DbError> {
        edit(
            self,
            &EditType::Insert,
            &MAX_PARAMS_POSTGRES,
            table,
            columns,
            rows,
            true,
            returning,
        )
        .await
    }

    /// Implements [DbQuery::update()] for PostgreSQL.
    async fn update(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
    ) -> Result<(), DbError> {
        let _: Vec<DbRow> = edit(
            self,
            &EditType::Update,
            &MAX_PARAMS_POSTGRES,
            table,
            columns,
            rows,
            false,
            &[],
        )
        .await?;
        Ok(())
    }

    /// Implements [DbQuery::update_returning()] for PostgreSQL.
    async fn update_returning<T: FromDbRows>(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
        returning: &[&str],
    ) -> Result<T, DbError> {
        edit(
            self,
            &EditType::Update,
            &MAX_PARAMS_POSTGRES,
            table,
            columns,
            rows,
            true,
            returning,
        )
        .await
    }

    /// Implements [DbQuery::upsert()] for PostgreSQL.
    async fn upsert(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
    ) -> Result<(), DbError> {
        let _: Vec<DbRow> = edit(
            self,
            &EditType::Upsert,
            &MAX_PARAMS_POSTGRES,
            table,
            columns,
            rows,
            false,
            &[],
        )
        .await?;
        Ok(())
    }

    /// Implements [DbQuery::upsert_returning()] for PostgreSQL.
    async fn upsert_returning<T: FromDbRows>(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
        returning: &[&str],
    ) -> Result<T, DbError> {
        edit(
            self,
            &EditType::Upsert,
            &MAX_PARAMS_POSTGRES,
            table,
            columns,
            rows,
            true,
            returning,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db_row, params};
    use pretty_assertions::assert_eq;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_aliases_and_builtin_functions() {
        let pool = TokioPostgresPool::connect("postgresql:///rltbl_db")
            .await
            .unwrap();
        pool.execute_batch(
            "DROP TABLE IF EXISTS test_table_indirect CASCADE;\
             CREATE TABLE test_table_indirect (\
                 text_value TEXT,\
                 alt_text_value TEXT,\
                 float_value FLOAT8,\
                 int_value INT8,\
                 bool_value BOOL\
             )",
        )
        .await
        .unwrap();
        pool.execute(
            r#"INSERT INTO test_table_indirect
               (text_value, alt_text_value, float_value, int_value, bool_value)
               VALUES ($1, $2, $3, $4, $5)"#,
            params!["foo", (), 1.05_f64, 1_i64, true],
        )
        .await
        .unwrap();

        // Test aggregate:
        let rows: Vec<DbRow> = pool
            .query("SELECT MAX(int_value) FROM test_table_indirect", ())
            .await
            .unwrap();
        assert_eq!(
            rows,
            [db_row! {
                "max" => 1_i64,
            }]
        );

        // Test alias:
        let rows: Vec<DbRow> = pool
            .query(
                "SELECT bool_value AS bool_value_alias FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows,
            [db_row! {
                "bool_value_alias" => true,
            }]
        );

        // Test aggregate with alias:
        let rows: Vec<DbRow> = pool
            .query(
                "SELECT MAX(int_value) AS max_int_value FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows,
            [db_row! {
                "max_int_value" => 1_i64,
            }]
        );

        // Test non-aggregate function:
        let rows: Vec<DbRow> = pool
            .query(
                "SELECT CAST(int_value AS TEXT) FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows,
            [db_row! {
                "int_value" => "1",
            }]
        );

        // Test non-aggregate function with alias:
        let rows: Vec<DbRow> = pool
            .query(
                "SELECT CAST(int_value AS TEXT) AS int_value_cast FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows,
            [db_row! {
                "int_value_cast" => "1",
            }]
        );

        // Clean up.
        pool.drop_table("test_table_indirect").await.unwrap();
    }

    /// This test is resource intensive and therefore ignored by default. It verifies that
    /// using [MAX_PARAMS_POSTGRES] parameters in a query is indeed supported.
    /// To run this and other ignored tests, use `cargo test -- --ignored` or
    /// `cargo test -- --include-ignored`
    #[tokio::test]
    #[ignore]
    async fn test_max_params() {
        let pool = TokioPostgresPool::connect("postgresql:///rltbl_db")
            .await
            .unwrap();

        pool.execute_batch(
            "DROP TABLE IF EXISTS test_max_params CASCADE;\
             CREATE TABLE test_max_params (\
                 column1 INT,\
                 column2 INT,\
                 column3 INT,\
                 column4 INT,\
                 column5 INT\
             )",
        )
        .await
        .unwrap();

        let mut sql = "INSERT INTO test_max_params VALUES ".to_string();
        let mut values = vec![];
        let mut params = vec![];
        let mut n = 1;
        while n <= MAX_PARAMS_POSTGRES {
            values.push(format!(
                "(${}, ${}, ${}, ${}, ${})",
                n,
                n + 1,
                n + 2,
                n + 3,
                n + 4
            ));
            params.push(1);
            params.push(1);
            params.push(1);
            params.push(1);
            params.push(1);
            n += 5;
        }
        sql.push_str(&values.join(", "));
        pool.execute(&sql, params).await.unwrap();
        pool.drop_table("text_max_params").await.unwrap();
    }

    #[tokio::test]
    async fn test_special_floats() {
        let pool = TokioPostgresPool::connect("postgresql:///rltbl_db")
            .await
            .unwrap();
        pool.drop_table("test_special_floats").await.unwrap();
        pool.execute(
            r#"CREATE TABLE test_special_floats (bar FLOAT, pseudo_bar TEXT)"#,
            (),
        )
        .await
        .unwrap();
        pool.execute(r#"insert into test_special_floats values (+0, '+0')"#, ())
            .await
            .unwrap();
        pool.execute(r#"insert into test_special_floats values (-0, '-0')"#, ())
            .await
            .unwrap();
        for value in ["Infinity", "-Infinity", "NaN"] {
            // Without params:
            let quoted_value = format!("'{value}'");
            pool.execute(
                &format!(
                    r#"insert into test_special_floats values ({quoted_value}, {quoted_value})"#
                ),
                (),
            )
            .await
            .unwrap();

            // With params:
            let float_param = f64::from_str(value).unwrap();
            pool.execute(
                r#"insert into test_special_floats values ($1, $2)"#,
                params![float_param, value],
            )
            .await
            .unwrap();
        }

        let value = pool
            .query_value("select max(bar) from test_special_floats", ())
            .await
            .unwrap();
        match value {
            DbValue::BigReal(num) if num.is_nan() => (),
            _ => panic!(),
        };

        let value = pool
            .query_value("select max(pseudo_bar) from test_special_floats", ())
            .await
            .unwrap();
        match value {
            DbValue::Text(txt) if txt == "NaN" => (),
            _ => panic!(),
        };

        let rows: Vec<DbRow> = pool
            .query(
                r#"select bar from test_special_floats where bar = $1"#,
                params![DbValue::BigReal(f64::NEG_INFINITY)],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);

        let value = rows[0].get("bar").unwrap();
        match value {
            DbValue::BigReal(num) => {
                assert!(num.is_sign_negative());
                assert!(num.is_infinite());
            }
            _ => panic!(),
        };

        let rows: Vec<DbRow> = pool
            .query(
                r#"select pseudo_bar from test_special_floats where pseudo_bar = $1"#,
                &["-Infinity"],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);

        let value = rows[0].get("pseudo_bar").unwrap();
        match value {
            DbValue::Text(txt) => assert_eq!(txt, "-Infinity"),
            _ => panic!(),
        };

        pool.drop_table("test_special_floats").await.unwrap();
    }

    #[tokio::test]
    async fn test_other_types() {
        let pool = TokioPostgresPool::connect("postgresql:///rltbl_db")
            .await
            .unwrap();

        ///////////////////////////////////
        pool.drop_table("test_other_types").await.unwrap();
        pool.execute(
            r#"CREATE TABLE test_other_types (bar JSONB, foo BOOL DEFAULT FALSE)"#,
            (),
        )
        .await
        .unwrap();

        pool.execute(r#"INSERT INTO test_other_types VALUES ('{}')"#, ())
            .await
            .unwrap();

        let mut db_row: Vec<DbRow> = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_row.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            r#"DbRow { map: {"bar": Other("jsonb", [1, 123, 125]), "foo": Boolean(false)} }"#
        );

        let db_value = db_row.get("bar").unwrap();
        pool.execute(
            r#"UPDATE test_other_types SET foo = TRUE WHERE bar = $1"#,
            params![db_value],
        )
        .await
        .unwrap();
        let mut db_row: Vec<DbRow> = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_row.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            r#"DbRow { map: {"bar": Other("jsonb", [1, 123, 125]), "foo": Boolean(true)} }"#
        );
        ///////////////////////////////////
        pool.drop_table("test_other_types").await.unwrap();
        pool.execute(
            r#"CREATE TABLE test_other_types (bar BYTEA, foo BOOL DEFAULT FALSE)"#,
            (),
        )
        .await
        .unwrap();

        pool.execute(
            r#"INSERT INTO test_other_types VALUES ('\xDEADBEEF'::bytea)"#,
            (),
        )
        .await
        .unwrap();
        let mut db_row: Vec<DbRow> = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_row.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            r#"DbRow { map: {"bar": Other("bytea", [222, 173, 190, 239]), "foo": Boolean(false)} }"#
        );
        let db_value = db_row.get("bar").unwrap();
        pool.execute(
            r#"UPDATE test_other_types SET foo = TRUE WHERE bar = $1"#,
            params![db_value],
        )
        .await
        .unwrap();
        let mut db_row: Vec<DbRow> = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_row.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            r#"DbRow { map: {"bar": Other("bytea", [222, 173, 190, 239]), "foo": Boolean(true)} }"#
        );
        ///////////////////////////////////
        pool.drop_table("test_other_types").await.unwrap();
        pool.execute(
            r#"CREATE TABLE test_other_types (bar TIMESTAMP, foo BOOL DEFAULT FALSE)"#,
            (),
        )
        .await
        .unwrap();
        pool.execute(
            r#"INSERT INTO test_other_types VALUES ('2004-10-19 10:23:54')"#,
            (),
        )
        .await
        .unwrap();
        let mut db_row: Vec<DbRow> = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_row.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            "DbRow { map: {\"bar\": Other(\"timestamp\", [0, 0, 137, 201, 15, 13, 226, 128]), \
             \"foo\": Boolean(false)} }"
        );
        let db_value = db_row.get("bar").unwrap();
        pool.execute(
            r#"UPDATE test_other_types SET foo = TRUE WHERE bar = $1"#,
            params![db_value],
        )
        .await
        .unwrap();
        let mut db_row: Vec<DbRow> = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_row.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            r#"DbRow { map: {"bar": Other("timestamp", [0, 0, 137, 201, 15, 13, 226, 128]), "foo": Boolean(true)} }"#
        );
        ///////////////////////////////////
        pool.drop_table("test_other_types").await.unwrap();
        pool.execute(
            r#"CREATE TABLE test_other_types (bar TEXT[], foo BOOL DEFAULT FALSE)"#,
            (),
        )
        .await
        .unwrap();
        pool.execute(
            r#"INSERT INTO test_other_types VALUES ('{"meeting", "lunch"}')"#,
            (),
        )
        .await
        .unwrap();
        let mut db_row: Vec<DbRow> = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_row.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            r#"DbRow { map: {"bar": Other("_text", [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 25, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 7, 109, 101, 101, 116, 105, 110, 103, 0, 0, 0, 5, 108, 117, 110, 99, 104]), "foo": Boolean(false)} }"#
        );
        let db_value = db_row.get("bar").unwrap();
        pool.execute(
            r#"UPDATE test_other_types SET foo = TRUE WHERE bar = $1"#,
            params![db_value],
        )
        .await
        .unwrap();
        let mut db_row: Vec<DbRow> = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_row.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            r#"DbRow { map: {"bar": Other("_text", [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 25, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 7, 109, 101, 101, 116, 105, 110, 103, 0, 0, 0, 5, 108, 117, 110, 99, 104]), "foo": Boolean(true)} }"#
        );

        ///////////////////////////////////
        // Check if NULL values for other types are handled correctly:
        pool.drop_table("test_other_types").await.unwrap();
        pool.execute(
            r#"CREATE TABLE test_other_types (bar TIMESTAMP, foo BOOL DEFAULT FALSE)"#,
            (),
        )
        .await
        .unwrap();

        let db_rows: Vec<DbRow> = pool
            .query(
                r#"SELECT * FROM test_other_types WHERE bar = $1"#,
                params![DbValue::Null],
            )
            .await
            .unwrap();
        assert_eq!(db_rows.len(), 0);

        pool.execute(
            r#"INSERT INTO test_other_types VALUES ('2004-10-19 10:23:54')"#,
            (),
        )
        .await
        .unwrap();

        pool.execute(
            r#"UPDATE test_other_types SET bar = $1"#,
            params![DbValue::Null],
        )
        .await
        .unwrap();

        let mut db_rows: Vec<DbRow> = pool
            .query(
                r#"SELECT * FROM test_other_types WHERE bar IS NOT DISTINCT FROM $1"#,
                params![DbValue::Null],
            )
            .await
            .unwrap();
        let db_row = db_rows.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            r#"DbRow { map: {"bar": Null, "foo": Boolean(false)} }"#
        );
    }
}
