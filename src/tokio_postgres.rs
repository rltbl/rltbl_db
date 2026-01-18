//! tokio-postgres implementation for rltbl_db.

use crate::{
    core::{
        CachingStrategy, ColumnMap, DbError, DbKind, DbQuery, DbRow, FromDbRows, IntoDbRows,
        IntoParams, ParamValue, Params, validate_table_name,
    },
    params,
    shared::{EditType, edit},
};

use deadpool_postgres::{Config, Pool, Runtime};
use rust_decimal::Decimal;
use tokio_postgres::{
    NoTls,
    row::Row,
    types::{ToSql, Type},
};

/// The [maximum number of parameters](https://www.postgresql.org/docs/current/limits.html)
/// that can be bound to a Postgres query is 65535. This has been true since at least PostgreSQL
/// version 12. However, for some (unknown) reason, tokio-postgres limits the actual number of
/// parameters to just under half that number.
pub static MAX_PARAMS_POSTGRES: usize = 32765;

/// Extracts the value at the given index from the given [Row].
fn extract_value(row: &Row, idx: usize) -> Result<ParamValue, DbError> {
    let column = &row.columns()[idx];
    match *column.type_() {
        Type::TEXT | Type::VARCHAR | Type::NAME => match row
            .try_get::<usize, Option<&str>>(idx)
            .map_err(|err| DbError::DataError(err.to_string()))?
        {
            Some(value) => Ok(value.into()),
            None => Ok(ParamValue::Null),
        },
        Type::INT2 => match row
            .try_get::<usize, Option<i16>>(idx)
            .map_err(|err| DbError::DataError(err.to_string()))?
        {
            Some(value) => Ok(value.into()),
            None => Ok(ParamValue::Null),
        },
        Type::INT4 => match row
            .try_get::<usize, Option<i32>>(idx)
            .map_err(|err| DbError::DataError(err.to_string()))?
        {
            Some(value) => Ok(value.into()),
            None => Ok(ParamValue::Null),
        },
        Type::INT8 => match row
            .try_get::<usize, Option<i64>>(idx)
            .map_err(|err| DbError::DataError(err.to_string()))?
        {
            Some(value) => Ok(value.into()),
            None => Ok(ParamValue::Null),
        },
        Type::BOOL => match row
            .try_get::<usize, Option<bool>>(idx)
            .map_err(|err| DbError::DataError(err.to_string()))?
        {
            Some(value) => Ok(value.into()),
            None => Ok(ParamValue::Null),
        },
        Type::FLOAT4 => match row
            .try_get::<usize, Option<f32>>(idx)
            .map_err(|err| DbError::DataError(err.to_string()))?
        {
            Some(value) => Ok(value.into()),
            None => Ok(ParamValue::Null),
        },
        Type::FLOAT8 => match row
            .try_get::<usize, Option<f64>>(idx)
            .map_err(|err| DbError::DataError(err.to_string()))?
        {
            Some(value) => Ok(value.into()),
            None => Ok(ParamValue::Null),
        },
        // WARN: This downcasts a Postgres NUMERIC to a 64 bit Number.
        Type::NUMERIC => match row
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
            None => Ok(ParamValue::Null),
        },
        _ => {
            eprint!("Unimplemented column type: {column:?}");
            unimplemented!();
        }
    }
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

    /// Implements [DbQuery::ensure_cache_table_exists()] for PostgreSQL.
    async fn ensure_cache_table_exists(&self) -> Result<(), DbError> {
        match self
            .execute_no_cache(
                r#"CREATE TABLE IF NOT EXISTS "cache" (
                     "tables" TEXT,
                     "statement" TEXT,
                     "parameters" TEXT,
                     "value" TEXT,
                     PRIMARY KEY ("tables", "statement", "parameters")
                   )"#,
                (),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(_) => {
                // Since we are not using transactions, a race condition could occur in
                // which two or more threads are trying to create the cache at the same
                // time, triggering a primary key violation in the metadata table. So if
                // there is an error creating the cache table we just check that it exists
                // and if it does we assume that all is ok.
                match self.table_exists("cache").await? {
                    false => Err(DbError::DatabaseError(
                        "The cache table could not be created".to_string(),
                    )),
                    true => Ok(()),
                }
            }
        }
    }

    /// Implements [DbQuery::ensure_caching_triggers_exist()] for PostgreSQL.
    async fn ensure_caching_triggers_exist(&self, tables: &[&str]) -> Result<(), DbError> {
        self.ensure_cache_table_exists().await?;
        for table in tables {
            let rows: Vec<DbRow> = self
                .query_no_cache(
                    r#"SELECT 1
                       FROM information_schema.triggers
                       WHERE trigger_name IN ($1, $2, $3)
                         AND "trigger_schema" IN (
                           SELECT REGEXP_SPLIT_TO_TABLE("setting", ', ')
                           FROM "pg_settings"
                           WHERE "name" = 'search_path'
                         )"#,
                    &[
                        &format!("{table}_cache_after_insert"),
                        &format!("{table}_cache_after_update"),
                        &format!("{table}_cache_after_delete"),
                    ],
                )
                .await?;

            // Only recreate the triggers if they don't all already exist:
            if rows.len() != 3 {
                // Note that parameters are not allowed in trigger creation statements in PostgreSQL.
                self.execute_batch(&format!(
                    r#"CREATE OR REPLACE FUNCTION "clean_cache_for_{table}"()
                         RETURNS TRIGGER
                         LANGUAGE PLPGSQL
                        AS
                        $$
                        BEGIN
                          DELETE FROM "cache" WHERE "tables" LIKE '%{table}%';
                          RETURN NEW;
                        END;
                        $$;
                        DROP TRIGGER IF EXISTS "{table}_cache_after_insert" ON "{table}";
                        CREATE TRIGGER "{table}_cache_after_insert"
                          AFTER INSERT ON "{table}"
                          EXECUTE FUNCTION "clean_cache_for_{table}"();
                        DROP TRIGGER IF EXISTS "{table}_cache_after_update" ON "{table}";
                        CREATE TRIGGER "{table}_cache_after_update"
                          AFTER UPDATE ON "{table}"
                          EXECUTE FUNCTION "clean_cache_for_{table}"();
                        DROP TRIGGER IF EXISTS "{table}_cache_after_delete" ON "{table}";
                        CREATE TRIGGER "{table}_cache_after_delete"
                          AFTER DELETE ON "{table}"
                          EXECUTE FUNCTION "clean_cache_for_{table}"()"#,
                    table = validate_table_name(table)?,
                ))
                .await?;
            }
        }
        Ok(())
    }

    /// Implements [DbQuery::parse()] for PostgreSQL.
    fn parse(&self, sql_type: &str, value: &str) -> Result<ParamValue, DbError> {
        let err = || {
            Err(DbError::ParseError(format!(
                "Could not parse '{sql_type}' from '{value}'"
            )))
        };
        match sql_type.to_lowercase().as_str() {
            "text" => Ok(ParamValue::Text(value.to_string())),
            "bool" | "boolean" => match value.to_lowercase().as_str() {
                // TODO: improve this
                "true" | "1" => Ok(ParamValue::Boolean(true)),
                _ => Ok(ParamValue::Boolean(false)),
            },
            "smallint" | "smallinteger" => match value.parse::<i16>() {
                Ok(int) => Ok(ParamValue::SmallInteger(int)),
                Err(_) => err(),
            },
            "int" | "integer" => match value.parse::<i32>() {
                Ok(int) => Ok(ParamValue::Integer(int)),
                Err(_) => err(),
            },
            "bigint" | "biginteger" => match value.parse::<i64>() {
                Ok(int) => Ok(ParamValue::BigInteger(int)),
                Err(_) => err(),
            },
            "real" => match value.parse::<f32>() {
                Ok(float) => Ok(ParamValue::Real(float)),
                Err(_) => err(),
            },
            "bigreal" => match value.parse::<f64>() {
                Ok(float) => Ok(ParamValue::BigReal(float)),
                Err(_) => err(),
            },
            // WARN: Treat NUMERIC as an f64.
            "numeric" => match value.parse::<f64>() {
                Ok(float) => Ok(ParamValue::Numeric(
                    Decimal::from_f64_retain(float).unwrap_or_default(),
                )),
                Err(_) => err(),
            },
            _ => Err(DbError::DatatypeError(format!(
                "Unhandled SQL type: {sql_type}"
            ))),
        }
    }

    /// Implements [DbQuery::columns()] for PostgreSQL.
    async fn columns(&self, table: &str) -> Result<ColumnMap, DbError> {
        let mut columns = ColumnMap::new();
        let sql = format!(
            r#"SELECT
                 "columns"."column_name"::TEXT,
                 "columns"."data_type"::TEXT
               FROM
                 "information_schema"."columns" "columns"
               WHERE
                 "columns"."table_schema" IN (
                   SELECT REGEXP_SPLIT_TO_TABLE("setting", ', ')
                   FROM "pg_settings"
                   WHERE "name" = 'search_path'
                 )
                 AND "columns"."table_name" = $1
               ORDER BY "columns"."ordinal_position""#
        );

        let rows: Vec<DbRow> = self.query_no_cache(&sql, params![&table]).await?;
        for row in &rows {
            match (
                row.get("column_name")
                    .and_then(|name| Some::<String>(name.into())),
                row.get("data_type")
                    .and_then(|name| Some::<String>(name.into())),
            ) {
                (Some(column), Some(sql_type)) => {
                    columns.insert(column.to_string(), sql_type.to_lowercase().to_string())
                }
                _ => {
                    return Err(DbError::DataError(format!(
                        "Error getting columns for table '{table}'"
                    )));
                }
            };
        }

        match columns.is_empty() {
            true => Err(DbError::DataError(format!(
                "No information found for table '{table}'"
            ))),
            false => Ok(columns),
        }
    }

    /// Implements [DbQuery::primary_keys()] for PostgreSQL.
    async fn primary_keys(&self, table: &str) -> Result<Vec<String>, DbError> {
        let rows: Vec<DbRow> = self
            .query_no_cache(
                r#"SELECT "kcu"."column_name"
               FROM "information_schema"."table_constraints" "tco"
               JOIN "information_schema"."key_column_usage" "kcu"
                 ON "kcu"."constraint_name" = "tco"."constraint_name"
                AND "kcu"."constraint_schema" = "tco"."constraint_schema"
                AND "kcu"."table_name" = $1
                AND "tco"."constraint_type" ILIKE 'primary key'
              WHERE "kcu"."table_schema" IN (
                SELECT REGEXP_SPLIT_TO_TABLE("setting", ', ')
                FROM "pg_settings"
                WHERE "name" = 'search_path'
              )
              ORDER by "kcu"."ordinal_position""#,
                params![&table],
            )
            .await?;

        rows.iter()
            .map(|row| {
                match row
                    .get("column_name")
                    .and_then(|name| Some::<String>(name.into()))
                {
                    Some(pk_col) => Ok(pk_col.to_string()),
                    None => Err(DbError::DataError("Empty row".to_owned())),
                }
            })
            .collect()
    }

    /// Implements [DbQuery::execute_batch()] for PostgreSQL
    async fn execute_batch(&self, sql: &str) -> Result<(), DbError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| DbError::ConnectError(format!("Unable to get pool: {err:?}")))?;
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
        into_params: impl IntoParams + Send,
    ) -> Result<T, DbError> {
        let into_params = into_params.into_params();
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| DbError::ConnectError(format!("Unable to get pool: {err:?}")))?;

        // The expected types of all of the parameters as reported by the database via prepare():
        let param_pg_types = client
            .prepare(sql)
            .await
            .map_err(|err| DbError::DatabaseError(format!("Error preparing statement: {err:?}")))?
            .params()
            .to_vec();

        let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
        let gen_err = |param: &ParamValue, sql_type: &str| -> String {
            format!("Param {param:?} is wrong type for {sql_type} in query: {sql}")
        };
        match into_params {
            Params::None => (),
            Params::Positional(plist) => {
                for (i, param) in plist.iter().enumerate() {
                    let pg_type = &param_pg_types[i];
                    match pg_type {
                        &Type::TEXT | &Type::VARCHAR | &Type::NAME => {
                            match param {
                                ParamValue::Null => params.push(Box::new(None::<String>)),
                                ParamValue::Text(text) => params.push(Box::new(text.to_string())),
                                _ => return Err(DbError::InputError(gen_err(&param, "TEXT"))),
                            };
                        }
                        &Type::INT2 => {
                            match param {
                                ParamValue::Null => params.push(Box::new(None::<i16>)),
                                ParamValue::SmallInteger(num) => params.push(Box::new(*num)),
                                _ => return Err(DbError::InputError(gen_err(&param, "INT2"))),
                            };
                        }
                        &Type::INT4 => {
                            match param {
                                ParamValue::Null => params.push(Box::new(None::<i32>)),
                                ParamValue::Integer(num) => params.push(Box::new(*num)),
                                _ => return Err(DbError::InputError(gen_err(&param, "INT4"))),
                            };
                        }
                        &Type::INT8 => {
                            match param {
                                ParamValue::Null => params.push(Box::new(None::<i64>)),
                                ParamValue::BigInteger(num) => params.push(Box::new(*num)),
                                _ => return Err(DbError::InputError(gen_err(&param, "INT8"))),
                            };
                        }
                        &Type::FLOAT4 => {
                            match param {
                                ParamValue::Null => params.push(Box::new(None::<f32>)),
                                ParamValue::Real(num) => params.push(Box::new(*num)),
                                _ => return Err(DbError::InputError(gen_err(&param, "FLOAT4"))),
                            };
                        }
                        &Type::FLOAT8 => {
                            match param {
                                ParamValue::Null => params.push(Box::new(None::<f64>)),
                                ParamValue::BigReal(num) => params.push(Box::new(*num)),
                                _ => return Err(DbError::InputError(gen_err(&param, "FLOAT8"))),
                            };
                        }
                        &Type::NUMERIC => {
                            match param {
                                ParamValue::Null => params.push(Box::new(None::<Decimal>)),
                                ParamValue::Numeric(num) => params.push(Box::new(*num)),
                                _ => return Err(DbError::InputError(gen_err(&param, "NUMERIC"))),
                            };
                        }
                        &Type::BOOL => {
                            match param {
                                ParamValue::Null => params.push(Box::new(None::<bool>)),
                                ParamValue::Boolean(flag) => params.push(Box::new(*flag)),
                                _ => return Err(DbError::InputError(gen_err(&param, "BOOL"))),
                            };
                        }
                        _ => unimplemented!(),
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
                db_row.insert(column.name().to_string(), extract_value(row, i)?);
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

    /// Implements [DbQuery::table_exists()] for PostgreSQL.
    async fn table_exists(&self, table: &str) -> Result<bool, DbError> {
        let rows: Vec<DbRow> = self
            .query_no_cache(
                r#"SELECT 1
                   FROM "information_schema"."tables"
                   WHERE "table_type" LIKE '%TABLE'
                     AND "table_name" = $1
                     AND "table_schema" IN (
                       SELECT REGEXP_SPLIT_TO_TABLE("setting", ', ')
                       FROM "pg_settings"
                       WHERE "name" = 'search_path'
                     )"#,
                &[table],
            )
            .await?;

        match rows.first() {
            None => Ok(false),
            Some(_) => Ok(true),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::params;
    use indexmap::indexmap as db_row;
    use pretty_assertions::assert_eq;

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
        assert_eq!(rows, [db_row! {"max".into() => ParamValue::from(1_i64)}]);

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
            [db_row! {"bool_value_alias".into() => ParamValue::from(true)}]
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
            [db_row! {"max_int_value".into() => ParamValue::from(1_i64)}]
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
            [db_row! {"int_value".into() => ParamValue::from("1")}]
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
            [db_row! {"int_value_cast".into() => ParamValue::from("1")}]
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
    }
}
