//! [libsql](<https://crates.io/crates/deadpool-libsql>) implementation for rltbl_db.

use crate::{
    cache::CachingStrategy,
    core::{DbError, DbQuery},
    db_kind::{DbKind, MAX_PARAMS_SQLITE},
    db_value::{DbParams, DbRow, DbRows, DbValue, IntoDbParams, IntoDbRows, JsonValue},
    parse::validate_table_name,
    shared::{EditType, edit},
};
use deadpool_libsql::{
    Manager, Pool,
    libsql::{Builder, Value},
};
use rust_decimal::prelude::ToPrimitive;
use std::str::from_utf8;

impl TryFrom<Value> for DbValue {
    type Error = DbError;

    fn try_from(item: Value) -> Result<Self, DbError> {
        match &item {
            Value::Null => Ok(Self::Null),
            Value::Integer(number) => Ok(Self::from(*number)),
            Value::Real(number) => Ok(Self::from(*number)),
            Value::Text(string) => Ok(Self::Text(string.to_string())),
            Value::Blob(blob) => {
                let text_blob = from_utf8(blob).map_err(|err| {
                    DbError::DatatypeError(format!("Error converting blob to text: {err}"))
                })?;
                Ok(Self::Text(text_blob.to_string()))
            }
        }
    }
}

impl TryFrom<DbParams> for Vec<Value> {
    type Error = DbError;

    fn try_from(item: DbParams) -> Result<Self, DbError> {
        match item {
            DbParams::None => Ok(vec![]),
            DbParams::Positional(pvalues) => {
                let mut values = vec![];
                for pvalue in pvalues {
                    match pvalue {
                        DbValue::Null => values.push(Value::Null),
                        // Libsql does not support booleans.
                        // See: https://docs.rs/libsql/0.9.29/libsql/enum.Value.html,
                        DbValue::Boolean(pvalue) => values.push(Value::Integer(pvalue.into())),
                        DbValue::SmallInteger(pvalue) => values.push(Value::Integer(pvalue.into())),
                        DbValue::Integer(pvalue) => values.push(Value::Integer(pvalue.into())),
                        DbValue::BigInteger(pvalue) => values.push(Value::Integer(pvalue.into())),
                        DbValue::Real(pvalue) => values.push(Value::Real(pvalue.into())),
                        DbValue::BigReal(pvalue) => values.push(Value::Real(pvalue.into())),
                        DbValue::Numeric(pvalue) => {
                            let pvalue = pvalue.to_f64().ok_or(DbError::DatatypeError(format!(
                                "Error converting value '{pvalue}' to f64"
                            )))?;
                            values.push(Value::Real(pvalue.into()))
                        }
                        DbValue::Text(pvalue) => values.push(Value::Text(pvalue)),
                        DbValue::Json(value) => {
                            let value = match value {
                                JsonValue::String(value) => value.to_string(),
                                _ => value.to_string(),
                            };
                            values.push(Value::Text(value))
                        }
                        DbValue::Other(type_name, bytes, string_opt) => {
                            return Err(DbError::InputError(format!(
                                "Not supported for SQLite: \
                                 DbValue::Other({type_name}, {bytes:?}, {string_opt:?})"
                            )));
                        }
                    };
                }
                Ok(values)
            }
        }
    }
}

/// Represents a SQLite database connection pool
#[derive(Clone, Debug)]
pub struct LibSQLPool {
    pool: Pool,
    caching_strategy: CachingStrategy,
    /// When set to true, SQL statements sent to the [DbQuery::query()] and [DbQuery::execute()]
    /// functions will be parsed and if they will result in tables being edited and/or dropped,
    /// the cache will be maintained in accordance with the given [CachingStrategy].
    /// For further information, see [DbQuery::set_cache_aware_query()].
    cache_aware_query: bool,
}

impl LibSQLPool {
    /// Connect to a SQLite database using the given url.
    pub async fn connect(url: &str) -> Result<Self, DbError> {
        let db = Builder::new_local(url).build().await.map_err(|err| {
            DbError::ConnectError(format!("Error creating pool from URL: '{url}': {err}"))
        })?;
        let manager = Manager::from_libsql_database(db);
        let pool = Pool::builder(manager).build().map_err(|err| {
            DbError::ConnectError(format!("Error creating pool from URL: '{url}': {err}"))
        })?;
        Ok(Self {
            pool: pool,
            caching_strategy: CachingStrategy::None,
            cache_aware_query: false,
        })
    }
}

impl DbQuery for LibSQLPool {
    /// Implements [DbQuery::kind()] for SQLite.
    fn kind(&self) -> DbKind {
        DbKind::SQLite
    }

    /// Implements [DbQuery::execute_batch()] for SQLite
    async fn execute_batch(&self, sql: &str) -> Result<(), DbError> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(|err| DbError::ConnectError(format!("Error getting from pool: {err}")))?;
        match conn.execute_batch(sql).await {
            Err(err) => {
                return Err(DbError::DatabaseError(format!("Error during query: {err}")));
            }
            Ok(_) => Ok(()),
        }
    }

    /// Implements [DbQuery::query()] for SQLite.
    async fn query(&self, sql: &str, params: impl IntoDbParams + Send) -> Result<DbRows, DbError> {
        let rows = self.query_no_cache_clean(sql, params).await?;
        if self.get_cache_aware_query() {
            self.clear_cache_for_affected_tables(sql).await?;
        }
        Ok(rows)
    }

    /// Implements [DbQuery::query_no_cache_clean()] for SQLite.
    async fn query_no_cache_clean(
        &self,
        sql: &str,
        params: impl IntoDbParams + Send,
    ) -> Result<DbRows, DbError> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(|err| DbError::ConnectError(format!("Error getting from pool: {err}")))?;

        let params: Vec<Value> = params.into_db_params().try_into()?;
        let mut rows = conn
            .query(sql, params)
            .await
            .map_err(|err| DbError::ConnectError(format!("Query error: {err}")))?;

        let mut db_rows = vec![];
        while let Some(row) = rows
            .next()
            .await
            .map_err(|err| DbError::DataError(err.to_string()))?
        {
            let mut db_row = DbRow::new();
            for i in 0..row.column_count() {
                let column = row.column_name(i).ok_or(DbError::DataError(format!(
                    "Error getting name of column {i} of row."
                )))?;
                let value = row.get_value(i).map_err(|err| {
                    DbError::DataError(format!("Error getting value of column {i} of row: {err}"))
                })?;
                db_row.insert(column.to_string(), value.try_into()?);
            }
            db_rows.push(db_row);
        }

        Ok(DbRows { content: db_rows })
    }

    /// Implements [DbQuery::insert()] for SQLite.
    async fn insert(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
    ) -> Result<(), DbError> {
        edit(
            self,
            &EditType::Insert,
            &MAX_PARAMS_SQLITE,
            table,
            columns,
            rows,
            false,
            &[],
        )
        .await?;
        Ok(())
    }

    /// Implements [DbQuery::insert_returning()] for SQLite.
    async fn insert_returning(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
        returning: &[&str],
    ) -> Result<DbRows, DbError> {
        edit(
            self,
            &EditType::Insert,
            &MAX_PARAMS_SQLITE,
            table,
            columns,
            rows,
            true,
            returning,
        )
        .await
    }

    /// Implements [DbQuery::update()] for SQLite.
    async fn update(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
    ) -> Result<(), DbError> {
        edit(
            self,
            &EditType::Update,
            &MAX_PARAMS_SQLITE,
            table,
            columns,
            rows,
            false,
            &[],
        )
        .await?;
        Ok(())
    }

    /// Implements [DbQuery::update_returning()] for SQLite.
    async fn update_returning(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
        returning: &[&str],
    ) -> Result<DbRows, DbError> {
        edit(
            self,
            &EditType::Update,
            &MAX_PARAMS_SQLITE,
            table,
            columns,
            rows,
            true,
            returning,
        )
        .await
    }

    /// Implements [DbQuery::upsert()] for SQLite.
    async fn upsert(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
    ) -> Result<(), DbError> {
        edit(
            self,
            &EditType::Upsert,
            &MAX_PARAMS_SQLITE,
            table,
            columns,
            rows,
            false,
            &[],
        )
        .await?;
        Ok(())
    }

    /// Implements [DbQuery::upsert_returning()] for SQLite.
    async fn upsert_returning(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
        returning: &[&str],
    ) -> Result<DbRows, DbError> {
        edit(
            self,
            &EditType::Upsert,
            &MAX_PARAMS_SQLITE,
            table,
            columns,
            rows,
            true,
            returning,
        )
        .await
    }

    /// Implements [DbQuery::drop_table()] for SQLite.
    async fn drop_table(&self, table: &str) -> Result<(), DbError> {
        let table = validate_table_name(table)?;
        // Drop the table:
        self.execute_no_cache_clean(&format!(r#"DROP TABLE IF EXISTS "{table}""#), ())
            .await?;

        // Delete dirty entries from the cache in accordance with our caching strategy:
        self.clear_cache_for_dropped_tables(&[&table]).await?;
        Ok(())
    }

    /// Implements [DbQuery::drop_table()] for SQLite.
    async fn drop_view(&self, view: &str) -> Result<(), DbError> {
        let view = validate_table_name(view)?;
        // Drop the view:
        self.execute_no_cache_clean(&format!(r#"DROP VIEW IF EXISTS "{view}""#), ())
            .await?;

        // Delete dirty entries from the cache in accordance with our caching strategy:
        self.clear_cache_for_dropped_tables(&[&view]).await?;
        Ok(())
    }

    /// Implements [DbQuery::set_caching_strategy()] for SQLite.
    fn set_caching_strategy(&mut self, strategy: &CachingStrategy) {
        self.caching_strategy = *strategy;
    }

    /// Implements [DbQuery::get_caching_strategy()] for SQLite.
    fn get_caching_strategy(&self) -> CachingStrategy {
        self.caching_strategy
    }

    /// Implements [DbQuery::set_cache_aware_query()] for SQLite.
    fn set_cache_aware_query(&mut self, flag: bool) {
        self.cache_aware_query = flag;
    }

    /// Implements [DbQuery::get_cache_aware_query()] for SQLite.
    fn get_cache_aware_query(&self) -> bool {
        self.cache_aware_query
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db_row, params};
    use std::ops::Deref;

    #[tokio::test]
    async fn test_aliases_and_builtin_functions() {
        let pool = LibSQLPool::connect(":memory:").await.unwrap();
        pool.execute_batch(
            "DROP TABLE IF EXISTS test_table_indirect;\
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
               VALUES (?1, ?2, ?3, ?4, ?5)"#,
            params!["foo", (), 1.05_f64, 1_i64, true],
        )
        .await
        .unwrap();

        // Test aggregate:
        let rows = pool
            .query("SELECT MAX(int_value) FROM test_table_indirect", ())
            .await
            .unwrap();
        assert_eq!(
            *rows.deref(),
            [db_row! {
                "MAX(int_value)" => 1_i64,
            }]
        );

        // Test alias:
        let rows = pool
            .query(
                "SELECT bool_value AS bool_value_alias FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            *rows.deref(),
            [db_row! {
                "bool_value_alias" => 1_i64,
            }]
        );

        // Test aggregate with alias:
        let rows = pool
            .query(
                "SELECT MAX(int_value) AS max_int_value FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        // Note that the alias is not shown in the results:
        assert_eq!(
            *rows.deref(),
            [db_row! {
                "max_int_value" => 1_i64,
            }]
        );

        // Test non-aggregate function:
        let rows = pool
            .query(
                "SELECT CAST(int_value AS TEXT) FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(*rows.deref(), [db_row! {"CAST(int_value AS TEXT)" => "1",}]);

        // Test non-aggregate function with alias:
        let rows = pool
            .query(
                "SELECT CAST(int_value AS TEXT) AS int_value_cast FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(*rows.deref(), [db_row! {"int_value_cast" => "1",}]);

        // Test functions over booleans:
        let rows = pool
            .query("SELECT MAX(bool_value) FROM test_table_indirect", ())
            .await
            .unwrap();
        // It is not possible to represent the boolean result of an aggregate function as a
        // boolean, since internally to sqlite it is stored as an integer, and we can't query
        // the metadata to get the datatype of an expression. If we want to represent it as a
        // boolean, we will need to parse the expression. Note that PostgreSQL does not support
        // MAX(bool_value) - it gives the error:
        //   ERROR: function max(boolean) does not exist\nHINT: No function matches the given
        //          name and argument types. You might need to add explicit type casts.
        // So, perhaps, this is tu quoque an argument that the behaviour below is acceptable for
        // sqlite.
        assert_eq!(*rows.deref(), [db_row! {"MAX(bool_value)" => 1_i64,}]);
    }

    /// This test is resource intensive and therefore ignored by default. It verifies that
    /// using [MAX_PARAMS_SQLITE] parameters in a query is indeed supported.
    /// To run this and other ignored tests, use `cargo test -- --ignored` or
    /// `cargo test -- --include-ignored`
    #[tokio::test]
    #[ignore]
    async fn test_max_params() {
        let pool = LibSQLPool::connect(":memory:").await.unwrap();
        pool.execute_batch(
            "DROP TABLE IF EXISTS test_max_params;\
             CREATE TABLE test_max_params (\
                 column1 INT,\
                 column2 INT,\
                 column3 INT,\
                 column4 INT,\
                 column5 INT,\
                 column6 INT\
             )",
        )
        .await
        .unwrap();

        let mut sql = "INSERT INTO test_max_params VALUES ".to_string();
        let mut values = vec![];
        let mut params = vec![];
        let mut n = 1;
        while n <= MAX_PARAMS_SQLITE {
            values.push(format!(
                "(?{}, ?{}, ?{}, ?{}, ?{}, ?{})",
                n,
                n + 1,
                n + 2,
                n + 3,
                n + 4,
                n + 5
            ));
            params.push(1);
            params.push(1);
            params.push(1);
            params.push(1);
            params.push(1);
            params.push(1);
            n += 6;
        }
        sql.push_str(&values.join(", "));
        pool.execute(&sql, params).await.unwrap();
        pool.drop_table("text_max_params").await.unwrap();
    }
}
