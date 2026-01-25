//! libsql implementation for rltbl_db.

use crate::{
    core::{
        CachingStrategy, DbError, DbQuery, DbRow, FromDbRows, IntoDbRows, IntoParams, ParamValue,
        Params,
    },
    db_kind::DbKind,
    shared::{EditType, edit},
};
use deadpool_libsql::{
    Manager, Pool,
    libsql::{Builder, Value},
};
use rust_decimal::prelude::ToPrimitive;
use std::str::from_utf8;

/// The [maximum number of parameters](https://www.sqlite.org/limits.html#max_variable_number)
/// that can be bound to a SQLite query
static MAX_PARAMS_SQLITE: usize = 32766;

impl TryFrom<Value> for ParamValue {
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

impl TryFrom<Params> for Vec<Value> {
    type Error = DbError;

    fn try_from(item: Params) -> Result<Self, DbError> {
        match item {
            Params::None => Ok(vec![Value::Null]),
            Params::Positional(pvalues) => {
                let mut values = vec![];
                for pvalue in pvalues {
                    match pvalue {
                        ParamValue::Null => values.push(Value::Null),
                        // Libsql does not support booleans.
                        // See: https://docs.rs/libsql/0.9.29/libsql/enum.Value.html,
                        ParamValue::Boolean(pvalue) => values.push(Value::Integer(pvalue.into())),
                        ParamValue::SmallInteger(pvalue) => {
                            values.push(Value::Integer(pvalue.into()))
                        }
                        ParamValue::Integer(pvalue) => values.push(Value::Integer(pvalue.into())),
                        ParamValue::BigInteger(pvalue) => {
                            values.push(Value::Integer(pvalue.into()))
                        }
                        ParamValue::Real(pvalue) => values.push(Value::Real(pvalue.into())),
                        ParamValue::BigReal(pvalue) => values.push(Value::Real(pvalue.into())),
                        ParamValue::Numeric(pvalue) => {
                            let pvalue = pvalue.to_f64().ok_or(DbError::DatatypeError(format!(
                                "Error converting value '{pvalue}' to f64"
                            )))?;
                            values.push(Value::Real(pvalue.into()))
                        }
                        ParamValue::Text(pvalue) => values.push(Value::Text(pvalue)),
                    };
                }
                Ok(values)
            }
        }
    }
}

/// Represents a SQLite database connection pool
#[derive(Debug)]
pub struct LibSQLPool {
    pool: Pool,
    caching_strategy: CachingStrategy,
    /// When set to true, SQL statements sent to the [DbQuery::query()] and [DbQuery::execute()]
    /// functions will be parsed and if they will result in tables being edited and/or dropped,
    /// the cache will be maintained in accordance with the given [CachingStrategy].
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

    /// Implements [DbQuery::execute_batch()] for PostgreSQL
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

    /// Implements [DbQuery::query_no_cache()] for SQLite.
    async fn query_no_cache<T: FromDbRows>(
        &self,
        sql: &str,
        params: impl IntoParams + Send,
    ) -> Result<T, DbError> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(|err| DbError::ConnectError(format!("Error getting from pool: {err}")))?;

        let params: Vec<Value> = params.into_params().try_into()?;
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

        Ok(FromDbRows::from(db_rows))
    }

    /// Implements [DbQuery::insert()] for SQLite.
    async fn insert(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
    ) -> Result<(), DbError> {
        let _: Vec<DbRow> = edit(
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
        let _: Vec<DbRow> = edit(
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
        let _: Vec<DbRow> = edit(
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
            &MAX_PARAMS_SQLITE,
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

    use crate::params;
    use indexmap::indexmap as db_row;

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
        let rows: Vec<DbRow> = pool
            .query("SELECT MAX(int_value) FROM test_table_indirect", ())
            .await
            .unwrap();
        assert_eq!(
            rows,
            [db_row! {"MAX(int_value)".into() => ParamValue::from(1_i64)}]
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
            [db_row! {"bool_value_alias".into() => ParamValue::from(1_i64)}]
        );

        // Test aggregate with alias:
        let rows: Vec<DbRow> = pool
            .query(
                "SELECT MAX(int_value) AS max_int_value FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        // Note that the alias is not shown in the results:
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
            [db_row! {"CAST(int_value AS TEXT)".into() => ParamValue::from("1")}]
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

        // Test functions over booleans:
        let rows: Vec<DbRow> = pool
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
        assert_eq!(
            rows,
            [db_row! {"MAX(bool_value)".into() => ParamValue::from(1_i64)}]
        );
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
    }
}
