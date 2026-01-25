//! libsql implementation for rltbl_db.

use crate::{
    core::{
        CachingStrategy, DbError, DbQuery, DbRow, FromDbRows, IntoDbRows, IntoParams, ParamValue,
    },
    db_kind::DbKind,
    shared::{EditType, edit},
};

use deadpool_libsql::{Manager, Pool, libsql::Value};
use std::str::from_utf8;

/// The [maximum number of parameters](https://www.sqlite.org/limits.html#max_variable_number)
/// that can be bound to a SQLite query
static MAX_PARAMS_SQLITE: usize = 32766;

impl From<Value> for ParamValue {
    fn from(item: Value) -> Self {
        match &item {
            Value::Null => Self::Null,
            Value::Integer(number) => Self::from(*number),
            Value::Real(number) => Self::from(*number),
            Value::Text(string) => Self::Text(string.to_string()),
            Value::Blob(blob) => Self::Text(from_utf8(blob).unwrap().to_string()),
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
        let db = deadpool_libsql::libsql::Builder::new_local(url)
            .build()
            .await
            .map_err(|err| {
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
    async fn execute_batch(&self, _sql: &str) -> Result<(), DbError> {
        // TODO: See https://docs.rs/libsql/0.9.29/libsql/struct.Connection.html
        todo!()
    }

    /// Implements [DbQuery::query_no_cache()] for SQLite.
    async fn query_no_cache<T: FromDbRows>(
        &self,
        sql: &str,
        _params: impl IntoParams + Send,
    ) -> Result<T, DbError> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(|err| DbError::ConnectError(format!("Error getting pool: {err}")))?;

        // TODO: Handle params.

        let mut rows = conn
            .query(sql, ())
            .await
            .map_err(|err| DbError::ConnectError(format!("Query error: {err}")))?;
        let mut db_rows = vec![];
        while let Some(row) = rows.next().await.unwrap() {
            let mut db_row = DbRow::new();
            for i in 0..row.column_count() {
                let column = row.column_name(i).unwrap();
                let value = row.get_value(i).unwrap();
                db_row.insert(column.to_string(), value.into());
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

    #[tokio::test]
    async fn test_basic() {
        let _pool = LibSQLPool::connect(":memory:").await.unwrap();
    }
}
