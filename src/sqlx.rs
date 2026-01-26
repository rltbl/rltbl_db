//! sqlx implementation for rltbl_db.

use crate::{
    core::{CachingStrategy, DbError, DbQuery, DbRow, FromDbRows, IntoDbRows, IntoParams},
    db_kind::DbKind,
    shared::{EditType, edit},
};
use sqlx::{postgres::PgPool, sqlite::SqlitePool};

/// The [maximum number of parameters](https://www.sqlite.org/limits.html#max_variable_number)
/// that can be bound to a SQLite query.
static MAX_PARAMS_SQLITE: usize = 32766;

/// The [maximum number of parameters](https://www.postgresql.org/docs/current/limits.html)
/// that can be bound to a PostgreSQL query.
static MAX_PARAMS_POSTGRES: usize = 65535;

#[derive(Debug)]
pub enum Pool {
    SQLite(SqlitePool),
    PostgreSQL(PgPool),
}

/// Represents a Sqlx database connection pool
#[derive(Debug)]
pub struct SqlxPool {
    pool: Pool,
    caching_strategy: CachingStrategy,
    cache_aware_query: bool,
}

impl SqlxPool {
    /// TODO: Add docstring here.
    pub async fn connect(_url: &str) -> Result<Self, DbError> {
        todo!()
    }
}

impl DbQuery for SqlxPool {
    /// Implements [DbQuery::kind()] for Sqlx.
    fn kind(&self) -> DbKind {
        match self.pool {
            Pool::SQLite(_) => DbKind::SQLite,
            Pool::PostgreSQL(_) => DbKind::PostgreSQL,
        }
    }

    /// Implements [DbQuery::set_caching_strategy()] for Sqlx.
    fn set_caching_strategy(&mut self, strategy: &CachingStrategy) {
        self.caching_strategy = *strategy;
    }

    /// Implements [DbQuery::get_caching_strategy()] for Sqlx.
    fn get_caching_strategy(&self) -> CachingStrategy {
        self.caching_strategy
    }

    /// Implements [DbQuery::set_cache_aware_query()] for Sqlx.
    fn set_cache_aware_query(&mut self, flag: bool) {
        self.cache_aware_query = flag;
    }

    /// Implements [DbQuery::get_cache_aware_query()] for Sqlx.
    fn get_cache_aware_query(&self) -> bool {
        self.cache_aware_query
    }

    /// Implements [DbQuery::execute_batch()] for Sqlx
    async fn execute_batch(&self, _sql: &str) -> Result<(), DbError> {
        todo!()
    }

    /// Implements [DbQuery::query_no_cache()] for Sqlx.
    async fn query_no_cache<T: FromDbRows>(
        &self,
        _sql: &str,
        _into_params: impl IntoParams + Send,
    ) -> Result<T, DbError> {
        todo!()
    }

    /// Implements [DbQuery::insert()] for Sqlx
    async fn insert(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
    ) -> Result<(), DbError> {
        let max_params = match self.pool {
            Pool::SQLite(_) => MAX_PARAMS_SQLITE,
            Pool::PostgreSQL(_) => MAX_PARAMS_POSTGRES,
        };
        let _: Vec<DbRow> = edit(
            self,
            &EditType::Insert,
            &max_params,
            table,
            columns,
            rows,
            false,
            &[],
        )
        .await?;
        Ok(())
    }

    /// Implements [DbQuery::insert_returning()] for Sqlx
    async fn insert_returning<T: FromDbRows>(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
        returning: &[&str],
    ) -> Result<T, DbError> {
        let max_params = match self.pool {
            Pool::SQLite(_) => MAX_PARAMS_SQLITE,
            Pool::PostgreSQL(_) => MAX_PARAMS_POSTGRES,
        };
        edit(
            self,
            &EditType::Insert,
            &max_params,
            table,
            columns,
            rows,
            true,
            returning,
        )
        .await
    }

    /// Implements [DbQuery::update()] for Sqlx.
    async fn update(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
    ) -> Result<(), DbError> {
        let max_params = match self.pool {
            Pool::SQLite(_) => MAX_PARAMS_SQLITE,
            Pool::PostgreSQL(_) => MAX_PARAMS_POSTGRES,
        };
        let _: Vec<DbRow> = edit(
            self,
            &EditType::Update,
            &max_params,
            table,
            columns,
            rows,
            false,
            &[],
        )
        .await?;
        Ok(())
    }

    /// Implements [DbQuery::update_returning()] for Sqlx.
    async fn update_returning<T: FromDbRows>(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
        returning: &[&str],
    ) -> Result<T, DbError> {
        let max_params = match self.pool {
            Pool::SQLite(_) => MAX_PARAMS_SQLITE,
            Pool::PostgreSQL(_) => MAX_PARAMS_POSTGRES,
        };
        edit(
            self,
            &EditType::Update,
            &max_params,
            table,
            columns,
            rows,
            true,
            returning,
        )
        .await
    }

    /// Implements [DbQuery::upsert()] for Sqlx.
    async fn upsert(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
    ) -> Result<(), DbError> {
        let max_params = match self.pool {
            Pool::SQLite(_) => MAX_PARAMS_SQLITE,
            Pool::PostgreSQL(_) => MAX_PARAMS_POSTGRES,
        };
        let _: Vec<DbRow> = edit(
            self,
            &EditType::Upsert,
            &max_params,
            table,
            columns,
            rows,
            false,
            &[],
        )
        .await?;
        Ok(())
    }

    /// Implements [DbQuery::upsert_returning()] for Sqlx.
    async fn upsert_returning<T: FromDbRows>(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
        returning: &[&str],
    ) -> Result<T, DbError> {
        let max_params = match self.pool {
            Pool::SQLite(_) => MAX_PARAMS_SQLITE,
            Pool::PostgreSQL(_) => MAX_PARAMS_POSTGRES,
        };
        edit(
            self,
            &EditType::Upsert,
            &max_params,
            table,
            columns,
            rows,
            true,
            returning,
        )
        .await
    }
}
