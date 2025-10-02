use std::future::Future;

use serde_json::Map as JsonMap;

#[cfg(feature = "postgres")]
use crate::postgres::PostgresConnection;
#[cfg(feature = "sqlite")]
use crate::sqlite::SqliteConnection;

pub type JsonValue = serde_json::Value;
pub type JsonRow = JsonMap<String, JsonValue>;
pub type DbError = String;

pub enum DbConnection {
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteConnection),
    #[cfg(feature = "postgres")]
    Postgres(PostgresConnection),
}

/// Connect to the database located at the given URL. If the postgres feature flag has been
/// set, this is assumed to be a PostgreSQL URL, otherwise the database is assumed to be a SQLite
/// database.
pub async fn connect(url: &str) -> Result<DbConnection, DbError> {
    if url.starts_with("postgresql://") {
        #[cfg(feature = "postgres")]
        {
            Ok(DbConnection::Postgres(
                crate::postgres::PostgresConnection::connect(url)
                    .await
                    .map_err(|err| format!("postgres error {err}"))?,
            ))
        }
        #[cfg(not(feature = "postgres"))]
        {
            Err(format!("postgres not configured"))
        }
    } else {
        #[cfg(feature = "sqlite")]
        {
            Ok(DbConnection::Sqlite(
                SqliteConnection::connect(url)
                    .await
                    .map_err(|err| format!("postgres error {err}"))?,
            ))
        }
        #[cfg(not(feature = "sqlite"))]
        {
            Err(format!("sqlite not configured"))
        }
    }
}

impl DbQuery for DbConnection {
    async fn execute(&self, sql: &str, params: &[JsonValue]) -> Result<(), DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            DbConnection::Sqlite(connection) => connection.execute(sql, params).await,
            #[cfg(feature = "postgres")]
            DbConnection::Postgres(connection) => connection.execute(sql, params).await,
        }
    }

    async fn query(&self, sql: &str, params: &[JsonValue]) -> Result<Vec<JsonRow>, DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            DbConnection::Sqlite(connection) => connection.query(sql, params).await,
            #[cfg(feature = "postgres")]
            DbConnection::Postgres(connection) => connection.query(sql, params).await,
        }
    }

    async fn query_row(&self, sql: &str, params: &[JsonValue]) -> Result<JsonRow, DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            DbConnection::Sqlite(connection) => connection.query_row(sql, params).await,
            #[cfg(feature = "postgres")]
            DbConnection::Postgres(connection) => connection.query_row(sql, params).await,
        }
    }

    async fn query_value(&self, sql: &str, params: &[JsonValue]) -> Result<JsonValue, DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            DbConnection::Sqlite(connection) => connection.query_value(sql, params).await,
            #[cfg(feature = "postgres")]
            DbConnection::Postgres(connection) => connection.query_value(sql, params).await,
        }
    }

    async fn query_string(&self, sql: &str, params: &[JsonValue]) -> Result<String, DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            DbConnection::Sqlite(connection) => connection.query_string(sql, params).await,
            #[cfg(feature = "postgres")]
            DbConnection::Postgres(connection) => connection.query_string(sql, params).await,
        }
    }

    async fn query_u64(&self, sql: &str, params: &[JsonValue]) -> Result<u64, DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            DbConnection::Sqlite(connection) => connection.query_u64(sql, params).await,
            #[cfg(feature = "postgres")]
            DbConnection::Postgres(connection) => connection.query_u64(sql, params).await,
        }
    }

    async fn query_i64(&self, sql: &str, params: &[JsonValue]) -> Result<i64, DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            DbConnection::Sqlite(connection) => connection.query_i64(sql, params).await,
            #[cfg(feature = "postgres")]
            DbConnection::Postgres(connection) => connection.query_i64(sql, params).await,
        }
    }

    async fn query_f64(&self, sql: &str, params: &[JsonValue]) -> Result<f64, DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            DbConnection::Sqlite(connection) => connection.query_f64(sql, params).await,
            #[cfg(feature = "postgres")]
            DbConnection::Postgres(connection) => connection.query_f64(sql, params).await,
        }
    }
}

pub trait DbQuery {
    /// Execute a SQL command, without a return value.
    fn execute(
        &self,
        sql: &str,
        params: &[JsonValue],
    ) -> impl Future<Output = Result<(), DbError>> + Send;
    /// Execute a SQL command, returning a vector of JSON rows.
    fn query(
        &self,
        sql: &str,
        params: &[JsonValue],
    ) -> impl Future<Output = Result<Vec<JsonRow>, DbError>> + Send;
    /// Execute a SQL command, returning a single JSON row.
    fn query_row(
        &self,
        sql: &str,
        params: &[JsonValue],
    ) -> impl Future<Output = Result<JsonRow, DbError>> + Send;
    /// Execute a SQL command, returning a single JSON value.
    fn query_value(
        &self,
        sql: &str,
        params: &[JsonValue],
    ) -> impl Future<Output = Result<JsonValue, DbError>> + Send;
    /// Execute a SQL command, returning a single string.
    fn query_string(
        &self,
        sql: &str,
        params: &[JsonValue],
    ) -> impl Future<Output = Result<String, DbError>> + Send;
    /// Execute a SQL command, returning a single unsigned integer.
    fn query_u64(
        &self,
        sql: &str,
        params: &[JsonValue],
    ) -> impl Future<Output = Result<u64, DbError>> + Send;
    /// Execute a SQL command, returning a single signed integer.
    fn query_i64(
        &self,
        sql: &str,
        params: &[JsonValue],
    ) -> impl Future<Output = Result<i64, DbError>> + Send;
    /// Execute a SQL command, returning a single float.
    fn query_f64(
        &self,
        sql: &str,
        params: &[JsonValue],
    ) -> impl Future<Output = Result<f64, DbError>> + Send;
}
