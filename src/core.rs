use std::future::Future;

use serde_json::Map as JsonMap;

pub type JsonValue = serde_json::Value;
pub type JsonRow = JsonMap<String, JsonValue>;
pub type DbError = String;

/// Connect to the database located at the given URL. If the postgres feature flag has been
/// set, this is assumed to be a PostgreSQL URL, otherwise the database is assumed to be a SQLite
/// database.
pub async fn connect(url: &str) -> Result<impl DbConnection, DbError> {
    let conn = {
        #[cfg(not(feature = "postgres"))]
        {
            crate::sqlite::SqliteConnection::connect(url).await?
        }
        #[cfg(feature = "postgres")]
        {
            crate::postgres::PostgresConnection::connect(url).await?
        }
    };
    Ok(conn)
}

pub trait DbConnection {
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
