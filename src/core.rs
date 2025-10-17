use std::future::Future;

use serde_json::Map as JsonMap;

#[cfg(feature = "postgres")]
use crate::postgres::PostgresError;
#[cfg(feature = "sqlite")]
use crate::sqlite::SqliteError;

pub type JsonValue = serde_json::Value;
pub type JsonRow = JsonMap<String, JsonValue>;

#[derive(Clone, Debug)]
pub enum DbError {
    /// An error that occurred while connecting to a database.
    ConnectError(String),
    /// An error in the arguments to a function.
    InputError(String),
    /// An error in the data retrieved from the database.
    DataError(String),
    #[cfg(feature = "sqlite")]
    /// An error that originated with a sqlite database.
    SqliteError(SqliteError),
    #[cfg(feature = "postgres")]
    /// An error that originated with a postgres database.
    PostgresError(PostgresError),
}

impl std::error::Error for DbError {}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DbError::ConnectError(err) | DbError::DataError(err) | DbError::InputError(err) => {
                write!(f, "{err}")
            }
            #[cfg(feature = "sqlite")]
            DbError::SqliteError(err) => write!(f, "{err}"),
            #[cfg(feature = "postgres")]
            DbError::PostgresError(err) => write!(f, "{err}"),
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
    /// Sequentially execute a semicolon-delimited list of statements, without parameters.
    fn execute_batch(&self, sql: &str) -> impl Future<Output = Result<(), DbError>> + Send;
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
