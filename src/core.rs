use std::future::Future;

use serde_json::Map as JsonMap;

pub type JsonValue = serde_json::Value;
pub type JsonRow = JsonMap<String, JsonValue>;

pub enum DbKind {
    SQLite,
    PostgreSQL,
}

#[derive(Clone, Debug)]
#[non_exhaustive] // We may add more specific error types in the future.
pub enum DbError {
    /// An error that occurred while connecting to a database.
    ConnectError(String),
    /// An error in the arguments to a function that accessed the database.
    InputError(String),
    /// An error in the data retrieved from the database.
    DataError(String),
    /// An error that originated from the database.
    DatabaseError(String),
}

impl std::error::Error for DbError {}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DbError::ConnectError(err)
            | DbError::DataError(err)
            | DbError::InputError(err)
            | DbError::DatabaseError(err) => write!(f, "{err}"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Params {
    None,
    Positional(Vec<ParamValue>),
    Named(Vec<(String, ParamValue)>),
}

#[derive(Debug, Clone)]
pub enum ParamValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

pub trait IntoParams {
    fn into_params(self) -> Result<Params, DbError>;

    fn len(self) -> usize;
}

impl IntoParams for () {
    fn into_params(self) -> Result<Params, DbError> {
        Ok(Params::None)
    }

    fn len(self) -> usize {
        0
    }
}

pub trait DbQuery {
    /// Execute a SQL command, without a return value.
    fn execute(
        &self,
        sql: &str,
        params: &[JsonValue],
    ) -> impl Future<Output = Result<(), DbError>> + Send;
    /// Execute a SQL command, without a return value.
    fn execute_new(
        &self,
        sql: &str,
        params: impl IntoParams + Send,
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
