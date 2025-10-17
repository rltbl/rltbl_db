use crate::core::{DbQuery, JsonRow, JsonValue};

#[cfg(feature = "postgres")]
use crate::postgres::{PostgresConnection, PostgresError};
#[cfg(feature = "sqlite")]
use crate::sqlite::{SqliteConnection, SqliteError};

#[derive(Debug)]
pub enum AnyError {
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

impl std::fmt::Display for AnyError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            AnyError::ConnectError(err) | AnyError::DataError(err) | AnyError::InputError(err) => {
                write!(f, "{err}")
            }
            #[cfg(feature = "sqlite")]
            AnyError::SqliteError(err) => write!(f, "{err}"),
            #[cfg(feature = "postgres")]
            AnyError::PostgresError(err) => write!(f, "{err}"),
        }
    }
}

pub enum AnyConnection {
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteConnection),
    #[cfg(feature = "postgres")]
    Postgres(PostgresConnection),
}

impl AnyConnection {
    /// Connect to the database located at the given URL.
    pub async fn connect(url: &str) -> Result<Self, AnyError> {
        if url.starts_with("postgresql://") {
            #[cfg(feature = "postgres")]
            {
                Ok(AnyConnection::Postgres(
                    crate::postgres::PostgresConnection::connect(url).await?,
                ))
            }
            #[cfg(not(feature = "postgres"))]
            {
                Err(AnyError::ConnectError(
                    "postgres not configured".to_string(),
                ))
            }
        } else {
            #[cfg(feature = "sqlite")]
            {
                Ok(AnyConnection::Sqlite(SqliteConnection::connect(url).await?))
            }
            #[cfg(not(feature = "sqlite"))]
            {
                Err(AnyError::ConnectError("sqlite not configured".to_string()))
            }
        }
    }
}

impl DbQuery for AnyConnection {
    async fn execute(&self, sql: &str, params: &[JsonValue]) -> Result<(), AnyError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.execute(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.execute(sql, params).await,
        }
    }

    async fn execute_batch(&self, sql: &str) -> Result<(), AnyError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.execute_batch(sql).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.execute_batch(sql).await,
        }
    }

    async fn query(&self, sql: &str, params: &[JsonValue]) -> Result<Vec<JsonRow>, AnyError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.query(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.query(sql, params).await,
        }
    }

    async fn query_row(&self, sql: &str, params: &[JsonValue]) -> Result<JsonRow, AnyError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.query_row(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.query_row(sql, params).await,
        }
    }

    async fn query_value(&self, sql: &str, params: &[JsonValue]) -> Result<JsonValue, AnyError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.query_value(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.query_value(sql, params).await,
        }
    }

    async fn query_string(&self, sql: &str, params: &[JsonValue]) -> Result<String, AnyError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.query_string(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.query_string(sql, params).await,
        }
    }

    async fn query_u64(&self, sql: &str, params: &[JsonValue]) -> Result<u64, AnyError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.query_u64(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.query_u64(sql, params).await,
        }
    }

    async fn query_i64(&self, sql: &str, params: &[JsonValue]) -> Result<i64, AnyError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.query_i64(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.query_i64(sql, params).await,
        }
    }

    async fn query_f64(&self, sql: &str, params: &[JsonValue]) -> Result<f64, AnyError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.query_f64(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.query_f64(sql, params).await,
        }
    }
}
