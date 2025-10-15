use crate::core::{DbQuery, JsonRow, JsonValue};

#[cfg(feature = "postgres")]
use crate::postgres::{PostgresConnection, PostgresError};
#[cfg(feature = "sqlite")]
use crate::sqlite::{SqliteConnection, SqliteError};

#[derive(Debug)]
pub enum DbError {
    Generic(String),
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteError),
    #[cfg(feature = "postgres")]
    Postgres(PostgresError),
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DbError::Generic(err) => write!(f, "Generic db error: {err}"),
            #[cfg(feature = "sqlite")]
            DbError::Sqlite(err) => write!(f, "Sqlite error: {err}"),
            #[cfg(feature = "postgres")]
            DbError::Postgres(err) => write!(f, "Postgres error: {err}"),
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
    pub async fn connect(url: &str) -> Result<Self, DbError> {
        if url.starts_with("postgresql://") {
            #[cfg(feature = "postgres")]
            {
                Ok(AnyConnection::Postgres(
                    crate::postgres::PostgresConnection::connect(url).await?,
                ))
            }
            #[cfg(not(feature = "postgres"))]
            {
                Err(DbError::Generic("postgres not configured".to_string()))
            }
        } else {
            #[cfg(feature = "sqlite")]
            {
                Ok(AnyConnection::Sqlite(SqliteConnection::connect(url).await?))
            }
            #[cfg(not(feature = "sqlite"))]
            {
                Err(DbError::Generic("sqlite not configured".to_string()))
            }
        }
    }
}

impl DbQuery for AnyConnection {
    async fn execute(&self, sql: &str, params: &[JsonValue]) -> Result<(), DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.execute(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.execute(sql, params).await,
        }
    }

    async fn execute_batch(&self, sql: &str) -> Result<(), DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.execute_batch(sql).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.execute_batch(sql).await,
        }
    }

    async fn query(&self, sql: &str, params: &[JsonValue]) -> Result<Vec<JsonRow>, DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.query(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.query(sql, params).await,
        }
    }

    async fn query_row(&self, sql: &str, params: &[JsonValue]) -> Result<JsonRow, DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.query_row(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.query_row(sql, params).await,
        }
    }

    async fn query_value(&self, sql: &str, params: &[JsonValue]) -> Result<JsonValue, DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.query_value(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.query_value(sql, params).await,
        }
    }

    async fn query_string(&self, sql: &str, params: &[JsonValue]) -> Result<String, DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.query_string(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.query_string(sql, params).await,
        }
    }

    async fn query_u64(&self, sql: &str, params: &[JsonValue]) -> Result<u64, DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.query_u64(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.query_u64(sql, params).await,
        }
    }

    async fn query_i64(&self, sql: &str, params: &[JsonValue]) -> Result<i64, DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.query_i64(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.query_i64(sql, params).await,
        }
    }

    async fn query_f64(&self, sql: &str, params: &[JsonValue]) -> Result<f64, DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.query_f64(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.query_f64(sql, params).await,
        }
    }
}
