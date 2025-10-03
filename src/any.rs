use serde_json::Map as JsonMap;

use crate::core::DbQuery;

#[cfg(feature = "postgres")]
use crate::postgres::PostgresConnection;
#[cfg(feature = "sqlite")]
use crate::sqlite::SqliteConnection;

pub type JsonValue = serde_json::Value;
pub type JsonRow = JsonMap<String, JsonValue>;
pub type DbError = String;

pub enum AnyConnection {
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteConnection),
    #[cfg(feature = "postgres")]
    Postgres(PostgresConnection),
}

impl AnyConnection {
    /// Connect to the database located at the given URL. If the postgres feature flag has been
    /// set, this is assumed to be a PostgreSQL URL, otherwise the database is assumed to be a SQLite
    /// database.
    pub async fn connect(url: &str) -> Result<Self, DbError> {
        if url.starts_with("postgresql://") {
            #[cfg(feature = "postgres")]
            {
                Ok(AnyConnection::Postgres(
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
                Ok(AnyConnection::Sqlite(
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
