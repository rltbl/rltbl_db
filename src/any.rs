/// Connect to any supported database using a URL.
///
/// ```
/// use rltbl_db::{any::AnyConnection, core::{DbError, DbQuery}};
///
/// async fn example() -> Result<String, DbError> {
///     let conn = AnyConnection::connect("test.db").await?;
///     conn.execute_batch(
///         "DROP TABLE IF EXISTS test;\
///          CREATE TABLE test ( value TEXT );\
///          INSERT INTO test VALUES ('foo');",
///     ).await?;
///     let value = conn.query_string("SELECT value FROM test;", &[]).await?;
///     Ok(value)
/// }
/// ```
use crate::core::{DbError, DbQuery, JsonRow, JsonValue};

#[cfg(feature = "rusqlite")]
use crate::rusqlite::RusqlitePool;
#[cfg(feature = "tokio-postgres")]
use crate::tokio_postgres::TokioPostgresPool;

#[derive(Debug)]
pub enum AnyConnection {
    #[cfg(feature = "rusqlite")]
    Sqlite(RusqlitePool),
    #[cfg(feature = "tokio-postgres")]
    Postgres(TokioPostgresPool),
}

impl AnyConnection {
    /// Connect to the database located at the given URL.
    pub async fn connect(url: &str) -> Result<Self, DbError> {
        if url.starts_with("postgresql://") {
            #[cfg(feature = "tokio-postgres")]
            {
                Ok(AnyConnection::Postgres(
                    TokioPostgresPool::connect(url).await?,
                ))
            }
            #[cfg(not(feature = "tokio-postgres"))]
            {
                Err(DbError::ConnectError("postgres not configured".to_string()))
            }
        } else {
            #[cfg(feature = "rusqlite")]
            {
                Ok(AnyConnection::Sqlite(RusqlitePool::connect(url).await?))
            }
            #[cfg(not(feature = "rusqlite"))]
            {
                Err(DbError::ConnectError("sqlite not configured".to_string()))
            }
        }
    }
}

impl DbQuery for AnyConnection {
    async fn execute(&self, sql: &str, params: &[JsonValue]) -> Result<(), DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyConnection::Sqlite(connection) => connection.execute(sql, params).await,
            #[cfg(feature = "tokio-postgres")]
            AnyConnection::Postgres(connection) => connection.execute(sql, params).await,
        }
    }

    async fn execute_batch(&self, sql: &str) -> Result<(), DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyConnection::Sqlite(connection) => connection.execute_batch(sql).await,
            #[cfg(feature = "tokio-postgres")]
            AnyConnection::Postgres(connection) => connection.execute_batch(sql).await,
        }
    }

    async fn query(&self, sql: &str, params: &[JsonValue]) -> Result<Vec<JsonRow>, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyConnection::Sqlite(connection) => connection.query(sql, params).await,
            #[cfg(feature = "tokio-postgres")]
            AnyConnection::Postgres(connection) => connection.query(sql, params).await,
        }
    }

    async fn query_row(&self, sql: &str, params: &[JsonValue]) -> Result<JsonRow, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyConnection::Sqlite(connection) => connection.query_row(sql, params).await,
            #[cfg(feature = "tokio-postgres")]
            AnyConnection::Postgres(connection) => connection.query_row(sql, params).await,
        }
    }

    async fn query_value(&self, sql: &str, params: &[JsonValue]) -> Result<JsonValue, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyConnection::Sqlite(connection) => connection.query_value(sql, params).await,
            #[cfg(feature = "tokio-postgres")]
            AnyConnection::Postgres(connection) => connection.query_value(sql, params).await,
        }
    }

    async fn query_string(&self, sql: &str, params: &[JsonValue]) -> Result<String, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyConnection::Sqlite(connection) => connection.query_string(sql, params).await,
            #[cfg(feature = "tokio-postgres")]
            AnyConnection::Postgres(connection) => connection.query_string(sql, params).await,
        }
    }

    async fn query_u64(&self, sql: &str, params: &[JsonValue]) -> Result<u64, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyConnection::Sqlite(connection) => connection.query_u64(sql, params).await,
            #[cfg(feature = "tokio-postgres")]
            AnyConnection::Postgres(connection) => connection.query_u64(sql, params).await,
        }
    }

    async fn query_i64(&self, sql: &str, params: &[JsonValue]) -> Result<i64, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyConnection::Sqlite(connection) => connection.query_i64(sql, params).await,
            #[cfg(feature = "tokio-postgres")]
            AnyConnection::Postgres(connection) => connection.query_i64(sql, params).await,
        }
    }

    async fn query_f64(&self, sql: &str, params: &[JsonValue]) -> Result<f64, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyConnection::Sqlite(connection) => connection.query_f64(sql, params).await,
            #[cfg(feature = "tokio-postgres")]
            AnyConnection::Postgres(connection) => connection.query_f64(sql, params).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    #[tokio::test]
    async fn test_mixed_column_query() {
        #[cfg(feature = "rusqlite")]
        mixed_column_query("test_any_sqlite.db").await;
        #[cfg(feature = "tokio-postgres")]
        mixed_column_query("postgresql:///rltbl_db").await;
    }

    async fn mixed_column_query(url: &str) {
        let conn = AnyConnection::connect(url).await.unwrap();
        conn.execute_batch(
            "DROP TABLE IF EXISTS test_any_table_mixed;\
             CREATE TABLE test_any_table_mixed (\
                 text_value TEXT,\
                 alt_text_value TEXT,\
                 float_value FLOAT8,\
                 alt_float_value FLOAT8,\
                 int_value INT8,\
                 alt_int_value INT8,\
                 bool_value BOOL,\
                 alt_bool_value BOOL,\
                 numeric_value NUMERIC,\
                 alt_numeric_value NUMERIC\
             )",
        )
        .await
        .unwrap();
        conn.execute(
            r#"INSERT INTO test_any_table_mixed
               (
                 text_value,
                 alt_text_value,
                 float_value,
                 alt_float_value,
                 int_value,
                 alt_int_value,
                 bool_value,
                 alt_bool_value,
                 numeric_value,
                 alt_numeric_value
               )
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"#,
            &[
                json!("foo"),
                JsonValue::Null,
                json!(1.05),
                JsonValue::Null,
                json!(1),
                JsonValue::Null,
                json!(true),
                JsonValue::Null,
                json!(1_000_000),
                JsonValue::Null,
            ],
        )
        .await
        .unwrap();

        let select_sql = "SELECT text_value FROM test_any_table_mixed WHERE text_value = $1";
        let params = [json!("foo")];
        let value = conn
            .query_value(select_sql, &params)
            .await
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!("foo", value);

        let select_sql = r#"SELECT
                              text_value,
                              alt_text_value,
                              float_value,
                              alt_float_value,
                              int_value,
                              alt_int_value,
                              bool_value,
                              alt_bool_value,
                              numeric_value,
                              alt_numeric_value
                            FROM test_any_table_mixed
                            WHERE text_value = $1
                              AND alt_text_value IS NOT DISTINCT FROM $2
                              AND float_value > $3
                              AND int_value > $4
                              AND bool_value = $5
                              AND numeric_value > $6"#;
        let params = [
            json!("foo"),
            JsonValue::Null,
            json!(1.0),
            json!(0),
            json!(true),
            json!(999_999),
        ];

        let row = conn.query_row(select_sql, &params).await.unwrap();
        assert_eq!(
            json!(row),
            json!({
                "text_value": "foo",
                "alt_text_value": JsonValue::Null,
                "float_value": 1.05,
                "alt_float_value": JsonValue::Null,
                "int_value": 1,
                "alt_int_value": JsonValue::Null,
                "bool_value": true,
                "alt_bool_value": JsonValue::Null,
                "numeric_value": 1_000_000,
                "alt_numeric_value": JsonValue::Null,
            })
        );

        let rows = conn.query(select_sql, &params).await.unwrap();
        assert_eq!(
            json!(rows),
            json!([{
                "text_value": "foo",
                "alt_text_value": JsonValue::Null,
                "float_value": 1.05,
                "alt_float_value": JsonValue::Null,
                "int_value": 1,
                "alt_int_value": JsonValue::Null,
                "bool_value": true,
                "alt_bool_value": JsonValue::Null,
                "numeric_value": 1_000_000,
                "alt_numeric_value": JsonValue::Null,
            }])
        );
    }
}
