/// Connect to any supported database using a URL.
///
/// ```
/// use rltbl_db::{any::AnyPool, core::{DbError, DbQuery}};
///
/// async fn example() -> Result<String, DbError> {
///     let pool = AnyPool::connect("test.db").await?;
///     pool.execute_batch(
///         "DROP TABLE IF EXISTS test;\
///          CREATE TABLE test ( value TEXT );\
///          INSERT INTO test VALUES ('foo');",
///     ).await?;
///     let value = pool.query_string("SELECT value FROM test;", ()).await?;
///     Ok(value)
/// }
/// ```
use crate::core::{
    ColumnMap, DbError, DbKind, DbQuery, IntoParams, JsonRow, JsonValue, ParamValue, StringRow,
};

#[cfg(feature = "rusqlite")]
use crate::rusqlite::RusqlitePool;
#[cfg(feature = "tokio-postgres")]
use crate::tokio_postgres::TokioPostgresPool;

#[derive(Debug)]
pub enum AnyPool {
    #[cfg(feature = "rusqlite")]
    Rusqlite(RusqlitePool),
    #[cfg(feature = "tokio-postgres")]
    TokioPostgres(TokioPostgresPool),
}

impl AnyPool {
    /// Connect to the database located at the given URL.
    pub async fn connect(url: &str) -> Result<Self, DbError> {
        if url.starts_with("postgresql://") {
            #[cfg(feature = "tokio-postgres")]
            {
                Ok(AnyPool::TokioPostgres(
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
                Ok(AnyPool::Rusqlite(RusqlitePool::connect(url).await?))
            }
            #[cfg(not(feature = "rusqlite"))]
            {
                Err(DbError::ConnectError("sqlite not configured".to_string()))
            }
        }
    }
}

impl DbQuery for AnyPool {
    fn kind(&self) -> DbKind {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.kind(),
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.kind(),
        }
    }

    fn parse(&self, sql_type: &str, value: &str) -> Result<ParamValue, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.parse(sql_type, value),
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.parse(sql_type, value),
        }
    }

    fn convert_json(&self, sql_type: &str, value: &JsonValue) -> Result<ParamValue, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.convert_json(sql_type, value),
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.convert_json(sql_type, value),
        }
    }

    async fn columns(&self, table: &str) -> Result<ColumnMap, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.columns(table).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.columns(table).await,
        }
    }

    async fn execute(&self, sql: &str, params: impl IntoParams + Send) -> Result<(), DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.execute(sql, params).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.execute(sql, params).await,
        }
    }

    async fn execute_batch(&self, sql: &str) -> Result<(), DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.execute_batch(sql).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.execute_batch(sql).await,
        }
    }

    async fn query(
        &self,
        sql: &str,
        params: impl IntoParams + Send,
    ) -> Result<Vec<JsonRow>, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.query(sql, params).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.query(sql, params).await,
        }
    }

    async fn query_row(
        &self,
        sql: &str,
        params: impl IntoParams + Send,
    ) -> Result<JsonRow, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.query_row(sql, params).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.query_row(sql, params).await,
        }
    }

    async fn query_value(
        &self,
        sql: &str,
        params: impl IntoParams + Send,
    ) -> Result<JsonValue, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.query_value(sql, params).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.query_value(sql, params).await,
        }
    }

    async fn query_string(
        &self,
        sql: &str,
        params: impl IntoParams + Send,
    ) -> Result<String, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.query_string(sql, params).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.query_string(sql, params).await,
        }
    }

    async fn query_strings(
        &self,
        sql: &str,
        params: impl IntoParams + Send,
    ) -> Result<Vec<String>, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.query_strings(sql, params).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.query_strings(sql, params).await,
        }
    }

    async fn query_string_row(
        &self,
        sql: &str,
        params: impl IntoParams + Send,
    ) -> Result<StringRow, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.query_string_row(sql, params).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.query_string_row(sql, params).await,
        }
    }

    async fn query_string_rows(
        &self,
        sql: &str,
        params: impl IntoParams + Send,
    ) -> Result<Vec<StringRow>, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.query_string_rows(sql, params).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.query_string_rows(sql, params).await,
        }
    }

    async fn query_u64(&self, sql: &str, params: impl IntoParams + Send) -> Result<u64, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.query_u64(sql, params).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.query_u64(sql, params).await,
        }
    }

    async fn query_i64(&self, sql: &str, params: impl IntoParams + Send) -> Result<i64, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.query_i64(sql, params).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.query_i64(sql, params).await,
        }
    }

    async fn query_f64(&self, sql: &str, params: impl IntoParams + Send) -> Result<f64, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.query_f64(sql, params).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.query_f64(sql, params).await,
        }
    }

    async fn insert(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[&JsonRow],
    ) -> Result<(), DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.insert(table, columns, rows).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.insert(table, columns, rows).await,
        }
    }

    async fn insert_returning(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[&JsonRow],
        returning: &[&str],
    ) -> Result<Vec<JsonRow>, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.insert_returning(table, columns, rows, returning).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => {
                pool.insert_returning(table, columns, rows, returning).await
            }
        }
    }

    async fn drop_table(&self, table: &str) -> Result<(), DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.drop_table(table).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.drop_table(table).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::params;
    use rust_decimal::dec;
    use serde_json::json;

    #[tokio::test]
    async fn test_mixed_column_query() {
        #[cfg(feature = "rusqlite")]
        mixed_column_query("test_any_sqlite.db").await;
        #[cfg(feature = "tokio-postgres")]
        mixed_column_query("postgresql:///rltbl_db").await;
    }

    async fn mixed_column_query(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        pool.execute_batch(
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
        pool.execute(
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
            params!["foo", (), 1.05_f64, (), 1_i64, (), true, (), dec!(1), ()],
        )
        .await
        .unwrap();

        let select_sql = "SELECT text_value FROM test_any_table_mixed WHERE text_value = $1";
        let value = pool
            .query_value(select_sql, ["foo"])
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
        let params = params!["foo", (), 1.0_f64, 0_i64, true, dec!(0.999)];

        let row = pool.query_row(select_sql, params.clone()).await.unwrap();
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
                "numeric_value": 1,
                "alt_numeric_value": JsonValue::Null,
            })
        );

        let rows = pool.query(select_sql, params.clone()).await.unwrap();
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
                "numeric_value": 1,
                "alt_numeric_value": JsonValue::Null,
            }])
        );

        // Clean up:
        pool.drop_table("test_any_table_mixed").await.unwrap();
    }

    #[tokio::test]
    async fn test_input_params() {
        #[cfg(feature = "rusqlite")]
        input_params("test_any_sqlite.db").await;
        #[cfg(feature = "tokio-postgres")]
        input_params("postgresql:///rltbl_db").await;
    }

    async fn input_params(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        pool.execute("DROP TABLE IF EXISTS test_any_table_input_params", ())
            .await
            .unwrap();
        pool.execute(
            "CREATE TABLE test_any_table_input_params (\
               bar TEXT,\
               car INT2,\
               dar INT4,\
               far INT8,\
               gar FLOAT4,\
               har FLOAT8,\
               jar NUMERIC,\
               kar BOOL
             )",
            (),
        )
        .await
        .unwrap();
        pool.execute(
            "INSERT INTO test_any_table_input_params (bar) VALUES ($1)",
            &["one"],
        )
        .await
        .unwrap();
        pool.execute(
            "INSERT INTO test_any_table_input_params (far) VALUES ($1)",
            &[1 as i64],
        )
        .await
        .unwrap();
        pool.execute(
            "INSERT INTO test_any_table_input_params (bar) VALUES ($1)",
            ["two"],
        )
        .await
        .unwrap();
        pool.execute(
            "INSERT INTO test_any_table_input_params (far) VALUES ($1)",
            [2 as i64],
        )
        .await
        .unwrap();
        pool.execute(
            "INSERT INTO test_any_table_input_params (bar) VALUES ($1)",
            vec!["three"],
        )
        .await
        .unwrap();
        pool.execute(
            "INSERT INTO test_any_table_input_params (far) VALUES ($1)",
            vec![3 as i64],
        )
        .await
        .unwrap();
        pool.execute(
            "INSERT INTO test_any_table_input_params (gar) VALUES ($1)",
            vec![3 as f32],
        )
        .await
        .unwrap();
        pool.execute(
            "INSERT INTO test_any_table_input_params (har) VALUES ($1)",
            vec![3 as f64],
        )
        .await
        .unwrap();
        pool.execute(
            "INSERT INTO test_any_table_input_params (jar) VALUES ($1)",
            vec![dec!(3)],
        )
        .await
        .unwrap();
        pool.execute(
            "INSERT INTO test_any_table_input_params (kar) VALUES ($1)",
            vec![true],
        )
        .await
        .unwrap();
        pool.execute(
            "INSERT INTO test_any_table_input_params \
             (bar, car, dar, far, gar, har, jar, kar) \
             VALUES ($1, $2, $3, $4, $5 ,$6, $7, $8)",
            params![
                "four",
                123_i16,
                123_i32,
                123_i64,
                123_f32,
                123_f64,
                dec!(123),
                true,
            ],
        )
        .await
        .unwrap();

        // Clean up:
        pool.drop_table("test_any_table_input_params")
            .await
            .unwrap();
    }
}
