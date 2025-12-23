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
    CachingStrategy, ColumnMap, DbError, DbKind, DbQuery, IntoParams, JsonRow, ParamValue,
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
    /// Get the DbKind for this connection URL.
    pub fn connection_kind(url: &str) -> Result<DbKind, DbError> {
        if url.starts_with("postgresql://") {
            #[cfg(feature = "tokio-postgres")]
            {
                Ok(DbKind::PostgreSQL)
            }
            #[cfg(not(feature = "tokio-postgres"))]
            {
                Err(DbError::ConnectError(
                    "PostgreSQL not configured".to_string(),
                ))
            }
        } else {
            #[cfg(feature = "rusqlite")]
            {
                Ok(DbKind::SQLite)
            }
            #[cfg(not(feature = "rusqlite"))]
            {
                Err(DbError::ConnectError("SQLite not configured".to_string()))
            }
        }
    }

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
                Err(DbError::ConnectError(
                    "PostgreSQL not configured".to_string(),
                ))
            }
        } else {
            #[cfg(feature = "rusqlite")]
            {
                Ok(AnyPool::Rusqlite(RusqlitePool::connect(url).await?))
            }
            #[cfg(not(feature = "rusqlite"))]
            {
                Err(DbError::ConnectError("SQLite not configured".to_string()))
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

    fn set_caching_strategy(&mut self, strategy: &CachingStrategy) {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.set_caching_strategy(strategy),
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.set_caching_strategy(strategy),
        }
    }

    fn get_caching_strategy(&self) -> CachingStrategy {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.get_caching_strategy(),
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.get_caching_strategy(),
        }
    }

    async fn ensure_cache_table_exists(&self) -> Result<(), DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.ensure_cache_table_exists().await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.ensure_cache_table_exists().await,
        }
    }

    async fn ensure_caching_triggers_exist(&self, tables: &[&str]) -> Result<(), DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.ensure_caching_triggers_exist(tables).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.ensure_caching_triggers_exist(tables).await,
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

    async fn columns(&self, table: &str) -> Result<ColumnMap, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.columns(table).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.columns(table).await,
        }
    }

    async fn primary_keys(&self, table: &str) -> Result<Vec<String>, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.primary_keys(table).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.primary_keys(table).await,
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

    async fn update(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[&JsonRow],
    ) -> Result<(), DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.update(table, columns, rows).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.update(table, columns, rows).await,
        }
    }

    async fn update_returning(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[&JsonRow],
        returning: &[&str],
    ) -> Result<Vec<JsonRow>, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.update_returning(table, columns, rows, returning).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => {
                pool.update_returning(table, columns, rows, returning).await
            }
        }
    }

    async fn upsert(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[&JsonRow],
    ) -> Result<(), DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.upsert(table, columns, rows).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.upsert(table, columns, rows).await,
        }
    }

    async fn upsert_returning(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[&JsonRow],
        returning: &[&str],
    ) -> Result<Vec<JsonRow>, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.upsert_returning(table, columns, rows, returning).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => {
                pool.upsert_returning(table, columns, rows, returning).await
            }
        }
    }

    async fn table_exists(&self, table: &str) -> Result<bool, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.table_exists(table).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.table_exists(table).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::JsonValue;
    use crate::core::StringRow;
    use crate::params;
    use rust_decimal::dec;
    use serde_json::json;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_text_column_query() {
        #[cfg(feature = "rusqlite")]
        text_column_query(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        text_column_query("postgresql:///rltbl_db").await;
    }

    async fn text_column_query(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let p = match pool.kind() {
            DbKind::PostgreSQL => "$",
            DbKind::SQLite => "?",
        };
        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS test_table_text{cascade};\
             CREATE TABLE test_table_text ( value TEXT )",
            cascade = match pool.kind() {
                DbKind::PostgreSQL => " CASCADE",
                DbKind::SQLite => "",
            }
        ))
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_table_text VALUES ({p}1)"),
            &["foo"],
        )
        .await
        .unwrap();
        let select_sql = format!("SELECT value FROM test_table_text WHERE value = {p}1");
        let value = pool
            .query_value(&select_sql, &["foo"])
            .await
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!("foo", value);

        let string = pool.query_string(&select_sql, &["foo"]).await.unwrap();
        assert_eq!("foo", string);

        let strings = pool.query_strings(&select_sql, &["foo"]).await.unwrap();
        assert_eq!(vec!["foo".to_owned()], strings);

        let string_row = pool.query_string_row(&select_sql, &["foo"]).await.unwrap();
        assert_eq!(
            StringRow::from([("value".to_owned(), "foo".to_owned())]),
            string_row
        );

        let string_rows = pool.query_string_rows(&select_sql, &["foo"]).await.unwrap();
        assert_eq!(
            vec![StringRow::from([("value".to_owned(), "foo".to_owned())])],
            string_rows
        );

        let row = pool.query_row(&select_sql, &["foo"]).await.unwrap();
        assert_eq!(json!(row), json!({"value":"foo"}));

        let rows = pool.query(&select_sql, &["foo"]).await.unwrap();
        assert_eq!(json!(rows), json!([{"value":"foo"}]));

        // Clean up:
        pool.drop_table("test_table_text").await.unwrap();
    }

    #[tokio::test]
    async fn test_integer_column_query() {
        #[cfg(feature = "rusqlite")]
        integer_column_query(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        integer_column_query("postgresql:///rltbl_db").await;
    }

    async fn integer_column_query(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let p = match pool.kind() {
            DbKind::PostgreSQL => "$",
            DbKind::SQLite => "?",
        };
        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS test_table_int{cascade};\
             CREATE TABLE test_table_int ( value_2 INT2, value_4 INT4, value_8 INT8 )",
            cascade = match pool.kind() {
                DbKind::PostgreSQL => " CASCADE",
                DbKind::SQLite => "",
            }
        ))
        .await
        .unwrap();

        pool.execute(
            &format!("INSERT INTO test_table_int VALUES ({p}1, {p}2, {p}3)"),
            params![1_i16, 1_i32, 1_i64],
        )
        .await
        .unwrap();

        for column in ["value_2", "value_4", "value_8"] {
            let params = match column {
                "value_2" => params![1_i16],
                "value_4" => params![1_i32],
                "value_8" => params![1_i64],
                _ => unreachable!(),
            };
            let select_sql = format!("SELECT {column} FROM test_table_int WHERE {column} = {p}1");
            let value = pool
                .query_value(&select_sql, params.clone())
                .await
                .unwrap()
                .as_i64()
                .unwrap();
            assert_eq!(1, value);

            let unsigned = pool.query_u64(&select_sql, params.clone()).await.unwrap();
            assert_eq!(1, unsigned);

            let signed = pool.query_i64(&select_sql, params.clone()).await.unwrap();
            assert_eq!(1, signed);

            let string = pool
                .query_string(&select_sql, params.clone())
                .await
                .unwrap();
            assert_eq!("1", string);

            let strings = pool
                .query_strings(&select_sql, params.clone())
                .await
                .unwrap();
            assert_eq!(vec!["1".to_owned()], strings);

            let row = pool.query_row(&select_sql, params.clone()).await.unwrap();
            assert_eq!(json!(row), json!({column:1}));

            let rows = pool.query(&select_sql, params.clone()).await.unwrap();
            assert_eq!(json!(rows), json!([{column:1}]));
        }

        // Clean up:
        pool.drop_table("test_table_int").await.unwrap();
    }

    #[tokio::test]
    async fn test_float_column_query() {
        #[cfg(feature = "rusqlite")]
        float_column_query(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        float_column_query("postgresql:///rltbl_db").await;
    }

    async fn float_column_query(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let p = match pool.kind() {
            DbKind::PostgreSQL => "$",
            DbKind::SQLite => "?",
        };

        // FLOAT8
        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS test_table_float{cascade};\
             CREATE TABLE test_table_float ( value FLOAT8 )",
            cascade = match pool.kind() {
                DbKind::PostgreSQL => " CASCADE",
                DbKind::SQLite => "",
            }
        ))
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_table_float VALUES ({p}1)"),
            &[1.05_f64],
        )
        .await
        .unwrap();
        let select_sql = format!("SELECT value FROM test_table_float WHERE value > {p}1");
        let value = pool
            .query_value(&select_sql, &[1.0_f64])
            .await
            .unwrap()
            .as_f64()
            .unwrap();
        assert_eq!("1.05", format!("{value:.2}"));

        let float = pool.query_f64(&select_sql, &[1.0_f64]).await.unwrap();
        assert_eq!(1.05, float);

        let string = pool.query_string(&select_sql, &[1.0_f64]).await.unwrap();
        assert_eq!("1.05", string);

        let strings = pool.query_strings(&select_sql, &[1.0_f64]).await.unwrap();
        assert_eq!(vec!["1.05".to_owned()], strings);

        let row = pool.query_row(&select_sql, &[1.0_f64]).await.unwrap();
        assert_eq!(json!(row), json!({"value":1.05}));

        let rows = pool.query(&select_sql, &[1.0_f64]).await.unwrap();
        assert_eq!(json!(rows), json!([{"value":1.05}]));

        // FLOAT4
        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS test_table_float{cascade};\
             CREATE TABLE test_table_float ( value FLOAT4 )",
            cascade = match pool.kind() {
                DbKind::PostgreSQL => " CASCADE",
                DbKind::SQLite => "",
            }
        ))
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_table_float VALUES ({p}1)"),
            &[1.05_f32],
        )
        .await
        .unwrap();
        let select_sql = format!("SELECT value FROM test_table_float WHERE value > {p}1");
        let value = pool
            .query_value(&select_sql, &[1.0_f32])
            .await
            .unwrap()
            .as_f64()
            .unwrap();
        assert_eq!("1.05", format!("{value:.2}"));

        // Clean up:
        pool.drop_table("test_table_float").await.unwrap();
    }

    #[tokio::test]
    async fn test_mixed_column_query() {
        #[cfg(feature = "rusqlite")]
        mixed_column_query(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        mixed_column_query("postgresql:///rltbl_db").await;
    }

    async fn mixed_column_query(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let p = match pool.kind() {
            DbKind::PostgreSQL => "$",
            DbKind::SQLite => "?",
        };
        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS test_table_mixed{cascade};\
             CREATE TABLE test_table_mixed (\
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
            cascade = match pool.kind() {
                DbKind::PostgreSQL => " CASCADE",
                DbKind::SQLite => "",
            }
        ))
        .await
        .unwrap();
        pool.execute(
            &format!(
                r#"INSERT INTO test_table_mixed
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
                   VALUES ({p}1, {p}2, {p}3, {p}4, {p}5, {p}6, {p}7, {p}8, {p}9, {p}10)"#,
            ),
            params!["foo", (), 1.05_f64, (), 1_i64, (), true, (), dec!(1), ()],
        )
        .await
        .unwrap();

        let select_sql = format!("SELECT text_value FROM test_table_mixed WHERE text_value = {p}1");
        let value = pool
            .query_value(&select_sql, ["foo"])
            .await
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!("foo", value);

        let select_sql = format!(
            r#"SELECT
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
               FROM test_table_mixed
               WHERE text_value = {p}1
                 AND alt_text_value IS NOT DISTINCT FROM {p}2
                 AND float_value > {p}3
                 AND int_value > {p}4
                 AND bool_value = {p}5
                 AND numeric_value > {p}6"#
        );
        let params = params!["foo", (), 1.0_f64, 0_i64, true, dec!(0.999)];

        let row = pool.query_row(&select_sql, params.clone()).await.unwrap();
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

        let rows = pool.query(&select_sql, params.clone()).await.unwrap();
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
        pool.drop_table("test_table_mixed").await.unwrap();
    }

    #[tokio::test]
    async fn test_input_params() {
        #[cfg(feature = "rusqlite")]
        input_params(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        input_params("postgresql:///rltbl_db").await;
    }

    async fn input_params(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let p = match pool.kind() {
            DbKind::PostgreSQL => "$",
            DbKind::SQLite => "?",
        };
        let cascade = match pool.kind() {
            DbKind::PostgreSQL => " CASCADE",
            DbKind::SQLite => "",
        };
        pool.execute(
            &format!("DROP TABLE IF EXISTS test_any_table_input_params{cascade}"),
            (),
        )
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
            &format!("INSERT INTO test_any_table_input_params (bar) VALUES ({p}1)"),
            &["one"],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (far) VALUES ({p}1)"),
            &[1 as i64],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (bar) VALUES ({p}1)"),
            ["two"],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (far) VALUES ({p}1)"),
            [2 as i64],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (bar) VALUES ({p}1)"),
            vec!["three"],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (far) VALUES ({p}1)"),
            vec![3 as i64],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (gar) VALUES ({p}1)"),
            vec![3 as f32],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (har) VALUES ({p}1)"),
            vec![3 as f64],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (jar) VALUES ({p}1)"),
            vec![dec!(3)],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (kar) VALUES ({p}1)"),
            vec![true],
        )
        .await
        .unwrap();
        pool.execute(
            &format!(
                "INSERT INTO test_any_table_input_params \
                 (bar, car, dar, far, gar, har, jar, kar) \
                 VALUES ({p}1, {p}2, {p}3, {p}4, {p}5 ,{p}6, {p}7, {p}8)"
            ),
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

    #[tokio::test]
    async fn test_insert() {
        #[cfg(feature = "rusqlite")]
        insert(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        insert("postgresql:///rltbl_db").await;
    }

    async fn insert(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let cascade = match pool.kind() {
            DbKind::PostgreSQL => " CASCADE",
            DbKind::SQLite => "",
        };
        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS test_insert{cascade};\
             CREATE TABLE test_insert (\
               text_value TEXT,\
               alt_text_value TEXT,\
               float_value FLOAT8,\
               int_value INT8,\
               bool_value BOOL\
             )"
        ))
        .await
        .unwrap();

        // Insert rows:
        pool.insert(
            "test_insert",
            &["text_value", "int_value", "bool_value"],
            &[
                &json!({"text_value": "TEXT"}).as_object().unwrap(),
                &json!({"int_value": 1, "bool_value": true})
                    .as_object()
                    .unwrap(),
            ],
        )
        .await
        .unwrap();

        // Validate the inserted data:
        let rows = pool
            .query(r#"SELECT * FROM test_insert"#, ())
            .await
            .unwrap();
        assert_eq!(
            json!(rows),
            json!([{
                "text_value": "TEXT",
                "alt_text_value": JsonValue::Null,
                "float_value": JsonValue::Null,
                "int_value": JsonValue::Null,
                "bool_value": JsonValue::Null,
            },{
                "text_value": JsonValue::Null,
                "alt_text_value": JsonValue::Null,
                "float_value": JsonValue::Null,
                "int_value": 1,
                "bool_value": true,
            }])
        );

        // Clean up.
        pool.drop_table("test_insert").await.unwrap();
    }

    #[tokio::test]
    async fn test_insert_returning() {
        #[cfg(feature = "rusqlite")]
        insert_returning(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        insert_returning("postgresql:///rltbl_db").await;
    }

    async fn insert_returning(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let cascade = match pool.kind() {
            DbKind::PostgreSQL => " CASCADE",
            DbKind::SQLite => "",
        };
        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS test_insert_returning{cascade};\
             CREATE TABLE test_insert_returning (\
               text_value TEXT,\
               alt_text_value TEXT,\
               float_value FLOAT8,\
               int_value INT8,\
               bool_value BOOL\
             )",
        ))
        .await
        .unwrap();

        // Without specific returning columns:
        let rows = pool
            .insert_returning(
                "test_insert_returning",
                &["text_value", "int_value", "bool_value"],
                &[
                    &json!({"text_value": "TEXT"}).as_object().unwrap(),
                    &json!({"int_value": 1, "bool_value": true})
                        .as_object()
                        .unwrap(),
                ],
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            json!(rows),
            json!([{
                "text_value": "TEXT",
                "alt_text_value": JsonValue::Null,
                "float_value": JsonValue::Null,
                "int_value": JsonValue::Null,
                "bool_value": JsonValue::Null,
            },{
                "text_value": JsonValue::Null,
                "alt_text_value": JsonValue::Null,
                "float_value": JsonValue::Null,
                "int_value": 1,
                "bool_value": true,
            }])
        );

        // With specific returning columns:
        let rows = pool
            .insert_returning(
                "test_insert_returning",
                &["text_value", "int_value", "bool_value"],
                &[
                    &json!({"text_value": "TEXT"}).as_object().unwrap(),
                    &json!({"int_value": 1, "bool_value": true})
                        .as_object()
                        .unwrap(),
                ],
                &["int_value", "float_value"],
            )
            .await
            .unwrap();
        assert_eq!(
            json!(rows),
            json!([{
                "float_value": JsonValue::Null,
                "int_value": JsonValue::Null,
            },{
                "float_value": JsonValue::Null,
                "int_value": 1,
            }])
        );

        // Clean up.
        pool.drop_table("test_insert_returning").await.unwrap();
    }

    #[tokio::test]
    async fn test_drop_table() {
        #[cfg(feature = "rusqlite")]
        drop_table(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        drop_table("postgresql:///rltbl_db").await;
    }

    async fn drop_table(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let cascade = match pool.kind() {
            DbKind::PostgreSQL => " CASCADE",
            DbKind::SQLite => "",
        };
        let table1 = "test_drop1";
        let table2 = "test_drop2";
        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS {table1}{cascade};\
             DROP TABLE IF EXISTS {table2}{cascade};\
             CREATE TABLE {table1} (\
                 foo TEXT PRIMARY KEY\
             );\
             CREATE TABLE {table2} (\
                 foo TEXT REFERENCES {table1}(foo)\
             );",
        ))
        .await
        .unwrap();

        let columns = pool.columns(table1).await.unwrap();
        assert_eq!(
            columns,
            ColumnMap::from([("foo".to_owned(), "text".to_owned())])
        );
        pool.drop_table(table1).await.unwrap();

        let columns = pool.columns(table1).await.unwrap();
        assert_eq!(columns.is_empty(), true);

        // Clean up.
        pool.drop_table(table2).await.unwrap();
    }

    #[tokio::test]
    async fn test_primary_keys() {
        #[cfg(feature = "rusqlite")]
        primary_keys(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        primary_keys("postgresql:///rltbl_db").await;
    }

    async fn primary_keys(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let cascade = match pool.kind() {
            DbKind::PostgreSQL => " CASCADE",
            DbKind::SQLite => "",
        };
        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS test_primary_keys1{cascade};\
             DROP TABLE IF EXISTS test_primary_keys2{cascade};\
             CREATE TABLE test_primary_keys1 (\
               foo TEXT PRIMARY KEY\
             );\
             CREATE TABLE test_primary_keys2 (\
               foo TEXT,\
               bar TEXT,\
               car TEXT,
               PRIMARY KEY (foo, bar)\
             )",
        ))
        .await
        .unwrap();

        assert_eq!(
            pool.primary_keys("test_primary_keys1").await.unwrap(),
            ["foo"]
        );
        assert_eq!(
            pool.primary_keys("test_primary_keys2").await.unwrap(),
            ["foo", "bar"]
        );

        // Clean up:
        pool.drop_table("test_primary_keys1").await.unwrap();
        pool.drop_table("test_primary_keys2").await.unwrap();
    }

    #[tokio::test]
    async fn test_update() {
        #[cfg(feature = "rusqlite")]
        update(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        update("postgresql:///rltbl_db").await;
    }

    async fn update(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let cascade = match pool.kind() {
            DbKind::PostgreSQL => " CASCADE",
            DbKind::SQLite => "",
        };
        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS test_update{cascade};\
             CREATE TABLE test_update (\
               foo BIGINT PRIMARY KEY,\
               bar BIGINT,\
               car BIGINT,\
               dar BIGINT,\
               ear BIGINT\
             )",
        ))
        .await
        .unwrap();

        pool.insert(
            "test_update",
            &["foo"],
            &[
                &json!({"foo": 1}).as_object().unwrap(),
                &json!({"foo": 2}).as_object().unwrap(),
                &json!({"foo": 3}).as_object().unwrap(),
            ],
        )
        .await
        .unwrap();

        pool.update(
            "test_update",
            &["foo", "bar", "car", "dar", "ear"],
            &[
                &json!({
                    "foo": 1,
                    "bar": 10,
                    "car": 11,
                    "dar": 12,
                    "ear": 13,
                })
                .as_object()
                .unwrap(),
                &json!({
                    "foo": 2,
                    "bar": 14,
                    "car": 15,
                    "dar": 16,
                    "ear": 17,
                })
                .as_object()
                .unwrap(),
                &json!({
                    "foo": 3,
                    "bar": 18,
                    "car": 19,
                    "dar": 20,
                    "ear": 21,
                })
                .as_object()
                .unwrap(),
            ],
        )
        .await
        .unwrap();

        let rows = pool.query("SELECT * from test_update", ()).await.unwrap();
        assert_eq!(
            json!(rows),
            json!([
                {
                    "foo": 1,
                    "bar": 10,
                    "car": 11,
                    "dar": 12,
                    "ear": 13,
                },
                {
                    "foo": 2,
                    "bar": 14,
                    "car": 15,
                    "dar": 16,
                    "ear": 17,
                },
                {
                    "foo": 3,
                    "bar": 18,
                    "car": 19,
                    "dar": 20,
                    "ear": 21,
                },
            ])
        );

        // Clean up:
        pool.drop_table("test_update").await.unwrap();
    }

    #[tokio::test]
    async fn test_update_returning() {
        #[cfg(feature = "rusqlite")]
        update_returning(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        update_returning("postgresql:///rltbl_db").await;
    }

    async fn update_returning(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let cascade = match pool.kind() {
            DbKind::PostgreSQL => " CASCADE",
            DbKind::SQLite => "",
        };
        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS test_update_returning{cascade};\
             CREATE TABLE test_update_returning (\
               foo BIGINT,\
               bar BIGINT,\
               car BIGINT,\
               dar BIGINT,\
               ear BIGINT,\
               PRIMARY KEY (foo, bar)\
             )",
        ))
        .await
        .unwrap();

        pool.insert(
            "test_update_returning",
            &["foo", "bar", "car", "dar", "ear"],
            &[
                &json!({"foo": 1, "bar": 1}).as_object().unwrap(),
                &json!({"foo": 2, "bar": 2}).as_object().unwrap(),
                &json!({"foo": 3, "bar": 3}).as_object().unwrap(),
            ],
        )
        .await
        .unwrap();

        let check_returning_rows = |rows: &Vec<JsonRow>| {
            assert!(rows.iter().all(|row| {
                [
                    json!({
                        "car": 10,
                        "dar": 11,
                        "ear": 12,
                    }),
                    json!({
                        "car": 13,
                        "dar": 14,
                        "ear": 15,
                    }),
                    json!({
                        "car": 16,
                        "dar": 17,
                        "ear": 18,
                    }),
                ]
                .contains(&json!(row))
            }));
        };

        check_returning_rows(
            &pool
                .update_returning(
                    "test_update_returning",
                    &["foo", "bar", "car", "dar", "ear"],
                    &[
                        &json!({
                            "foo": 1,
                            "bar": 1,
                            "car": 10,
                            "dar": 11,
                            "ear": 12,
                        })
                        .as_object()
                        .unwrap(),
                        &json!({
                            "foo": 2,
                            "bar": 2,
                            "car": 13,
                            "dar": 14,
                            "ear": 15,
                        })
                        .as_object()
                        .unwrap(),
                        &json!({
                            "foo": 3,
                            "bar": 3,
                            "car": 16,
                            "dar": 17,
                            "ear": 18,
                        })
                        .as_object()
                        .unwrap(),
                    ],
                    &["car", "dar", "ear"],
                )
                .await
                .unwrap(),
        );

        // This is the same update as the first one above, just with the columns of the input
        // rows to the update, as well as the rows themselves, specified in a different order.
        pool.execute("DELETE FROM test_update_returning", ())
            .await
            .unwrap();

        pool.insert(
            "test_update_returning",
            &["foo", "bar"],
            &[
                &json!({"foo": 1, "bar": 1}).as_object().unwrap(),
                &json!({"foo": 2, "bar": 2}).as_object().unwrap(),
                &json!({"foo": 3, "bar": 3}).as_object().unwrap(),
            ],
        )
        .await
        .unwrap();

        check_returning_rows(
            &pool
                .update_returning(
                    "test_update_returning",
                    &["foo", "bar", "car", "dar", "ear"],
                    &[
                        &json!({
                            "ear": 15,
                            "bar": 2,
                            "car": 13,
                            "dar": 14,
                            "foo": 2,
                        })
                        .as_object()
                        .unwrap(),
                        &json!({
                            "foo": 1,
                            "car": 10,
                            "bar": 1,
                            "ear": 12,
                            "dar": 11,
                        })
                        .as_object()
                        .unwrap(),
                        &json!({
                            "car": 16,
                            "dar": 17,
                            "ear": 18,
                            "bar": 3,
                            "foo": 3,
                        })
                        .as_object()
                        .unwrap(),
                    ],
                    &["car", "dar", "ear"],
                )
                .await
                .unwrap(),
        );

        // Final sanity check on the values of all columns:
        let rows = pool
            .query("SELECT * from test_update_returning", ())
            .await
            .unwrap();
        assert!(rows.iter().all(|row| {
            [
                json!({
                    "foo": 1,
                    "bar": 1,
                    "car": 10,
                    "dar": 11,
                    "ear": 12,
                }),
                json!({
                    "foo": 2,
                    "bar": 2,
                    "car": 13,
                    "dar": 14,
                    "ear": 15,
                }),
                json!({
                    "foo": 3,
                    "bar": 3,
                    "car": 16,
                    "dar": 17,
                    "ear": 18,
                }),
            ]
            .contains(&json!(row))
        }));

        // Clean up:
        pool.drop_table("test_update_returning").await.unwrap();
    }

    #[tokio::test]
    async fn test_upsert() {
        #[cfg(feature = "rusqlite")]
        upsert(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        upsert("postgresql:///rltbl_db").await;
    }

    async fn upsert(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let cascade = match pool.kind() {
            DbKind::PostgreSQL => " CASCADE",
            DbKind::SQLite => "",
        };
        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS test_upsert{cascade};\
             CREATE TABLE test_upsert (\
               foo BIGINT PRIMARY KEY,\
               bar BIGINT,\
               car BIGINT,\
               dar BIGINT,\
               ear BIGINT\
             )",
        ))
        .await
        .unwrap();

        pool.insert(
            "test_upsert",
            &["foo"],
            &[
                &json!({"foo": 1}).as_object().unwrap(),
                &json!({"foo": 2}).as_object().unwrap(),
                &json!({"foo": 3}).as_object().unwrap(),
            ],
        )
        .await
        .unwrap();

        pool.upsert(
            "test_upsert",
            &["foo", "bar", "car", "dar", "ear"],
            &[
                &json!({
                    "foo": 1,
                    "bar": 10,
                    "car": 11,
                    "dar": 12,
                    "ear": 13,
                })
                .as_object()
                .unwrap(),
                &json!({
                    "foo": 2,
                    "bar": 14,
                    "car": 15,
                    "dar": 16,
                    "ear": 17,
                })
                .as_object()
                .unwrap(),
                &json!({
                    "foo": 3,
                    "bar": 18,
                    "car": 19,
                    "dar": 20,
                    "ear": 21,
                })
                .as_object()
                .unwrap(),
            ],
        )
        .await
        .unwrap();

        let rows = pool.query("SELECT * from test_upsert", ()).await.unwrap();
        assert_eq!(
            json!(rows),
            json!([
                {
                    "foo": 1,
                    "bar": 10,
                    "car": 11,
                    "dar": 12,
                    "ear": 13,
                },
                {
                    "foo": 2,
                    "bar": 14,
                    "car": 15,
                    "dar": 16,
                    "ear": 17,
                },
                {
                    "foo": 3,
                    "bar": 18,
                    "car": 19,
                    "dar": 20,
                    "ear": 21,
                },
            ])
        );

        // Clean up:
        pool.drop_table("test_upsert").await.unwrap();
    }

    #[tokio::test]
    async fn test_upsert_returning() {
        #[cfg(feature = "rusqlite")]
        upsert_returning(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        upsert_returning("postgresql:///rltbl_db").await;
    }

    async fn upsert_returning(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let cascade = match pool.kind() {
            DbKind::PostgreSQL => " CASCADE",
            DbKind::SQLite => "",
        };
        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS test_upsert_returning{cascade};\
             CREATE TABLE test_upsert_returning (\
               foo BIGINT,\
               bar BIGINT,\
               car BIGINT,\
               dar BIGINT,\
               ear BIGINT,\
               PRIMARY KEY (foo, bar)\
             )",
        ))
        .await
        .unwrap();

        pool.insert(
            "test_upsert_returning",
            &["foo", "bar", "car", "dar", "ear"],
            &[
                &json!({"foo": 1, "bar": 1}).as_object().unwrap(),
                &json!({"foo": 2, "bar": 2}).as_object().unwrap(),
                &json!({"foo": 3, "bar": 3}).as_object().unwrap(),
            ],
        )
        .await
        .unwrap();

        let rows = pool
            .upsert_returning(
                "test_upsert_returning",
                &["foo", "bar", "car", "dar", "ear"],
                &[
                    &json!({
                        "foo": 1,
                        "bar": 1,
                        "car": 10,
                        "dar": 11,
                        "ear": 12,
                    })
                    .as_object()
                    .unwrap(),
                    &json!({
                        "foo": 2,
                        "bar": 2,
                        "car": 13,
                        "dar": 14,
                        "ear": 15,
                    })
                    .as_object()
                    .unwrap(),
                    &json!({
                        "foo": 3,
                        "bar": 3,
                        "car": 16,
                        "dar": 17,
                        "ear": 18,
                    })
                    .as_object()
                    .unwrap(),
                ],
                &["car", "dar", "ear"],
            )
            .await
            .unwrap();
        assert!(rows.iter().all(|row| {
            [
                json!({
                    "car": 10,
                    "dar": 11,
                    "ear": 12,
                }),
                json!({
                    "car": 13,
                    "dar": 14,
                    "ear": 15,
                }),
                json!({
                    "car": 16,
                    "dar": 17,
                    "ear": 18,
                }),
            ]
            .contains(&json!(row))
        }));

        // Clean up:
        pool.drop_table("test_upsert_returning").await.unwrap();
    }

    #[tokio::test]
    async fn test_caching() {
        let all_strategies = ["truncate_all", "truncate", "trigger", "memory:5"]
            .iter()
            .map(|strategy| CachingStrategy::from_str(strategy).unwrap())
            .collect::<Vec<_>>();
        #[cfg(feature = "rusqlite")]
        {
            let mut pool = AnyPool::connect(":memory:").await.unwrap();
            for caching_strategy in &all_strategies {
                cache_with_strategy(&mut pool, &caching_strategy).await;
            }
        }
        #[cfg(feature = "tokio-postgres")]
        {
            let mut pool = AnyPool::connect("postgresql:///rltbl_db").await.unwrap();
            for caching_strategy in &all_strategies {
                cache_with_strategy(&mut pool, &caching_strategy).await;
            }
        }
    }

    async fn cache_with_strategy(pool: &mut AnyPool, strategy: &CachingStrategy) {
        pool.set_caching_strategy(strategy);
        pool.drop_table("test_table_caching").await.unwrap();
        pool.execute(
            "CREATE TABLE test_table_caching (\
             value TEXT
             )",
            (),
        )
        .await
        .unwrap();

        pool.insert(
            "test_table_caching",
            &["value"],
            &[
                &json!({"value": "alpha"}).as_object().unwrap(),
                &json!({"value": "beta"}).as_object().unwrap(),
            ],
        )
        .await
        .unwrap();

        let rows = pool
            .cache(
                &["test_table_caching"],
                "SELECT * from test_table_caching",
                (),
            )
            .await
            .unwrap();

        assert_eq!(
            rows,
            vec![
                json!({"value": "alpha"}).as_object().unwrap().clone(),
                json!({"value": "beta"}).as_object().unwrap().clone(),
            ]
        );

        let rows = pool
            .cache(
                &["test_table_caching"],
                "SELECT * from test_table_caching",
                (),
            )
            .await
            .unwrap();

        assert_eq!(
            rows,
            vec![
                json!({"value": "alpha"}).as_object().unwrap().clone(),
                json!({"value": "beta"}).as_object().unwrap().clone(),
            ]
        );

        pool.insert(
            "test_table_caching",
            &["value"],
            &[
                &json!({"value": "gamma"}).as_object().unwrap(),
                &json!({"value": "delta"}).as_object().unwrap(),
            ],
        )
        .await
        .unwrap();

        let rows = pool
            .cache(
                &["test_table_caching"],
                "SELECT * from test_table_caching",
                (),
            )
            .await
            .unwrap();

        assert_eq!(
            rows,
            vec![
                json!({"value": "alpha"}).as_object().unwrap().clone(),
                json!({"value": "beta"}).as_object().unwrap().clone(),
                json!({"value": "gamma"}).as_object().unwrap().clone(),
                json!({"value": "delta"}).as_object().unwrap().clone(),
            ]
        );

        let rows = pool
            .cache(
                &["test_table_caching"],
                "SELECT * from test_table_caching",
                (),
            )
            .await
            .unwrap();

        assert_eq!(
            rows,
            vec![
                json!({"value": "alpha"}).as_object().unwrap().clone(),
                json!({"value": "beta"}).as_object().unwrap().clone(),
                json!({"value": "gamma"}).as_object().unwrap().clone(),
                json!({"value": "delta"}).as_object().unwrap().clone(),
            ]
        );
    }
}
