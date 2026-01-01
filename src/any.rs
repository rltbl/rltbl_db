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
    CachingStrategy, ColumnMap, DbError, DbKind, DbQuery, DbRow, IntoParams, ParamValue,
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
    ) -> Result<Vec<DbRow>, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.query(sql, params).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.query(sql, params).await,
        }
    }

    async fn insert(&self, table: &str, columns: &[&str], rows: &[&DbRow]) -> Result<(), DbError> {
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
        rows: &[&DbRow],
        returning: &[&str],
    ) -> Result<Vec<DbRow>, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.insert_returning(table, columns, rows, returning).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => {
                pool.insert_returning(table, columns, rows, returning).await
            }
        }
    }

    async fn update(&self, table: &str, columns: &[&str], rows: &[&DbRow]) -> Result<(), DbError> {
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
        rows: &[&DbRow],
        returning: &[&str],
    ) -> Result<Vec<DbRow>, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.update_returning(table, columns, rows, returning).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => {
                pool.update_returning(table, columns, rows, returning).await
            }
        }
    }

    async fn upsert(&self, table: &str, columns: &[&str], rows: &[&DbRow]) -> Result<(), DbError> {
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
        rows: &[&DbRow],
        returning: &[&str],
    ) -> Result<Vec<DbRow>, DbError> {
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
    use crate::core::{CachingStrategy, JsonValue, StringRow, get_memory_cache_contents};
    use crate::params;
    use indexmap::indexmap;
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
        let value: String = pool
            .query_value(&select_sql, &["foo"])
            .await
            .unwrap()
            .into();
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
        assert_eq!(row, indexmap! {"value".into() => ParamValue::from("foo")});

        let rows = pool.query(&select_sql, &["foo"]).await.unwrap();
        assert_eq!(
            rows,
            [indexmap! {"value".into() => ParamValue::from("foo")}]
        );

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
            let value = pool.query_value(&select_sql, params.clone()).await.unwrap();
            // TODO: Refactor
            let value = match value {
                ParamValue::Null => JsonValue::Null,
                ParamValue::Boolean(flag) => json!(flag),
                ParamValue::SmallInteger(number) => json!(number),
                ParamValue::Integer(number) => json!(number),
                ParamValue::BigInteger(number) => json!(number),
                ParamValue::Real(number) => json!(number),
                ParamValue::BigReal(number) => json!(number),
                ParamValue::Numeric(number) => json!(number),
                ParamValue::Text(string) => json!(string),
            };
            let value = value.as_i64().unwrap();
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
            assert_eq!(row, indexmap! {column.into() => ParamValue::from(1_i64)});

            let rows = pool.query(&select_sql, params.clone()).await.unwrap();
            assert_eq!(rows, [indexmap! {column.into() => ParamValue::from(1_i64)}]);
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
        let value = pool.query_value(&select_sql, &[1.0_f64]).await.unwrap();
        // TODO: Refactor
        let value = match value {
            ParamValue::Null => JsonValue::Null,
            ParamValue::Boolean(flag) => json!(flag),
            ParamValue::SmallInteger(number) => json!(number),
            ParamValue::Integer(number) => json!(number),
            ParamValue::BigInteger(number) => json!(number),
            ParamValue::Real(number) => json!(number),
            ParamValue::BigReal(number) => json!(number),
            ParamValue::Numeric(number) => json!(number),
            ParamValue::Text(string) => json!(string),
        };
        let value = value.as_f64().unwrap();
        assert_eq!("1.05", format!("{value:.2}"));

        let float = pool.query_f64(&select_sql, &[1.0_f64]).await.unwrap();
        assert_eq!(1.05, float);

        let string = pool.query_string(&select_sql, &[1.0_f64]).await.unwrap();
        assert_eq!("1.05", string);

        let strings = pool.query_strings(&select_sql, &[1.0_f64]).await.unwrap();
        assert_eq!(vec!["1.05".to_owned()], strings);

        let row = pool.query_row(&select_sql, &[1.0_f64]).await.unwrap();
        assert_eq!(row, indexmap! {"value".into() => ParamValue::from(1.05)});

        let rows = pool.query(&select_sql, &[1.0_f64]).await.unwrap();
        assert_eq!(rows, [indexmap! {"value".into() => ParamValue::from(1.05)}]);

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
        let value = pool.query_value(&select_sql, &[1.0_f32]).await.unwrap();
        // TODO: Refactor
        let value = match value {
            ParamValue::Null => JsonValue::Null,
            ParamValue::Boolean(flag) => json!(flag),
            ParamValue::SmallInteger(number) => json!(number),
            ParamValue::Integer(number) => json!(number),
            ParamValue::BigInteger(number) => json!(number),
            ParamValue::Real(number) => json!(number),
            ParamValue::BigReal(number) => json!(number),
            ParamValue::Numeric(number) => json!(number),
            ParamValue::Text(string) => json!(string),
        };
        let value = value.as_f64().unwrap();
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
        let value: String = pool.query_value(&select_sql, ["foo"]).await.unwrap().into();
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
            row,
            indexmap! {
                "text_value".into() => ParamValue::from("foo"),
                "alt_text_value".into() => ParamValue::Null,
                "float_value".into() => ParamValue::from(1.05),
                "alt_float_value".into() => ParamValue::Null,
                "int_value".into() => ParamValue::from(1_i64),
                "alt_int_value".into() => ParamValue::Null,
                "bool_value".into() => ParamValue::from(true),
                "alt_bool_value".into() => ParamValue::Null,
                "numeric_value".into() => ParamValue::from(1_i64),
                "alt_numeric_value".into() => ParamValue::Null,
            }
        );

        let rows = pool.query(&select_sql, params.clone()).await.unwrap();
        assert_eq!(
            rows,
            [indexmap! {
                "text_value".into() => ParamValue::from("foo"),
                "alt_text_value".into() => ParamValue::Null,
                "float_value".into() => ParamValue::from(1.05),
                "alt_float_value".into() => ParamValue::Null,
                "int_value".into() => ParamValue::from(1_i64),
                "alt_int_value".into() => ParamValue::Null,
                "bool_value".into() => ParamValue::from(true),
                "alt_bool_value".into() => ParamValue::Null,
                "numeric_value".into() => ParamValue::from(1_i64),
                "alt_numeric_value".into() => ParamValue::Null,
            }]
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
                &indexmap! {"text_value".into() => ParamValue::from("TEXT")},
                &indexmap! {
                    "int_value".into() => ParamValue::from(1_i64),
                    "bool_value".into() => ParamValue::from(true)
                },
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
            rows,
            [
                indexmap! {
                    "text_value".into() => ParamValue::from("TEXT"),
                    "alt_text_value".into() => ParamValue::Null,
                    "float_value".into() => ParamValue::Null,
                    "int_value".into() => ParamValue::Null,
                    "bool_value".into() => ParamValue::Null,
                },
                indexmap! {
                    "text_value".into() => ParamValue::Null,
                    "alt_text_value".into() => ParamValue::Null,
                    "float_value".into() => ParamValue::Null,
                    "int_value".into() => ParamValue::from(1_i64),
                    "bool_value".into() => ParamValue::from(true),
                }
            ]
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
                    &indexmap! {"text_value".into() => ParamValue::from("TEXT")},
                    &indexmap! {
                        "int_value".into() => ParamValue::from(1_i64),
                        "bool_value".into() => ParamValue::from(true)
                    },
                ],
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            rows,
            [
                indexmap! {
                    "text_value".into() => ParamValue::from("TEXT"),
                    "alt_text_value".into() => ParamValue::Null,
                    "float_value".into() => ParamValue::Null,
                    "int_value".into() => ParamValue::Null,
                    "bool_value".into() => ParamValue::Null,
                },
                indexmap! {
                    "text_value".into() => ParamValue::Null,
                    "alt_text_value".into() => ParamValue::Null,
                    "float_value".into() => ParamValue::Null,
                    "int_value".into() => ParamValue::from(1_i64),
                    "bool_value".into() => ParamValue::from(true),
                }
            ]
        );

        // With specific returning columns:
        let rows = pool
            .insert_returning(
                "test_insert_returning",
                &["text_value", "int_value", "bool_value"],
                &[
                    &indexmap! {"text_value".into() => ParamValue::from("TEXT")},
                    &indexmap! {
                        "int_value".into() => ParamValue::from(1_i64),
                        "bool_value".into() => ParamValue::from(true)
                    },
                ],
                &["int_value", "float_value"],
            )
            .await
            .unwrap();
        assert_eq!(
            rows,
            [
                indexmap! {
                    "float_value".into() => ParamValue::Null,
                    "int_value".into() => ParamValue::Null,
                },
                indexmap! {
                    "float_value".into() => ParamValue::Null,
                    "int_value".into() => ParamValue::from(1_i64),
                }
            ]
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

        match pool.columns(table1).await {
            Ok(columns) => panic!("No columns expected for '{table1}' but got {columns:?}"),
            Err(_) => (),
        };

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
                &indexmap! {"foo".into() => ParamValue::from(1_i64)},
                &indexmap! {"foo".into() => ParamValue::from(2_i64)},
                &indexmap! {"foo".into() => ParamValue::from(3_i64)},
            ],
        )
        .await
        .unwrap();

        pool.update(
            "test_update",
            &["foo", "bar", "car", "dar", "ear"],
            &[
                &indexmap! {
                    "foo".into() => ParamValue::from(1_i64),
                    "bar".into() => ParamValue::from(10_i64),
                    "car".into() => ParamValue::from(11_i64),
                    "dar".into() => ParamValue::from(12_i64),
                    "ear".into() => ParamValue::from(13_i64),
                },
                &indexmap! {
                    "foo".into() => ParamValue::from(2_i64),
                    "bar".into() => ParamValue::from(14_i64),
                    "car".into() => ParamValue::from(15_i64),
                    "dar".into() => ParamValue::from(16_i64),
                    "ear".into() => ParamValue::from(17_i64),
                },
                &indexmap! {
                    "foo".into() => ParamValue::from(3_i64),
                    "bar".into() => ParamValue::from(18_i64),
                    "car".into() => ParamValue::from(19_i64),
                    "dar".into() => ParamValue::from(20_i64),
                    "ear".into() => ParamValue::from(21_i64),
                },
            ],
        )
        .await
        .unwrap();

        let rows = pool.query("SELECT * from test_update", ()).await.unwrap();
        assert_eq!(
            rows,
            [
                indexmap! {
                    "foo".into() => ParamValue::from(1_i64),
                    "bar".into() => ParamValue::from(10_i64),
                    "car".into() => ParamValue::from(11_i64),
                    "dar".into() => ParamValue::from(12_i64),
                    "ear".into() => ParamValue::from(13_i64),
                },
                indexmap! {
                    "foo".into() => ParamValue::from(2_i64),
                    "bar".into() => ParamValue::from(14_i64),
                    "car".into() => ParamValue::from(15_i64),
                    "dar".into() => ParamValue::from(16_i64),
                    "ear".into() => ParamValue::from(17_i64),
                },
                indexmap! {
                    "foo".into() => ParamValue::from(3_i64),
                    "bar".into() => ParamValue::from(18_i64),
                    "car".into() => ParamValue::from(19_i64),
                    "dar".into() => ParamValue::from(20_i64),
                    "ear".into() => ParamValue::from(21_i64),
                },
            ]
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
                &indexmap! {
                    "foo".into() => ParamValue::from(1_i64),
                    "bar".into() => ParamValue::from(1_i64)
                },
                &indexmap! {
                    "foo".into() => ParamValue::from(2_i64),
                    "bar".into() => ParamValue::from(2_i64),
                },
                &indexmap! {
                    "foo".into() => ParamValue::from(3_i64),
                    "bar".into() => ParamValue::from(3_i64),
                },
            ],
        )
        .await
        .unwrap();

        let check_returning_rows = |rows: &Vec<DbRow>| {
            assert!(rows.iter().all(|row| {
                [
                    indexmap! {
                        "car".into() => ParamValue::from(10_i64),
                        "dar".into() => ParamValue::from(11_i64),
                        "ear".into() => ParamValue::from(12_i64),
                    },
                    indexmap! {
                        "car".into() => ParamValue::from(13_i64),
                        "dar".into() => ParamValue::from(14_i64),
                        "ear".into() => ParamValue::from(15_i64),
                    },
                    indexmap! {
                        "car".into() => ParamValue::from(16_i64),
                        "dar".into() => ParamValue::from(17_i64),
                        "ear".into() => ParamValue::from(18_i64),
                    },
                ]
                .contains(&row)
            }));
        };

        check_returning_rows(
            &pool
                .update_returning(
                    "test_update_returning",
                    &["foo", "bar", "car", "dar", "ear"],
                    &[
                        &indexmap! {
                            "foo".into() => ParamValue::from(1_i64),
                            "bar".into() => ParamValue::from(1_i64),
                            "car".into() => ParamValue::from(10_i64),
                            "dar".into() => ParamValue::from(11_i64),
                            "ear".into() => ParamValue::from(12_i64),
                        },
                        &indexmap! {
                            "foo".into() => ParamValue::from(2_i64),
                            "bar".into() => ParamValue::from(2_i64),
                            "car".into() => ParamValue::from(13_i64),
                            "dar".into() => ParamValue::from(14_i64),
                            "ear".into() => ParamValue::from(15_i64),
                        },
                        &indexmap! {
                            "foo".into() => ParamValue::from(3_i64),
                            "bar".into() => ParamValue::from(3_i64),
                            "car".into() => ParamValue::from(16_i64),
                            "dar".into() => ParamValue::from(17_i64),
                            "ear".into() => ParamValue::from(18_i64),
                        },
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
                &indexmap! {
                    "foo".into() => ParamValue::from(1_i64),
                    "bar".into() => ParamValue::from(1_i64),
                },
                &indexmap! {
                    "foo".into() => ParamValue::from(2_i64),
                    "bar".into() => ParamValue::from(2_i64),
                },
                &indexmap! {
                    "foo".into() => ParamValue::from(3_i64),
                    "bar".into() => ParamValue::from(3_i64),
                },
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
                        &indexmap! {
                            "ear".into() => ParamValue::from(15_i64),
                            "bar".into() => ParamValue::from(2_i64),
                            "car".into() => ParamValue::from(13_i64),
                            "dar".into() => ParamValue::from(14_i64),
                            "foo".into() => ParamValue::from(2_i64),
                        },
                        &indexmap! {
                            "foo".into() => ParamValue::from(1_i64),
                            "car".into() => ParamValue::from(10_i64),
                            "bar".into() => ParamValue::from(1_i64),
                            "ear".into() => ParamValue::from(12_i64),
                            "dar".into() => ParamValue::from(11_i64),
                        },
                        &indexmap! {
                            "car".into() => ParamValue::from(16_i64),
                            "dar".into() => ParamValue::from(17_i64),
                            "ear".into() => ParamValue::from(18_i64),
                            "bar".into() => ParamValue::from(3_i64),
                            "foo".into() => ParamValue::from(3_i64),
                        },
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
                indexmap! {
                    "foo".into() => ParamValue::from(1_i64),
                    "bar".into() => ParamValue::from(1_i64),
                    "car".into() => ParamValue::from(10_i64),
                    "dar".into() => ParamValue::from(11_i64),
                    "ear".into() => ParamValue::from(12_i64),
                },
                indexmap! {
                    "foo".into() => ParamValue::from(2_i64),
                    "bar".into() => ParamValue::from(2_i64),
                    "car".into() => ParamValue::from(13_i64),
                    "dar".into() => ParamValue::from(14_i64),
                    "ear".into() => ParamValue::from(15_i64),
                },
                indexmap! {
                    "foo".into() => ParamValue::from(3_i64),
                    "bar".into() => ParamValue::from(3_i64),
                    "car".into() => ParamValue::from(16_i64),
                    "dar".into() => ParamValue::from(17_i64),
                    "ear".into() => ParamValue::from(18_i64),
                },
            ]
            .contains(&row)
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
                &indexmap! {"foo".into() => ParamValue::from(1_i64)},
                &indexmap! {"foo".into() => ParamValue::from(2_i64)},
                &indexmap! {"foo".into() => ParamValue::from(3_i64)},
            ],
        )
        .await
        .unwrap();

        pool.upsert(
            "test_upsert",
            &["foo", "bar", "car", "dar", "ear"],
            &[
                &indexmap! {
                    "foo".into() => ParamValue::from(1_i64),
                    "bar".into() => ParamValue::from(10_i64),
                    "car".into() => ParamValue::from(11_i64),
                    "dar".into() => ParamValue::from(12_i64),
                    "ear".into() => ParamValue::from(13_i64),
                },
                &indexmap! {
                    "foo".into() => ParamValue::from(2_i64),
                    "bar".into() => ParamValue::from(14_i64),
                    "car".into() => ParamValue::from(15_i64),
                    "dar".into() => ParamValue::from(16_i64),
                    "ear".into() => ParamValue::from(17_i64),
                },
                &indexmap! {
                    "foo".into() => ParamValue::from(3_i64),
                    "bar".into() => ParamValue::from(18_i64),
                    "car".into() => ParamValue::from(19_i64),
                    "dar".into() => ParamValue::from(20_i64),
                    "ear".into() => ParamValue::from(21_i64),
                },
            ],
        )
        .await
        .unwrap();

        let rows = pool.query("SELECT * from test_upsert", ()).await.unwrap();
        assert_eq!(
            rows,
            [
                indexmap! {
                    "foo".into() => ParamValue::from(1_i64),
                    "bar".into() => ParamValue::from(10_i64),
                    "car".into() => ParamValue::from(11_i64),
                    "dar".into() => ParamValue::from(12_i64),
                    "ear".into() => ParamValue::from(13_i64),
                },
                indexmap! {
                    "foo".into() => ParamValue::from(2_i64),
                    "bar".into() => ParamValue::from(14_i64),
                    "car".into() => ParamValue::from(15_i64),
                    "dar".into() => ParamValue::from(16_i64),
                    "ear".into() => ParamValue::from(17_i64),
                },
                indexmap! {
                    "foo".into() => ParamValue::from(3_i64),
                    "bar".into() => ParamValue::from(18_i64),
                    "car".into() => ParamValue::from(19_i64),
                    "dar".into() => ParamValue::from(20_i64),
                    "ear".into() => ParamValue::from(21_i64),
                },
            ]
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
                &indexmap! {
                    "foo".into() => ParamValue::from(1_i64),
                    "bar".into() => ParamValue::from(1_i64),
                },
                &indexmap! {
                    "foo".into() => ParamValue::from(2_i64),
                    "bar".into() => ParamValue::from(2_i64),
                },
                &indexmap! {
                    "foo".into() => ParamValue::from(3_i64),
                    "bar".into() => ParamValue::from(3_i64),
                },
            ],
        )
        .await
        .unwrap();

        let rows = pool
            .upsert_returning(
                "test_upsert_returning",
                &["foo", "bar", "car", "dar", "ear"],
                &[
                    &indexmap! {
                        "foo".into() => ParamValue::from(1_i64),
                        "bar".into() => ParamValue::from(1_i64),
                        "car".into() => ParamValue::from(10_i64),
                        "dar".into() => ParamValue::from(11_i64),
                        "ear".into() => ParamValue::from(12_i64),
                    },
                    &indexmap! {
                        "foo".into() => ParamValue::from(2_i64),
                        "bar".into() => ParamValue::from(2_i64),
                        "car".into() => ParamValue::from(13_i64),
                        "dar".into() => ParamValue::from(14_i64),
                        "ear".into() => ParamValue::from(15_i64),
                    },
                    &indexmap! {
                        "foo".into() => ParamValue::from(3_i64),
                        "bar".into() => ParamValue::from(3_i64),
                        "car".into() => ParamValue::from(16_i64),
                        "dar".into() => ParamValue::from(17_i64),
                        "ear".into() => ParamValue::from(18_i64),
                    },
                ],
                &["car", "dar", "ear"],
            )
            .await
            .unwrap();
        assert!(rows.iter().all(|row| {
            [
                indexmap! {
                    "car".into() => ParamValue::from(10_i64),
                    "dar".into() => ParamValue::from(11_i64),
                    "ear".into() => ParamValue::from(12_i64),
                },
                indexmap! {
                    "car".into() => ParamValue::from(13_i64),
                    "dar".into() => ParamValue::from(14_i64),
                    "ear".into() => ParamValue::from(15_i64),
                },
                indexmap! {
                    "car".into() => ParamValue::from(16_i64),
                    "dar".into() => ParamValue::from(17_i64),
                    "ear".into() => ParamValue::from(18_i64),
                },
            ]
            .contains(&row)
        }));

        // Clean up:
        pool.drop_table("test_upsert_returning").await.unwrap();
    }

    #[tokio::test]
    async fn test_caching() {
        let all_strategies = ["none", "truncate_all", "truncate", "trigger", "memory:5"]
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
        async fn count_cache_table_rows(pool: &mut AnyPool) -> u64 {
            pool.query_u64("SELECT COUNT(1) from cache", ())
                .await
                .unwrap()
        }

        fn count_memory_cache_rows() -> u64 {
            let cache = get_memory_cache_contents().unwrap();
            cache.keys().len().try_into().unwrap()
        }

        pool.set_caching_strategy(strategy);
        pool.drop_table("test_table_caching_1").await.unwrap();
        pool.drop_table("test_table_caching_2").await.unwrap();
        pool.execute_batch(
            "CREATE TABLE test_table_caching_1 (\
               value TEXT \
             );\
             CREATE TABLE test_table_caching_2 (\
               value TEXT \
             )",
        )
        .await
        .unwrap();

        pool.insert(
            "test_table_caching_1",
            &["value"],
            &[
                &indexmap! {
                    "value".into() => ParamValue::from("alpha"),
                },
                &indexmap! {
                    "value".into() => ParamValue::from("beta"),
                },
            ],
        )
        .await
        .unwrap();

        let rows = pool
            .cache(
                &["test_table_caching_1"],
                "SELECT * from test_table_caching_1",
                (),
            )
            .await
            .unwrap();

        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => assert_eq!(count_memory_cache_rows(), 1),
            _ => assert_eq!(count_cache_table_rows(pool).await, 1),
        };
        assert_eq!(
            rows,
            vec![
                indexmap! {"value".into() => ParamValue::from("alpha")},
                indexmap! {"value".into() => ParamValue::from("beta")},
            ]
        );

        let rows = pool
            .cache(
                &["test_table_caching_1"],
                "SELECT * from test_table_caching_1",
                (),
            )
            .await
            .unwrap();

        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => assert_eq!(count_memory_cache_rows(), 1),
            _ => assert_eq!(count_cache_table_rows(pool).await, 1),
        };
        assert_eq!(
            rows,
            vec![
                indexmap! {"value".into() => ParamValue::from("alpha")},
                indexmap! {"value".into() => ParamValue::from("beta")},
            ]
        );

        pool.insert(
            "test_table_caching_1",
            &["value"],
            &[
                &indexmap! {"value".into() => ParamValue::from("gamma")},
                &indexmap! {"value".into() => ParamValue::from("delta")},
            ],
        )
        .await
        .unwrap();

        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => assert_eq!(count_memory_cache_rows(), 0),
            _ => assert_eq!(count_cache_table_rows(pool).await, 0),
        };

        let rows = pool
            .cache(
                &["test_table_caching_1"],
                "SELECT * from test_table_caching_1",
                (),
            )
            .await
            .unwrap();

        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => assert_eq!(count_memory_cache_rows(), 1),
            _ => assert_eq!(count_cache_table_rows(pool).await, 1),
        };
        assert_eq!(
            rows,
            vec![
                indexmap! {"value".into() => ParamValue::from("alpha")},
                indexmap! {"value".into() => ParamValue::from("beta")},
                indexmap! {"value".into() => ParamValue::from("gamma")},
                indexmap! {"value".into() => ParamValue::from("delta")},
            ]
        );

        let rows = pool
            .cache(
                &["test_table_caching_1"],
                "SELECT * from test_table_caching_1",
                (),
            )
            .await
            .unwrap();

        assert_eq!(
            rows,
            vec![
                indexmap! {"value".into() => ParamValue::from("alpha")},
                indexmap! {"value".into() => ParamValue::from("beta")},
                indexmap! {"value".into() => ParamValue::from("gamma")},
                indexmap! {"value".into() => ParamValue::from("delta")},
            ]
        );

        pool.cache(
            &["test_table_caching_1"],
            "SELECT COUNT(1) FROM test_table_caching_1",
            (),
        )
        .await
        .unwrap();
        pool.cache(
            &["test_table_caching_2"],
            "SELECT COUNT(1) FROM test_table_caching_2",
            (),
        )
        .await
        .unwrap();

        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => assert_eq!(count_memory_cache_rows(), 3),
            _ => assert_eq!(count_cache_table_rows(pool).await, 3),
        };

        pool.execute(
            r#"INSERT INTO test_table_caching_1 VALUES ('rho'), ('sigma')"#,
            (),
        )
        .await
        .unwrap();

        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => assert_eq!(count_memory_cache_rows(), 1),
            CachingStrategy::Truncate | CachingStrategy::Trigger => {
                assert_eq!(count_cache_table_rows(pool).await, 1)
            }
            CachingStrategy::TruncateAll => assert_eq!(count_cache_table_rows(pool).await, 0),
        };

        let rows = pool
            .cache(
                &["test_table_caching_1"],
                "SELECT * from test_table_caching_1",
                (),
            )
            .await
            .unwrap();

        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => assert_eq!(count_memory_cache_rows(), 2),
            CachingStrategy::Truncate | CachingStrategy::Trigger => {
                assert_eq!(count_cache_table_rows(pool).await, 2)
            }
            CachingStrategy::TruncateAll => assert_eq!(count_cache_table_rows(pool).await, 1),
        };
        assert_eq!(
            rows,
            vec![
                indexmap! {"value".into() => ParamValue::from("alpha")},
                indexmap! {"value".into() => ParamValue::from("beta")},
                indexmap! {"value".into() => ParamValue::from("gamma")},
                indexmap! {"value".into() => ParamValue::from("delta")},
                indexmap! {"value".into() => ParamValue::from("rho")},
                indexmap! {"value".into() => ParamValue::from("sigma")},
            ]
        );
    }

    #[tokio::test]
    async fn test_sql_parsing() {
        #[cfg(feature = "rusqlite")]
        sql_parsing(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        sql_parsing("postgresql:///rltbl_db").await;
    }

    async fn sql_parsing(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let prefix = match pool.kind() {
            DbKind::SQLite => "?",
            DbKind::PostgreSQL => "$",
        };

        // Single statements, possibly with parameters:

        let tables: Vec<_> = pool
            .get_modified_tables(&format!(
                r#"INSERT INTO "alpha" VALUES ({prefix}1, {prefix}2, {prefix}3)"#
            ))
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(tables, ["alpha"]);

        let tables: Vec<_> = pool
            .get_modified_tables(
                r#"WITH bar AS (SELECT * FROM alpha),
                        mar AS (SELECT * FROM beta)
                   INSERT INTO gamma
                   SELECT alpha.*
                   FROM alpha, beta
                   WHERE alpha.value = beta.value"#,
            )
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(tables, ["gamma"]);

        let tables: Vec<_> = pool
            .get_modified_tables(&format!(
                r#"UPDATE "delta" set bar = {prefix}1 WHERE bar = {prefix}2"#
            ))
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(tables, ["delta"]);

        let tables: Vec<_> = pool
            .get_modified_tables(&format!(
                r#"WITH bar AS (SELECT * FROM test),
                        mar AS (SELECT * FROM test)
                   UPDATE delta
                   SET value = bar.value
                   FROM bar, mar
                   WHERE bar.value = {prefix}1 AND bar.value = mar.value"#,
            ))
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(tables, ["delta"]);

        let tables: Vec<_> = pool
            .get_modified_tables(&format!(r#"DELETE FROM "epsilon" WHERE bar >= {prefix}1"#))
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(tables, ["epsilon"]);

        let tables: Vec<_> = pool
            .get_modified_tables(
                r#"WITH bar AS (SELECT * FROM test),
                        mar AS (SELECT * FROM test)
                   DELETE FROM lambda WHERE value IN (SELECT value FROM bar)"#,
            )
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(tables, ["lambda"]);

        let tables: Vec<_> = pool
            .get_modified_tables(r#"DROP TABLE "rho""#)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(tables, ["rho"]);

        let tables: Vec<_> = pool
            .get_modified_tables(r#"DROP TABLE IF EXISTS "phi" CASCADE"#)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(tables, ["phi"]);

        // Multiple statements, no parameters:

        let sql = r#"
            INSERT INTO "alpha" VALUES (1, 2, 3), (4, 5, 6);

            INSERT INTO gamma
            SELECT alpha.*
            FROM alpha, beta
            WHERE alpha.value = beta.value;

            WITH t AS (
              SELECT * from delta_base ORDER BY quality LIMIT 1
            )
            UPDATE delta SET price = t.price * 1.05;

            WITH t AS (
              SELECT * FROM phi_base
              WHERE
                "date" >= '2010-10-01' AND
                "date" < '2010-11-01'
            )
            INSERT INTO phi
            SELECT * FROM t;

            DELETE FROM "psi" WHERE bar >= 10;

            WITH RECURSIVE included_lambda(sub_lambda, lambda) AS (
                SELECT sub_lambda, lambda FROM lambda WHERE lambda = 'our_product'
              UNION ALL
                SELECT p.sub_lambda, p.lambda
                FROM included_lambda pr, lambda p
                WHERE p.lambda = pr.sub_lambda
            )
            DELETE FROM lambda
              WHERE lambda IN (SELECT lambda FROM included_lambda);

            DROP TABLE "rho";

            DROP TABLE "sigma" CASCADE"#;

        let mut tables: Vec<_> = pool
            .get_modified_tables(&sql)
            .unwrap()
            .into_iter()
            .collect();
        tables.sort();
        assert_eq!(
            tables,
            [
                "alpha", "delta", "gamma", "lambda", "phi", "psi", "rho", "sigma",
            ]
        );
    }
}
