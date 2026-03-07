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
use crate::{
    core::{CachingStrategy, DbError, DbQuery, FromDbRows, IntoDbRows, IntoParams},
    db_kind::DbKind,
};

#[cfg(feature = "rusqlite")]
use crate::rusqlite::RusqlitePool;

#[cfg(feature = "tokio-postgres")]
use crate::tokio_postgres::TokioPostgresPool;

#[cfg(feature = "libsql")]
use crate::libsql::LibSQLPool;

#[derive(Debug)]
pub enum AnyPool {
    #[cfg(feature = "rusqlite")]
    Rusqlite(RusqlitePool),
    #[cfg(feature = "tokio-postgres")]
    TokioPostgres(TokioPostgresPool),
    #[cfg(feature = "libsql")]
    LibSQL(LibSQLPool),
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
                Err(DbError::ConnectError(format!("Unsupported URL: '{url}'")))
            }
        } else {
            #[cfg(feature = "rusqlite")]
            {
                Ok(DbKind::SQLite)
            }
            #[cfg(not(feature = "rusqlite"))]
            {
                #[cfg(feature = "libsql")]
                {
                    Ok(DbKind::SQLite)
                }
                #[cfg(not(feature = "libsql"))]
                {
                    Err(DbError::ConnectError(format!("Unsupported URL: '{url}'")))
                }
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
                Err(DbError::ConnectError(format!("Unsupported URL: '{url}'")))
            }
        } else {
            #[cfg(feature = "rusqlite")]
            {
                Ok(AnyPool::Rusqlite(RusqlitePool::connect(url).await?))
            }
            #[cfg(not(feature = "rusqlite"))]
            {
                #[cfg(feature = "libsql")]
                {
                    Ok(AnyPool::LibSQL(LibSQLPool::connect(url).await?))
                }
                #[cfg(not(feature = "libsql"))]
                {
                    Err(DbError::ConnectError(format!("Unsupported URL: '{url}'")))
                }
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
            #[cfg(feature = "libsql")]
            AnyPool::LibSQL(pool) => pool.kind(),
        }
    }

    fn set_caching_strategy(&mut self, strategy: &CachingStrategy) {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.set_caching_strategy(strategy),
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.set_caching_strategy(strategy),
            #[cfg(feature = "libsql")]
            AnyPool::LibSQL(pool) => pool.set_caching_strategy(strategy),
        }
    }

    fn get_caching_strategy(&self) -> CachingStrategy {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.get_caching_strategy(),
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.get_caching_strategy(),
            #[cfg(feature = "libsql")]
            AnyPool::LibSQL(pool) => pool.get_caching_strategy(),
        }
    }

    fn set_cache_aware_query(&mut self, value: bool) {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.set_cache_aware_query(value),
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.set_cache_aware_query(value),
            #[cfg(feature = "libsql")]
            AnyPool::LibSQL(pool) => pool.set_cache_aware_query(value),
        }
    }

    fn get_cache_aware_query(&self) -> bool {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.get_cache_aware_query(),
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.get_cache_aware_query(),
            #[cfg(feature = "libsql")]
            AnyPool::LibSQL(pool) => pool.get_cache_aware_query(),
        }
    }

    async fn execute_batch(&self, sql: &str) -> Result<(), DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.execute_batch(sql).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.execute_batch(sql).await,
            #[cfg(feature = "libsql")]
            AnyPool::LibSQL(pool) => pool.execute_batch(sql).await,
        }
    }

    async fn query_no_cache<T: FromDbRows>(
        &self,
        sql: &str,
        params: impl IntoParams + Send,
    ) -> Result<T, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.query_no_cache(sql, params).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.query_no_cache(sql, params).await,
            #[cfg(feature = "libsql")]
            AnyPool::LibSQL(pool) => pool.query_no_cache(sql, params).await,
        }
    }

    async fn insert(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
    ) -> Result<(), DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.insert(table, columns, rows).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.insert(table, columns, rows).await,
            #[cfg(feature = "libsql")]
            AnyPool::LibSQL(pool) => pool.insert(table, columns, rows).await,
        }
    }

    async fn insert_returning<T: FromDbRows>(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
        returning: &[&str],
    ) -> Result<T, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.insert_returning(table, columns, rows, returning).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => {
                pool.insert_returning(table, columns, rows, returning).await
            }
            #[cfg(feature = "libsql")]
            AnyPool::LibSQL(pool) => pool.insert_returning(table, columns, rows, returning).await,
        }
    }

    async fn update(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
    ) -> Result<(), DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.update(table, columns, rows).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.update(table, columns, rows).await,
            #[cfg(feature = "libsql")]
            AnyPool::LibSQL(pool) => pool.update(table, columns, rows).await,
        }
    }

    async fn update_returning<T: FromDbRows>(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
        returning: &[&str],
    ) -> Result<T, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.update_returning(table, columns, rows, returning).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => {
                pool.update_returning(table, columns, rows, returning).await
            }
            #[cfg(feature = "libsql")]
            AnyPool::LibSQL(pool) => pool.update_returning(table, columns, rows, returning).await,
        }
    }

    async fn upsert(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
    ) -> Result<(), DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.upsert(table, columns, rows).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => pool.upsert(table, columns, rows).await,
            #[cfg(feature = "libsql")]
            AnyPool::LibSQL(pool) => pool.upsert(table, columns, rows).await,
        }
    }

    async fn upsert_returning<T: FromDbRows>(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
        returning: &[&str],
    ) -> Result<T, DbError> {
        match self {
            #[cfg(feature = "rusqlite")]
            AnyPool::Rusqlite(pool) => pool.upsert_returning(table, columns, rows, returning).await,
            #[cfg(feature = "tokio-postgres")]
            AnyPool::TokioPostgres(pool) => {
                pool.upsert_returning(table, columns, rows, returning).await
            }
            #[cfg(feature = "libsql")]
            AnyPool::LibSQL(pool) => pool.upsert_returning(table, columns, rows, returning).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        core::{
            CachingStrategy, ColumnMap, DbRow, ParamValue, QUERY_CACHE_TABLE, StringRow,
            TABLE_CACHE_TABLE,
        },
        memory::{
            clear_memory_query_cache, clear_memory_table_cache, clear_meta_cache,
            get_memory_query_cache_contents, get_memory_table_cache_contents,
        },
        params,
    };
    use indexmap::indexmap as db_row;
    use rand::{
        SeedableRng as _,
        distr::{Distribution as _, Uniform},
        rngs::StdRng,
    };
    use rust_decimal::dec;
    use std::{
        collections::BTreeMap,
        str::FromStr,
        thread,
        time::{Duration, Instant},
    };

    #[tokio::test]
    async fn test_text_column_query() {
        #[cfg(feature = "rusqlite")]
        text_column_query(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        text_column_query("postgresql:///rltbl_db").await;
        #[cfg(feature = "libsql")]
        text_column_query(":memory:").await;
    }

    async fn text_column_query(url: &str) {
        clear_meta_cache().unwrap();
        let pool = AnyPool::connect(url).await.unwrap();
        let p = pool.kind().param_prefix().to_string();
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
        assert_eq!(row, db_row! {"value".into() => ParamValue::from("foo")});

        let rows: Vec<DbRow> = pool.query(&select_sql, &["foo"]).await.unwrap();
        assert_eq!(rows, [db_row! {"value".into() => ParamValue::from("foo")}]);

        // Clean up:
        pool.drop_table("test_table_text").await.unwrap();
    }

    #[tokio::test]
    async fn test_integer_column_query() {
        #[cfg(feature = "rusqlite")]
        integer_column_query(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        integer_column_query("postgresql:///rltbl_db").await;
        #[cfg(feature = "libsql")]
        integer_column_query(":memory:").await;
    }

    async fn integer_column_query(url: &str) {
        clear_meta_cache().unwrap();
        let pool = AnyPool::connect(url).await.unwrap();
        let p = pool.kind().param_prefix().to_string();
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
            let value = TryInto::<i64>::try_into(value).unwrap();
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
        #[cfg(feature = "libsql")]
        float_column_query(":memory:").await;
    }

    async fn float_column_query(url: &str) {
        clear_meta_cache().unwrap();
        let pool = AnyPool::connect(url).await.unwrap();
        let p = pool.kind().param_prefix().to_string();

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
        let value = TryInto::<f64>::try_into(value).unwrap();
        assert_eq!("1.05", format!("{value:.2}"));

        let float = pool.query_f64(&select_sql, &[1.0_f64]).await.unwrap();
        assert_eq!(1.05, float);

        let string = pool.query_string(&select_sql, &[1.0_f64]).await.unwrap();
        assert_eq!("1.05", string);

        let strings = pool.query_strings(&select_sql, &[1.0_f64]).await.unwrap();
        assert_eq!(vec!["1.05".to_owned()], strings);

        let row = pool.query_row(&select_sql, &[1.0_f64]).await.unwrap();
        assert_eq!(row, db_row! {"value".into() => ParamValue::from(1.05)});

        let rows: Vec<DbRow> = pool.query(&select_sql, &[1.0_f64]).await.unwrap();
        assert_eq!(rows, [db_row! {"value".into() => ParamValue::from(1.05)}]);

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
        let value = TryInto::<f32>::try_into(value).unwrap();
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
        #[cfg(feature = "libsql")]
        mixed_column_query(":memory:").await;
    }

    async fn mixed_column_query(url: &str) {
        clear_meta_cache().unwrap();
        let pool = AnyPool::connect(url).await.unwrap();
        let p = pool.kind().param_prefix().to_string();
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
            db_row! {
                "text_value".into() => ParamValue::from("foo"),
                "alt_text_value".into() => ParamValue::Null,
                "float_value".into() => ParamValue::from(1.05),
                "alt_float_value".into() => ParamValue::Null,
                "int_value".into() => ParamValue::from(1_i64),
                "alt_int_value".into() => ParamValue::Null,
                "bool_value".into() => match pool.kind() {
                    DbKind::SQLite => ParamValue::from(1_i64),
                    DbKind::PostgreSQL => ParamValue::from(true),
                },
                "alt_bool_value".into() => ParamValue::Null,
                "numeric_value".into() => ParamValue::from(1_i64),
                "alt_numeric_value".into() => ParamValue::Null,
            }
        );

        let rows: Vec<DbRow> = pool.query(&select_sql, params.clone()).await.unwrap();
        assert_eq!(
            rows,
            [db_row! {
                "text_value".into() => ParamValue::from("foo"),
                "alt_text_value".into() => ParamValue::Null,
                "float_value".into() => ParamValue::from(1.05),
                "alt_float_value".into() => ParamValue::Null,
                "int_value".into() => ParamValue::from(1_i64),
                "alt_int_value".into() => ParamValue::Null,
                "bool_value".into() => match pool.kind() {
                    DbKind::SQLite => ParamValue::from(1_i64),
                    DbKind::PostgreSQL => ParamValue::from(true),
                },
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
        #[cfg(feature = "libsql")]
        input_params(":memory:").await;
    }

    async fn input_params(url: &str) {
        clear_meta_cache().unwrap();
        let pool = AnyPool::connect(url).await.unwrap();
        let p = pool.kind().param_prefix().to_string();
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
        #[cfg(feature = "libsql")]
        insert(":memory:").await;
    }

    async fn insert(url: &str) {
        clear_meta_cache().unwrap();
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
                &db_row! {"text_value".into() => ParamValue::from("TEXT")},
                &db_row! {
                    "int_value".into() => ParamValue::from(1_i64),
                    "bool_value".into() => match pool.kind() {
                        DbKind::SQLite => ParamValue::from(1_i64),
                        DbKind::PostgreSQL => ParamValue::from(true),
                    },
                },
            ],
        )
        .await
        .unwrap();

        // Validate the inserted data:
        let rows: Vec<DbRow> = pool
            .query(r#"SELECT * FROM test_insert"#, ())
            .await
            .unwrap();
        assert_eq!(
            rows,
            [
                db_row! {
                    "text_value".into() => ParamValue::from("TEXT"),
                    "alt_text_value".into() => ParamValue::Null,
                    "float_value".into() => ParamValue::Null,
                    "int_value".into() => ParamValue::Null,
                    "bool_value".into() => ParamValue::Null,
                },
                db_row! {
                    "text_value".into() => ParamValue::Null,
                    "alt_text_value".into() => ParamValue::Null,
                    "float_value".into() => ParamValue::Null,
                    "int_value".into() => ParamValue::from(1_i64),
                    "bool_value".into() => match pool.kind() {
                        DbKind::SQLite => ParamValue::from(1_i64),
                        DbKind::PostgreSQL => ParamValue::from(true),
                    },
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
        #[cfg(feature = "libsql")]
        insert_returning(":memory:").await;
    }

    async fn insert_returning(url: &str) {
        clear_meta_cache().unwrap();
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
        let rows: Vec<DbRow> = pool
            .insert_returning(
                "test_insert_returning",
                &["text_value", "int_value", "bool_value"],
                &[
                    &db_row! {"text_value".into() => ParamValue::from("TEXT")},
                    &db_row! {
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
                db_row! {
                    "text_value".into() => ParamValue::from("TEXT"),
                    "alt_text_value".into() => ParamValue::Null,
                    "float_value".into() => ParamValue::Null,
                    "int_value".into() => ParamValue::Null,
                    "bool_value".into() => ParamValue::Null,
                },
                db_row! {
                    "text_value".into() => ParamValue::Null,
                    "alt_text_value".into() => ParamValue::Null,
                    "float_value".into() => ParamValue::Null,
                    "int_value".into() => ParamValue::from(1_i64),
                    "bool_value".into() => match pool.kind() {
                        DbKind::SQLite => ParamValue::from(1_i64),
                        DbKind::PostgreSQL => ParamValue::from(true),
                    }
                }
            ]
        );

        // With specific returning columns:
        let rows: Vec<DbRow> = pool
            .insert_returning(
                "test_insert_returning",
                &["text_value", "int_value", "bool_value"],
                &[
                    &db_row! {"text_value".into() => ParamValue::from("TEXT")},
                    &db_row! {
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
                db_row! {
                    "float_value".into() => ParamValue::Null,
                    "int_value".into() => ParamValue::Null,
                },
                db_row! {
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
        #[cfg(feature = "libsql")]
        drop_table(":memory:").await;
    }

    async fn drop_table(url: &str) {
        clear_meta_cache().unwrap();
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
        #[cfg(feature = "libsql")]
        primary_keys(":memory:").await;
    }

    async fn primary_keys(url: &str) {
        clear_meta_cache().unwrap();
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
        #[cfg(feature = "libsql")]
        update(":memory:").await;
    }

    async fn update(url: &str) {
        clear_meta_cache().unwrap();
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
                &db_row! {"foo".into() => ParamValue::from(1_i64)},
                &db_row! {"foo".into() => ParamValue::from(2_i64)},
                &db_row! {"foo".into() => ParamValue::from(3_i64)},
            ],
        )
        .await
        .unwrap();

        pool.update(
            "test_update",
            &["foo", "bar", "car", "dar", "ear"],
            &[
                &db_row! {
                    "foo".into() => ParamValue::from(1_i64),
                    "bar".into() => ParamValue::from(10_i64),
                    "car".into() => ParamValue::from(11_i64),
                    "dar".into() => ParamValue::from(12_i64),
                    "ear".into() => ParamValue::from(13_i64),
                },
                &db_row! {
                    "foo".into() => ParamValue::from(2_i64),
                    "bar".into() => ParamValue::from(14_i64),
                    "car".into() => ParamValue::from(15_i64),
                    "dar".into() => ParamValue::from(16_i64),
                    "ear".into() => ParamValue::from(17_i64),
                },
                &db_row! {
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

        let rows: Vec<DbRow> = pool.query("SELECT * from test_update", ()).await.unwrap();
        assert_eq!(
            rows,
            [
                db_row! {
                    "foo".into() => ParamValue::from(1_i64),
                    "bar".into() => ParamValue::from(10_i64),
                    "car".into() => ParamValue::from(11_i64),
                    "dar".into() => ParamValue::from(12_i64),
                    "ear".into() => ParamValue::from(13_i64),
                },
                db_row! {
                    "foo".into() => ParamValue::from(2_i64),
                    "bar".into() => ParamValue::from(14_i64),
                    "car".into() => ParamValue::from(15_i64),
                    "dar".into() => ParamValue::from(16_i64),
                    "ear".into() => ParamValue::from(17_i64),
                },
                db_row! {
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
        #[cfg(feature = "libsql")]
        update_returning(":memory:").await;
    }

    async fn update_returning(url: &str) {
        clear_meta_cache().unwrap();
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
                &db_row! {
                    "foo".into() => ParamValue::from(1_i64),
                    "bar".into() => ParamValue::from(1_i64)
                },
                &db_row! {
                    "foo".into() => ParamValue::from(2_i64),
                    "bar".into() => ParamValue::from(2_i64),
                },
                &db_row! {
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
                    db_row! {
                        "car".into() => ParamValue::from(10_i64),
                        "dar".into() => ParamValue::from(11_i64),
                        "ear".into() => ParamValue::from(12_i64),
                    },
                    db_row! {
                        "car".into() => ParamValue::from(13_i64),
                        "dar".into() => ParamValue::from(14_i64),
                        "ear".into() => ParamValue::from(15_i64),
                    },
                    db_row! {
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
                        &db_row! {
                            "foo".into() => ParamValue::from(1_i64),
                            "bar".into() => ParamValue::from(1_i64),
                            "car".into() => ParamValue::from(10_i64),
                            "dar".into() => ParamValue::from(11_i64),
                            "ear".into() => ParamValue::from(12_i64),
                        },
                        &db_row! {
                            "foo".into() => ParamValue::from(2_i64),
                            "bar".into() => ParamValue::from(2_i64),
                            "car".into() => ParamValue::from(13_i64),
                            "dar".into() => ParamValue::from(14_i64),
                            "ear".into() => ParamValue::from(15_i64),
                        },
                        &db_row! {
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
                &db_row! {
                    "foo".into() => ParamValue::from(1_i64),
                    "bar".into() => ParamValue::from(1_i64),
                },
                &db_row! {
                    "foo".into() => ParamValue::from(2_i64),
                    "bar".into() => ParamValue::from(2_i64),
                },
                &db_row! {
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
                        &db_row! {
                            "ear".into() => ParamValue::from(15_i64),
                            "bar".into() => ParamValue::from(2_i64),
                            "car".into() => ParamValue::from(13_i64),
                            "dar".into() => ParamValue::from(14_i64),
                            "foo".into() => ParamValue::from(2_i64),
                        },
                        &db_row! {
                            "foo".into() => ParamValue::from(1_i64),
                            "car".into() => ParamValue::from(10_i64),
                            "bar".into() => ParamValue::from(1_i64),
                            "ear".into() => ParamValue::from(12_i64),
                            "dar".into() => ParamValue::from(11_i64),
                        },
                        &db_row! {
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
        let rows: Vec<DbRow> = pool
            .query("SELECT * from test_update_returning", ())
            .await
            .unwrap();
        assert!(rows.iter().all(|row| {
            [
                db_row! {
                    "foo".into() => ParamValue::from(1_i64),
                    "bar".into() => ParamValue::from(1_i64),
                    "car".into() => ParamValue::from(10_i64),
                    "dar".into() => ParamValue::from(11_i64),
                    "ear".into() => ParamValue::from(12_i64),
                },
                db_row! {
                    "foo".into() => ParamValue::from(2_i64),
                    "bar".into() => ParamValue::from(2_i64),
                    "car".into() => ParamValue::from(13_i64),
                    "dar".into() => ParamValue::from(14_i64),
                    "ear".into() => ParamValue::from(15_i64),
                },
                db_row! {
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
        #[cfg(feature = "libsql")]
        upsert(":memory:").await;
    }

    async fn upsert(url: &str) {
        clear_meta_cache().unwrap();
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
                &db_row! {"foo".into() => ParamValue::from(1_i64)},
                &db_row! {"foo".into() => ParamValue::from(2_i64)},
                &db_row! {"foo".into() => ParamValue::from(3_i64)},
            ],
        )
        .await
        .unwrap();

        pool.upsert(
            "test_upsert",
            &["foo", "bar", "car", "dar", "ear"],
            &[
                &db_row! {
                    "foo".into() => ParamValue::from(1_i64),
                    "bar".into() => ParamValue::from(10_i64),
                    "car".into() => ParamValue::from(11_i64),
                    "dar".into() => ParamValue::from(12_i64),
                    "ear".into() => ParamValue::from(13_i64),
                },
                &db_row! {
                    "foo".into() => ParamValue::from(2_i64),
                    "bar".into() => ParamValue::from(14_i64),
                    "car".into() => ParamValue::from(15_i64),
                    "dar".into() => ParamValue::from(16_i64),
                    "ear".into() => ParamValue::from(17_i64),
                },
                &db_row! {
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

        let rows: Vec<DbRow> = pool.query("SELECT * from test_upsert", ()).await.unwrap();
        assert_eq!(
            rows,
            [
                db_row! {
                    "foo".into() => ParamValue::from(1_i64),
                    "bar".into() => ParamValue::from(10_i64),
                    "car".into() => ParamValue::from(11_i64),
                    "dar".into() => ParamValue::from(12_i64),
                    "ear".into() => ParamValue::from(13_i64),
                },
                db_row! {
                    "foo".into() => ParamValue::from(2_i64),
                    "bar".into() => ParamValue::from(14_i64),
                    "car".into() => ParamValue::from(15_i64),
                    "dar".into() => ParamValue::from(16_i64),
                    "ear".into() => ParamValue::from(17_i64),
                },
                db_row! {
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
        #[cfg(feature = "libsql")]
        upsert_returning(":memory:").await;
    }

    async fn upsert_returning(url: &str) {
        clear_meta_cache().unwrap();
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
                &db_row! {
                    "foo".into() => ParamValue::from(1_i64),
                    "bar".into() => ParamValue::from(1_i64),
                },
                &db_row! {
                    "foo".into() => ParamValue::from(2_i64),
                    "bar".into() => ParamValue::from(2_i64),
                },
                &db_row! {
                    "foo".into() => ParamValue::from(3_i64),
                    "bar".into() => ParamValue::from(3_i64),
                },
            ],
        )
        .await
        .unwrap();

        let rows: Vec<DbRow> = pool
            .upsert_returning(
                "test_upsert_returning",
                &["foo", "bar", "car", "dar", "ear"],
                &[
                    &db_row! {
                        "foo".into() => ParamValue::from(1_i64),
                        "bar".into() => ParamValue::from(1_i64),
                        "car".into() => ParamValue::from(10_i64),
                        "dar".into() => ParamValue::from(11_i64),
                        "ear".into() => ParamValue::from(12_i64),
                    },
                    &db_row! {
                        "foo".into() => ParamValue::from(2_i64),
                        "bar".into() => ParamValue::from(2_i64),
                        "car".into() => ParamValue::from(13_i64),
                        "dar".into() => ParamValue::from(14_i64),
                        "ear".into() => ParamValue::from(15_i64),
                    },
                    &db_row! {
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
                db_row! {
                    "car".into() => ParamValue::from(10_i64),
                    "dar".into() => ParamValue::from(11_i64),
                    "ear".into() => ParamValue::from(12_i64),
                },
                db_row! {
                    "car".into() => ParamValue::from(13_i64),
                    "dar".into() => ParamValue::from(14_i64),
                    "ear".into() => ParamValue::from(15_i64),
                },
                db_row! {
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

    async fn count_query_cache_rows(pool: &mut AnyPool) -> u64 {
        pool.query_u64(&format!("SELECT COUNT(1) from {QUERY_CACHE_TABLE}"), ())
            .await
            .unwrap()
    }

    async fn count_table_cache_rows(pool: &mut AnyPool) -> u64 {
        pool.query_u64(&format!("SELECT COUNT(1) from {TABLE_CACHE_TABLE}"), ())
            .await
            .unwrap()
    }

    fn count_memory_query_cache_rows() -> u64 {
        let cache = get_memory_query_cache_contents().unwrap();
        cache.keys().len().try_into().unwrap()
    }

    fn count_memory_table_cache_rows() -> u64 {
        let cache = get_memory_table_cache_contents().unwrap();
        cache.keys().len().try_into().unwrap()
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
                table_caching(&mut pool, &caching_strategy).await;
            }
            for strategy in &all_strategies {
                view_caching(&mut pool, strategy).await;
            }
        }
        #[cfg(feature = "tokio-postgres")]
        {
            let mut pool = AnyPool::connect("postgresql:///rltbl_db").await.unwrap();
            for caching_strategy in &all_strategies {
                table_caching(&mut pool, &caching_strategy).await;
            }
            for strategy in &all_strategies {
                view_caching(&mut pool, strategy).await;
            }
        }
        #[cfg(feature = "libsql")]
        {
            let mut pool = AnyPool::connect(":memory:").await.unwrap();
            for caching_strategy in &all_strategies {
                table_caching(&mut pool, &caching_strategy).await;
            }
            for strategy in &all_strategies {
                view_caching(&mut pool, strategy).await;
            }
        }
    }

    async fn table_caching(pool: &mut AnyPool, strategy: &CachingStrategy) {
        clear_meta_cache().unwrap();
        clear_memory_table_cache(&[]).unwrap();
        clear_memory_query_cache(&[]).unwrap();
        pool.drop_table(&format!("{QUERY_CACHE_TABLE}"))
            .await
            .unwrap();
        pool.drop_table(&format!("{TABLE_CACHE_TABLE}"))
            .await
            .unwrap();
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

        pool.set_caching_strategy(strategy);
        pool.set_cache_aware_query(true);

        pool.insert(
            "test_table_caching_1",
            &["value"],
            &[
                &db_row! {
                    "value".into() => ParamValue::from("alpha"),
                },
                &db_row! {
                    "value".into() => ParamValue::from("beta"),
                },
            ],
        )
        .await
        .unwrap();

        let rows: Vec<DbRow> = pool
            .cache(
                &["test_table_caching_1"],
                "SELECT * from test_table_caching_1",
                (),
            )
            .await
            .unwrap();

        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => assert_eq!(count_memory_query_cache_rows(), 1),
            _ => assert_eq!(count_query_cache_rows(pool).await, 1),
        };
        assert_eq!(
            rows,
            vec![
                db_row! {"value".into() => ParamValue::from("alpha")},
                db_row! {"value".into() => ParamValue::from("beta")},
            ]
        );

        let rows: Vec<DbRow> = pool
            .cache(
                &["test_table_caching_1"],
                "SELECT * from test_table_caching_1",
                (),
            )
            .await
            .unwrap();

        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => assert_eq!(count_memory_query_cache_rows(), 1),
            _ => assert_eq!(count_query_cache_rows(pool).await, 1),
        };
        assert_eq!(
            rows,
            vec![
                db_row! {"value".into() => ParamValue::from("alpha")},
                db_row! {"value".into() => ParamValue::from("beta")},
            ]
        );

        pool.insert(
            "test_table_caching_1",
            &["value"],
            &[
                &db_row! {"value".into() => ParamValue::from("gamma")},
                &db_row! {"value".into() => ParamValue::from("delta")},
            ],
        )
        .await
        .unwrap();

        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => assert_eq!(count_memory_query_cache_rows(), 0),
            _ => assert_eq!(count_query_cache_rows(pool).await, 0),
        };

        let rows: Vec<DbRow> = pool
            .cache(
                &["test_table_caching_1"],
                "SELECT * from test_table_caching_1",
                (),
            )
            .await
            .unwrap();

        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => assert_eq!(count_memory_query_cache_rows(), 1),
            _ => assert_eq!(count_query_cache_rows(pool).await, 1),
        };
        assert_eq!(
            rows,
            vec![
                db_row! {"value".into() => ParamValue::from("alpha")},
                db_row! {"value".into() => ParamValue::from("beta")},
                db_row! {"value".into() => ParamValue::from("gamma")},
                db_row! {"value".into() => ParamValue::from("delta")},
            ]
        );

        let rows: Vec<DbRow> = pool
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
                db_row! {"value".into() => ParamValue::from("alpha")},
                db_row! {"value".into() => ParamValue::from("beta")},
                db_row! {"value".into() => ParamValue::from("gamma")},
                db_row! {"value".into() => ParamValue::from("delta")},
            ]
        );

        let _: Vec<DbRow> = pool
            .cache(
                &["test_table_caching_1"],
                "SELECT COUNT(1) FROM test_table_caching_1",
                (),
            )
            .await
            .unwrap();

        let _: Vec<DbRow> = pool
            .cache(
                &["test_table_caching_2"],
                "SELECT COUNT(1) FROM test_table_caching_2",
                (),
            )
            .await
            .unwrap();

        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => assert_eq!(count_memory_query_cache_rows(), 3),
            _ => assert_eq!(count_query_cache_rows(pool).await, 3),
        };

        pool.execute(
            r#"INSERT INTO test_table_caching_1 VALUES ('rho'), ('sigma')"#,
            (),
        )
        .await
        .unwrap();

        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => assert_eq!(count_memory_query_cache_rows(), 1),
            CachingStrategy::Truncate | CachingStrategy::Trigger => {
                assert_eq!(count_query_cache_rows(pool).await, 1)
            }
            CachingStrategy::TruncateAll => assert_eq!(count_query_cache_rows(pool).await, 0),
        };

        let rows: Vec<DbRow> = pool
            .cache(
                &["test_table_caching_1"],
                "SELECT * from test_table_caching_1",
                (),
            )
            .await
            .unwrap();

        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => assert_eq!(count_memory_query_cache_rows(), 2),
            CachingStrategy::Truncate | CachingStrategy::Trigger => {
                assert_eq!(count_query_cache_rows(pool).await, 2)
            }
            CachingStrategy::TruncateAll => assert_eq!(count_query_cache_rows(pool).await, 1),
        };
        assert_eq!(
            rows,
            vec![
                db_row! {"value".into() => ParamValue::from("alpha")},
                db_row! {"value".into() => ParamValue::from("beta")},
                db_row! {"value".into() => ParamValue::from("gamma")},
                db_row! {"value".into() => ParamValue::from("delta")},
                db_row! {"value".into() => ParamValue::from("rho")},
                db_row! {"value".into() => ParamValue::from("sigma")},
            ]
        );

        let rows: Vec<DbRow> = pool
            .cache(
                &["test_table_caching_1", "test_table_caching_2"],
                "SELECT * FROM test_table_caching_1 t1, test_table_caching_2 t2 \
                 WHERE t1.value = t2.value",
                (),
            )
            .await
            .unwrap();
        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => assert_eq!(count_memory_query_cache_rows(), 3),
            CachingStrategy::Truncate | CachingStrategy::Trigger => {
                assert_eq!(count_query_cache_rows(pool).await, 3)
            }
            CachingStrategy::TruncateAll => assert_eq!(count_query_cache_rows(pool).await, 2),
        };
        assert_eq!(rows.len(), 0);

        let rows: Vec<DbRow> = pool
            .cache(
                &["test_table_caching_1", "test_table_caching_2"],
                "SELECT * FROM test_table_caching_1 t1, test_table_caching_2 t2 \
                 WHERE t1.value = t2.value",
                (),
            )
            .await
            .unwrap();
        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => assert_eq!(count_memory_query_cache_rows(), 3),
            CachingStrategy::Truncate | CachingStrategy::Trigger => {
                assert_eq!(count_query_cache_rows(pool).await, 3)
            }
            CachingStrategy::TruncateAll => assert_eq!(count_query_cache_rows(pool).await, 2),
        };
        assert_eq!(rows.len(), 0);

        // Cleanup:
        pool.drop_table("test_table_caching_1").await.unwrap();
        pool.drop_table("test_table_caching_2").await.unwrap();
    }

    async fn view_caching(pool: &mut AnyPool, strategy: &CachingStrategy) {
        clear_meta_cache().unwrap();
        clear_memory_table_cache(&[]).unwrap();
        clear_memory_query_cache(&[]).unwrap();
        pool.drop_table(&format!("{QUERY_CACHE_TABLE}"))
            .await
            .unwrap();
        pool.drop_table(&format!("{TABLE_CACHE_TABLE}"))
            .await
            .unwrap();
        pool.drop_table(&format!("test_vcaching_table"))
            .await
            .unwrap();
        pool.drop_view(&format!("test_vcaching_view_1"))
            .await
            .unwrap();
        pool.drop_view(&format!("test_vcaching_view_2"))
            .await
            .unwrap();

        pool.execute_no_cache(
            "CREATE TABLE test_vcaching_table ( \
               foo BIGINT, \
               bar BIGINT, \
               PRIMARY KEY (foo) \
             )",
            (),
        )
        .await
        .unwrap();
        pool.execute_no_cache("INSERT INTO test_vcaching_table VALUES (1, 1000)", ())
            .await
            .unwrap();
        pool.execute_no_cache(
            "CREATE VIEW test_vcaching_view_1 AS \
             SELECT bar \
             FROM test_vcaching_table",
            (),
        )
        .await
        .unwrap();

        pool.execute_no_cache(
            "CREATE VIEW test_vcaching_view_2 AS \
             SELECT bar \
             FROM test_vcaching_table",
            (),
        )
        .await
        .unwrap();

        pool.set_caching_strategy(strategy);
        pool.set_cache_aware_query(true);

        let _: Vec<DbRow> = pool
            .cache(
                &["test_vcaching_view_1"],
                "SELECT * FROM test_vcaching_view_1",
                (),
            )
            .await
            .unwrap();

        match strategy {
            CachingStrategy::None => unimplemented!(),
            CachingStrategy::Memory(_) => assert_eq!(count_memory_query_cache_rows(), 1),
            _ => assert_eq!(count_query_cache_rows(pool).await, 1),
        };
        match strategy {
            CachingStrategy::None => unimplemented!(),
            CachingStrategy::Memory(_) => assert_eq!(count_memory_table_cache_rows(), 0),
            _ => assert_eq!(count_table_cache_rows(pool).await, 0),
        };

        pool.insert(
            "test_vcaching_table",
            &["foo", "bar"],
            &[&db_row! {
                "foo".into() => ParamValue::from(2_u64),
                "bar".into() => ParamValue::from(2_u64),
            }],
        )
        .await
        .unwrap();

        match strategy {
            CachingStrategy::None => unimplemented!(),
            CachingStrategy::Memory(_) => assert_eq!(count_memory_query_cache_rows(), 1),
            // Truncate and trigger give different answers here because the query cache is cleaned
            // for views at different times according to each strategy. The query cache is cleaned
            // immediately after an edit in the case of the trigger option, while when using the
            // truncate option, the cache is only cleaned at the next view access.
            CachingStrategy::Truncate => assert_eq!(count_query_cache_rows(pool).await, 1),
            _ => assert_eq!(count_query_cache_rows(pool).await, 0),
        };

        match strategy {
            CachingStrategy::None => unimplemented!(),
            CachingStrategy::Memory(_) => assert_eq!(count_memory_table_cache_rows(), 1),
            _ => assert_eq!(count_table_cache_rows(pool).await, 1),
        };

        let _: Vec<DbRow> = pool
            .cache(
                &["test_vcaching_view_1"],
                "SELECT * FROM test_vcaching_view_1",
                (),
            )
            .await
            .unwrap();

        match strategy {
            CachingStrategy::None => unimplemented!(),
            CachingStrategy::Memory(_) => assert_eq!(count_memory_query_cache_rows(), 1),
            _ => assert_eq!(count_query_cache_rows(pool).await, 1),
        };

        let _: Vec<DbRow> = pool
            .cache(
                &["test_vcaching_view_2"],
                "SELECT * FROM test_vcaching_view_2",
                (),
            )
            .await
            .unwrap();

        match strategy {
            CachingStrategy::None => unimplemented!(),
            CachingStrategy::Memory(_) => assert_eq!(count_memory_query_cache_rows(), 2),
            _ => assert_eq!(count_query_cache_rows(pool).await, 2),
        };

        let _: Vec<DbRow> = pool
            .cache(
                &["test_vcaching_table"],
                "SELECT * FROM test_vcaching_table",
                (),
            )
            .await
            .unwrap();

        match strategy {
            CachingStrategy::None => unimplemented!(),
            CachingStrategy::Memory(_) => assert_eq!(count_memory_query_cache_rows(), 3),
            _ => assert_eq!(count_query_cache_rows(pool).await, 3),
        };

        pool.insert(
            "test_vcaching_table",
            &["foo", "bar"],
            &[&db_row! {
                "foo".into() => ParamValue::from(27_u64),
                "bar".into() => ParamValue::from(27_u64),
            }],
        )
        .await
        .unwrap();

        match strategy {
            CachingStrategy::None => unimplemented!(),
            CachingStrategy::Memory(_) => assert_eq!(count_memory_query_cache_rows(), 2),
            // Truncate and trigger give different answers here because the query cache is cleaned
            // for views at different times according to each strategy. The query cache is cleaned
            // immediately after an edit in the case of the trigger option, while when using the
            // truncate option, the cache is only cleaned at the next view access. The reason
            // there are two rows and not three below is because an edit of a table (as opposed
            // to a view) triggers a cleaning of cache entries for itself in the query cache
            // automatically.
            CachingStrategy::Truncate => assert_eq!(count_query_cache_rows(pool).await, 2),
            _ => assert_eq!(count_query_cache_rows(pool).await, 0),
        };

        // Cleanup:
        pool.drop_table(&format!("test_vcaching_table"))
            .await
            .unwrap();
        pool.drop_view(&format!("test_vcaching_view_1"))
            .await
            .unwrap();
        pool.drop_view(&format!("test_vcaching_view_2"))
            .await
            .unwrap();
    }

    // This test takes a few minutes to run and is ignored by default.
    // Use `cargo test -- --ignored` or `cargo test -- --include-ignored` to run it.
    #[tokio::test]
    #[ignore]
    async fn test_caching_performance() {
        let runs = 2500;
        let edit_rate = 25;
        let timeout = 37;
        #[cfg(feature = "rusqlite")]
        perform_caching(":memory:", runs, edit_rate, timeout).await;
        #[cfg(feature = "tokio-postgres")]
        perform_caching("postgresql:///rltbl_db", runs, edit_rate, timeout).await;
        #[cfg(feature = "libsql")]
        perform_caching(":memory:", runs, edit_rate, timeout).await;
    }

    // Performs the caching performance test on the database located at the given url, using
    // the given number of runs and edit rate. The latter represents the rate at which the
    // tables in the simulation are edited (e.g., a value of 25 means that a table will be edited
    // in one out every 25th run, on average), which causes the cache to become out of date and
    // require maintenance in accordance with the current caching strategy. The test is run
    // for the given number of runs for each of the supported caching strategies. The running
    // time for each strategy is then summarized and reported via STDOUT.
    async fn perform_caching(url: &str, runs: usize, edit_rate: usize, fail_after: usize) {
        clear_meta_cache().unwrap();
        let mut pool = AnyPool::connect(url).await.unwrap();
        let all_strategies = ["none", "truncate_all", "truncate", "trigger", "memory:1000"]
            .iter()
            .map(|strategy| CachingStrategy::from_str(strategy).unwrap())
            .collect::<Vec<_>>();

        pool.set_cache_aware_query(true);
        let this_test = "Caching Performance Test -";
        println!(
            "{this_test} Starting test for {} connection '{}' with cache_aware_query {}.",
            pool.kind(),
            url,
            match pool.get_cache_aware_query() {
                true => "on",
                false => "off",
            }
        );
        let mut times = BTreeMap::new();
        let mut elapsed_none: u64 = 0;
        for strategy in &all_strategies {
            println!("{this_test} Using strategy: {strategy}.");
            pool.set_caching_strategy(&strategy);
            let elapsed: u64 = perform_caching_detail(&pool, runs, fail_after, edit_rate).await;
            times.insert(format!("{strategy}"), elapsed);
            if *strategy == CachingStrategy::None {
                elapsed_none = elapsed;
            } else {
                // The elapsed time for strategy 'none' should always be greater than for the
                // other caching strategies. Note that it is assumed that the None strategy
                // is always tested before any of the other strategies (otherwise this assertion
                // is certain to fail).
                assert!(elapsed_none > elapsed);
            }
        }

        println!("{this_test} Elapsed times for {} (summary):", pool.kind());
        for (strategy, elapsed) in times.iter() {
            println!("  Strategy: {strategy}, elapsed time: {elapsed}s");
        }
    }

    async fn perform_caching_detail(
        pool: &AnyPool,
        runs: usize,
        fail_after: usize,
        edit_rate: usize,
    ) -> u64 {
        fn random_between(min: usize, max: usize, seed: &mut i64) -> usize {
            let between = Uniform::try_from(min..max).unwrap();
            let mut rng = if *seed < 0 {
                StdRng::from_rng(&mut rand::rng())
            } else {
                *seed += 10;
                StdRng::seed_from_u64(*seed as u64)
            };
            between.sample(&mut rng)
        }

        fn random_table<'a>(tables_to_choose_from: &'a Vec<&str>) -> &'a str {
            match random_between(0, 4, &mut -1) {
                0 => tables_to_choose_from[0],
                1 => tables_to_choose_from[1],
                2 => tables_to_choose_from[2],
                3 => tables_to_choose_from[3],
                _ => unreachable!(),
            }
        }

        let tables_to_choose_from = vec!["alpha", "beta", "gamma", "delta"];
        for table in tables_to_choose_from.iter() {
            pool.drop_table(table).await.unwrap();
            pool.drop_view(&format!("{table}_view")).await.unwrap();
            pool.execute(&format!("CREATE TABLE {table} ( foo INT, bar INT )"), ())
                .await
                .unwrap();
            pool.execute(
                &format!("CREATE VIEW {table}_view AS SELECT * FROM {table}"),
                (),
            )
            .await
            .unwrap();

            // Add a few thousand values to the table:
            let mut values = vec![];
            for i in 0..5 {
                for j in 0..random_between(2000, 4000, &mut -1) {
                    values.push(format!("({i}, {j})"));
                }
            }
            let values = values.join(", ");
            pool.execute(
                &format!("INSERT INTO {table} (foo, bar) VALUES {}", values),
                (),
            )
            .await
            .unwrap();
        }

        let now = Instant::now();
        let mut i = 0;
        let mut elapsed;
        let mut actual_edits = 0;
        while i < runs {
            let select_table = random_table(&tables_to_choose_from);
            let _: Vec<DbRow> = pool
                .cache(
                    &[select_table],
                    &format!(
                        "SELECT foo, SUM(bar) FROM {select_table}_view GROUP BY foo ORDER BY foo"
                    ),
                    (),
                )
                .await
                .unwrap();
            elapsed = now.elapsed().as_secs();
            if elapsed > fail_after as u64 {
                panic!("Taking longer than {fail_after}s. Timing out.");
            }
            if edit_rate != 0 && random_between(0, edit_rate, &mut -1) == 0 {
                actual_edits += 1;
                let table_to_edit = random_table(&tables_to_choose_from);
                pool.execute(
                    &format!("INSERT INTO {table_to_edit} (foo) VALUES (1), (1)"),
                    (),
                )
                .await
                .unwrap();
            }

            // A small sleep to prevent over-taxing the CPU:
            thread::sleep(Duration::from_millis(5));
            i += 1;
        }
        elapsed = now.elapsed().as_secs();
        println!(
            "Caching Performance Test - Elapsed time for strategy {}: {elapsed}s \
             ({actual_edits} edits in {runs} runs)",
            pool.get_caching_strategy()
        );
        for table in tables_to_choose_from.iter() {
            pool.drop_table(table).await.unwrap();
        }
        elapsed
    }
}
