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

#[cfg(feature = "sqlx")]
use crate::sqlx::SqlxPool;

#[derive(Debug)]
pub enum AnyPool {
    #[cfg(feature = "rusqlite")]
    Rusqlite(RusqlitePool),
    #[cfg(feature = "tokio-postgres")]
    TokioPostgres(TokioPostgresPool),
    #[cfg(feature = "libsql")]
    LibSQL(LibSQLPool),
    #[cfg(feature = "sqlx")]
    Sqlx(SqlxPool),
}

impl AnyPool {
    /// Get the DbKind for this connection URL.
    pub fn connection_kind(url: &str) -> Result<DbKind, DbError> {
        if url.starts_with("postgresql://") {
            #[cfg(feature = "tokio-postgres")]
            {
                return Ok(DbKind::PostgreSQL);
            }
            #[cfg(not(feature = "tokio-postgres"))]
            {
                #[cfg(feature = "sqlx")]
                {
                    return Ok(DbKind::PostgreSQL);
                }
                #[cfg(not(feature = "tokio-postgres"))]
                {
                    return Err(DbError::ConnectError(
                        "PostgreSQL not configured".to_string(),
                    ));
                }
            }
        } else {
            #[cfg(feature = "rusqlite")]
            {
                return Ok(DbKind::SQLite);
            }
            #[cfg(not(feature = "rusqlite"))]
            {
                #[cfg(feature = "libsql")]
                {
                    return Ok(DbKind::SQLite);
                }
                #[cfg(not(feature = "libsql"))]
                {
                    #[cfg(feature = "sqlx")]
                    {
                        return Ok(DbKind::SQLite);
                    }
                    #[cfg(not(feature = "sqlx"))]
                    {
                        return Err(DbError::ConnectError("SQLite not configured".to_string()));
                    }
                }
            }
        }
    }

    /// Connect to the database located at the given URL.
    pub async fn connect(url: &str) -> Result<Self, DbError> {
        if url.starts_with("postgresql://") {
            #[cfg(feature = "tokio-postgres")]
            {
                return Ok(AnyPool::TokioPostgres(
                    TokioPostgresPool::connect(url).await?,
                ));
            }
            #[cfg(not(feature = "tokio-postgres"))]
            {
                #[cfg(feature = "sqlx")]
                {
                    return Ok(AnyPool::Sqlx(SqlxPool::connect(url).await?));
                }
                #[cfg(not(feature = "tokio-postgres"))]
                {
                    return Err(DbError::ConnectError(
                        "PostgreSQL not configured".to_string(),
                    ));
                }
            }
        } else {
            #[cfg(feature = "rusqlite")]
            {
                return Ok(AnyPool::Rusqlite(RusqlitePool::connect(url).await?));
            }
            #[cfg(not(feature = "rusqlite"))]
            {
                #[cfg(feature = "libsql")]
                {
                    return Ok(AnyPool::LibSQL(LibSQLPool::connect(url).await?));
                }
                #[cfg(not(feature = "libsql"))]
                {
                    #[cfg(feature = "sqlx")]
                    {
                        return Ok(AnyPool::Sqlx(SqlxPool::connect(url).await?));
                    }
                    #[cfg(not(feature = "sqlx"))]
                    {
                        return Err(DbError::ConnectError("SQLite not configured".to_string()));
                    }
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
            #[cfg(feature = "sqlx")]
            AnyPool::Sqlx(pool) => pool.kind(),
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
            #[cfg(feature = "sqlx")]
            AnyPool::Sqlx(pool) => pool.set_caching_strategy(strategy),
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
            #[cfg(feature = "sqlx")]
            AnyPool::Sqlx(pool) => pool.get_caching_strategy(),
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
            #[cfg(feature = "sqlx")]
            AnyPool::Sqlx(pool) => pool.set_cache_aware_query(value),
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
            #[cfg(feature = "sqlx")]
            AnyPool::Sqlx(pool) => pool.get_cache_aware_query(),
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
            #[cfg(feature = "sqlx")]
            AnyPool::Sqlx(pool) => pool.execute_batch(sql).await,
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
            #[cfg(feature = "sqlx")]
            AnyPool::Sqlx(pool) => pool.query_no_cache(sql, params).await,
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
            #[cfg(feature = "sqlx")]
            AnyPool::Sqlx(pool) => pool.insert(table, columns, rows).await,
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
            #[cfg(feature = "sqlx")]
            AnyPool::Sqlx(pool) => pool.insert_returning(table, columns, rows, returning).await,
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
            #[cfg(feature = "sqlx")]
            AnyPool::Sqlx(pool) => pool.update(table, columns, rows).await,
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
            #[cfg(feature = "sqlx")]
            AnyPool::Sqlx(pool) => pool.update_returning(table, columns, rows, returning).await,
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
            #[cfg(feature = "sqlx")]
            AnyPool::Sqlx(pool) => pool.upsert(table, columns, rows).await,
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
            #[cfg(feature = "sqlx")]
            AnyPool::Sqlx(pool) => pool.upsert_returning(table, columns, rows, returning).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        core::{
            CachingStrategy, ColumnMap, DbRow, ParamValue, StringRow, get_memory_cache_contents,
        },
        params,
    };
    use indexmap::indexmap as db_row;
    use rand::{
        Rng, SeedableRng as _,
        distr::{Alphanumeric, Distribution as _, Uniform},
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
        #[cfg(feature = "sqlx")]
        {
            text_column_query(":memory:").await;
            text_column_query("postgresql:///rltbl_db").await;
        }
    }

    async fn text_column_query(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let p = pool.kind().param_prefix().to_string();
        let table_name = format!("test_table_text");

        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS {table_name}{cascade};\
             CREATE TABLE {table_name} ( value TEXT )",
            cascade = match pool.kind() {
                DbKind::PostgreSQL => " CASCADE",
                DbKind::SQLite => "",
            }
        ))
        .await
        .unwrap();
        pool.execute(&format!("INSERT INTO {table_name} VALUES ({p}1)"), &["foo"])
            .await
            .unwrap();
        let select_sql = format!("SELECT value FROM {table_name} WHERE value = {p}1");
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
        pool.drop_table(&format!("{table_name}")).await.unwrap();
    }

    #[tokio::test]
    async fn test_integer_column_query() {
        #[cfg(feature = "rusqlite")]
        integer_column_query(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        integer_column_query("postgresql:///rltbl_db").await;
        #[cfg(feature = "libsql")]
        integer_column_query(":memory:").await;
        #[cfg(feature = "sqlx")]
        {
            text_column_query(":memory:").await;
            text_column_query("postgresql:///rltbl_db").await;
        }
    }

    async fn integer_column_query(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let p = pool.kind().param_prefix().to_string();
        let table_name = format!("test_table_int");
        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS {table_name}{cascade};\
             CREATE TABLE {table_name} ( value_2 INT2, value_4 INT4, value_8 INT8 )",
            cascade = match pool.kind() {
                DbKind::PostgreSQL => " CASCADE",
                DbKind::SQLite => "",
            }
        ))
        .await
        .unwrap();

        pool.execute(
            &format!("INSERT INTO {table_name} VALUES ({p}1, {p}2, {p}3)"),
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
            let select_sql = format!("SELECT {column} FROM {table_name} WHERE {column} = {p}1");
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
        pool.drop_table(&format!("{table_name}")).await.unwrap();
    }

    #[tokio::test]
    async fn test_float_column_query() {
        #[cfg(feature = "rusqlite")]
        float_column_query(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        float_column_query("postgresql:///rltbl_db").await;
        #[cfg(feature = "libsql")]
        integer_column_query(":memory:").await;
        #[cfg(feature = "sqlx")]
        {
            text_column_query(":memory:").await;
            text_column_query("postgresql:///rltbl_db").await;
        }
    }

    async fn float_column_query(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let p = pool.kind().param_prefix().to_string();
        let table_name = format!("test_table_float");

        // FLOAT8
        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS {table_name}{cascade};\
             CREATE TABLE {table_name} ( value FLOAT8 )",
            cascade = match pool.kind() {
                DbKind::PostgreSQL => " CASCADE",
                DbKind::SQLite => "",
            }
        ))
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO {table_name} VALUES ({p}1)"),
            &[1.05_f64],
        )
        .await
        .unwrap();
        let select_sql = format!("SELECT value FROM {table_name} WHERE value > {p}1");
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
            "DROP TABLE IF EXISTS {table_name}{cascade};\
             CREATE TABLE {table_name} ( value FLOAT4 )",
            cascade = match pool.kind() {
                DbKind::PostgreSQL => " CASCADE",
                DbKind::SQLite => "",
            }
        ))
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO {table_name} VALUES ({p}1)"),
            &[1.05_f32],
        )
        .await
        .unwrap();
        let select_sql = format!("SELECT value FROM {table_name} WHERE value > {p}1");
        let value = pool.query_value(&select_sql, &[1.0_f32]).await.unwrap();
        let value = TryInto::<f32>::try_into(value).unwrap();
        assert_eq!("1.05", format!("{value:.2}"));

        // Clean up:
        pool.drop_table(&format!("{table_name}")).await.unwrap();
    }

    #[tokio::test]
    async fn test_mixed_column_query() {
        #[cfg(feature = "rusqlite")]
        mixed_column_query(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        mixed_column_query("postgresql:///rltbl_db").await;
        #[cfg(feature = "libsql")]
        integer_column_query(":memory:").await;
        #[cfg(feature = "sqlx")]
        {
            text_column_query(":memory:").await;
            text_column_query("postgresql:///rltbl_db").await;
        }
    }

    async fn mixed_column_query(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let p = pool.kind().param_prefix().to_string();
        let table_name = format!("test_table_mixed");
        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS {table_name}{cascade};\
             CREATE TABLE {table_name} (\
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
                r#"INSERT INTO {table_name}
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

        let select_sql = format!("SELECT text_value FROM {table_name} WHERE text_value = {p}1");
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
               FROM {table_name}
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
                    // libsql does not support booleans:
                    // https://docs.rs/libsql/0.9.29/libsql/enum.Value.html,
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
                    // libsql does not support booleans:
                    // https://docs.rs/libsql/0.9.29/libsql/enum.Value.html,
                    DbKind::SQLite => ParamValue::from(1_i64),
                    DbKind::PostgreSQL => ParamValue::from(true),
                },
                "alt_bool_value".into() => ParamValue::Null,
                "numeric_value".into() => ParamValue::from(1_i64),
                "alt_numeric_value".into() => ParamValue::Null,
            }]
        );

        // Clean up:
        pool.drop_table(&format!("{table_name}")).await.unwrap();
    }

    #[tokio::test]
    async fn test_input_params() {
        #[cfg(feature = "rusqlite")]
        input_params(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        input_params("postgresql:///rltbl_db").await;
        #[cfg(feature = "libsql")]
        integer_column_query(":memory:").await;
        #[cfg(feature = "sqlx")]
        {
            text_column_query(":memory:").await;
            text_column_query("postgresql:///rltbl_db").await;
        }
    }

    async fn input_params(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let p = pool.kind().param_prefix().to_string();
        let cascade = match pool.kind() {
            DbKind::PostgreSQL => " CASCADE",
            DbKind::SQLite => "",
        };
        let table_name = format!("test_input_params");

        pool.execute(&format!("DROP TABLE IF EXISTS {table_name}{cascade}"), ())
            .await
            .unwrap();
        pool.execute(
            &format!(
                "CREATE TABLE {table_name} (\
               bar TEXT,\
               car INT2,\
               dar INT4,\
               far INT8,\
               gar FLOAT4,\
               har FLOAT8,\
               jar NUMERIC,\
               kar BOOL
             )"
            ),
            (),
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO {table_name} (bar) VALUES ({p}1)"),
            &["one"],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO {table_name} (far) VALUES ({p}1)"),
            &[1 as i64],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO {table_name} (bar) VALUES ({p}1)"),
            ["two"],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO {table_name} (far) VALUES ({p}1)"),
            [2 as i64],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO {table_name} (bar) VALUES ({p}1)"),
            vec!["three"],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO {table_name} (far) VALUES ({p}1)"),
            vec![3 as i64],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO {table_name} (gar) VALUES ({p}1)"),
            vec![3 as f32],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO {table_name} (har) VALUES ({p}1)"),
            vec![3 as f64],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO {table_name} (jar) VALUES ({p}1)"),
            vec![dec!(3)],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO {table_name} (kar) VALUES ({p}1)"),
            vec![true],
        )
        .await
        .unwrap();
        pool.execute(
            &format!(
                "INSERT INTO {table_name} \
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
        pool.drop_table(&format!("{table_name}")).await.unwrap();
    }

    #[tokio::test]
    async fn test_insert() {
        #[cfg(feature = "rusqlite")]
        insert(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        insert("postgresql:///rltbl_db").await;
        #[cfg(feature = "libsql")]
        integer_column_query(":memory:").await;
        #[cfg(feature = "sqlx")]
        {
            text_column_query(":memory:").await;
            text_column_query("postgresql:///rltbl_db").await;
        }
    }

    async fn insert(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let cascade = match pool.kind() {
            DbKind::PostgreSQL => " CASCADE",
            DbKind::SQLite => "",
        };
        let table_name = format!("test_insert");
        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS {table_name}{cascade};\
             CREATE TABLE {table_name} (\
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
            &format!("{table_name}"),
            &["text_value", "int_value", "bool_value"],
            &[
                &db_row! {"text_value".into() => ParamValue::from("TEXT")},
                &db_row! {
                    "int_value".into() => ParamValue::from(1_i64),
                    "bool_value".into() => {
                        #[cfg(feature = "libsql")]
                        {
                            // libsql does not support booleans:
                            // https://docs.rs/libsql/0.9.29/libsql/enum.Value.html,
                            ParamValue::from(1_i64)
                        }
                        #[cfg(not(feature = "libsql"))]
                        {
                            ParamValue::from(true)
                        }
                    }
                },
            ],
        )
        .await
        .unwrap();

        // Validate the inserted data:
        let rows: Vec<DbRow> = pool
            .query(&format!(r#"SELECT * FROM {table_name}"#), ())
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
                        // libsql does not support booleans:
                        // https://docs.rs/libsql/0.9.29/libsql/enum.Value.html,
                        DbKind::SQLite => ParamValue::from(1_i64),
                        DbKind::PostgreSQL => ParamValue::from(true),
                    },
                }
            ]
        );

        // Clean up.
        pool.drop_table(&format!("{table_name}")).await.unwrap();
    }

    #[tokio::test]
    async fn test_insert_returning() {
        #[cfg(feature = "rusqlite")]
        insert_returning(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        insert_returning("postgresql:///rltbl_db").await;
        #[cfg(feature = "libsql")]
        integer_column_query(":memory:").await;
        #[cfg(feature = "sqlx")]
        {
            text_column_query(":memory:").await;
            text_column_query("postgresql:///rltbl_db").await;
        }
    }

    async fn insert_returning(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let cascade = match pool.kind() {
            DbKind::PostgreSQL => " CASCADE",
            DbKind::SQLite => "",
        };
        let table_name = format!("test_insert_returning");

        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS {table_name}{cascade};\
             CREATE TABLE {table_name} (\
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
                &format!("{table_name}"),
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
                        // libsql does not support booleans:
                        // https://docs.rs/libsql/0.9.29/libsql/enum.Value.html,
                        DbKind::SQLite => ParamValue::from(1_i64),
                        DbKind::PostgreSQL => ParamValue::from(true),
                    }
                }
            ]
        );

        // With specific returning columns:
        let rows: Vec<DbRow> = pool
            .insert_returning(
                &format!("{table_name}"),
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
        pool.drop_table(&format!("{table_name}")).await.unwrap();
    }

    #[tokio::test]
    async fn test_drop_table() {
        #[cfg(feature = "rusqlite")]
        drop_table(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        drop_table("postgresql:///rltbl_db").await;
        #[cfg(feature = "libsql")]
        integer_column_query(":memory:").await;
        #[cfg(feature = "sqlx")]
        {
            text_column_query(":memory:").await;
            text_column_query("postgresql:///rltbl_db").await;
        }
    }

    async fn drop_table(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let cascade = match pool.kind() {
            DbKind::PostgreSQL => " CASCADE",
            DbKind::SQLite => "",
        };
        let table1 = format!("test_drop1");
        let table2 = format!("test_drop2");
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

        let columns = pool.columns(&table1).await.unwrap();
        assert_eq!(
            columns,
            ColumnMap::from([("foo".to_owned(), "text".to_owned())])
        );
        pool.drop_table(&table1).await.unwrap();

        match pool.columns(&table1).await {
            Ok(columns) => panic!("No columns expected for '{table1}' but got {columns:?}"),
            Err(_) => (),
        };

        // Clean up.
        pool.drop_table(&table2).await.unwrap();
    }

    #[tokio::test]
    async fn test_primary_keys() {
        #[cfg(feature = "rusqlite")]
        primary_keys(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        primary_keys("postgresql:///rltbl_db").await;
        #[cfg(feature = "libsql")]
        integer_column_query(":memory:").await;
        #[cfg(feature = "sqlx")]
        {
            text_column_query(":memory:").await;
            text_column_query("postgresql:///rltbl_db").await;
        }
    }

    async fn primary_keys(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let cascade = match pool.kind() {
            DbKind::PostgreSQL => " CASCADE",
            DbKind::SQLite => "",
        };
        let table1 = format!("test_primary_keys1");
        let table2 = format!("test_primary_keys2");

        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS {table1}{cascade};\
             DROP TABLE IF EXISTS {table2}{cascade};\
             CREATE TABLE {table1} (\
               foo TEXT PRIMARY KEY\
             );\
             CREATE TABLE {table2} (\
               foo TEXT,\
               bar TEXT,\
               car TEXT,
               PRIMARY KEY (foo, bar)\
             )",
        ))
        .await
        .unwrap();

        assert_eq!(
            pool.primary_keys(&format!("{table1}")).await.unwrap(),
            ["foo"]
        );
        assert_eq!(
            pool.primary_keys(&format!("{table2}")).await.unwrap(),
            ["foo", "bar"]
        );

        // Clean up:
        pool.drop_table(&format!("{table1}")).await.unwrap();
        pool.drop_table(&format!("{table2}")).await.unwrap();
    }

    #[tokio::test]
    async fn test_update() {
        #[cfg(feature = "rusqlite")]
        update(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        update("postgresql:///rltbl_db").await;
        #[cfg(feature = "libsql")]
        integer_column_query(":memory:").await;
        #[cfg(feature = "sqlx")]
        {
            text_column_query(":memory:").await;
            text_column_query("postgresql:///rltbl_db").await;
        }
    }

    async fn update(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let cascade = match pool.kind() {
            DbKind::PostgreSQL => " CASCADE",
            DbKind::SQLite => "",
        };
        let table_name = format!("test_update");

        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS {table_name}{cascade};\
             CREATE TABLE {table_name} (\
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
            &format!("{table_name}"),
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
            &format!("{table_name}"),
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

        let rows: Vec<DbRow> = pool
            .query(&format!("SELECT * from {table_name}"), ())
            .await
            .unwrap();
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
        pool.drop_table(&format!("{table_name}")).await.unwrap();
    }

    #[tokio::test]
    async fn test_update_returning() {
        #[cfg(feature = "rusqlite")]
        update_returning(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        update_returning("postgresql:///rltbl_db").await;
        #[cfg(feature = "libsql")]
        integer_column_query(":memory:").await;
        #[cfg(feature = "sqlx")]
        {
            text_column_query(":memory:").await;
            text_column_query("postgresql:///rltbl_db").await;
        }
    }

    async fn update_returning(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let cascade = match pool.kind() {
            DbKind::PostgreSQL => " CASCADE",
            DbKind::SQLite => "",
        };
        let table_name = format!("test_update_returning");

        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS {table_name}{cascade};\
             CREATE TABLE {table_name} (\
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
            &format!("{table_name}"),
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
                    &format!("{table_name}"),
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
        pool.execute(&format!("DELETE FROM {table_name}"), ())
            .await
            .unwrap();

        pool.insert(
            &format!("{table_name}"),
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
                    &format!("{table_name}"),
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
            .query(&format!("SELECT * from {table_name}"), ())
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
        pool.drop_table(&format!("{table_name}")).await.unwrap();
    }

    #[tokio::test]
    async fn test_upsert() {
        #[cfg(feature = "rusqlite")]
        upsert(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        upsert("postgresql:///rltbl_db").await;
        #[cfg(feature = "libsql")]
        integer_column_query(":memory:").await;
        #[cfg(feature = "sqlx")]
        {
            text_column_query(":memory:").await;
            text_column_query("postgresql:///rltbl_db").await;
        }
    }

    async fn upsert(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let cascade = match pool.kind() {
            DbKind::PostgreSQL => " CASCADE",
            DbKind::SQLite => "",
        };
        let table_name = format!("test_upsert");

        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS {table_name}{cascade};\
             CREATE TABLE {table_name} (\
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
            &format!("{table_name}"),
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
            &format!("{table_name}"),
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

        let rows: Vec<DbRow> = pool
            .query(&format!("SELECT * from {table_name}"), ())
            .await
            .unwrap();
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
        pool.drop_table(&format!("{table_name}")).await.unwrap();
    }

    #[tokio::test]
    async fn test_upsert_returning() {
        #[cfg(feature = "rusqlite")]
        upsert_returning(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        upsert_returning("postgresql:///rltbl_db").await;
        #[cfg(feature = "libsql")]
        integer_column_query(":memory:").await;
        #[cfg(feature = "sqlx")]
        {
            text_column_query(":memory:").await;
            text_column_query("postgresql:///rltbl_db").await;
        }
    }

    async fn upsert_returning(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let cascade = match pool.kind() {
            DbKind::PostgreSQL => " CASCADE",
            DbKind::SQLite => "",
        };
        let table_name = format!("test_upsert_returning");

        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS {table_name}{cascade};\
             CREATE TABLE {table_name} (\
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
            &format!("{table_name}"),
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
                &format!("{table_name}"),
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
        pool.drop_table(&format!("{table_name}")).await.unwrap();
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
        #[cfg(feature = "libsql")]
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
        #[cfg(feature = "sqlx")]
        {
            let mut pool = AnyPool::connect(":memory:").await.unwrap();
            for caching_strategy in &all_strategies {
                cache_with_strategy(&mut pool, &caching_strategy).await;
            }
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
        pool.set_cache_aware_query(true);

        let table1 = format!("test_table_caching_1");
        let table2 = format!("test_table_caching_2");

        pool.drop_table(&format!("{table1}")).await.unwrap();
        pool.drop_table(&format!("{table2}")).await.unwrap();
        pool.execute_batch(&format!(
            "CREATE TABLE {table1} (\
               value TEXT \
             );\
             CREATE TABLE {table2} (\
               value TEXT \
             )",
        ))
        .await
        .unwrap();

        pool.insert(
            &format!("{table1}"),
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
                &[&format!("{table1}")],
                &format!("SELECT * from {table1}"),
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
                db_row! {"value".into() => ParamValue::from("alpha")},
                db_row! {"value".into() => ParamValue::from("beta")},
            ]
        );

        let rows: Vec<DbRow> = pool
            .cache(
                &[&format!("{table1}")],
                &format!("SELECT * from {table1}"),
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
                db_row! {"value".into() => ParamValue::from("alpha")},
                db_row! {"value".into() => ParamValue::from("beta")},
            ]
        );

        pool.insert(
            &format!("{table1}"),
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
            CachingStrategy::Memory(_) => assert_eq!(count_memory_cache_rows(), 0),
            _ => assert_eq!(count_cache_table_rows(pool).await, 0),
        };

        let rows: Vec<DbRow> = pool
            .cache(
                &[&format!("{table1}")],
                &format!("SELECT * from {table1}"),
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
                db_row! {"value".into() => ParamValue::from("alpha")},
                db_row! {"value".into() => ParamValue::from("beta")},
                db_row! {"value".into() => ParamValue::from("gamma")},
                db_row! {"value".into() => ParamValue::from("delta")},
            ]
        );

        let rows: Vec<DbRow> = pool
            .cache(
                &[&format!("{table1}")],
                &format!("SELECT * from {table1}"),
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
                &[&format!("{table1}")],
                &format!("SELECT COUNT(1) FROM {table1}"),
                (),
            )
            .await
            .unwrap();

        let _: Vec<DbRow> = pool
            .cache(
                &[&format!("{table2}")],
                &format!("SELECT COUNT(1) FROM {table2}"),
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
            &format!(r#"INSERT INTO {table1} VALUES ('rho'), ('sigma')"#),
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

        let rows: Vec<DbRow> = pool
            .cache(
                &[&format!("{table1}")],
                &format!("SELECT * from {table1}"),
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
                db_row! {"value".into() => ParamValue::from("alpha")},
                db_row! {"value".into() => ParamValue::from("beta")},
                db_row! {"value".into() => ParamValue::from("gamma")},
                db_row! {"value".into() => ParamValue::from("delta")},
                db_row! {"value".into() => ParamValue::from("rho")},
                db_row! {"value".into() => ParamValue::from("sigma")},
            ]
        );
    }

    // This test takes a few minutes to run and is ignored by default.
    // Use `cargo test -- --ignored` or `cargo test -- --include-ignored` to run it.
    #[tokio::test]
    #[ignore]
    async fn test_caching_performance() {
        let runs = 5000;
        let edit_rate = 25;
        #[cfg(feature = "rusqlite")]
        perform_caching(":memory:", runs, edit_rate, 75).await;
        #[cfg(feature = "tokio-postgres")]
        perform_caching("postgresql:///rltbl_db", runs, edit_rate, 75).await;
        #[cfg(feature = "libsql")]
        perform_caching(":memory:", runs, edit_rate, 75).await;
        #[cfg(feature = "sqlx")]
        perform_caching(":memory:", runs, edit_rate, 75).await;
    }

    // Performs the caching performance test on the database located at the given url, using
    // the given number of runs and edit rate. The latter represents the rate at which the
    // tables in the simulation are edited (e.g., a value of 25 means that a table will be edited
    // in one out every 25th run, on average), which causes the cache to become out of date and
    // require maintenance in accordance with the current caching strategy. The test is run
    // for the given number of runs for each of the supported caching strategies. The running
    // time for each strategy is then summarized and reported via STDOUT.
    async fn perform_caching(url: &str, runs: usize, edit_rate: usize, fail_after: usize) {
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
        for strategy in &all_strategies {
            println!("{this_test} Using strategy: {strategy}.");
            pool.set_caching_strategy(&strategy);
            let elapsed = perform_caching_detail(&pool, runs, fail_after, edit_rate).await;
            times.insert(format!("{strategy}"), elapsed);
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
            let table = format!("{table}");
            pool.execute(&format!("DROP TABLE IF EXISTS {table}"), ())
                .await
                .unwrap();
            pool.execute(
                &format!("CREATE TABLE IF NOT EXISTS {table} ( foo INT, bar INT )"),
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
                    &format!("SELECT foo, SUM(bar) FROM {select_table} GROUP BY foo ORDER BY foo"),
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
            pool.execute(&format!("DROP TABLE IF EXISTS {table}"), ())
                .await
                .unwrap();
        }
        elapsed
    }
}
