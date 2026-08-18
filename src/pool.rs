//! Trait for database connection pool

use std::{iter::IntoIterator, sync::Arc};

use async_trait::async_trait;
use indexmap::IndexMap;

use crate::{AnyTransaction, Error, Query, Rows, Syntax, Transaction, Value};

// MC: But why is it named Pool if it is for using transactions? How About CreateTransaction?
// JO: The Pool is a database pool.
// It implements Query, and it also lets you start a transaction.

// MC: Why do you say it is synchronous when the method is asynchronous?
// JO: It should not have said "synchronous."
#[async_trait]
pub trait Pool: Query + std::fmt::Debug {
    // MC: This *creates* a transaction correct?
    async fn transaction(&self) -> Result<Box<dyn Transaction>, Error>;
}

#[derive(Debug)]
pub struct AnyPool {
    pool: Box<dyn Pool>,
    // MC: Why is this needed?
    // JO: It's just dead-code in this stub.
    // I would like the caches to be fields on the AnyPool, not global.
    #[allow(dead_code)]
    caching_strategy: String,
    // JO: This shouldn't be "pub" -- I was just testing.
    // It doesn't have to be an IndexMap.
    pub meta_cache: Arc<IndexMap<String, String>>,
}

// This is dyn compatible ONLY if every impl Query uses #[async_trait].
pub async fn connect(url: &str) -> Result<Box<dyn Pool>, Error> {
    if url.starts_with("postgresql://") {
        #[cfg(feature = "tokio-postgres")]
        {
            let pool = crate::tokio_postgres::PostgresPool::connect(url).await?;
            Ok(Box::new(pool))
        }
        #[cfg(not(feature = "tokio-postgres"))]
        {
            Err(Error::ConnectError(format!("Unsupported URL: '{url}'")))
        }
    } else {
        #[cfg(feature = "rusqlite")]
        {
            let pool = crate::rusqlite::RusqlitePool::connect(url).await?;
            Ok(Box::new(pool))
        }
        #[cfg(not(feature = "rusqlite"))]
        {
            Err(Error::ConnectError(format!("Unsupported URL: '{url}'")))
        }
    }
}

impl From<Box<dyn Pool>> for AnyPool {
    fn from(value: Box<dyn Pool>) -> Self {
        AnyPool {
            // MC: So an AnyPool has three fields: for caching strategy, meta cache, and
            // for something called "pool" that implements a trait called "Pool" which
            // is for using database transactions.
            pool: value,
            caching_strategy: "None".to_string(),
            meta_cache: Arc::new(IndexMap::default()),
        }
    }
}

// MC: This is *a lot* cleaner than before.
impl AnyPool {
    pub async fn connect(url: &str) -> Result<Self, Error> {
        let pool = connect(url).await?;
        Ok(AnyPool::from(pool))
    }

    // MC: "syntax" / "Syntax" are just the new names for "kind" / "DbKind", correct?
    // JO: Yes
    pub fn syntax(&self) -> &dyn Syntax {
        self.pool.syntax()
    }

    pub async fn execute(
        &self,
        sql: &str,
        params: impl IntoIterator<Item = &Value>,
    ) -> Result<(), Error> {
        let refs: Vec<&Value> = params.into_iter().collect();
        self.pool.execute(sql, &refs).await
        // TODO: handle cache
    }

    #[allow(unused)]
    async fn execute_batch(&self, sql: &str) -> Result<(), Error> {
        self.pool.execute_batch(sql).await
        // TODO: handle cache
    }

    pub async fn query(
        &self,
        sql: &str,
        params: impl IntoIterator<Item = &Value>,
    ) -> Result<Rows, Error> {
        let refs: Vec<&Value> = params.into_iter().collect();
        self.pool.query(sql, &refs).await
        // TODO: handle cache
    }

    pub async fn cache(
        &self,
        _sql: &str,
        _params: impl IntoIterator<Item = &Value>,
    ) -> Result<Rows, Error> {
        todo!("implement AnyPool::cache")
    }

    pub async fn insert(&self, table: &str, columns: &[&str], rows: &Rows) -> Result<(), Error> {
        self.pool.insert(table, columns, rows).await
        // TODO: handle cache here? Or in shared.rs?
    }

    // TODO: insert_returning
    // TODO: update
    // TODO: update_returning
    // TODO: upsert
    // TODO: upsert_returning

    #[allow(unused)]
    async fn drop_table(&self, table: &str) -> Result<(), Error> {
        self.pool.drop_table(table).await
    }

    // MC: Right. This is needed for AnyPool to implement "Pool".
    pub async fn transaction(&self) -> Result<AnyTransaction, Error> {
        Ok(AnyTransaction::begin(self.pool.transaction().await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{row, row::StringRow, values};
    use rust_decimal::dec;

    #[tokio::test]
    async fn test_text_column_query() {
        #[cfg(feature = "rusqlite")]
        text_column_query(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        text_column_query("postgresql:///rltbl_db").await;
        // TODO:
        //#[cfg(feature = "libsql")]
        //text_column_query(":memory:").await;
    }

    // TODO (later): Remove #[allow(unused)] from everywhere in the code.
    // This particular compiler warning happens because this test isn't implemented yet
    // for libsql.
    #[allow(unused)]
    async fn text_column_query(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let syntax = pool.syntax();
        let pp = syntax.param_prefix();

        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS test_table_text{cascade};\
             CREATE TABLE test_table_text ( value TEXT )",
            cascade = match syntax.name() {
                "postgresql" => " CASCADE",
                "sqlite" => "",
                _ => panic!("Invalid syntax '{}'", syntax.name()),
            }
        ))
        .await
        .unwrap();

        pool.execute(
            &format!("INSERT INTO test_table_text VALUES ({pp}1)"),
            // TODO: don't require explicitly calling into() (or Value::from()) here and elsewhere.
            &["foo".into()],
            // ["foo"],
        )
        .await
        .unwrap();

        let select_sql = format!("SELECT value FROM test_table_text WHERE value = {pp}1");
        let value: String = pool
            .query(&select_sql, &[Value::from("foo")])
            .await
            .unwrap()
            .try_into_value::<String>()
            .unwrap();
        assert_eq!("foo", value);

        let string: String = pool
            .query(&select_sql, &[Value::from("foo")])
            .await
            .unwrap()
            .try_into_value::<String>()
            .unwrap();
        assert_eq!("foo", string);

        let strings = pool
            .query(&select_sql, &[Value::from("foo")])
            .await
            .unwrap()
            .to_strings()
            .unwrap();
        assert_eq!(vec!["foo".to_owned()], strings);

        let string_row: StringRow = pool
            .query(&select_sql, &[Value::from("foo")])
            .await
            .unwrap()
            .row()
            .unwrap()
            .into();
        assert_eq!(
            StringRow::from([("value".to_owned(), "foo".to_owned())]),
            string_row
        );

        let string_rows: Vec<StringRow> = pool
            .query(&select_sql, &[Value::from("foo")])
            .await
            .unwrap()
            .into();
        assert_eq!(
            vec![StringRow::from([("value".to_owned(), "foo".to_owned())])],
            string_rows
        );

        let rows = pool
            .query(&select_sql, &[Value::from("foo")])
            .await
            .unwrap();
        let row = rows.row().unwrap();
        assert_eq!(row, row! {"value" => "foo",});

        let rows = pool
            .query(&select_sql, &[Value::from("foo")])
            .await
            .unwrap();
        assert_eq!(rows.rows, [row! {"value" => "foo",}]);

        // Clean up:
        pool.drop_table("test_table_text").await.unwrap();
    }

    #[tokio::test]
    async fn test_integer_column_query() {
        #[cfg(feature = "rusqlite")]
        integer_column_query(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        integer_column_query("postgresql:///rltbl_db").await;
        // TODO:
        //#[cfg(feature = "libsql")]
        //integer_column_query(":memory:").await;
    }

    #[allow(unused)]
    async fn integer_column_query(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let syntax = pool.syntax();
        let pp = syntax.param_prefix();

        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS test_table_int{cascade};\
             CREATE TABLE test_table_int ( value_2 INT2, value_4 INT4, value_8 INT8 )",
            cascade = match syntax.name() {
                "postgresql" => " CASCADE",
                "sqlite" => "",
                _ => panic!("Invalid syntax '{}'", syntax.name()),
            }
        ))
        .await
        .unwrap();

        pool.execute(
            &format!("INSERT INTO test_table_int VALUES ({pp}1, {pp}2, {pp}3)"),
            &values![Value::from(1_i16), Value::from(1_i32), Value::from(1_i64)],
        )
        .await
        .unwrap();

        for column in ["value_2", "value_4", "value_8"] {
            let params = match column {
                "value_2" => values![1_i16],
                "value_4" => values![1_i32],
                "value_8" => values![1_i64],
                _ => unreachable!(),
            };
            let select_sql = format!("SELECT {column} FROM test_table_int WHERE {column} = {pp}1");
            let rows = pool.query(&select_sql, &params.clone()).await.unwrap();
            let value: i64 = rows.try_into_value::<i64>().unwrap();
            assert_eq!(1, value);

            let rows = pool.query(&select_sql, &params.clone()).await.unwrap();
            let unsigned: u64 = rows.try_into_value::<u64>().unwrap();
            assert_eq!(1, unsigned);

            let rows = pool.query(&select_sql, &params.clone()).await.unwrap();
            let signed: i64 = rows.try_into_value::<i64>().unwrap();
            assert_eq!(1, signed);

            let string: String = pool
                .query(&select_sql, &params.clone())
                .await
                .unwrap()
                .try_into_value::<String>()
                .unwrap();
            assert_eq!("1", string);

            let strings = pool
                .query(&select_sql, &params.clone())
                .await
                .unwrap()
                .to_strings()
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
        // TODO:
        //#[cfg(feature = "libsql")]
        //float_column_query(":memory:").await;
    }

    #[allow(unused)]
    async fn float_column_query(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let syntax = pool.syntax();
        let pp = syntax.param_prefix().to_string();

        // FLOAT8
        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS test_table_float{cascade};\
             CREATE TABLE test_table_float ( value FLOAT8 )",
            cascade = match syntax.name() {
                "postgresql" => " CASCADE",
                "sqlite" => "",
                _ => panic!("Invalid syntax '{}'", syntax.name()),
            }
        ))
        .await
        .unwrap();

        pool.execute(
            &format!("INSERT INTO test_table_float VALUES ({pp}1)"),
            &[1.05_f64.into()],
        )
        .await
        .unwrap();

        let select_sql = format!("SELECT value FROM test_table_float WHERE value > {pp}1");
        let rows = pool.query(&select_sql, &[1.0_f64.into()]).await.unwrap();
        let float = rows.try_into_value::<f64>().unwrap();
        assert_eq!("1.05", format!("{float:.2}"));

        let rows = pool.query(&select_sql, &[1.0_f64.into()]).await.unwrap();
        let float = rows.try_into_value::<f64>().unwrap();
        assert_eq!(1.05, float);

        let string: String = pool
            .query(&select_sql, &[1.0_f64.into()])
            .await
            .unwrap()
            .try_into_value::<String>()
            .unwrap();
        assert_eq!("1.05", string);

        let strings = pool
            .query(&select_sql, &[1.0_f64.into()])
            .await
            .unwrap()
            .to_strings()
            .unwrap();
        assert_eq!(vec!["1.05".to_owned()], strings);

        let rows = pool.query(&select_sql, &[1.0_f64.into()]).await.unwrap();
        let row = rows.row().unwrap();
        assert_eq!(row, row! {"value" => 1.05,});

        let rows = pool.query(&select_sql, &[1.0_f64.into()]).await.unwrap();
        assert_eq!(rows.rows, [row! {"value" => 1.05,}]);

        // FLOAT4
        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS test_table_float{cascade};\
             CREATE TABLE test_table_float ( value FLOAT4 )",
            cascade = match syntax.name() {
                "postgresql" => " CASCADE",
                "sqlite" => "",
                _ => panic!("Invalid syntax '{}'", syntax.name()),
            }
        ))
        .await
        .unwrap();

        pool.execute(
            &format!("INSERT INTO test_table_float VALUES ({pp}1)"),
            &[1.05_f32.into()],
        )
        .await
        .unwrap();

        let select_sql = format!("SELECT value FROM test_table_float WHERE value > {pp}1");
        let rows = pool.query(&select_sql, &[1.0_f32.into()]).await.unwrap();
        let float = rows.try_into_value::<f32>().unwrap();
        assert_eq!("1.05", format!("{float:.2}"));

        // Clean up:
        pool.drop_table("test_table_float").await.unwrap();
    }

    #[tokio::test]
    async fn test_mixed_column_query() {
        #[cfg(feature = "rusqlite")]
        mixed_column_query(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        mixed_column_query("postgresql:///rltbl_db").await;
        // TODO:
        // #[cfg(feature = "libsql")]
        // mixed_column_query(":memory:").await;
    }

    #[allow(unused)]
    async fn mixed_column_query(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let syntax = pool.syntax();
        let pp = syntax.param_prefix().to_string();

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
            cascade = match syntax.name() {
                "postgresql" => " CASCADE",
                "sqlite" => "",
                _ => panic!("Invalid syntax '{}'", syntax.name()),
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
                   VALUES ({pp}1, {pp}2, {pp}3, {pp}4, {pp}5, {pp}6, {pp}7, {pp}8, {pp}9, {pp}10)"#,
            ),
            // TODO: It would be nice if we could use the old syntax here:
            //&values!["foo", (), 1.05_f64, (), 1_i64, (), true, (), dec!(1), ()],
            &values![
                "foo",
                Value::Null,
                1.05_f64,
                Value::Null,
                1_i64,
                Value::Null,
                true,
                Value::Null,
                dec!(1),
                Value::Null
            ],
        )
        .await
        .unwrap();

        let select_sql =
            format!("SELECT text_value FROM test_table_mixed WHERE text_value = {pp}1");
        let value: String = pool
            .query(&select_sql, &["foo".into()])
            .await
            .unwrap()
            .try_into_value::<String>()
            .unwrap();
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
               WHERE text_value = {pp}1
                 AND alt_text_value IS NOT DISTINCT FROM {pp}2
                 AND float_value > {pp}3
                 AND int_value > {pp}4
                 AND bool_value = {pp}5
                 AND numeric_value > {pp}6"#
        );
        let params = values!["foo", Value::Null, 1.0_f64, 0_i64, true, dec!(0.999)];

        let rows = pool.query(&select_sql, &params.clone()).await.unwrap();
        let row = rows.row().unwrap();
        assert_eq!(
            row,
            row! {
                "text_value" => "foo",
                "alt_text_value" => Value::Null,
                "float_value" => 1.05,
                "alt_float_value" => Value::Null,
                "int_value" => 1_i64,
                "alt_int_value" => Value::Null,
                "bool_value" => match syntax.name() {
                    "sqlite" => Value::from(1_i64),
                    "postgresql" => Value::from(true),
                    _ => panic!("Invalid syntax '{}'", syntax.name()),
                },
                "alt_bool_value" => Value::Null,
                "numeric_value" => 1_i64,
                "alt_numeric_value" => Value::Null,
            }
        );

        let rows = pool.query(&select_sql, &params.clone()).await.unwrap();
        assert_eq!(
            rows.rows,
            [row! {
                "text_value" => "foo",
                "alt_text_value" => Value::Null,
                "float_value" => 1.05,
                "alt_float_value" => Value::Null,
                "int_value" => 1_i64,
                "alt_int_value" => Value::Null,
                "bool_value" => match syntax.name() {
                    "sqlite" => Value::from(1_i64),
                    "postgresql" => Value::from(true),
                    _ => panic!("Invalid syntax '{}'", syntax.name()),
                },
                "alt_bool_value" => Value::Null,
                "numeric_value" => 1_i64,
                "alt_numeric_value" => Value::Null,
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
        // TODO:
        // #[cfg(feature = "libsql")]
        // input_params(":memory:").await;
    }

    #[allow(unused)]
    async fn input_params(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let syntax = pool.syntax();
        let pp = syntax.param_prefix().to_string();
        let cascade = match syntax.name() {
            "postgresql" => " CASCADE",
            "sqlite" => "",
            _ => panic!("Invalid syntax '{}'", syntax.name()),
        };
        pool.execute(
            &format!("DROP TABLE IF EXISTS test_any_table_input_params{cascade}"),
            &[],
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
            &[],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (bar) VALUES ({pp}1)"),
            &["one".into()],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (far) VALUES ({pp}1)"),
            &[1_i64.into()],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (bar) VALUES ({pp}1)"),
            &["two".into()],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (far) VALUES ({pp}1)"),
            &[2_i64.into()],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (bar) VALUES ({pp}1)"),
            &vec!["three".into()],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (far) VALUES ({pp}1)"),
            &vec![3_i64.into()],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (gar) VALUES ({pp}1)"),
            &vec![3_f32.into()],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (har) VALUES ({pp}1)"),
            &vec![3_f64.into()],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (jar) VALUES ({pp}1)"),
            &vec![dec!(3).into()],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (kar) VALUES ({pp}1)"),
            &vec![true.into()],
        )
        .await
        .unwrap();
        pool.execute(
            &format!(
                "INSERT INTO test_any_table_input_params \
                 (bar, car, dar, far, gar, har, jar, kar) \
                 VALUES ({pp}1, {pp}2, {pp}3, {pp}4, {pp}5 ,{pp}6, {pp}7, {pp}8)"
            ),
            &values![
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
        // TODO:
        // #[cfg(feature = "libsql")]
        // insert(":memory:").await;
    }

    #[allow(unused)]
    async fn insert(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let syntax = pool.syntax();
        let cascade = match syntax.name() {
            "postgresql" => " CASCADE",
            "sqlite" => "",
            _ => panic!("Invalid syntax '{}'", syntax.name()),
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
            &Rows {
                rows: vec![
                    row! {"text_value" => "TEXT",},
                    row! {
                        "int_value" => 1_i64,
                        "bool_value" => match syntax.name() {
                            "sqlite" => Value::from(1_i64),
                            "postgresql" => Value::from(true),
                            _ => panic!("Invalid syntax '{}'", syntax.name()),
                        },
                    },
                ],
            },
        )
        .await
        .unwrap();

        // Validate the inserted data:
        let rows = pool
            .query(r#"SELECT * FROM test_insert"#, &[])
            .await
            .unwrap();
        assert_eq!(
            rows.rows,
            [
                row! {
                    "text_value" => "TEXT",
                    "alt_text_value" => Value::Null,
                    "float_value" => Value::Null,
                    "int_value" => Value::Null,
                    "bool_value" => Value::Null,
                },
                row! {
                    "text_value" => Value::Null,
                    "alt_text_value" => Value::Null,
                    "float_value" => Value::Null,
                    "int_value" => 1_i64,
                    "bool_value" => match syntax.name() {
                        "sqlite" => Value::from(1_i64),
                        "postgresql" => Value::from(true),
                        _ => panic!("Invalid syntax '{}'", syntax.name()),
                    },
                }
            ]
        );

        // Clean up.
        pool.drop_table("test_insert").await.unwrap();
    }
}
