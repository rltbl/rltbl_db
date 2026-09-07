#[cfg(test)]
mod tests {
    use indexmap::IndexMap;
    use rand::{SeedableRng, distr::Distribution, distr::Uniform, rngs::StdRng};
    use rust_decimal::dec;
    use std::{
        collections::{BTreeMap, HashMap},
        str::FromStr,
        thread,
        time::{Duration, Instant},
    };

    use crate::{
        AnyPool, Row, Value,
        cache::{CachingStrategy, QUERY_CACHE_TABLE, TABLE_CACHE_TABLE},
        row,
        row::StringRow,
        sql_parse::{
            get_accessed_tables, get_affected_tables, get_view_tables, validate_table_name,
        },
        values,
    };

    #[cfg(feature = "rusqlite")]
    use crate::sqlite::MAX_PARAMS_SQLITE;

    #[cfg(feature = "tokio-postgres")]
    use crate::postgres::MAX_PARAMS_POSTGRES;

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
        let pool = AnyPool::connect(url).await.unwrap();
        let syntax = pool.syntax();
        let cascade = match syntax.name() {
            "postgres" => " CASCADE",
            "sqlite" => "",
            _ => panic!("Invalid syntax '{}'", syntax.name()),
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
    #[allow(unused)]
    async fn text_column_query(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let syntax = pool.syntax();
        let pp = syntax.param_prefix();

        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS test_table_text{cascade};\
             CREATE TABLE test_table_text ( value TEXT )",
            cascade = match syntax.name() {
                "postgres" => " CASCADE",
                "sqlite" => "",
                _ => panic!("Invalid syntax '{}'", syntax.name()),
            }
        ))
        .await
        .unwrap();

        pool.execute(
            &format!("INSERT INTO test_table_text VALUES ({pp}1)"),
            ["foo"],
        )
        .await
        .unwrap();

        let select_sql = format!("SELECT value FROM test_table_text WHERE value = {pp}1");
        let value: String = pool
            .query(&select_sql, &["foo"])
            .await
            .unwrap()
            .try_into_value::<String>()
            .unwrap();
        assert_eq!("foo", value);

        let string: String = pool
            .query(&select_sql, &["foo"])
            .await
            .unwrap()
            .try_into_value::<String>()
            .unwrap();
        assert_eq!("foo", string);

        let strings = pool
            .query(&select_sql, &["foo"])
            .await
            .unwrap()
            .to_strings()
            .unwrap();
        assert_eq!(vec!["foo".to_owned()], strings);

        let string_row: StringRow = pool
            .query(&select_sql, &["foo"])
            .await
            .unwrap()
            .row()
            .unwrap()
            .into();
        assert_eq!(
            StringRow::from([("value".to_owned(), "foo".to_owned())]),
            string_row
        );

        let string_rows: Vec<StringRow> = pool.query(&select_sql, &["foo"]).await.unwrap().into();
        assert_eq!(
            vec![StringRow::from([("value".to_owned(), "foo".to_owned())])],
            string_rows
        );

        let rows = pool.query(&select_sql, &["foo"]).await.unwrap();
        let row = rows.row().unwrap();
        assert_eq!(row, row! {"value" => "foo",});

        let rows = pool.query(&select_sql, &["foo"]).await.unwrap();
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
                "postgres" => " CASCADE",
                "sqlite" => "",
                _ => panic!("Invalid syntax '{}'", syntax.name()),
            }
        ))
        .await
        .unwrap();

        pool.execute(
            &format!("INSERT INTO test_table_int VALUES ({pp}1, {pp}2, {pp}3)"),
            &values![1_i16, 1_i32, 1_i64],
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
                "postgres" => " CASCADE",
                "sqlite" => "",
                _ => panic!("Invalid syntax '{}'", syntax.name()),
            }
        ))
        .await
        .unwrap();

        pool.execute(
            &format!("INSERT INTO test_table_float VALUES ({pp}1)"),
            &[1.05_f64],
        )
        .await
        .unwrap();

        let select_sql = format!("SELECT value FROM test_table_float WHERE value > {pp}1");
        let rows = pool.query(&select_sql, &[1.0_f64]).await.unwrap();
        let float = rows.try_into_value::<f64>().unwrap();
        assert_eq!("1.05", format!("{float:.2}"));

        let rows = pool.query(&select_sql, &[1.0_f64]).await.unwrap();
        let float = rows.try_into_value::<f64>().unwrap();
        assert_eq!(1.05, float);

        let string: String = pool
            .query(&select_sql, &[1.0_f64])
            .await
            .unwrap()
            .try_into_value::<String>()
            .unwrap();
        assert_eq!("1.05", string);

        let strings = pool
            .query(&select_sql, &[1.0_f64])
            .await
            .unwrap()
            .to_strings()
            .unwrap();
        assert_eq!(vec!["1.05".to_owned()], strings);

        let rows = pool.query(&select_sql, &[1.0_f64]).await.unwrap();
        let row = rows.row().unwrap();
        assert_eq!(row, row! {"value" => 1.05,});

        let rows = pool.query(&select_sql, &[1.0_f64]).await.unwrap();
        assert_eq!(rows.rows, [row! {"value" => 1.05,}]);

        // FLOAT4
        pool.execute_batch(&format!(
            "DROP TABLE IF EXISTS test_table_float{cascade};\
             CREATE TABLE test_table_float ( value FLOAT4 )",
            cascade = match syntax.name() {
                "postgres" => " CASCADE",
                "sqlite" => "",
                _ => panic!("Invalid syntax '{}'", syntax.name()),
            }
        ))
        .await
        .unwrap();

        pool.execute(
            &format!("INSERT INTO test_table_float VALUES ({pp}1)"),
            &[1.05_f32],
        )
        .await
        .unwrap();

        let select_sql = format!("SELECT value FROM test_table_float WHERE value > {pp}1");
        let rows = pool.query(&select_sql, &[1.0_f32]).await.unwrap();
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
                "postgres" => " CASCADE",
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
            .query(&select_sql, &["foo"])
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
                    "postgres" => Value::from(true),
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
                    "postgres" => Value::from(true),
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
            "postgres" => " CASCADE",
            "sqlite" => "",
            _ => panic!("Invalid syntax '{}'", syntax.name()),
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
            &format!("INSERT INTO test_any_table_input_params (bar) VALUES ({pp}1)"),
            &["one"],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (far) VALUES ({pp}1)"),
            &[1_i64],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (bar) VALUES ({pp}1)"),
            &["two"],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (far) VALUES ({pp}1)"),
            &[2_i64],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (bar) VALUES ({pp}1)"),
            &vec!["three"],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (far) VALUES ({pp}1)"),
            &vec![3_i64],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (gar) VALUES ({pp}1)"),
            &vec![3_f32],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (har) VALUES ({pp}1)"),
            &vec![3_f64],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (jar) VALUES ({pp}1)"),
            &vec![dec!(3)],
        )
        .await
        .unwrap();
        pool.execute(
            &format!("INSERT INTO test_any_table_input_params (kar) VALUES ({pp}1)"),
            &vec![true],
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
    async fn test_drop_table() {
        #[cfg(feature = "rusqlite")]
        drop_table(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        drop_table("postgresql:///rltbl_db").await;
        // TODO:
        //#[cfg(feature = "libsql")]
        //drop_table(":memory:").await;
    }

    #[allow(unused)]
    async fn drop_table(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let syntax = pool.syntax();
        let cascade = match syntax.name() {
            "postgres" => " CASCADE",
            "sqlite" => "",
            _ => panic!("Invalid syntax '{}'", syntax.name()),
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
            IndexMap::from([("foo".to_owned(), "text".to_owned())])
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
            "postgres" => " CASCADE",
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
            &[
                row! {"text_value" => "TEXT",},
                row! {
                    "int_value" => 1_i64,
                    "bool_value" => match syntax.name() {
                        "sqlite" => Value::from(1_i64),
                        "postgres" => Value::from(true),
                        _ => panic!("Invalid syntax '{}'", syntax.name()),
                    },
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
                        "postgres" => Value::from(true),
                        _ => panic!("Invalid syntax '{}'", syntax.name()),
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
        // TODO:
        // #[cfg(feature = "libsql")]
        // insert_returning(":memory:").await;
    }

    #[allow(unused)]
    async fn insert_returning(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let syntax = pool.syntax();
        let cascade = match syntax.name() {
            "postgres" => " CASCADE",
            "sqlite" => "",
            _ => panic!("Invalid syntax '{}'", syntax.name()),
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
                    row! {"text_value" => "TEXT",},
                    row! {
                        "int_value" => 1_i64,
                        "bool_value" => true,
                    },
                ],
                &[],
            )
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
                        "postgres" => Value::from(true),
                        _ => panic!("Invalid syntax '{}'", syntax.name()),
                    },
                }
            ]
        );

        // With specific returning columns:
        let rows = pool
            .insert_returning(
                "test_insert_returning",
                &["text_value", "int_value", "bool_value"],
                &[
                    row! {
                        "text_value" => "TEXT",
                    },
                    row! {
                        "int_value" => 1_i64,
                        "bool_value" => true,
                    },
                ],
                &["int_value", "float_value"],
            )
            .await
            .unwrap();
        assert_eq!(
            rows.rows,
            [
                row! {
                    "float_value" => Value::Null,
                    "int_value" => Value::Null,
                },
                row! {
                    "float_value" => Value::Null,
                    "int_value" => 1_i64,
                }
            ]
        );

        // Clean up.
        pool.drop_table("test_insert_returning").await.unwrap();
    }

    #[tokio::test]
    async fn test_update() {
        #[cfg(feature = "rusqlite")]
        update(":memory:").await;
        #[cfg(feature = "tokio-postgres")]
        update("postgresql:///rltbl_db").await;
        // TODO:
        //#[cfg(feature = "libsql")]
        //update(":memory:").await;
    }

    #[allow(unused)]
    async fn update(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let syntax = pool.syntax();
        let cascade = match syntax.name() {
            "postgres" => " CASCADE",
            "sqlite" => "",
            _ => panic!("Invalid syntax '{}'", syntax.name()),
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
                row! {"foo" => 1_i64,},
                row! {"foo" => 2_i64,},
                row! {"foo" => 3_i64,},
            ],
        )
        .await
        .unwrap();

        pool.update(
            "test_update",
            &["foo", "bar", "car", "dar", "ear"],
            &[
                row! {
                    "foo" => 1_i64,
                    "bar" => 10_i64,
                    "car" => 11_i64,
                    "dar" => 12_i64,
                    "ear" => 13_i64,
                },
                row! {
                    "foo" => 2_i64,
                    "bar" => 14_i64,
                    "car" => 15_i64,
                    "dar" => 16_i64,
                    "ear" => 17_i64,
                },
                row! {
                    "foo" => 3_i64,
                    "bar" => 18_i64,
                    "car" => 19_i64,
                    "dar" => 20_i64,
                    "ear" => 21_i64,
                },
            ],
        )
        .await
        .unwrap();

        let rows = pool.query("SELECT * from test_update", ()).await.unwrap();
        assert_eq!(
            rows.rows,
            [
                row! {
                    "foo" => 1_i64,
                    "bar" => 10_i64,
                    "car" => 11_i64,
                    "dar" => 12_i64,
                    "ear" => 13_i64,
                },
                row! {
                    "foo" => 2_i64,
                    "bar" => 14_i64,
                    "car" => 15_i64,
                    "dar" => 16_i64,
                    "ear" => 17_i64,
                },
                row! {
                    "foo" => 3_i64,
                    "bar" => 18_i64,
                    "car" => 19_i64,
                    "dar" => 20_i64,
                    "ear" => 21_i64,
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
        // TODO:
        // #[cfg(feature = "libsql")]
        // update_returning(":memory:").await;
    }

    #[allow(unused)]
    async fn update_returning(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let syntax = pool.syntax();
        let cascade = match syntax.name() {
            "postgres" => " CASCADE",
            "sqlite" => "",
            _ => panic!("Invalid syntax '{}'", syntax.name()),
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
                row! {
                    "foo" => 1_i64,
                    "bar" => 1_i64,
                },
                row! {
                    "foo" => 2_i64,
                    "bar" => 2_i64,
                },
                row! {
                    "foo" => 3_i64,
                    "bar" => 3_i64,
                },
            ],
        )
        .await
        .unwrap();

        let check_returning_rows = |rows: &Vec<Row>| {
            assert!(rows.iter().all(|row| {
                [
                    row! {
                        "car" => 10_i64,
                        "dar" => 11_i64,
                        "ear" => 12_i64,
                    },
                    row! {
                        "car" => 13_i64,
                        "dar" => 14_i64,
                        "ear" => 15_i64,
                    },
                    row! {
                        "car" => 16_i64,
                        "dar" => 17_i64,
                        "ear" => 18_i64,
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
                        row! {
                            "foo" => 1_i64,
                            "bar" => 1_i64,
                            "car" => 10_i64,
                            "dar" => 11_i64,
                            "ear" => 12_i64,
                        },
                        row! {
                            "foo" => 2_i64,
                            "bar" => 2_i64,
                            "car" => 13_i64,
                            "dar" => 14_i64,
                            "ear" => 15_i64,
                        },
                        row! {
                            "foo" => 3_i64,
                            "bar" => 3_i64,
                            "car" => 16_i64,
                            "dar" => 17_i64,
                            "ear" => 18_i64,
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
                row! {
                    "foo" => 1_i64,
                    "bar" => 1_i64,
                },
                row! {
                    "foo" => 2_i64,
                    "bar" => 2_i64,
                },
                row! {
                    "foo" => 3_i64,
                    "bar" => 3_i64,
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
                        row! {
                            "ear" => 15_i64,
                            "bar" => 2_i64,
                            "car" => 13_i64,
                            "dar" => 14_i64,
                            "foo" => 2_i64,
                        },
                        row! {
                            "foo" => 1_i64,
                            "car" => 10_i64,
                            "bar" => 1_i64,
                            "ear" => 12_i64,
                            "dar" => 11_i64,
                        },
                        row! {
                            "car" => 16_i64,
                            "dar" => 17_i64,
                            "ear" => 18_i64,
                            "bar" => 3_i64,
                            "foo" => 3_i64,
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
                row! {
                    "foo" => 1_i64,
                    "bar" => 1_i64,
                    "car" => 10_i64,
                    "dar" => 11_i64,
                    "ear" => 12_i64,
                },
                row! {
                    "foo" => 2_i64,
                    "bar" => 2_i64,
                    "car" => 13_i64,
                    "dar" => 14_i64,
                    "ear" => 15_i64,
                },
                row! {
                    "foo" => 3_i64,
                    "bar" => 3_i64,
                    "car" => 16_i64,
                    "dar" => 17_i64,
                    "ear" => 18_i64,
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
        // TODO:
        // #[cfg(feature = "libsql")]
        // upsert(":memory:").await;
    }

    #[allow(unused)]
    async fn upsert(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let syntax = pool.syntax();
        let cascade = match syntax.name() {
            "postgres" => " CASCADE",
            "sqlite" => "",
            _ => panic!("Invalid syntax '{}'", syntax.name()),
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
                row! {
                    "foo" => 1_i64,
                },
                row! {
                    "foo" => 2_i64,
                },
                row! {
                    "foo" => 3_i64,
                },
            ],
        )
        .await
        .unwrap();

        pool.upsert(
            "test_upsert",
            &["foo", "bar", "car", "dar", "ear"],
            &[
                row! {
                    "foo" => 1_i64,
                    "bar" => 10_i64,
                    "car" => 11_i64,
                    "dar" => 12_i64,
                    "ear" => 13_i64,
                },
                row! {
                    "foo" => 2_i64,
                    "bar" => 14_i64,
                    "car" => 15_i64,
                    "dar" => 16_i64,
                    "ear" => 17_i64,
                },
                row! {
                    "foo" => 3_i64,
                    "bar" => 18_i64,
                    "car" => 19_i64,
                    "dar" => 20_i64,
                    "ear" => 21_i64,
                },
            ],
        )
        .await
        .unwrap();

        let rows = pool.query("SELECT * from test_upsert", ()).await.unwrap();
        assert_eq!(
            rows.rows,
            [
                row! {
                    "foo" => 1_i64,
                    "bar" => 10_i64,
                    "car" => 11_i64,
                    "dar" => 12_i64,
                    "ear" => 13_i64,
                },
                row! {
                    "foo" => 2_i64,
                    "bar" => 14_i64,
                    "car" => 15_i64,
                    "dar" => 16_i64,
                    "ear" => 17_i64,
                },
                row! {
                    "foo" => 3_i64,
                    "bar" => 18_i64,
                    "car" => 19_i64,
                    "dar" => 20_i64,
                    "ear" => 21_i64,
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
        // TODO:
        //#[cfg(feature = "libsql")]
        //upsert_returning(":memory:").await;
    }

    #[allow(unused)]
    async fn upsert_returning(url: &str) {
        let pool = AnyPool::connect(url).await.unwrap();
        let syntax = pool.syntax();
        let cascade = match syntax.name() {
            "postgres" => " CASCADE",
            "sqlite" => "",
            _ => panic!("Invalid syntax '{}'", syntax.name()),
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
                row! {
                    "foo" => 1_i64,
                    "bar" => 1_i64,
                },
                row! {
                    "foo" => 2_i64,
                    "bar" => 2_i64,
                },
                row! {
                    "foo" => 3_i64,
                    "bar" => 3_i64,
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
                    row! {
                        "foo" => 1_i64,
                        "bar" => 1_i64,
                        "car" => 10_i64,
                        "dar" => 11_i64,
                        "ear" => 12_i64,
                    },
                    row! {
                        "foo" => 2_i64,
                        "bar" => 2_i64,
                        "car" => 13_i64,
                        "dar" => 14_i64,
                        "ear" => 15_i64,
                    },
                    row! {
                        "foo" => 3_i64,
                        "bar" => 3_i64,
                        "car" => 16_i64,
                        "dar" => 17_i64,
                        "ear" => 18_i64,
                    },
                ],
                &["car", "dar", "ear"],
            )
            .await
            .unwrap();
        assert!(rows.iter().all(|row| {
            [
                row! {
                    "car" => 10_i64,
                    "dar" => 11_i64,
                    "ear" => 12_i64,
                },
                row! {
                    "car" => 13_i64,
                    "dar" => 14_i64,
                    "ear" => 15_i64,
                },
                row! {
                    "car" => 16_i64,
                    "dar" => 17_i64,
                    "ear" => 18_i64,
                },
            ]
            .contains(&row)
        }));

        // Clean up:
        pool.drop_table("test_upsert_returning").await.unwrap();
    }

    #[tokio::test]
    async fn test_caching() {
        #[allow(unused)]
        let all_strategies = ["truncate_all", "truncate", "trigger", "memory:5"]
            .iter()
            .map(|strategy| CachingStrategy::from_str(strategy).unwrap())
            .collect::<Vec<_>>();
        #[cfg(feature = "rusqlite")]
        {
            let mut pool = AnyPool::connect(":memory:").await.unwrap();
            for caching_strategy in &all_strategies {
                table_caching(&mut pool, &caching_strategy).await;
                pool.clear_meta_cache().unwrap();
            }
            for strategy in &all_strategies {
                view_caching(&mut pool, strategy).await;
                pool.clear_meta_cache().unwrap();
            }
        }
        #[cfg(feature = "tokio-postgres")]
        {
            let mut pool = AnyPool::connect("postgresql:///rltbl_db").await.unwrap();
            for caching_strategy in &all_strategies {
                table_caching(&mut pool, &caching_strategy).await;
                pool.clear_meta_cache().unwrap();
            }
            for strategy in &all_strategies {
                view_caching(&mut pool, strategy).await;
                pool.clear_meta_cache().unwrap();
            }
        }
        // TODO:
        // #[cfg(feature = "libsql")]
        // {
        //     let mut pool = AnyPool::connect(":memory:").await.unwrap();
        //     for caching_strategy in &all_strategies {
        //         table_caching(&mut pool, &caching_strategy).await;
        //         pool.clear_meta_cache()?;
        //     }
        //     for strategy in &all_strategies {
        //         view_caching(&mut pool, strategy).await;
        //         pool.clear_meta_cache()?;
        //     }
        // }
    }

    #[allow(unused)]
    async fn table_caching(pool: &mut AnyPool, strategy: &CachingStrategy) {
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
                row! {
                    "value" => "alpha",
                },
                row! {
                    "value" => "beta",
                },
            ],
        )
        .await
        .unwrap();

        let rows = pool
            .cache("SELECT * from test_table_caching_1", ())
            .await
            .unwrap();

        assert_eq!(pool.count_query_cache_rows().await.unwrap(), 1);
        assert_eq!(
            rows.rows,
            vec![
                row! {
                    "value" => "alpha",
                },
                row! {
                    "value" => "beta",
                },
            ]
        );

        let rows = pool
            .cache("SELECT * from test_table_caching_1", ())
            .await
            .unwrap();

        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => {
                assert_eq!(pool.count_query_cache_rows().await.unwrap(), 1)
            }
            _ => assert_eq!(pool.count_query_cache_rows().await.unwrap(), 1),
        };
        assert_eq!(
            rows.rows,
            vec![
                row! {
                    "value" => "alpha",
                },
                row! {
                    "value" => "beta",
                },
            ]
        );

        pool.insert(
            "test_table_caching_1",
            &["value"],
            &[
                row! {
                    "value" => "gamma",
                },
                row! {
                    "value" => "delta",
                },
            ],
        )
        .await
        .unwrap();

        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => {
                assert_eq!(pool.count_query_cache_rows().await.unwrap(), 0)
            }
            _ => assert_eq!(pool.count_query_cache_rows().await.unwrap(), 0),
        };

        let rows = pool
            .cache("SELECT * from test_table_caching_1", ())
            .await
            .unwrap();

        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => {
                assert_eq!(pool.count_query_cache_rows().await.unwrap(), 1)
            }
            _ => assert_eq!(pool.count_query_cache_rows().await.unwrap(), 1),
        };
        assert_eq!(
            rows.rows,
            vec![
                row! {
                    "value" => "alpha",
                },
                row! {
                    "value" => "beta",
                },
                row! {
                    "value" => "gamma",
                },
                row! {
                    "value" => "delta",
                },
            ]
        );

        let rows = pool
            .cache("SELECT * from test_table_caching_1", ())
            .await
            .unwrap();

        assert_eq!(
            rows.rows,
            vec![
                row! {
                    "value" => "alpha",
                },
                row! {
                    "value" => "beta",
                },
                row! {
                    "value" => "gamma",
                },
                row! {
                    "value" => "delta",
                },
            ]
        );

        pool.cache("SELECT COUNT(1) FROM test_table_caching_1", ())
            .await
            .unwrap();

        pool.cache("SELECT COUNT(1) FROM test_table_caching_2", ())
            .await
            .unwrap();

        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => {
                assert_eq!(pool.count_query_cache_rows().await.unwrap(), 3)
            }
            _ => assert_eq!(pool.count_query_cache_rows().await.unwrap(), 3),
        };

        pool.execute(
            r#"INSERT INTO test_table_caching_1 VALUES ('rho'), ('sigma')"#,
            (),
        )
        .await
        .unwrap();

        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => {
                assert_eq!(pool.count_query_cache_rows().await.unwrap(), 1)
            }
            CachingStrategy::Truncate | CachingStrategy::Trigger => {
                assert_eq!(pool.count_query_cache_rows().await.unwrap(), 1)
            }
            CachingStrategy::TruncateAll => {
                assert_eq!(pool.count_query_cache_rows().await.unwrap(), 0)
            }
        };

        let rows = pool
            .cache("SELECT * from test_table_caching_1", ())
            .await
            .unwrap();

        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => {
                assert_eq!(pool.count_query_cache_rows().await.unwrap(), 2)
            }
            CachingStrategy::Truncate | CachingStrategy::Trigger => {
                assert_eq!(pool.count_query_cache_rows().await.unwrap(), 2)
            }
            CachingStrategy::TruncateAll => {
                assert_eq!(pool.count_query_cache_rows().await.unwrap(), 1)
            }
        };
        assert_eq!(
            rows.rows,
            vec![
                row! {
                    "value" => "alpha",
                },
                row! {
                    "value" => "beta",
                },
                row! {
                    "value" => "gamma",
                },
                row! {
                    "value" => "delta",
                },
                row! {
                    "value" => "rho",
                },
                row! {
                    "value" => "sigma",
                },
            ]
        );

        let rows = pool
            .cache(
                "SELECT * FROM test_table_caching_1 t1, test_table_caching_2 t2 \
                 WHERE t1.value = t2.value",
                (),
            )
            .await
            .unwrap();
        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => {
                assert_eq!(pool.count_query_cache_rows().await.unwrap(), 3)
            }
            CachingStrategy::Truncate | CachingStrategy::Trigger => {
                assert_eq!(pool.count_query_cache_rows().await.unwrap(), 3)
            }
            CachingStrategy::TruncateAll => {
                assert_eq!(pool.count_query_cache_rows().await.unwrap(), 2)
            }
        };
        assert_eq!(rows.len(), 0);

        let rows = pool
            .cache(
                "SELECT * FROM test_table_caching_1 t1, test_table_caching_2 t2 \
                 WHERE t1.value = t2.value",
                (),
            )
            .await
            .unwrap();
        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => {
                assert_eq!(pool.count_query_cache_rows().await.unwrap(), 3)
            }
            CachingStrategy::Truncate | CachingStrategy::Trigger => {
                assert_eq!(pool.count_query_cache_rows().await.unwrap(), 3)
            }
            CachingStrategy::TruncateAll => {
                assert_eq!(pool.count_query_cache_rows().await.unwrap(), 2)
            }
        };
        assert_eq!(rows.len(), 0);

        // Cleanup:
        pool.drop_table("test_table_caching_1").await.unwrap();
        pool.drop_table("test_table_caching_2").await.unwrap();
    }

    #[allow(unused)]
    async fn view_caching(_pool: &mut AnyPool, _strategy: &CachingStrategy) {
        // TODO ...
    }

    // This test takes a few minutes to run and is ignored by default.
    // Use `cargo test -- --ignored` or `cargo test -- --include-ignored` to run it.
    #[tokio::test]
    #[ignore]
    async fn test_caching_performance() {
        let runs = 1000;
        let edit_rate = 25;
        let fail_after = 60;
        #[cfg(feature = "rusqlite")]
        perform_caching(":memory:", runs, edit_rate, fail_after).await;
        #[cfg(feature = "tokio-postgres")]
        perform_caching("postgresql:///rltbl_db", runs, edit_rate, fail_after).await;
        #[cfg(feature = "libsql")]
        {
            // perform_caching(":memory:", runs, edit_rate, fail_after).await;
        }
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
        let syntax = pool.syntax().name().to_string();
        println!(
            "{this_test} Starting test for {} connection '{}' with cache_aware_query {}.",
            syntax,
            url,
            match pool.get_cache_aware_query() {
                true => "on",
                false => "off",
            }
        );
        let mut times = BTreeMap::new();
        let mut elapsed_none: u64 = 0;
        let mut actual_edits_none: usize = 0;
        for strategy in &all_strategies {
            println!("{this_test} Using strategy: {strategy}.");
            pool.set_caching_strategy(&strategy);
            pool.clear_meta_cache().unwrap();
            let fail_after = match strategy {
                CachingStrategy::None => 0,
                _ => fail_after,
            };
            let (elapsed, actual_edits) =
                perform_caching_detail(&pool, runs, fail_after, edit_rate).await;
            times.insert(format!("{strategy}"), elapsed);
            if *strategy == CachingStrategy::None {
                elapsed_none = elapsed;
                actual_edits_none = actual_edits;
            } else {
                // The elapsed time for strategy 'none' should always be greater than for the
                // other caching strategies. Note that it is assumed that the None strategy
                // is always tested before any of the other strategies (otherwise this assertion
                // is certain to fail).
                //
                if actual_edits <= actual_edits_none {
                    assert!(elapsed_none > elapsed);
                } else {
                    if elapsed >= elapsed_none {
                        println!(
                            "WARNING: elapsed time for {strategy} took longer or just as long \
                             as none, but there were more edits for {strategy}: \
                             {actual_edits} vs. {actual_edits_none}."
                        );
                    }
                }
            }
        }

        println!("{this_test} Elapsed times for {} (summary):", syntax);
        for (strategy, elapsed) in times.iter() {
            println!("  Strategy: {strategy}, elapsed time: {elapsed}s");
        }
    }

    async fn perform_caching_detail(
        pool: &AnyPool,
        runs: usize,
        fail_after: usize,
        edit_rate: usize,
    ) -> (u64, usize) {
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

            // Add a few tens of thousands of values to the table:
            let mut values = vec![];
            for i in 0..5 {
                for j in 0..random_between(34000, 35000, &mut -1) {
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
            pool.cache(
                &format!("SELECT foo, SUM(bar) FROM {select_table}_view GROUP BY foo ORDER BY foo"),
                (),
            )
            .await
            .unwrap();
            elapsed = now.elapsed().as_secs();
            if fail_after != 0 && elapsed > fail_after as u64 {
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
        (elapsed, actual_edits)
    }

    #[tokio::test]
    // #[ignore] // TODO: This should be ignored by default?
    async fn test_table_names() {
        // Valid table names:
        assert_eq!(
            validate_table_name(r#"table"#).expect("Expected table name to be valid"),
            "table"
        );
        assert_eq!(
            validate_table_name(r#"my_table"#).expect("Expected table name to be valid"),
            "my_table"
        );
        assert_eq!(
            validate_table_name(r#"my_2nd_table"#).expect("Expected table name to be valid"),
            "my_2nd_table"
        );
        assert_eq!(
            validate_table_name(r#"my_table_2"#).expect("Expected table name to be valid"),
            "my_table_2"
        );
        assert_eq!(
            validate_table_name(r#"my_table2"#).expect("Expected table name to be valid"),
            "my_table2"
        );
        assert_eq!(
            validate_table_name(r#"My_Table_2"#).expect("Expected table name to be valid"),
            "My_Table_2"
        );

        // Valid table name surrounded by quotes:
        assert_eq!(
            validate_table_name(r#""table""#).expect("Expected table name to be valid"),
            "table"
        );

        // Invalid first character:
        if let Ok(_) = validate_table_name(r#"1table"#) {
            panic!("Expected an error");
        };
        if let Ok(_) = validate_table_name(r#""1table""#) {
            panic!("Expected an error");
        }

        // Beginning or trailing double-quote is missing:
        if let Ok(_) = validate_table_name(r#"table""#) {
            panic!("Expected an error");
        }
        if let Ok(_) = validate_table_name(r#""table"#) {
            panic!("Expected an error");
        }

        // Table name with spaces:
        if let Ok(_) = validate_table_name(r#"my table"#) {
            panic!("Expected an error");
        }
    }

    #[tokio::test]
    // #[ignore] ignore by default
    async fn test_sql_parsing() {
        let tables_read = get_accessed_tables(&format!(
            r#"SELECT t1.foo
               FROM alpha t1, beta t2
               WHERE t1.foo = t2.foo"#
        ))
        .unwrap();
        let tables_read: Vec<_> = tables_read.into_iter().collect();
        assert_eq!(tables_read, ["alpha", "beta"]);

        let tables_read = get_accessed_tables(&format!(
            r#"SELECT t1.foo
               FROM alpha t1
               INNER JOIN beta t2 ON (t1.foo = t2.foo)"#
        ))
        .unwrap();
        let tables_read: Vec<_> = tables_read.into_iter().collect();
        assert_eq!(tables_read, ["alpha", "beta"]);

        let tables_read = get_accessed_tables(&format!(
            r#"SELECT t1.foo
               FROM alpha t1
               INNER JOIN beta t2 ON t1.foo = t2.foo
               WHERE t2.foo in (
                 SELECT t3.foo
                 FROM gamma t3
               )
               AND NOT t1.foo"#
        ))
        .unwrap();
        let tables_read: Vec<_> = tables_read.into_iter().collect();
        assert_eq!(tables_read, ["alpha", "beta", "gamma"]);

        let tables_read = get_accessed_tables(&format!(
            r#"SELECT t1.foo
               FROM alpha t1
               INNER JOIN beta t2 ON (
                 t1.foo = t2.foo AND
                 t2.foo in (
                   SELECT t3.foo
                   FROM gamma t3
                 )
               )
               WHERE NOT t1.foo"#
        ))
        .unwrap();
        let tables_read: Vec<_> = tables_read.into_iter().collect();
        assert_eq!(tables_read, ["alpha", "beta", "gamma"]);

        let view_sql = r#"CREATE VIEW greek_letter_foo AS
                          SELECT alpha.foo
                          FROM alpha, beta"#;
        let view_tables = get_view_tables(&view_sql).unwrap();
        assert_eq!(view_tables, ["alpha", "beta"]);

        let tables_read = get_accessed_tables(&format!(
            r#"SELECT t1.foo
                 FROM alpha t1, beta t2
                 WHERE t1.foo = t2.foo
               UNION ALL
               SELECT t3.foo
                 FROM gamma t3"#
        ))
        .unwrap();
        let tables_read: Vec<_> = tables_read.into_iter().collect();
        assert_eq!(tables_read, ["alpha", "beta", "gamma"]);

        let tables_read = get_accessed_tables(&format!(
            r#"WITH goo as (
                 SELECT alpha.foo FROM alpha
                 LEFT JOIN gamma ON gamma.foo = alpha.foo
               ), hoo as (
                 SELECT foo FROM beta
               ), ioo as (
                 SELECT foo FROM gamma
               )
               SELECT t1.foo
               FROM goo t1, hoo t2
               WHERE t1.foo = t2.foo
                 AND t2.foo in (
                   SELECT t3.foo
                     FROM delta t3
                 )
               UNION ALL
               SELECT * from ioo"#
        ))
        .unwrap();
        let tables_read: Vec<_> = tables_read.into_iter().collect();
        assert_eq!(tables_read, ["alpha", "beta", "delta", "gamma"]);

        let view_sql = r#"CREATE VIEW greek_letter_foo AS
                          SELECT alpha.foo
                          FROM alpha, beta
                          WHERE alpha.foo = beta.foo
                          UNION ALL
                          SELECT gamma.foo
                          FROM gamma"#;
        let view_tables = get_view_tables(&view_sql).unwrap();
        assert_eq!(view_tables, ["alpha", "beta", "gamma"]);

        let (edited_tables, dropped_tables) =
            get_affected_tables(&format!(r#"INSERT INTO "alpha" VALUES ($1, $2, $3)"#)).unwrap();
        let edited_tables: Vec<_> = edited_tables.into_iter().collect();
        assert_eq!(edited_tables, ["alpha"]);
        assert_eq!(dropped_tables, [].into());

        let (edited_tables, dropped_tables) = get_affected_tables(
            r#"WITH bar AS (SELECT * FROM alpha),
                    mar AS (SELECT * FROM beta)
                 INSERT INTO gamma
                 SELECT alpha.*
                 FROM alpha, beta
                 WHERE alpha.value = beta.value"#,
        )
        .unwrap();
        let edited_tables: Vec<_> = edited_tables.into_iter().collect();
        assert_eq!(edited_tables, ["gamma"]);
        assert_eq!(dropped_tables, [].into());

        let (edited_tables, dropped_tables) =
            get_affected_tables(&format!(r#"UPDATE "delta" set bar = $1 WHERE bar = $2"#)).unwrap();
        let edited_tables: Vec<_> = edited_tables.into_iter().collect();
        assert_eq!(edited_tables, ["delta"]);
        assert_eq!(dropped_tables, [].into());

        let (edited_tables, dropped_tables) = get_affected_tables(&format!(
            r#"WITH bar AS (SELECT * FROM test),
                    mar AS (SELECT * FROM test)
                 UPDATE delta
                 SET value = bar.value
                 FROM bar, mar
                 WHERE bar.value = $1 AND bar.value = mar.value"#,
        ))
        .unwrap();
        let edited_tables: Vec<_> = edited_tables.into_iter().collect();
        assert_eq!(edited_tables, ["delta"]);
        assert_eq!(dropped_tables, [].into());

        let (edited_tables, dropped_tables) =
            get_affected_tables(&format!(r#"DELETE FROM "epsilon" WHERE bar >= $1"#)).unwrap();
        let edited_tables: Vec<_> = edited_tables.into_iter().collect();
        assert_eq!(edited_tables, ["epsilon"]);
        assert_eq!(dropped_tables, [].into());

        let (edited_tables, dropped_tables) = get_affected_tables(
            r#"WITH bar AS (SELECT * FROM test),
                    mar AS (SELECT * FROM test)
                 DELETE FROM lambda WHERE value IN (SELECT value FROM bar)"#,
        )
        .unwrap();
        let edited_tables: Vec<_> = edited_tables.into_iter().collect();
        assert_eq!(edited_tables, ["lambda"]);
        assert_eq!(dropped_tables, [].into());

        let (edited_tables, dropped_tables) = get_affected_tables(r#"DROP TABLE "rho""#).unwrap();
        let dropped_tables: Vec<_> = dropped_tables.into_iter().collect();
        assert_eq!(dropped_tables, ["rho"]);
        assert_eq!(edited_tables, [].into());

        let (edited_tables, dropped_tables) =
            get_affected_tables(r#"DROP TABLE IF EXISTS "phi" CASCADE"#).unwrap();
        let dropped_tables: Vec<_> = dropped_tables.into_iter().collect();
        assert_eq!(dropped_tables, ["phi"]);
        assert_eq!(edited_tables, [].into());

        let (edited_tables, dropped_tables) =
            get_affected_tables("TRUNCATE TABLE mu, nu CASCADE").unwrap();
        let edited_tables: Vec<_> = edited_tables.into_iter().collect();
        assert_eq!(edited_tables, ["mu", "nu"]);
        assert_eq!(dropped_tables, [].into());

        let (edited_tables, dropped_tables) =
            get_affected_tables("ALTER TABLE phi ADD COLUMN varphi INT").unwrap();
        let edited_tables: Vec<_> = edited_tables.into_iter().collect();
        assert_eq!(edited_tables, ["phi"]);
        assert_eq!(dropped_tables, [].into());

        let (edited_tables, dropped_tables) = get_affected_tables("DROP VIEW theta").unwrap();
        let dropped_tables: Vec<_> = dropped_tables.into_iter().collect();
        assert_eq!(edited_tables, [].into());
        assert_eq!(dropped_tables, ["theta"]);

        let (edited_tables, dropped_tables) = get_affected_tables(
            r#"UPDATE epsilon
                 SET alpha = new_beta
                 FROM (
                   SELECT 9 AS new_alpha, 3 AS new_beta, 2 AS new_gamma, 1 AS new_delta
                   UNION ALL
                   SELECT 4 AS new_alpha, 3 as new_beta, 2 AS new_gamma, 1 AS new_delta
                 ) foo_alias
                 WHERE alpha = new_alpha"#,
        )
        .unwrap();
        let edited_tables: Vec<_> = edited_tables.into_iter().collect();
        assert_eq!(edited_tables, ["epsilon"]);
        assert_eq!(dropped_tables, [].into());

        // Multiple statements, no parameters:

        let sql = r#"BEGIN TRANSACTION;

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

                     DROP TABLE "sigma" CASCADE;

                     COMMIT"#;

        let (edited_tables, dropped_tables) = get_affected_tables(&sql).unwrap();
        let edited_tables: Vec<_> = edited_tables.into_iter().collect();
        let dropped_tables: Vec<_> = dropped_tables.into_iter().collect();
        assert_eq!(
            edited_tables,
            ["alpha", "delta", "gamma", "lambda", "phi", "psi",]
        );
        assert_eq!(dropped_tables, ["rho", "sigma",]);
    }

    #[test]
    fn test_hashing() {
        let mut test_map = HashMap::new();
        for (i, value) in [
            Value::Null,
            Value::Text("NULL".to_string()),
            Value::Boolean(true),
            Value::SmallInteger(1),
            Value::BigInteger(1),
            Value::Real(0.0f32),
            Value::Real(0.456f32),
            Value::BigReal(0.123f64),
            Value::BigReal(0.0f64),
            // TODO:
            // Value::Numeric(dec!(1)),
            // Value::Json(json!({"foo":1})),
            // Value::Other("bpchar".to_string(), vec![97], Some("a".to_string())),
        ]
        .iter()
        .enumerate()
        {
            test_map.insert(value.clone(), i);
            assert_eq!(*test_map.get(&value).unwrap(), i);
        }
    }

    #[cfg(feature = "rusqlite")]
    #[tokio::test]
    async fn test_rusqlite_aliases_and_builtin_functions() {
        let pool = AnyPool::connect(":memory:").await.unwrap();
        pool.execute_batch(
            "DROP TABLE IF EXISTS test_table_indirect;\
             CREATE TABLE test_table_indirect (\
                 text_value TEXT,\
                 alt_text_value TEXT,\
                 float_value FLOAT8,\
                 int_value INT8,\
                 bool_value BOOL\
             )",
        )
        .await
        .unwrap();
        pool.execute(
            r#"INSERT INTO test_table_indirect
               (text_value, alt_text_value, float_value, int_value, bool_value)
               VALUES (?1, ?2, ?3, ?4, ?5)"#,
            values!["foo", Value::Null, 1.05_f64, 1_i64, true],
        )
        .await
        .unwrap();

        // Test aggregate:
        let rows = pool
            .query("SELECT MAX(int_value) FROM test_table_indirect", ())
            .await
            .unwrap();
        assert_eq!(
            rows.rows,
            [row! {
                "MAX(int_value)" => 1_i64,
            }]
        );

        // Test alias:
        let rows = pool
            .query(
                "SELECT bool_value AS bool_value_alias FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(rows.rows, [row! {"bool_value_alias" => 1_i64,}]);

        // Test aggregate with alias:
        let rows = pool
            .query(
                "SELECT MAX(int_value) AS max_int_value FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        // Note that the alias is not shown in the results:
        assert_eq!(rows.rows, [row! {"max_int_value" => 1_i64,}]);

        // Test non-aggregate function:
        let rows = pool
            .query(
                "SELECT CAST(int_value AS TEXT) FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows.rows,
            [row! {
                "CAST(int_value AS TEXT)" => "1",
            }]
        );

        // Test non-aggregate function with alias:
        let rows = pool
            .query(
                "SELECT CAST(int_value AS TEXT) AS int_value_cast FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows.rows,
            [row! {
                "int_value_cast" => "1",
            }]
        );

        // Test functions over booleans:
        let rows = pool
            .query("SELECT MAX(bool_value) FROM test_table_indirect", ())
            .await
            .unwrap();
        // It is not possible to represent the boolean result of an aggregate function as a
        // boolean, since internally to sqlite it is stored as an integer, and we can't query
        // the metadata to get the datatype of an expression. If we want to represent it as a
        // boolean, we will need to parse the expression. Note that PostgreSQL does not support
        // MAX(bool_value) - it gives the error:
        //   ERROR: function max(boolean) does not exist\nHINT: No function matches the given
        //          name and argument types. You might need to add explicit type casts.
        // So, perhaps, this is tu quoque an argument that the behaviour below is acceptable for
        // sqlite.
        assert_eq!(
            rows.rows,
            [row! {
                "MAX(bool_value)" => 1_i64,
            }]
        );
    }

    /// This test is resource intensive and therefore ignored by default. It verifies that
    /// using [MAX_PARAMS_SQLITE] parameters in a query is indeed supported.
    /// To run this and other ignored tests, use `cargo test -- --ignored` or
    /// `cargo test -- --include-ignored`
    #[cfg(feature = "rusqlite")]
    #[tokio::test]
    // #[ignore]
    async fn test_rusqlite_max_params() {
        let pool = AnyPool::connect(":memory:").await.unwrap();
        pool.execute_batch(
            "DROP TABLE IF EXISTS test_max_params;\
             CREATE TABLE test_max_params (\
                 column1 INT,\
                 column2 INT,\
                 column3 INT,\
                 column4 INT,\
                 column5 INT,\
                 column6 INT\
             )",
        )
        .await
        .unwrap();

        let mut sql = "INSERT INTO test_max_params VALUES ".to_string();
        let mut values = vec![];
        let mut params = vec![];
        let mut n = 1;
        while n <= MAX_PARAMS_SQLITE {
            values.push(format!(
                "(?{}, ?{}, ?{}, ?{}, ?{}, ?{})",
                n,
                n + 1,
                n + 2,
                n + 3,
                n + 4,
                n + 5
            ));
            params.push(1);
            params.push(1);
            params.push(1);
            params.push(1);
            params.push(1);
            params.push(1);
            n += 6;
        }
        sql.push_str(&values.join(", "));
        pool.execute(&sql, params).await.unwrap();
        pool.drop_table("text_max_params").await.unwrap();
    }

    #[cfg(feature = "rusqlite")]
    #[tokio::test]
    async fn test_rusqlite_match() {
        let conn = AnyPool::connect("test_match_columns.db").await.unwrap();
        conn.execute_batch(
            "DROP TABLE IF EXISTS test_table_match;\
             CREATE TABLE test_table_match (\
                 text_value TEXT,\
                 alt_text_value TEXT\
             )",
        )
        .await
        .unwrap();
        conn.execute(
            r#"INSERT INTO test_table_match
               (text_value, alt_text_value)
               VALUES ($1, $2)"#,
            &["foo", "123"],
        )
        .await
        .unwrap();

        let value: String = conn
            .query(
                "SELECT text_value from test_table_match
                 WHERE regexp_match(text_value, $1) = 1",
                values!["foo"],
            )
            .await
            .unwrap()
            .try_into_value()
            .unwrap();

        assert_eq!(value, "foo");

        let value: String = conn
            .query(
                r#"SELECT alt_text_value from test_table_match
                   WHERE regexp_match(alt_text_value, '\d+') = 1"#,
                (),
            )
            .await
            .unwrap()
            .try_into_value()
            .unwrap();

        assert_eq!(value, "123");
    }

    #[tokio::test]
    async fn test_postgres_aliases_and_builtin_functions() {
        let pool = AnyPool::connect("postgresql:///rltbl_db").await.unwrap();
        pool.execute_batch(
            "DROP TABLE IF EXISTS test_table_indirect CASCADE;\
             CREATE TABLE test_table_indirect (\
                 text_value TEXT,\
                 alt_text_value TEXT,\
                 float_value FLOAT8,\
                 int_value INT8,\
                 bool_value BOOL\
             )",
        )
        .await
        .unwrap();
        pool.execute(
            r#"INSERT INTO test_table_indirect
               (text_value, alt_text_value, float_value, int_value, bool_value)
               VALUES ($1, $2, $3, $4, $5)"#,
            values!["foo", Value::Null, 1.05_f64, 1_i64, true],
        )
        .await
        .unwrap();

        // Test aggregate:
        let rows = pool
            .query("SELECT MAX(int_value) FROM test_table_indirect", ())
            .await
            .unwrap();
        assert_eq!(
            rows.rows,
            [row! {
                "max" => 1_i64,
            }]
        );

        // Test alias:
        let rows = pool
            .query(
                "SELECT bool_value AS bool_value_alias FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows.rows,
            [row! {
                "bool_value_alias" => true,
            }]
        );

        // Test aggregate with alias:
        let rows = pool
            .query(
                "SELECT MAX(int_value) AS max_int_value FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows.rows,
            [row! {
                "max_int_value" => 1_i64,
            }]
        );

        // Test non-aggregate function:
        let rows = pool
            .query(
                "SELECT CAST(int_value AS TEXT) FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows.rows,
            [row! {
                "int_value" => "1",
            }]
        );

        // Test non-aggregate function with alias:
        let rows = pool
            .query(
                "SELECT CAST(int_value AS TEXT) AS int_value_cast FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows.rows,
            [row! {
                "int_value_cast" => "1",
            }]
        );

        // Clean up.
        pool.drop_table("test_table_indirect").await.unwrap();
    }

    /// This test is resource intensive and therefore ignored by default. It verifies that
    /// using [MAX_PARAMS_POSTGRES] parameters in a query is indeed supported.
    /// To run this and other ignored tests, use `cargo test -- --ignored` or
    /// `cargo test -- --include-ignored`
    #[tokio::test]
    // #[ignore]
    async fn test_postgres_max_params() {
        let pool = AnyPool::connect("postgresql:///rltbl_db").await.unwrap();

        pool.execute_batch(
            "DROP TABLE IF EXISTS test_max_params CASCADE;\
             CREATE TABLE test_max_params (\
                 column1 INT,\
                 column2 INT,\
                 column3 INT,\
                 column4 INT,\
                 column5 INT\
             )",
        )
        .await
        .unwrap();

        let mut sql = "INSERT INTO test_max_params VALUES ".to_string();
        let mut values = vec![];
        let mut params = vec![];
        let mut n = 1;
        while n <= MAX_PARAMS_POSTGRES {
            values.push(format!(
                "(${}, ${}, ${}, ${}, ${})",
                n,
                n + 1,
                n + 2,
                n + 3,
                n + 4
            ));
            params.push(1);
            params.push(1);
            params.push(1);
            params.push(1);
            params.push(1);
            n += 5;
        }
        sql.push_str(&values.join(", "));
        pool.execute(&sql, params).await.unwrap();
        pool.drop_table("text_max_params").await.unwrap();
    }

    #[tokio::test]
    async fn test_postgres_special_floats() {
        let pool = AnyPool::connect("postgresql:///rltbl_db").await.unwrap();
        pool.drop_table("test_special_floats").await.unwrap();
        pool.execute(
            r#"CREATE TABLE test_special_floats (bar FLOAT, pseudo_bar TEXT)"#,
            (),
        )
        .await
        .unwrap();
        pool.execute(r#"insert into test_special_floats values (+0, '+0')"#, ())
            .await
            .unwrap();
        pool.execute(r#"insert into test_special_floats values (-0, '-0')"#, ())
            .await
            .unwrap();
        for value in ["Infinity", "-Infinity", "NaN"] {
            // Without params:
            let quoted_value = format!("'{value}'");
            pool.execute(
                &format!(
                    r#"insert into test_special_floats values ({quoted_value}, {quoted_value})"#
                ),
                (),
            )
            .await
            .unwrap();

            // With params:
            let float_param = f64::from_str(value).unwrap();
            pool.execute(
                r#"insert into test_special_floats values ($1, $2)"#,
                values![float_param, value],
            )
            .await
            .unwrap();
        }

        let value = pool
            .query("select max(bar) from test_special_floats", ())
            .await
            .unwrap();
        let value = value.try_into_value().unwrap();
        match value {
            Value::BigReal(num) if num.is_nan() => (),
            _ => panic!(),
        };

        let value = pool
            .query("select max(pseudo_bar) from test_special_floats", ())
            .await
            .unwrap();
        let value = value.try_into_value().unwrap();
        match value {
            Value::Text(txt) if txt == "NaN" => (),
            _ => panic!(),
        };

        let rows = pool
            .query(
                r#"select bar from test_special_floats where bar = $1"#,
                values![Value::BigReal(f64::NEG_INFINITY)],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);

        let value = rows[0].get("bar").unwrap();
        match value {
            Value::BigReal(num) => {
                assert!(num.is_sign_negative());
                assert!(num.is_infinite());
            }
            _ => panic!(),
        };

        let rows = pool
            .query(
                r#"select pseudo_bar from test_special_floats where pseudo_bar = $1"#,
                &["-Infinity"],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);

        let value = rows[0].get("pseudo_bar").unwrap();
        match value {
            Value::Text(txt) => assert_eq!(txt, "-Infinity"),
            _ => panic!(),
        };

        pool.drop_table("test_special_floats").await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_other_types() {
        /*
        let pool = AnyPool::connect("postgresql:///rltbl_db")
            .await
            .unwrap();

        // CHAR
        pool.drop_table("test_other_types").await.unwrap();
        pool.execute(
            r#"CREATE TABLE test_other_types (bar CHAR, foo BOOL DEFAULT FALSE)"#,
            (),
        )
        .await
        .unwrap();
        pool.execute(r#"INSERT INTO test_other_types VALUES ('a')"#, ())
            .await
            .unwrap();

        // Get the value that was just inserted and use it to edit the table and verify the result:
        let mut db_rows = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_rows.rows.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            "Row { \
             map: {\
             \"bar\": Other(\"bpchar\", [97], Some(\"a\")), \
             \"foo\": Boolean(false)} \
             }"
        );
        let db_value = db_row.get("bar").unwrap();
        pool.execute(
            r#"UPDATE test_other_types SET foo = TRUE WHERE bar = $1"#,
            values![db_value],
        )
        .await
        .unwrap();
        let mut db_rows = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_rows.rows.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            "Row { \
             map: {\
             \"bar\": Other(\"bpchar\", [97], Some(\"a\")), \
             \"foo\": Boolean(true)} \
             }"
        );

        // BYTEA
        pool.drop_table("test_other_types").await.unwrap();
        pool.execute(
            r#"CREATE TABLE test_other_types (bar BYTEA, foo BOOL DEFAULT FALSE)"#,
            (),
        )
        .await
        .unwrap();
        pool.execute(r#"INSERT INTO test_other_types VALUES ('\xDEADBEEF')"#, ())
            .await
            .unwrap();

        // Get the value that was just inserted and use it to edit the table and verify the result:
        let mut db_rows = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_rows.rows.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            "Row { \
             map: {\
             \"bar\": Other(\"bytea\", [222, 173, 190, 239], None), \
             \"foo\": Boolean(false)} \
             }"
        );
        let db_value = db_row.get("bar").unwrap();
        pool.execute(
            r#"UPDATE test_other_types SET foo = TRUE WHERE bar = $1"#,
            values![db_value],
        )
        .await
        .unwrap();
        let mut db_rows = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_rows.rows.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            "Row { \
             map: {\
             \"bar\": Other(\"bytea\", [222, 173, 190, 239], None), \
             \"foo\": Boolean(true)} \
             }"
        );

        // TIMESTAMP
        pool.drop_table("test_other_types").await.unwrap();
        pool.execute(
            r#"CREATE TABLE test_other_types (bar TIMESTAMP, foo BOOL DEFAULT FALSE)"#,
            (),
        )
        .await
        .unwrap();
        pool.execute(
            r#"INSERT INTO test_other_types VALUES ('2004-10-19 10:23:54')"#,
            (),
        )
        .await
        .unwrap();

        // Get the value that was just inserted and use it to edit the table and verify the result:
        let mut db_rows = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_rows.rows.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            "Row { \
             map: {\"bar\": Other(\"timestamp\", [0, 0, 137, 201, 15, 13, 226, 128], None), \
             \"foo\": Boolean(false)} \
             }"
        );
        let db_value = db_row.get("bar").unwrap();
        pool.execute(
            r#"UPDATE test_other_types SET foo = TRUE WHERE bar = $1"#,
            values![db_value],
        )
        .await
        .unwrap();
        let mut db_rows = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_rows.rows.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            "Row { \
             map: {\
             \"bar\": Other(\"timestamp\", [0, 0, 137, 201, 15, 13, 226, 128], None), \
             \"foo\": Boolean(true)} \
             }"
        );

        // DATE
        pool.drop_table("test_other_types").await.unwrap();
        pool.execute(
            r#"CREATE TABLE test_other_types (bar DATE, foo BOOL DEFAULT FALSE)"#,
            (),
        )
        .await
        .unwrap();
        pool.execute(r#"INSERT INTO test_other_types VALUES ('1724-04-22')"#, ())
            .await
            .unwrap();

        // Get the value that was just inserted and use it to edit the table and verify the result:
        let mut db_rows = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_rows.rows.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            "Row { \
             map: {\
             \"bar\": Other(\
             \"date\", [255, 254, 118, 169], None), \
             \"foo\": Boolean(false)} \
             }"
        );
        let db_value = db_row.get("bar").unwrap();
        pool.execute(
            r#"UPDATE test_other_types SET foo = TRUE WHERE bar = $1"#,
            values![db_value],
        )
        .await
        .unwrap();
        let mut db_rows = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_rows.rows.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            "Row { \
             map: {\
             \"bar\": Other(\
             \"date\", [255, 254, 118, 169], None), \
             \"foo\": Boolean(true)} \
             }"
        );

        // TIME
        pool.drop_table("test_other_types").await.unwrap();
        pool.execute(
            r#"CREATE TABLE test_other_types (bar TIME, foo BOOL DEFAULT FALSE)"#,
            (),
        )
        .await
        .unwrap();
        pool.execute(r#"INSERT INTO test_other_types VALUES ('23:59')"#, ())
            .await
            .unwrap();

        // Get the value that was just inserted and use it to edit the table and verify the result:
        let mut db_rows = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_rows.rows.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            "Row { \
             map: {\
             \"bar\": Other(\
             \"time\", [0, 0, 0, 20, 26, 67, 217, 0], None), \
             \"foo\": Boolean(false)} \
             }"
        );
        let db_value = db_row.get("bar").unwrap();
        pool.execute(
            r#"UPDATE test_other_types SET foo = TRUE WHERE bar = $1"#,
            values![db_value],
        )
        .await
        .unwrap();
        let mut db_rows = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_rows.rows.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            "Row { \
             map: {\
             \"bar\": Other(\
             \"time\", [0, 0, 0, 20, 26, 67, 217, 0], None), \
             \"foo\": Boolean(true)} \
             }"
        );

        // TEXT[]
        pool.drop_table("test_other_types").await.unwrap();
        pool.execute(
            r#"CREATE TABLE test_other_types (bar TEXT[], foo BOOL DEFAULT FALSE)"#,
            (),
        )
        .await
        .unwrap();
        pool.execute(
            r#"INSERT INTO test_other_types VALUES ('{"meeting", "lunch"}')"#,
            (),
        )
        .await
        .unwrap();

        // Get the value that was just inserted and use it to edit the table and verify the result:
        let mut db_rows = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_rows.rows.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            "Row { \
             map: {\
             \"bar\": Other(\
             \"_text\", \
             [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 25, 0, 0, \
             0, 2, 0, 0, 0, 1, 0, 0, 0, 7, 109, 101, 101, 116, 105, 110, 103, 0, 0, 0, 5, 108, \
             117, 110, 99, 104], \
             Some(\"\\0\\0\\0\\u{1}\\0\\0\\0\\0\\0\\0\\0\\u{19}\\0\\0\\0\\u{2}\\0\\0\\0\
             \\u{1}\\0\\0\\0\\u{7}meeting\\0\\0\\0\\u{5}lunch\")), \
             \"foo\": Boolean(false)} \
             }"
        );
        let db_value = db_row.get("bar").unwrap();
        pool.execute(
            r#"UPDATE test_other_types SET foo = TRUE WHERE bar = $1"#,
            values![db_value],
        )
        .await
        .unwrap();
        let mut db_rows = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_rows.rows.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            "Row { \
             map: {\
             \"bar\": Other(\
             \"_text\", \
             [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 25, 0, 0, \
             0, 2, 0, 0, 0, 1, 0, 0, 0, 7, 109, 101, 101, 116, 105, 110, 103, 0, 0, 0, 5, 108, \
             117, 110, 99, 104], \
             Some(\"\\0\\0\\0\\u{1}\\0\\0\\0\\0\\0\\0\\0\\u{19}\\0\\0\\0\\u{2}\\0\\0\\0\
             \\u{1}\\0\\0\\0\\u{7}meeting\\0\\0\\0\\u{5}lunch\")), \
             \"foo\": Boolean(true)} \
             }"
        );

        // INT8[]
        pool.drop_table("test_other_types").await.unwrap();
        pool.execute(
            r#"CREATE TABLE test_other_types (bar INT8[], foo BOOL DEFAULT FALSE)"#,
            (),
        )
        .await
        .unwrap();
        pool.execute(r#"INSERT INTO test_other_types VALUES ('{1, 2}')"#, ())
            .await
            .unwrap();

        // Get the value that was just inserted and use it to edit the table and verify the result:
        let mut db_rows = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_rows.rows.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            "Row { \
             map: {\
             \"bar\": Other(\
             \"_int8\", \
             [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 8, 0, 0, \
             0, 0, 0, 0, 0, 1, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 2], \
             Some(\"\\0\\0\\0\\u{1}\\0\\0\\0\\0\\0\\0\\0\\u{14}\\0\\0\\0\\u{2}\\0\\0\\0\
             \\u{1}\\0\\0\\0\\u{8}\\0\\0\\0\\0\\0\\0\\0\\u{1}\\0\\0\\0\\u{8}\\0\\0\\0\\0\
             \\0\\0\\0\\u{2}\")), \
             \"foo\": Boolean(false)} \
             }"
        );
        let db_value = db_row.get("bar").unwrap();
        pool.execute(
            r#"UPDATE test_other_types SET foo = TRUE WHERE bar = $1"#,
            values![db_value],
        )
        .await
        .unwrap();
        let mut db_rows = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_rows.rows.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            "Row { \
             map: {\
             \"bar\": Other(\
             \"_int8\", \
             [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 8, 0, 0, \
             0, 0, 0, 0, 0, 1, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 2], \
             Some(\"\\0\\0\\0\\u{1}\\0\\0\\0\\0\\0\\0\\0\\u{14}\\0\\0\\0\\u{2}\\0\\0\\0\
             \\u{1}\\0\\0\\0\\u{8}\\0\\0\\0\\0\\0\\0\\0\\u{1}\\0\\0\\0\\u{8}\\0\\0\\0\\0\
             \\0\\0\\0\\u{2}\")), \
             \"foo\": Boolean(true)} \
             }"
        );

        // FLOAT8[]
        pool.drop_table("test_other_types").await.unwrap();
        pool.execute(
            r#"CREATE TABLE test_other_types (bar FLOAT8[], foo BOOL DEFAULT FALSE)"#,
            (),
        )
        .await
        .unwrap();
        pool.execute(r#"INSERT INTO test_other_types VALUES ('{1, 2}')"#, ())
            .await
            .unwrap();

        // Get the value that was just inserted and use it to edit the table and verify the result:
        let mut db_rows = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_rows.rows.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            "Row { \
             map: {\
             \"bar\": Other(\
             \"_float8\", \
             [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 189, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 8, 63, \
             240, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0, 0], None), \
             \"foo\": Boolean(false)} \
             }"
        );
        let db_value = db_row.get("bar").unwrap();
        pool.execute(
            r#"UPDATE test_other_types SET foo = TRUE WHERE bar = $1"#,
            values![db_value],
        )
        .await
        .unwrap();
        let mut db_rows = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_rows.rows.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            "Row { \
             map: {\
             \"bar\": Other(\
             \"_float8\", \
             [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2, 189, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 8, 63, \
             240, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0, 0], None), \
             \"foo\": Boolean(true)} \
             }"
        );

        // NUMERIC[]
        pool.drop_table("test_other_types").await.unwrap();
        pool.execute(
            r#"CREATE TABLE test_other_types (bar NUMERIC[], foo BOOL DEFAULT FALSE)"#,
            (),
        )
        .await
        .unwrap();
        pool.execute(r#"INSERT INTO test_other_types VALUES ('{1, 2}')"#, ())
            .await
            .unwrap();

        // Get the value that was just inserted and use it to edit the table and verify the result:
        let mut db_rows = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_rows.rows.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            "Row { \
             map: {\
             \"bar\": Other(\
             \"_numeric\", \
             [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 6, 164, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 10, 0, 1, \
             0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2], None), \
             \"foo\": Boolean(false)} \
             }"
        );
        let db_value = db_row.get("bar").unwrap();
        pool.execute(
            r#"UPDATE test_other_types SET foo = TRUE WHERE bar = $1"#,
            values![db_value],
        )
        .await
        .unwrap();
        let mut db_rows = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_rows.rows.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            "Row { \
             map: {\
             \"bar\": Other(\
             \"_numeric\", \
             [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 6, 164, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 10, 0, 1, \
             0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2], None), \
             \"foo\": Boolean(true)} \
             }"
        );

        // CIRCLE
        pool.drop_table("test_other_types").await.unwrap();
        pool.execute(
            r#"CREATE TABLE test_other_types (bar CIRCLE, foo BOOL DEFAULT FALSE)"#,
            (),
        )
        .await
        .unwrap();
        pool.execute(r#"INSERT INTO test_other_types VALUES ('<(0,0),1>')"#, ())
            .await
            .unwrap();

        // Get the value that was just inserted and use it to edit the table and verify the result:
        let mut db_rows = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_rows.rows.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            "Row { \
             map: {\
             \"bar\": Other(\
             \"circle\", \
             [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 63, 240, 0, 0, 0, 0, 0, 0], None), \
             \"foo\": Boolean(false)} \
             }"
        );
        let db_value = db_row.get("bar").unwrap();
        pool.execute(
            r#"UPDATE test_other_types SET foo = TRUE WHERE bar = $1"#,
            values![db_value],
        )
        .await
        .unwrap();
        let mut db_rows = pool
            .query(r#"SELECT * FROM test_other_types"#, ())
            .await
            .unwrap();
        let db_row = db_rows.rows.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            "Row { \
             map: {\
             \"bar\": Other(\
             \"circle\", \
             [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 63, 240, 0, 0, 0, 0, 0, 0], None), \
             \"foo\": Boolean(true)} \
             }"
        );

        // Other types that are NULLs
        pool.drop_table("test_other_types").await.unwrap();
        pool.execute(
            r#"CREATE TABLE test_other_types (bar TIMESTAMP, foo BOOL DEFAULT FALSE)"#,
            (),
        )
        .await
        .unwrap();

        let db_rows = pool
            .query(
                r#"SELECT * FROM test_other_types WHERE bar = $1"#,
                values![Value::Null],
            )
            .await
            .unwrap();
        assert_eq!(db_rows.len(), 0);

        pool.execute(
            r#"INSERT INTO test_other_types VALUES ('2004-10-19 10:23:54')"#,
            (),
        )
        .await
        .unwrap();

        pool.execute(
            r#"UPDATE test_other_types SET bar = $1"#,
            values![Value::Null],
        )
        .await
        .unwrap();

        let mut db_rows = pool
            .query(
                r#"SELECT * FROM test_other_types WHERE bar IS NOT DISTINCT FROM $1"#,
                values![Value::Null],
            )
            .await
            .unwrap();
        let db_row = db_rows.rows.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            r#"Row { map: {"bar": Null, "foo": Boolean(false)} }"#
        );

        // Clean up.
        pool.drop_table("test_other_types").await.unwrap();
        */
    }

    #[tokio::test]
    #[ignore]
    async fn test_jsonb() {
        /*
        let pool = TokioPostgresPool::connect("postgresql:///rltbl_db")
            .await
            .unwrap();

        pool.drop_table("test_jsonb").await.unwrap();
        pool.execute(
            r#"CREATE TABLE test_jsonb (bar JSONB, foo BOOL DEFAULT FALSE)"#,
            (),
        )
        .await
        .unwrap();
        pool.execute(r#"INSERT INTO test_jsonb VALUES ('["foo", 1]')"#, ())
            .await
            .unwrap();

        // Get the value that was just inserted and use it to edit the table and verify the result:
        let mut db_rows = pool.query(r#"SELECT * FROM test_jsonb"#, ()).await.unwrap();
        let db_row = db_rows.rows.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            "Row { \
             map: {\
             \"bar\": Json(Array [String(\"foo\"), Number(1)]), \
             \"foo\": Boolean(false)} \
             }"
        );

        let db_value = db_row.get("bar").unwrap();

        pool.execute(
            r#"UPDATE test_jsonb SET foo = TRUE WHERE bar = $1"#,
            values![db_value],
        )
        .await
        .unwrap();

        let mut db_rows = pool.query(r#"SELECT * FROM test_jsonb"#, ()).await.unwrap();
        let db_row = db_rows.rows.pop().unwrap();
        assert_eq!(
            format!("{db_row:?}"),
            "Row { \
             map: {\
             \"bar\": Json(Array [String(\"foo\"), Number(1)]), \
             \"foo\": Boolean(true)} \
             }"
        );
         */
    }
}
