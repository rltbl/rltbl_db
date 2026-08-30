#[cfg(test)]
mod tests {
    use indexmap::IndexMap;
    use rust_decimal::dec;
    use std::str::FromStr;

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

        assert_eq!(pool.query_cache_size().await.unwrap(), 1);
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
            CachingStrategy::Memory(_) => assert_eq!(pool.query_cache_size().await.unwrap(), 1),
            _ => assert_eq!(pool.query_cache_size().await.unwrap(), 1),
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
            CachingStrategy::Memory(_) => assert_eq!(pool.query_cache_size().await.unwrap(), 0),
            _ => assert_eq!(pool.query_cache_size().await.unwrap(), 0),
        };

        let rows = pool
            .cache("SELECT * from test_table_caching_1", ())
            .await
            .unwrap();

        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => assert_eq!(pool.query_cache_size().await.unwrap(), 1),
            _ => assert_eq!(pool.query_cache_size().await.unwrap(), 1),
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
            CachingStrategy::Memory(_) => assert_eq!(pool.query_cache_size().await.unwrap(), 3),
            _ => assert_eq!(pool.query_cache_size().await.unwrap(), 3),
        };

        pool.execute(
            r#"INSERT INTO test_table_caching_1 VALUES ('rho'), ('sigma')"#,
            (),
        )
        .await
        .unwrap();

        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => assert_eq!(pool.query_cache_size().await.unwrap(), 1),
            CachingStrategy::Truncate | CachingStrategy::Trigger => {
                assert_eq!(pool.query_cache_size().await.unwrap(), 1)
            }
            CachingStrategy::TruncateAll => assert_eq!(pool.query_cache_size().await.unwrap(), 0),
        };

        let rows = pool
            .cache("SELECT * from test_table_caching_1", ())
            .await
            .unwrap();

        match strategy {
            CachingStrategy::None => (),
            CachingStrategy::Memory(_) => assert_eq!(pool.query_cache_size().await.unwrap(), 2),
            CachingStrategy::Truncate | CachingStrategy::Trigger => {
                assert_eq!(pool.query_cache_size().await.unwrap(), 2)
            }
            CachingStrategy::TruncateAll => assert_eq!(pool.query_cache_size().await.unwrap(), 1),
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
            CachingStrategy::Memory(_) => assert_eq!(pool.query_cache_size().await.unwrap(), 3),
            CachingStrategy::Truncate | CachingStrategy::Trigger => {
                assert_eq!(pool.query_cache_size().await.unwrap(), 3)
            }
            CachingStrategy::TruncateAll => assert_eq!(pool.query_cache_size().await.unwrap(), 2),
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
            CachingStrategy::Memory(_) => assert_eq!(pool.query_cache_size().await.unwrap(), 3),
            CachingStrategy::Truncate | CachingStrategy::Trigger => {
                assert_eq!(pool.query_cache_size().await.unwrap(), 3)
            }
            CachingStrategy::TruncateAll => assert_eq!(pool.query_cache_size().await.unwrap(), 2),
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
    // #[ignore] // TODO: This should be ignored by default?
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
}
