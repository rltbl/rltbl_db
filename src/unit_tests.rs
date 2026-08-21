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
                "postgres" => " CASCADE",
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
                "postgres" => " CASCADE",
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
                "postgres" => " CASCADE",
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

        let rows = pool.query("SELECT * from test_update", &[]).await.unwrap();
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
        pool.execute("DELETE FROM test_update_returning", &[])
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
            .query("SELECT * from test_update_returning", &[])
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

        let rows = pool.query("SELECT * from test_upsert", &[]).await.unwrap();
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
        // TODO:
        // #[cfg(feature = "libsql")]
        // {
        //     let mut pool = AnyPool::connect(":memory:").await.unwrap();
        //     for caching_strategy in &all_strategies {
        //         table_caching(&mut pool, &caching_strategy).await;
        //     }
        //     for strategy in &all_strategies {
        //         view_caching(&mut pool, strategy).await;
        //     }
        // }
    }

    #[allow(unused)]
    async fn table_caching(pool: &mut AnyPool, _strategy: &CachingStrategy) {
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

        // TODO:
        // pool.set_caching_strategy(strategy);
        // pool.set_cache_aware_query(true);
        // etc.
    }

    #[allow(unused)]
    async fn view_caching(_pool: &mut AnyPool, _strategy: &CachingStrategy) {
        // TODO ...
    }
}
