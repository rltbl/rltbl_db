//! PostgreSQL support for sql_json.

use crate::core::{DbError, DbQuery, JsonRow, JsonValue};

use deadpool_postgres::{Config, Pool, Runtime};
use tokio_postgres::{
    row::Row,
    types::{ToSql, Type},
    NoTls,
};

/// Represents a PostgreSQL database connection pool
pub struct PostgresConnection {
    pool: Pool,
}

impl PostgresConnection {
    /// Connect to a PostgreSQL database using the given url, which should be of the form
    /// postgresql:///DATABASE_NAME
    pub async fn connect(url: &str) -> Result<Self, DbError> {
        match url.starts_with("postgresql:///") {
            true => {
                let mut cfg = Config::new();
                let db_name = url.strip_prefix("postgresql:///").ok_or("Invalid URL")?;
                cfg.dbname = Some(db_name.to_string());
                let pool = cfg
                    .create_pool(Some(Runtime::Tokio1), NoTls)
                    .map_err(|err| format!("Error creating pool: {err}"))?;
                Ok(Self { pool })
            }
            false => Err(format!("Invalid PostgreSQL database path: '{url}'")),
        }
    }
}

/// Extracts the value at the given index from the given [Row].
fn extract_value(row: &Row, idx: usize) -> JsonValue {
    let column = &row.columns()[idx];
    match *column.type_() {
        Type::TEXT | Type::VARCHAR => {
            let value: &str = row.get(idx);
            value.into()
        }
        Type::INT2 | Type::INT4 => {
            let value: i32 = row.get(idx);
            value.into()
        }
        Type::INT8 => {
            let value: i64 = row.get(idx);
            value.into()
        }
        Type::FLOAT4 => {
            let value: f32 = row.get(idx);
            value.into()
        }
        Type::FLOAT8 => {
            let value: f64 = row.get(idx);
            value.into()
        }
        Type::NUMERIC => {
            todo!()
        }
        _ => unimplemented!(),
    }
}

impl DbQuery for PostgresConnection {
    /// Implements [DbQuery::execute()] for PostgreSQL.
    async fn execute(&self, sql: &str, params: &[JsonValue]) -> Result<(), DbError> {
        self.query(sql, params).await?;
        Ok(())
    }

    /// Implements [DbQuery::query()] for PostgreSQL.
    async fn query(&self, sql: &str, params: &[JsonValue]) -> Result<Vec<JsonRow>, DbError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| format!("Unable to get pool: {err}"))?;

        // The rust compiler needs the parameters to the query to explicitly be in scope when
        // passing them to client.execute(). So we build three vectors to keep them accessible,
        // and one more vector to represent the sequence of types, where:
        // 0 => &str
        // 1 => i64
        // 2 => f64
        let mut param_sequence: Vec<usize> = vec![];
        let mut integer_params = vec![];
        let mut string_params = vec![];
        let mut float_params = vec![];
        for param in params {
            match param {
                JsonValue::String(s) => {
                    param_sequence.push(0);
                    string_params.push(s.as_str())
                }
                JsonValue::Number(n) => match n.as_i64() {
                    Some(n) => {
                        param_sequence.push(1);
                        integer_params.push(n);
                    }
                    None => match n.as_f64() {
                        Some(n) => {
                            param_sequence.push(2);
                            float_params.push(n);
                        }
                        None => return Err(format!("Unsupported number type for {n}")),
                    },
                },
                _ => return Err(format!("Unsupported JSON type: {param}")),
            }
        }

        // Now use the three typed lists of parameters and the param_sequence to build a list of
        // parameters that implement &(dyn ToSql + Sync), which is what client.execute() needs.
        let mut strings_idx = 0;
        let mut integers_idx = 0;
        let mut floats_idx = 0;
        let mut generic_params = vec![];
        for param_code in &param_sequence {
            match param_code {
                0 => {
                    let param = &string_params[strings_idx];
                    strings_idx += 1;
                    generic_params.push(param as &(dyn ToSql + Sync));
                }
                1 => {
                    let param = &integer_params[integers_idx];
                    integers_idx += 1;
                    generic_params.push(param as &(dyn ToSql + Sync));
                }
                2 => {
                    let param = &float_params[floats_idx];
                    floats_idx += 1;
                    generic_params.push(param as &(dyn ToSql + Sync));
                }
                _ => unreachable!(),
            }
        }

        let rows = client
            .query(sql, &generic_params)
            .await
            .map_err(|err| format!("Error in query(): {err}"))?;
        let mut json_rows = vec![];
        for row in &rows {
            let mut json_row = JsonRow::new();
            let columns = row.columns();
            for (i, column) in columns.iter().enumerate() {
                json_row.insert(column.name().to_string(), extract_value(row, i));
            }
            json_rows.push(json_row);
        }
        Ok(json_rows)
    }

    /// Implements [DbQuery::query_row()] for PostgreSQL.
    async fn query_row(&self, sql: &str, params: &[JsonValue]) -> Result<JsonRow, DbError> {
        let mut rows = self.query(sql, params).await?;
        if rows.len() > 1 {
            tracing::warn!("More than one row returned for query_row()");
        }
        match rows.pop() {
            Some(row) => Ok(row),
            None => Err("No rows found".to_string()),
        }
    }

    /// Implements [DbQuery::query_value()] for PostgreSQL.
    async fn query_value(&self, sql: &str, params: &[JsonValue]) -> Result<JsonValue, DbError> {
        let row = self.query_row(sql, params).await?;
        match row.values().next() {
            Some(value) => Ok(value.clone()),
            None => Err("No values found".into()),
        }
    }

    /// Implements [DbQuery::query_string()] for PostgreSQL.
    async fn query_string(&self, sql: &str, params: &[JsonValue]) -> Result<String, DbError> {
        let value = self.query_value(sql, params).await?;
        match value.as_str() {
            Some(str_val) => Ok(str_val.to_string()),
            None => {
                tracing::warn!("Not a string: {value}");
                Ok(value.to_string())
            }
        }
    }

    /// Implements [DbQuery::query_u64()] for PostgreSQL.
    async fn query_u64(&self, sql: &str, params: &[JsonValue]) -> Result<u64, DbError> {
        let value = self.query_value(sql, params).await?;
        match value.as_u64() {
            Some(val) => Ok(val),
            None => Err(format!("Not a u64: {value}")),
        }
    }

    /// Implements [DbQuery::query_i64()] for PostgreSQL.
    async fn query_i64(&self, sql: &str, params: &[JsonValue]) -> Result<i64, DbError> {
        let value = self.query_value(sql, params).await?;
        match value.as_i64() {
            Some(val) => Ok(val),
            None => Err(format!("Not a i64: {value}")),
        }
    }

    /// Implements [DbQuery::query_f64] for PostgreSQL.
    async fn query_f64(&self, sql: &str, params: &[JsonValue]) -> Result<f64, DbError> {
        let value = self.query_value(sql, params).await?;
        match value.as_f64() {
            Some(val) => Ok(val),
            None => Err(format!("Not a f64: {value}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    #[tokio::test]
    async fn test_text_column_query() {
        let conn = PostgresConnection::connect("postgresql:///sql_json_db")
            .await
            .unwrap();
        conn.execute("DROP TABLE IF EXISTS test_table_text", &[])
            .await
            .unwrap();
        conn.execute("CREATE TABLE test_table_text ( value TEXT )", &[])
            .await
            .unwrap();
        conn.execute("INSERT INTO test_table_text VALUES ($1)", &[json!("foo")])
            .await
            .unwrap();
        let select_sql = "SELECT value FROM test_table_text WHERE value = $1";
        let value = conn
            .query_value(select_sql, &[json!("foo")])
            .await
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!("foo", value);

        let value = conn
            .query_string(select_sql, &[json!("foo")])
            .await
            .unwrap();
        assert_eq!("foo", value);

        let row = conn.query_row(select_sql, &[json!("foo")]).await.unwrap();
        assert_eq!(json!(row), json!({"value":"foo"}));

        let rows = conn.query(select_sql, &[json!("foo")]).await.unwrap();
        assert_eq!(json!(rows), json!([{"value":"foo"}]));
    }

    #[tokio::test]
    async fn test_integer_column_query() {
        let conn = PostgresConnection::connect("postgresql:///sql_json_db")
            .await
            .unwrap();
        conn.execute("DROP TABLE IF EXISTS test_table_int", &[])
            .await
            .unwrap();
        conn.execute("CREATE TABLE test_table_int ( value INT8 )", &[])
            .await
            .unwrap();
        conn.execute("INSERT INTO test_table_int VALUES ($1)", &[json!(1)])
            .await
            .unwrap();
        let select_sql = "SELECT value FROM test_table_int WHERE value = $1";
        let value = conn
            .query_value(select_sql, &[json!(1)])
            .await
            .unwrap()
            .as_i64()
            .unwrap();
        assert_eq!(1, value);

        let value = conn.query_u64(select_sql, &[json!(1)]).await.unwrap();
        assert_eq!(1, value);

        let value = conn.query_i64(select_sql, &[json!(1)]).await.unwrap();
        assert_eq!(1, value);

        let value = conn.query_string(select_sql, &[json!(1)]).await.unwrap();
        assert_eq!("1", value);

        let row = conn.query_row(select_sql, &[json!(1)]).await.unwrap();
        assert_eq!(json!(row), json!({"value":1}));

        let rows = conn.query(select_sql, &[json!(1)]).await.unwrap();
        assert_eq!(json!(rows), json!([{"value":1}]));
    }

    #[tokio::test]
    async fn test_float_column_query() {
        let conn = PostgresConnection::connect("postgresql:///sql_json_db")
            .await
            .unwrap();
        conn.execute("DROP TABLE IF EXISTS test_table_float", &[])
            .await
            .unwrap();
        conn.execute("CREATE TABLE test_table_float ( value FLOAT8 )", &[])
            .await
            .unwrap();
        conn.execute("INSERT INTO test_table_float VALUES ($1)", &[json!(1.05)])
            .await
            .unwrap();
        let select_sql = "SELECT value FROM test_table_float WHERE value > $1";
        let value = conn
            .query_value(select_sql, &[json!(1.0)])
            .await
            .unwrap()
            .as_f64()
            .unwrap();
        assert_eq!("1.05", format!("{value:.2}"));

        let value = conn.query_f64(select_sql, &[json!(1.0)]).await.unwrap();
        assert_eq!(1.05, value);

        let value = conn.query_string(select_sql, &[json!(1.0)]).await.unwrap();
        assert_eq!("1.05", value);

        let row = conn.query_row(select_sql, &[json!(1.0)]).await.unwrap();
        assert_eq!(json!(row), json!({"value":1.05}));

        let rows = conn.query(select_sql, &[json!(1.0)]).await.unwrap();
        assert_eq!(json!(rows), json!([{"value":1.05}]));
    }

    #[tokio::test]
    async fn test_mixed_column_query() {
        let conn = PostgresConnection::connect("postgresql:///sql_json_db")
            .await
            .unwrap();
        conn.execute("DROP TABLE IF EXISTS test_table_mixed", &[])
            .await
            .unwrap();
        conn.execute(
            r#"CREATE TABLE test_table_mixed (
                 text_value TEXT,
                 float_value FLOAT8,
                 int_value INT8
               )"#,
            &[],
        )
        .await
        .unwrap();
        conn.execute(
            r#"INSERT INTO test_table_mixed
               (text_value, float_value, int_value)
               VALUES ($1, $2, $3)"#,
            &[json!("foo"), json!(1.05), json!(1)],
        )
        .await
        .unwrap();

        let select_sql = r#"SELECT text_value, float_value, int_value
                            FROM test_table_mixed
                            WHERE text_value = $1 AND float_value > $2 AND int_value > $3"#;
        let params = [json!("foo"), json!(1.0), json!(0)];
        let value = conn
            .query_value(select_sql, &params)
            .await
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!("foo", value);

        let row = conn.query_row(select_sql, &params).await.unwrap();
        assert_eq!(
            json!(row),
            json!({
                "text_value": "foo",
                "float_value": 1.05,
                "int_value": 1,
            })
        );

        let rows = conn.query(select_sql, &params).await.unwrap();
        assert_eq!(
            json!(rows),
            json!([{
                "text_value": "foo",
                "float_value": 1.05,
                "int_value": 1,
            }])
        );
    }
}
