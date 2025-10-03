//! PostgreSQL support for sql_json.

use crate::core::{DbConnection, DbError, JsonRow, JsonValue};

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
    pub async fn connect(url: &str) -> Result<impl DbConnection, DbError> {
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

impl DbConnection for PostgresConnection {
    /// Implements [DbConnection::execute()] for PostgreSQL.
    async fn execute(&self, sql: &str, params: &[JsonValue]) -> Result<(), DbError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| format!("Unable to get pool: {err}"))?;
        let params = params
            .iter()
            .map(|p| {
                match p {
                    JsonValue::String(s) => s as &(dyn ToSql + Sync),
                    //JsonValue::Number(p) => {
                    //    p as &(dyn tokio_postgres::types::ToSql + Sync)
                    //},
                    //JsonValue::Number(p) => match p.as_f64() {
                    //    Some(p) => p.clone() as &(dyn ToSql + Sync),
                    //    None => panic!("Ouff!"),
                    //},
                    _ => panic!("Ooof!"),
                }
            })
            .collect::<Vec<_>>();
        client
            .execute(sql, params.as_ref())
            .await
            .map_err(|err| format!("Error in execute(): {err}"))?;
        Ok(())
    }

    /// Implements [DbConnection::query()] for PostgreSQL.
    async fn query(&self, sql: &str, params: &[JsonValue]) -> Result<Vec<JsonRow>, DbError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| format!("Unable to get pool: {err}"))?;
        let params = params
            .iter()
            .map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect::<Vec<_>>();
        let rows = client
            .query(sql, params.as_ref())
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

    /// Implements [DbConnection::query_row()] for PostgreSQL.
    async fn query_row(&self, sql: &str, params: &[JsonValue]) -> Result<JsonRow, DbError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| format!("Unable to get pool: {err}"))?;
        let params = params
            .iter()
            .map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect::<Vec<_>>();
        let rows = client
            .query(sql, params.as_ref())
            .await
            .map_err(|err| format!("Error in query_row(): {err}"))?;
        if rows.is_empty() {
            return Err("No rows found".to_string());
        }
        if rows.len() > 1 {
            tracing::warn!("More than one row returned for query_row()");
        }
        let row = rows[0].clone();
        let mut json_row = JsonRow::new();
        let columns = row.columns();
        for (i, column) in columns.iter().enumerate() {
            json_row.insert(column.name().to_string(), extract_value(&row, i));
        }
        Ok(json_row)
    }

    /// Implements [DbConnection::query_value()] for PostgreSQL.
    async fn query_value(&self, sql: &str, params: &[JsonValue]) -> Result<JsonValue, DbError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| format!("Unable to get pool: {err}"))?;
        let params = params
            .iter()
            .map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect::<Vec<_>>();
        let rows = client
            .query(sql, params.as_ref())
            .await
            .map_err(|err| format!("Error in query_value(): {err}"))?;
        if rows.len() > 1 {
            tracing::warn!("More than one row returned for query_value()");
        }
        match rows.iter().next() {
            Some(row) => Ok(extract_value(&row, 0)),
            None => Err("No rows found".to_string()),
        }
    }

    /// Implements [DbConnection::query_string()] for PostgreSQL.
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

    /// Implements [DbConnection::query_u64()] for PostgreSQL.
    async fn query_u64(&self, sql: &str, params: &[JsonValue]) -> Result<u64, DbError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| format!("Unable to get pool: {err}"))?;
        let params = params
            .iter()
            .map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect::<Vec<_>>();
        let rows = client
            .query(sql, params.as_ref())
            .await
            .map_err(|err| format!("Error in query_u64(): {err}"))?;
        if rows.len() > 1 {
            tracing::warn!("More than one row returned for query_u64()");
        }
        match rows.iter().next() {
            Some(row) => {
                let column = &row.columns()[0];
                let value = match *column.type_() {
                    Type::INT2 | Type::INT4 => {
                        let value: i32 = row.get(0);
                        if value < 0 {
                            return Err(format!("Invalid value: {value}"));
                        }
                        value as u64
                    }
                    Type::INT8 => {
                        let value: i64 = row.get(0);
                        if value < 0 {
                            return Err(format!("Invalid value: {value}"));
                        }
                        let value = u64::try_from(value)
                            .map_err(|err| format!("Can't convert to u64: {value}: {err}"))?;
                        value
                    }
                    _ => return Err(format!("Cannot convert to u64: {}", column.type_())),
                };
                Ok(value)
            }
            None => return Err("No rows found".to_string()),
        }
    }

    /// Implements [DbConnection::query_i64()] for PostgreSQL.
    async fn query_i64(&self, sql: &str, params: &[JsonValue]) -> Result<i64, DbError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| format!("Unable to get pool: {err}"))?;
        let params = params
            .iter()
            .map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect::<Vec<_>>();
        let rows = client
            .query(sql, params.as_ref())
            .await
            .map_err(|err| format!("Error in query_i64(): {err}"))?;
        if rows.len() > 1 {
            tracing::warn!("More than one row returned for query_i64()");
        }
        match rows.iter().next() {
            Some(row) => {
                let column = &row.columns()[0];
                let value = match *column.type_() {
                    Type::INT2 | Type::INT4 => {
                        let value: i32 = row.get(0);
                        value as i64
                    }
                    Type::INT8 => {
                        let value: i64 = row.get(0);
                        value
                    }
                    _ => return Err(format!("Cannot convert to i64: {}", column.type_())),
                };
                Ok(value)
            }
            None => return Err("No rows found".to_string()),
        }
    }

    /// Implements [DbConnection::query_f64] for PostgreSQL.
    async fn query_f64(&self, sql: &str, params: &[JsonValue]) -> Result<f64, DbError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|err| format!("Unable to get pool: {err}"))?;
        let params = params
            .iter()
            .map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect::<Vec<_>>();
        let rows = client
            .query(sql, params.as_ref())
            .await
            .map_err(|err| format!("Error in query_i64(): {err}"))?;
        if rows.len() > 1 {
            tracing::warn!("More than one row returned for query_f64()");
        }
        match rows.iter().next() {
            Some(row) => {
                let column = &row.columns()[0];
                let value = match *column.type_() {
                    Type::FLOAT4 => {
                        let value: f32 = row.get(0);
                        value as f64
                    }
                    Type::FLOAT8 => {
                        let value: f64 = row.get(0);
                        value
                    }
                    _ => return Err(format!("Cannot convert to f64: {}", column.type_())),
                };
                Ok(value)
            }
            None => return Err("No rows found".to_string()),
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
        // let select_sql = "SELECT value FROM test_table_text WHERE value = $1";
        // let value = conn
        //     .query_value(select_sql, &[json!("foo")])
        //     .await
        //     .unwrap()
        //     .as_str()
        //     .unwrap()
        //     .to_string();
        // assert_eq!("foo", value);

        // let value = conn
        //     .query_string(select_sql, &[json!("foo")])
        //     .await
        //     .unwrap();
        // assert_eq!("foo", value);

        // let row = conn.query_row(select_sql, &[json!("foo")]).await.unwrap();
        // assert_eq!(json!(row), json!({"value":"foo"}));

        // let rows = conn.query(select_sql, &[json!("foo")]).await.unwrap();
        // assert_eq!(json!(rows), json!([{"value":"foo"}]));
    }

    #[ignore]
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

    #[ignore]
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
            .query_value(select_sql, &[json!(1)])
            .await
            .unwrap()
            .as_f64()
            .unwrap();
        assert_eq!("1.05", format!("{value:.2}"));

        let value = conn.query_f64(select_sql, &[json!(1)]).await.unwrap();
        assert_eq!(1.05, value);

        let value = conn.query_string(select_sql, &[json!(1)]).await.unwrap();
        assert_eq!("1.05", value);

        let row = conn.query_row(select_sql, &[json!(1)]).await.unwrap();
        assert_eq!(json!(row), json!({"value":1.05}));

        let rows = conn.query(select_sql, &[json!(1)]).await.unwrap();
        assert_eq!(json!(rows), json!([{"value":1.05}]));
    }
}
