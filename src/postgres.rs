use crate::core::{DbConnection, DbError, JsonRow, JsonValue};
use deadpool_postgres::{Config, Pool, Runtime};
use tokio_postgres::{types::Type, NoTls};

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
                let db_name = url.strip_prefix("postgresql:///").expect("Invalid URL");
                cfg.dbname = Some(db_name.to_string());
                let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();
                Ok(Self { pool })
            }
            // TODO: Replace panics and unwraps with proper errors, here and elsewhere.
            false => panic!("Invalid PostgreSQL database path: '{url}'"),
        }
    }
}

impl DbConnection for PostgresConnection {
    /// Implements [DbConnection::execute()] for PostgreSQL.
    async fn execute(&self, sql: &str, _params: &[JsonValue]) -> Result<(), DbError> {
        let client = self.pool.get().await.unwrap();
        client.execute(sql, &[]).await.unwrap();
        Ok(())
    }

    /// Implements [DbConnection::query()] for PostgreSQL.
    async fn query(&self, _sql: &str, _params: &[JsonValue]) -> Result<Vec<JsonRow>, DbError> {
        todo!()
    }

    /// Implements [DbConnection::query_row()] for PostgreSQL.
    async fn query_row(&self, _sql: &str, _params: &[JsonValue]) -> Result<JsonRow, DbError> {
        todo!()
    }

    /// Implements [DbConnection::query_value()] for PostgreSQL.
    async fn query_value(&self, sql: &str, _params: &[JsonValue]) -> Result<JsonValue, DbError> {
        let client = self.pool.get().await.unwrap();
        let rows = client.query(sql, &[]).await.unwrap();
        match rows.iter().next() {
            Some(row) => {
                let column = &row.columns()[0];
                match *column.type_() {
                    Type::TEXT | Type::VARCHAR => {
                        let value: &str = row.get(0);
                        Ok(value.into())
                    }
                    Type::INT2 | Type::INT4 => {
                        let value: i32 = row.get(0);
                        Ok(value.into())
                    }
                    Type::INT8 => {
                        let value: i64 = row.get(0);
                        Ok(value.into())
                    }
                    Type::FLOAT4 => {
                        let value: f32 = row.get(0);
                        Ok(value.into())
                    }
                    Type::FLOAT8 => {
                        let value: f64 = row.get(0);
                        Ok(value.into())
                    }
                    _ => unimplemented!(),
                }
            }
            None => panic!("No value found"),
        }
    }

    /// Implements [DbConnection::query_string()] for PostgreSQL.
    async fn query_string(&self, sql: &str, params: &[JsonValue]) -> Result<String, DbError> {
        let value = self.query_value(sql, params).await?;
        Ok(value.to_string())
    }

    /// Implements [DbConnection::query_u64()] for PostgreSQL.
    async fn query_u64(&self, sql: &str, _params: &[JsonValue]) -> Result<u64, DbError> {
        let client = self.pool.get().await.unwrap();
        let rows = client.query(sql, &[]).await.unwrap();
        match rows.iter().next() {
            Some(row) => {
                let column = &row.columns()[0];
                let value = match *column.type_() {
                    Type::INT2 | Type::INT4 => {
                        let value: i32 = row.get(0);
                        if value < 0 {
                            panic!("Invalid value: {value}");
                        }
                        value as u64
                    }
                    Type::INT8 => {
                        let value: i64 = row.get(0);
                        if value < 0 {
                            panic!("Invalid value: {value}");
                        }
                        let value = u64::try_from(value).expect("Can't convert to u64: {value}");
                        value
                    }
                    _ => panic!("Cannot convert to u64: {}", column.type_()),
                };
                Ok(value)
            }
            None => panic!("No value found"),
        }
    }

    /// Implements [DbConnection::query_i64()] for PostgreSQL.
    async fn query_i64(&self, sql: &str, _params: &[JsonValue]) -> Result<i64, DbError> {
        let client = self.pool.get().await.unwrap();
        let rows = client.query(sql, &[]).await.unwrap();
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
                    _ => panic!("Cannot convert to i64: {}", column.type_()),
                };
                Ok(value)
            }
            None => panic!("No value found"),
        }
    }

    /// Implements [DbConnection::query_f64] for PostgreSQL.
    async fn query_f64(&self, sql: &str, _params: &[JsonValue]) -> Result<f64, DbError> {
        let client = self.pool.get().await.unwrap();
        let rows = client.query(sql, &[]).await.unwrap();
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
                    _ => panic!("Cannot convert to f64: {}", column.type_()),
                };
                Ok(value)
            }
            None => panic!("No value found"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TODO: Once query_string(), query_i64(), query_f64, etc. have been implemented,
    // replace the calls to query_value() below with the appropriate method.

    #[tokio::test]
    async fn it_works_with_text() {
        let conn = PostgresConnection::connect("postgresql:///sql_json_db")
            .await
            .unwrap();
        conn.execute("DROP TABLE IF EXISTS test_table_text", &[])
            .await
            .unwrap();
        conn.execute("CREATE TABLE test_table_text ( value TEXT )", &[])
            .await
            .unwrap();
        conn.execute("INSERT INTO test_table_text VALUES ('foo')", &[])
            .await
            .unwrap();
        let value = conn
            .query_value("SELECT value FROM test_table_text LIMIT 1", &[])
            .await
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!("foo", value);
    }

    #[tokio::test]
    async fn it_works_with_int() {
        let conn = PostgresConnection::connect("postgresql:///sql_json_db")
            .await
            .unwrap();
        conn.execute("DROP TABLE IF EXISTS test_table_int", &[])
            .await
            .unwrap();
        conn.execute("CREATE TABLE test_table_int ( value INT8 )", &[])
            .await
            .unwrap();
        conn.execute("INSERT INTO test_table_int VALUES (-1)", &[])
            .await
            .unwrap();
        let value = conn
            .query_value("SELECT value FROM test_table_int LIMIT 1", &[])
            .await
            .unwrap()
            .as_i64()
            .unwrap();
        assert_eq!(-1, value);
    }

    #[tokio::test]
    async fn it_works_with_float() {
        let conn = PostgresConnection::connect("postgresql:///sql_json_db")
            .await
            .unwrap();
        conn.execute("DROP TABLE IF EXISTS test_table_float", &[])
            .await
            .unwrap();
        conn.execute("CREATE TABLE test_table_float ( value FLOAT8 )", &[])
            .await
            .unwrap();
        conn.execute("INSERT INTO test_table_float VALUES (1.05)", &[])
            .await
            .unwrap();
        let value = conn
            .query_value("SELECT value FROM test_table_float LIMIT 1", &[])
            .await
            .unwrap()
            .as_f64()
            .unwrap();
        let value = format!("{value:.2}");
        assert_eq!("1.05", value);
    }
}
