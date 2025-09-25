use crate::core::{DbConnection, JsonRow};

use deadpool_sqlite::{Config, Pool, Runtime};
use serde_json::json;

type JsonValue = serde_json::Value;
type DbError = String;

pub struct SqliteConnection {
    pool: Pool,
}

impl SqliteConnection {
    pub async fn connect(url: &str) -> Result<impl DbConnection, DbError> {
        let cfg = Config::new(url);
        let pool = cfg.create_pool(Runtime::Tokio1).unwrap();
        Ok(Self { pool })
    }
}

impl DbConnection for SqliteConnection {
    async fn execute(&self, sql: &str, _params: &[JsonValue]) -> Result<(), DbError> {
        let conn = self.pool.get().await.unwrap();
        let sql = sql.to_string();
        conn.interact(move |conn| {
            let mut stmt = conn.prepare(&sql).unwrap();
            stmt.execute([]).unwrap();
        })
        .await
        .unwrap();
        Ok(())
    }

    async fn query(&self, _sql: &str, _params: &[JsonValue]) -> Result<Vec<JsonRow>, DbError> {
        todo!()
    }

    async fn query_row(&self, _sql: &str, _params: &[JsonValue]) -> Result<JsonRow, DbError> {
        todo!()
    }

    async fn query_value(&self, sql: &str, _params: &[JsonValue]) -> Result<JsonValue, DbError> {
        let conn = self.pool.get().await.unwrap();
        let sql = sql.to_string();
        let result: String = conn
            .interact(move |conn| {
                let mut stmt = conn.prepare(&sql)?;
                stmt.query_one([], |row| row.get(0))
            })
            .await
            .unwrap()
            .unwrap();
        Ok(json!(result))
    }

    async fn query_string(&self, _sql: &str, _params: &[JsonValue]) -> Result<String, DbError> {
        todo!()
    }

    async fn query_u64(&self, _sql: &str, _params: &[JsonValue]) -> Result<u64, DbError> {
        todo!()
    }

    async fn query_i64(&self, _sql: &str, _params: &[JsonValue]) -> Result<i64, DbError> {
        todo!()
    }

    async fn query_f64(&self, _sql: &str, _params: &[JsonValue]) -> Result<f64, DbError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_works() {
        let conn = SqliteConnection::connect("test_axum.db").await.unwrap();
        conn.execute("DROP TABLE IF EXISTS test", &[])
            .await
            .unwrap();
        conn.execute("CREATE TABLE test ( value TEXT )", &[])
            .await
            .unwrap();
        conn.execute("INSERT INTO test VALUES ('foo')", &[])
            .await
            .unwrap();
        let value = conn
            .query_value("SELECT value FROM test LIMIT 1", &[])
            .await
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!("foo", value);
    }
}
