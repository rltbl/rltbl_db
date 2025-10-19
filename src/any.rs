use crate::core::{DbError, DbQuery, JsonRow, JsonValue};

#[cfg(feature = "postgres")]
use crate::postgres::{PostgresConnection, PostgresTransaction};
#[cfg(feature = "sqlite")]
use crate::sqlite::{SqliteConnection, SqliteTransaction};

#[derive(Debug)]
pub enum AnyConnection {
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteConnection),
    #[cfg(feature = "postgres")]
    Postgres(PostgresConnection),
}

impl AnyConnection {
    /// Connect to the database located at the given URL.
    pub async fn connect(url: &str) -> Result<Self, DbError> {
        if url.starts_with("postgresql://") {
            #[cfg(feature = "postgres")]
            {
                Ok(AnyConnection::Postgres(
                    crate::postgres::PostgresConnection::connect(url).await?,
                ))
            }
            #[cfg(not(feature = "postgres"))]
            {
                Err(DbError::ConnectError("postgres not configured".to_string()))
            }
        } else {
            #[cfg(feature = "sqlite")]
            {
                Ok(AnyConnection::Sqlite(SqliteConnection::connect(url).await?))
            }
            #[cfg(not(feature = "sqlite"))]
            {
                Err(DbError::ConnectError("sqlite not configured".to_string()))
            }
        }
    }

    /// Given an async function (or closure), run that function inside a database transaction,
    /// and only commit the transaction if the result is Ok.
    pub async fn transact(
        &self,
        func: impl AsyncFnOnce(&AnyTransaction) -> Result<Vec<JsonRow>, DbError>,
    ) -> Result<Vec<JsonRow>, DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(conn) => {
                let obj = conn.get().await?;
                let mut lock = obj.lock().map_err(|err| {
                    DbError::DatabaseError(format!("Failed to lock SQLite database: {err}"))
                })?;
                let tx = lock.transaction().map_err(|err| {
                    DbError::DatabaseError(format!("Failed to start SQLite transaction: {err}"))
                })?;
                let sqlite_tx = SqliteTransaction { tx };
                let any_tx = AnyTransaction::Sqlite(sqlite_tx);
                match func(&any_tx).await {
                    Ok(rows) => {
                        any_tx.commit().await?;
                        Ok(rows)
                    }
                    Err(err) => Err(err),
                }
            }
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(conn) => {
                let mut obj = conn.get().await?;
                let tx = obj.transaction().await.map_err(|err| {
                    DbError::DatabaseError(format!("Failed to start Postgres transaction: {err}"))
                })?;
                let pg_tx = PostgresTransaction { tx };
                let any_tx = AnyTransaction::Postgres(pg_tx);
                match func(&any_tx).await {
                    Ok(rows) => {
                        any_tx.commit().await?;
                        Ok(rows)
                    }
                    Err(err) => Err(err),
                }
            }
        }
    }
}

impl DbQuery for AnyConnection {
    async fn execute(&self, sql: &str, params: &[JsonValue]) -> Result<(), DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.execute(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.execute(sql, params).await,
        }
    }

    async fn execute_batch(&self, sql: &str) -> Result<(), DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.execute_batch(sql).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.execute_batch(sql).await,
        }
    }

    async fn query(&self, sql: &str, params: &[JsonValue]) -> Result<Vec<JsonRow>, DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.query(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.query(sql, params).await,
        }
    }

    async fn query_row(&self, sql: &str, params: &[JsonValue]) -> Result<JsonRow, DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.query_row(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.query_row(sql, params).await,
        }
    }

    async fn query_value(&self, sql: &str, params: &[JsonValue]) -> Result<JsonValue, DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.query_value(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.query_value(sql, params).await,
        }
    }

    async fn query_string(&self, sql: &str, params: &[JsonValue]) -> Result<String, DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.query_string(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.query_string(sql, params).await,
        }
    }

    async fn query_u64(&self, sql: &str, params: &[JsonValue]) -> Result<u64, DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.query_u64(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.query_u64(sql, params).await,
        }
    }

    async fn query_i64(&self, sql: &str, params: &[JsonValue]) -> Result<i64, DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.query_i64(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.query_i64(sql, params).await,
        }
    }

    async fn query_f64(&self, sql: &str, params: &[JsonValue]) -> Result<f64, DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyConnection::Sqlite(connection) => connection.query_f64(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyConnection::Postgres(connection) => connection.query_f64(sql, params).await,
        }
    }
}

pub enum AnyTransaction<'a> {
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteTransaction<'a>),
    #[cfg(feature = "postgres")]
    Postgres(PostgresTransaction<'a>),
}

impl<'a> AnyTransaction<'a> {
    pub async fn execute(&self, sql: &str, params: &[JsonValue]) -> Result<(), DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyTransaction::Sqlite(tx) => tx.execute(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyTransaction::Postgres(tx) => tx.execute(sql, params).await,
        }
    }
    pub async fn query(&self, sql: &str, params: &[JsonValue]) -> Result<Vec<JsonRow>, DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyTransaction::Sqlite(tx) => tx.query(sql, params).await,
            #[cfg(feature = "postgres")]
            AnyTransaction::Postgres(tx) => tx.query(sql, params).await,
        }
    }

    pub async fn commit(self) -> Result<(), DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyTransaction::Sqlite(tx) => tx.commit().await,
            #[cfg(feature = "postgres")]
            AnyTransaction::Postgres(tx) => tx.commit().await,
        }
    }

    pub async fn rollback(self) -> Result<(), DbError> {
        match self {
            #[cfg(feature = "sqlite")]
            AnyTransaction::Sqlite(tx) => tx.rollback().await,
            #[cfg(feature = "postgres")]
            AnyTransaction::Postgres(tx) => tx.rollback().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    #[cfg(feature = "postgres")]
    use crate::postgres::PostgresTransaction;

    use super::*;

    #[tokio::test]
    async fn test_tx() {
        // create a pool
        // get an object from the pool
        // for SQLite lock, then get a transaction
        // for Postgres just get a transaction
        // pass the transaction around, synchronously
        //   to functions that accept AnyTransaction
        // commit the transaction

        // SQLite
        let conn = SqliteConnection::connect("test_tx.db").await.unwrap();
        let obj = conn.get().await.unwrap();
        let mut lock = obj.lock().unwrap();
        let tx = lock.transaction().unwrap();
        let sqlite_tx = SqliteTransaction { tx };
        let any_tx = AnyTransaction::Sqlite(sqlite_tx);
        let rows = select_1(&any_tx).await.unwrap();
        any_tx.commit().await.unwrap();
        assert_eq!(json!(rows), json!([{"foo": 1}]));

        // Postgres
        #[cfg(feature = "postgres")]
        {
            let conn = PostgresConnection::connect("postgresql:///sql_json_db")
                .await
                .unwrap();
            let mut obj = conn.get().await.unwrap();
            let tx = obj.transaction().await.unwrap();
            let pg_tx = PostgresTransaction { tx };
            let any_tx = AnyTransaction::Postgres(pg_tx);
            let rows = select_1(&any_tx).await.unwrap();
            any_tx.commit().await.unwrap();
            assert_eq!(json!(rows), json!([{"foo": 1}]));
        }

        // Any Sqlite
        let conn = AnyConnection::connect("test_tx.db").await.unwrap();
        let rows = conn.transact(select_1).await.unwrap();
        assert_eq!(json!(rows), json!([{"foo": 1}]));

        // Any Postgres
        #[cfg(feature = "postgres")]
        {
            let conn = AnyConnection::connect("postgresql:///sql_json_db")
                .await
                .unwrap();
            let rows = conn.transact(select_1).await.unwrap();
            assert_eq!(json!(rows), json!([{"foo": 1}]));
        }

        // SQLite Closure
        let conn = AnyConnection::connect("test_tx.db").await.unwrap();
        let mut fred = true;
        let rows = conn
            .transact(async |tx: &AnyTransaction| {
                fred = false;
                let sql = "SELECT 1 AS foo";
                tx.query(sql, &[]).await
            })
            .await
            .unwrap();
        assert_eq!(json!(rows), json!([{"foo": 1}]));
        assert_eq!(fred, false);

        // Postgres Closure
        #[cfg(feature = "postgres")]
        {
            let conn = AnyConnection::connect("postgresql:///sql_json_db")
                .await
                .unwrap();
            let mut fred = true;
            let rows = conn
                .transact(async |tx: &AnyTransaction| {
                    fred = false;
                    let sql = "SELECT 1 AS foo";
                    tx.query(sql, &[]).await
                })
                .await
                .unwrap();
            assert_eq!(json!(rows), json!([{"foo": 1}]));
            assert_eq!(fred, false);
        }

        // SQLite rollback
        let conn = AnyConnection::connect("test_tx.db").await.unwrap();
        conn.execute("DROP TABLE IF EXISTS tx_test", &[])
            .await
            .unwrap();
        conn.execute("CREATE TABLE tx_test ( value TEXT )", &[])
            .await
            .unwrap();
        conn.execute("INSERT INTO tx_test VALUES ('foo')", &[])
            .await
            .unwrap();
        conn.transact(async |tx: &AnyTransaction| {
            let sql = "UPDATE tx_test SET value='bar' WHERE value='foo'";
            tx.execute(sql, &[]).await?;
            let sql = "FOOBAR";
            tx.query(sql, &[]).await
        })
        .await
        .expect_err("Expected to fail");
        let rows = conn
            .transact(async |tx: &AnyTransaction| {
                let sql = "SELECT * FROM tx_test";
                tx.query(sql, &[]).await
            })
            .await
            .unwrap();
        assert_eq!(json!(rows), json!([{"value": "foo"}]));

        // Postgres rollback
        #[cfg(feature = "postgres")]
        {
            let conn = AnyConnection::connect("postgresql:///sql_json_db")
                .await
                .unwrap();
            conn.execute("DROP TABLE IF EXISTS tx_test", &[])
                .await
                .unwrap();
            conn.execute("CREATE TABLE tx_test ( value TEXT )", &[])
                .await
                .unwrap();
            conn.execute("INSERT INTO tx_test VALUES ('foo')", &[])
                .await
                .unwrap();
            conn.transact(async |tx: &AnyTransaction| {
                let sql = "UPDATE tx_test SET value='bar' WHERE value='foo'";
                tx.execute(sql, &[]).await?;
                let sql = "FOOBAR";
                tx.query(sql, &[]).await
            })
            .await
            .expect_err("Expected to fail");
            let rows = conn
                .transact(async |tx: &AnyTransaction| {
                    let sql = "SELECT * FROM tx_test";
                    tx.query(sql, &[]).await
                })
                .await
                .unwrap();
            assert_eq!(json!(rows), json!([{"value": "foo"}]));
        }
    }

    async fn select_1(tx: &AnyTransaction<'_>) -> Result<Vec<JsonRow>, DbError> {
        select_2(&tx).await.unwrap();
        let sql = "SELECT 1 AS foo";
        tx.query(sql, &[]).await
    }

    async fn select_2(tx: &AnyTransaction<'_>) -> Result<Vec<JsonRow>, DbError> {
        let sql = "SELECT 2 AS foo";
        tx.query(sql, &[]).await
    }
}
