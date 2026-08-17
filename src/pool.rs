//! Trait for database connection pool

use std::{iter::IntoIterator, sync::Arc};

use async_trait::async_trait;
use indexmap::IndexMap;

use crate::{AnyTransaction, Error, Query, Row, Rows, Syntax, Transaction, Value};

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

// This is dyn compatible ONLY if every impl DbQuery uses #[async_trait].
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

    pub async fn insert(
        &self,
        _table: &str,
        _columns: &[&str],
        _rows: impl IntoIterator<Item = &Row>,
    ) -> Result<(), Error> {
        let sql = "INSERT INTO foo(bar) VALUES (2)";
        self.pool.execute(sql, &[]).await
        // TODO: handle cache
    }

    // TODO: insert
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
            // TODO: don't require explicitly calling Value::from() here and elsewhere.
            &[Value::from("foo")],
        )
        .await
        .unwrap();

        let select_sql = format!("SELECT value FROM test_table_text WHERE value = {pp}1");
        let value: String = pool
            .query(&select_sql, &[Value::from("foo")])
            .await
            .unwrap()
            .try_into_value::<String>()
            .unwrap()
            .into();
        assert_eq!("foo", value);

        let string: String = pool
            .query(&select_sql, &[Value::from("foo")])
            .await
            .unwrap()
            .try_into_value::<String>()
            .unwrap()
            .into();
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
            let value: i64 = rows.try_into_value::<i64>().unwrap().try_into().unwrap();
            assert_eq!(1, value);

            let rows = pool.query(&select_sql, &params.clone()).await.unwrap();
            let unsigned: u64 = rows.try_into_value::<u64>().unwrap().try_into().unwrap();
            assert_eq!(1, unsigned);

            let rows = pool.query(&select_sql, &params.clone()).await.unwrap();
            let signed: i64 = rows.try_into_value::<i64>().unwrap().try_into().unwrap();
            assert_eq!(1, signed);

            let string: String = pool
                .query(&select_sql, &params.clone())
                .await
                .unwrap()
                .try_into_value::<String>()
                .unwrap()
                .into();
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
}
