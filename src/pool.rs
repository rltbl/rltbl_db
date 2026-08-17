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
            Err(DbError::ConnectError(format!("Unsupported URL: '{url}'")))
        }
    } else {
        #[cfg(feature = "rusqlite")]
        {
            let pool = crate::rusqlite::RusqlitePool::connect(url).await?;
            Ok(Box::new(pool))
        }
        #[cfg(not(feature = "rusqlite"))]
        {
            Err(DbError::ConnectError(format!("Unsupported URL: '{url}'")))
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

    // TODO: execute_batch

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
    // TODO: drop

    // MC: Right. This is needed for AnyPool to implement "Pool".
    pub async fn transaction(&self) -> Result<AnyTransaction, Error> {
        Ok(AnyTransaction::begin(self.pool.transaction().await?))
    }
}
