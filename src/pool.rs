//! The Pool Trait, for implementing a database connection pool

use std::{iter::IntoIterator, sync::Arc};

use async_trait::async_trait;
use indexmap::IndexMap;

use crate::{
    AnyTransaction, Error, Query, Row, Rows, Syntax, Transaction, Value, cache::CachingStrategy,
};

/// A trait for implementing a database connection pool.
#[async_trait]
pub trait Pool: Query + std::fmt::Debug {
    /// Start a transaction.
    async fn transaction(&self) -> Result<Box<dyn Transaction>, Error>;
}

/// An abstraction over the supported pool types.
#[derive(Debug)]
pub struct AnyPool {
    pool: Box<dyn Pool>,
    #[allow(dead_code)]
    caching_strategy: CachingStrategy,
    // TODO: This should not be an IndexMap. It should be a plain vector.
    #[allow(dead_code)]
    meta_cache: Arc<IndexMap<String, String>>,
    // TODO: Other caches will go here (these others are IndexMaps).
}

// Note: This is dyn compatible ONLY if every impl Query uses #[async_trait].
pub async fn connect(url: &str) -> Result<Box<dyn Pool>, Error> {
    // TODO: Add libsql support.
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
    fn from(pool: Box<dyn Pool>) -> Self {
        AnyPool {
            pool,
            caching_strategy: CachingStrategy::None,
            meta_cache: Arc::new(IndexMap::default()),
        }
    }
}

impl AnyPool {
    /// Returns a connection to the database located at the giveb URL.
    pub async fn connect(url: &str) -> Result<Self, Error> {
        let pool = connect(url).await?;
        Ok(AnyPool::from(pool))
    }

    /// [Query::syntax()] for [AnyPool]
    pub fn syntax(&self) -> &dyn Syntax {
        self.pool.syntax()
    }

    /// [Query::columns()] for [AnyPool]
    pub async fn columns(&self, table: &str) -> Result<IndexMap<String, String>, Error> {
        self.pool.columns(table).await
    }

    // TODO: Combine this with columns() if possible
    /// [Query::primary_keys()] for [AnyPool]
    pub async fn primary_keys(&self, table: &str) -> Result<Vec<String>, Error> {
        self.pool.primary_keys(table).await
    }

    /// [Query::execute()] for [AnyPool]
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
    /// [Query::execute_batch()]
    pub async fn execute_batch(&self, sql: &str) -> Result<(), Error> {
        self.pool.execute_batch(sql).await
        // TODO: handle cache
    }

    /// [Query::query()]
    pub async fn query(
        &self,
        sql: &str,
        params: impl IntoIterator<Item = &Value>,
    ) -> Result<Rows, Error> {
        // MC: By collecting the iterator into a Vec here, don't we nullify much of the
        // advantagge of having an iterator in the first place? How is this better than
        // simply accepting a &[&Row] argument? Is there some rust feature that I'm unaware of
        // that makes this better? Not that I can think of an alternative. The trouble is,
        // ultimately, that query() and execute() are trait methods and we want those to have
        // concrete arguments for independent reasons.
        let refs: Vec<&Value> = params.into_iter().collect();
        self.pool.query(sql, &refs).await
        // TODO: handle cache
    }

    /// TODO: Add docstring.
    pub async fn cache(
        &self,
        _sql: &str,
        _params: impl IntoIterator<Item = &Value>,
    ) -> Result<Rows, Error> {
        todo!("implement AnyPool::cache")
    }

    /// [Query::insert()]
    pub async fn insert(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoIterator<Item = &Row>,
    ) -> Result<(), Error> {
        // MC: In lib.rs, we promised users that they wouldn't have to implement many new
        // methods when they are adding their own drivers. However, with the current design,
        // despite the code in shared.rs being shared by all drivers, we still have to include
        // a concrete call to edit() within the code for each concrete driver, otherwise rust's
        // compiler complains that the size of the pool is not known at compile time. That
        // means users will have to define all of their own insert_*(), update_*() and
        // upsert_*() methods, even though all that they need to do is to call edit() as
        // follows (and likewise for insert(), update(), and the _returning() variants of each):
        //
        // async fn upsert(
        //     &self, table: &str, columns: &[&str], rows: &[&Row]
        // ) -> Result<(), Error> {
        // edit(
        //     self,
        //     &EditType::Upsert,
        //     &MAX_PARAMS_POSTGRES,
        //     table,
        //     columns,
        //     rows,
        //     false,
        //     &[],
        // )
        // .await?;
        // Ok(())

        let refs: Vec<&Row> = rows.into_iter().collect();
        self.pool.insert(table, columns, &refs).await
    }

    /// [Query::insert_returning()]
    pub async fn insert_returning(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoIterator<Item = &Row>,
        returning: &[&str],
    ) -> Result<Rows, Error> {
        let refs: Vec<&Row> = rows.into_iter().collect();
        self.pool
            .insert_returning(table, columns, &refs, returning)
            .await
    }

    /// [Query::update()]
    pub async fn update(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoIterator<Item = &Row>,
    ) -> Result<(), Error> {
        let refs: Vec<&Row> = rows.into_iter().collect();
        self.pool.update(table, columns, &refs).await
    }

    /// [Query::update_returning()]
    pub async fn update_returning(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoIterator<Item = &Row>,
        returning: &[&str],
    ) -> Result<Rows, Error> {
        let refs: Vec<&Row> = rows.into_iter().collect();
        self.pool
            .update_returning(table, columns, &refs, returning)
            .await
    }

    /// [Query::upsert()]
    pub async fn upsert(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoIterator<Item = &Row>,
    ) -> Result<(), Error> {
        let refs: Vec<&Row> = rows.into_iter().collect();
        self.pool.upsert(table, columns, &refs).await
    }

    /// [Query::upsert_returning()]
    pub async fn upsert_returning(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoIterator<Item = &Row>,
        returning: &[&str],
    ) -> Result<Rows, Error> {
        let refs: Vec<&Row> = rows.into_iter().collect();
        self.pool
            .upsert_returning(table, columns, &refs, returning)
            .await
    }

    #[allow(unused)]
    /// [Query::drop_table()]
    pub async fn drop_table(&self, table: &str) -> Result<(), Error> {
        self.pool.drop_table(table).await
    }

    /// TODO: Add docstring.
    pub async fn transaction(&self) -> Result<AnyTransaction, Error> {
        Ok(AnyTransaction::begin(self.pool.transaction().await?))
    }
}
