//! The Pool Trait, for implementing a database connection pool

use std::{iter::IntoIterator, sync::Arc};

use async_trait::async_trait;
use indexmap::IndexMap;
use std::collections::HashMap;

use crate::{
    AnyTransaction, Error, Query, Row, Rows, Syntax, Transaction, Value,
    cache::{
        CachingStrategy, MemoryQueryCacheKey, MemoryQueryCacheValue, QUERY_CACHE_TABLE,
        TABLE_CACHE_TABLE,
    },
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
    caching_strategy: CachingStrategy,
    /// When set to true, SQL statements sent to the [Query::query()] and [Query::execute()]
    /// functions will be parsed and if they will result in tables being edited and/or dropped,
    /// the cache will be maintained in accordance with the given [CachingStrategy].
    /// For further information, see [Query::set_cache_aware_query()].
    cache_aware_query: bool,
    #[allow(unused)]
    meta_cache: Arc<Vec<String>>,
    query_cache: Arc<IndexMap<MemoryQueryCacheKey, MemoryQueryCacheValue>>,
    table_cache: Arc<HashMap<String, u128>>,
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
            cache_aware_query: false,
            meta_cache: Arc::new(Vec::default()),
            query_cache: Arc::new(IndexMap::default()),
            table_cache: Arc::new(HashMap::default()),
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

    ////////////// Caching ///////////////
    /// TODO: Add docstring.
    pub async fn cache(
        &self,
        sql: &str,
        params: impl IntoIterator<Item = &Value>,
    ) -> Result<Rows, Error> {
        match self.get_caching_strategy() {
            CachingStrategy::None => self.cache_tables(&[], sql, params).await,
            _ => {
                // TODO: The following line is only to get this to compile. We need to
                // implement get_accessed_tables() and uncomment the line below it.
                let tables_read: std::collections::BTreeSet<String> =
                    std::collections::BTreeSet::new();
                // let tables_read = get_accessed_tables(sql)?;
                let tables_read: Vec<_> = tables_read.iter().map(|s| s.as_str()).collect();
                match tables_read.is_empty() {
                    false => self.cache_tables(&tables_read, sql, params).await,
                    true => Err(Error::InputError(format!(
                        "No tables are read from in SQL: {sql}"
                    ))),
                }
            }
        }
    }

    /// Similar to [Query::cache()]. This version accepts an explicit list of tables, which
    /// must correspond to the tables queried from in the given SQL command(s).
    async fn cache_tables(
        &self,
        _tables: &[&str],
        _sql: &str,
        _params: impl IntoIterator<Item = &Value>,
    ) -> Result<Rows, Error> {
        match self.get_caching_strategy() {
            CachingStrategy::None => {
                todo!()
            }
            CachingStrategy::TruncateAll | CachingStrategy::Truncate => {
                todo!()
            }
            CachingStrategy::Trigger => {
                todo!()
            }
            CachingStrategy::Memory(_cache_size) => {
                todo!()
            }
        }
    }

    pub fn set_caching_strategy(&mut self, strategy: &CachingStrategy) {
        self.caching_strategy = *strategy;
    }

    /// Implements [Query::get_caching_strategy()]
    pub fn get_caching_strategy(&self) -> CachingStrategy {
        self.caching_strategy
    }

    /// Implements [Query::set_cache_aware_query()]
    pub fn set_cache_aware_query(&mut self, value: bool) {
        self.cache_aware_query = value;
    }

    /// Implements [Query::get_cache_aware_query()]
    pub fn get_cache_aware_query(&self) -> bool {
        self.cache_aware_query
    }

    /// TODO: Add docstring.
    pub async fn query_cache_len(&self) -> Result<u64, Error> {
        match self.caching_strategy {
            CachingStrategy::None => Ok(0),
            CachingStrategy::Memory(_) => Ok(self.query_cache.keys().len() as u64),
            _ => {
                let rows = self
                    .query(&format!("SELECT COUNT(1) from {QUERY_CACHE_TABLE}"), &[])
                    .await
                    .unwrap();
                let value: u64 = rows.try_into_value::<u64>()?;
                Ok(value)
            }
        }
    }

    /// TODO: Add docstring.
    pub async fn table_cache_len(&self) -> Result<u64, Error> {
        match self.caching_strategy {
            CachingStrategy::None => Ok(0),
            CachingStrategy::Memory(_) => Ok(self.table_cache.keys().len() as u64),
            _ => {
                let rows = self
                    .query(&format!("SELECT COUNT(1) from {TABLE_CACHE_TABLE}"), &[])
                    .await
                    .unwrap();
                let value: u64 = rows.try_into_value::<u64>()?;
                Ok(value)
            }
        }
    }
}
