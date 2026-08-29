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
    postgres,
    shared::{EditType, edit},
    sqlite,
    value::IntoValues,
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
    pub async fn execute(&self, sql: &str, values: impl IntoValues) -> Result<(), Error> {
        let values = values.into_values()?.collect::<Vec<_>>();
        self.pool.execute(sql, &values).await
        // TODO: handle cache
    }

    #[allow(unused)]
    /// [Query::execute_batch()]
    pub async fn execute_batch(&self, sql: &str) -> Result<(), Error> {
        self.pool.execute_batch(sql).await
        // TODO: handle cache
    }

    /// [Query::query()]
    pub async fn query(&self, sql: &str, values: impl IntoValues) -> Result<Rows, Error> {
        let values = values.into_values()?.collect::<Vec<_>>();
        self.pool.query(sql, &values).await
        // TODO: handle cache
    }

    /// Insert rows into the given columns of the given table. If an input row does not have a
    /// key corresponding to one of the given columns, use NULL as the value of that column when
    /// inserting the row to the table.
    pub async fn insert(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoIterator<Item = &Row>,
    ) -> Result<(), Error> {
        let max_params = match self.syntax().name() {
            "sqlite" => sqlite::MAX_PARAMS_SQLITE,
            "postgres" => postgres::MAX_PARAMS_POSTGRES,
            _ => panic!(),
        };
        edit(
            self,
            &EditType::Insert,
            &max_params,
            table,
            columns,
            rows,
            true,
            &[],
        )
        .await?;
        Ok(())
    }

    /// Like [Query::insert()], but in addition this function also returns the data that was
    /// inserted into the columns included in `returning`, or all of the inserted data if
    /// `returning` is an empty list.
    pub async fn insert_returning(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoIterator<Item = &Row>,
        returning: &[&str],
    ) -> Result<Rows, Error> {
        let max_params = match self.syntax().name() {
            "sqlite" => sqlite::MAX_PARAMS_SQLITE,
            "postgres" => postgres::MAX_PARAMS_POSTGRES,
            _ => panic!(),
        };
        edit(
            self,
            &EditType::Insert,
            &max_params,
            table,
            columns,
            rows,
            true,
            returning,
        )
        .await
    }

    /// Update the given columns of the given table using the given rows. The table should have a
    /// primary key and any columns that are part of the primary key should be present within each
    /// input row. The primary key column values will be used as a way of identifying the rows to
    /// update, while the other columns in the row will be updated to the given new values.
    pub async fn update(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoIterator<Item = &Row>,
    ) -> Result<(), Error> {
        let max_params = match self.syntax().name() {
            "sqlite" => sqlite::MAX_PARAMS_SQLITE,
            "postgres" => postgres::MAX_PARAMS_POSTGRES,
            _ => panic!(),
        };
        edit(
            self,
            &EditType::Update,
            &max_params,
            table,
            columns,
            rows,
            true,
            &[],
        )
        .await?;
        Ok(())
    }

    /// Like [Query::update()], but in addition this function also returns the data that was
    /// updated for the columns included in `returning`, or all of the updated data if
    /// `returning` is an empty list.
    pub async fn update_returning(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoIterator<Item = &Row>,
        returning: &[&str],
    ) -> Result<Rows, Error> {
        let max_params = match self.syntax().name() {
            "sqlite" => sqlite::MAX_PARAMS_SQLITE,
            "postgres" => postgres::MAX_PARAMS_POSTGRES,
            _ => panic!(),
        };
        edit(
            self,
            &EditType::Update,
            &max_params,
            table,
            columns,
            rows,
            true,
            returning,
        )
        .await
    }

    /// Attempt to insert the given rows to the given table, similarly to [Query::insert()].
    /// In case there is a conflict, update the table instead, similarly to [Query::update()].
    pub async fn upsert(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoIterator<Item = &Row>,
    ) -> Result<(), Error> {
        let max_params = match self.syntax().name() {
            "sqlite" => sqlite::MAX_PARAMS_SQLITE,
            "postgres" => postgres::MAX_PARAMS_POSTGRES,
            _ => panic!(),
        };
        edit(
            self,
            &EditType::Upsert,
            &max_params,
            table,
            columns,
            rows,
            true,
            &[],
        )
        .await?;
        Ok(())
    }

    /// Like [Query::upsert()], but in addition this function also returns the data that was
    /// upserted for the columns included in `returning`, or all of the upserted data if
    /// `returning` is an empty list.
    pub async fn upsert_returning(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoIterator<Item = &Row>,
        returning: &[&str],
    ) -> Result<Rows, Error> {
        let max_params = match self.syntax().name() {
            "sqlite" => sqlite::MAX_PARAMS_SQLITE,
            "postgres" => postgres::MAX_PARAMS_POSTGRES,
            _ => panic!(),
        };
        edit(
            self,
            &EditType::Upsert,
            &max_params,
            table,
            columns,
            rows,
            true,
            returning,
        )
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
                    .query(&format!("SELECT COUNT(1) from {QUERY_CACHE_TABLE}"), ())
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
                    .query(&format!("SELECT COUNT(1) from {TABLE_CACHE_TABLE}"), ())
                    .await
                    .unwrap();
                let value: u64 = rows.try_into_value::<u64>()?;
                Ok(value)
            }
        }
    }
}
