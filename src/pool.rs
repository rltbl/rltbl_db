//! The Pool Trait, for implementing a database connection pool

use async_trait::async_trait;
use indexmap::IndexMap;
use std::{
    collections::HashSet,
    fmt::Display,
    iter::IntoIterator,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    AnyTransaction, Error, Query, Row, Rows, Syntax, Transaction, Value,
    cache::{
        CachingStrategy, MemoryQueryCache, MemoryQueryCacheKey, MemoryQueryCacheValue,
        MemoryTableCache, MetaCache, QUERY_CACHE_TABLE, TABLE_CACHE_TABLE,
    },
    postgres,
    sql_parse::{self, get_accessed_tables, validate_table_name},
    sqlite,
    value::IntoValues,
    values,
};

// Note: This is dyn compatible ONLY if every impl Query uses #[async_trait].
/// Create a database connection pool associated with the given URL.
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

/// A trait for implementing a database connection pool.
#[async_trait]
pub trait Pool: Query + std::fmt::Debug {
    /// Start a transaction.
    async fn transaction(&self) -> Result<Box<dyn Transaction>, Error>;
}

/// An abstraction over the supported pool types.
#[derive(Debug)]
pub struct AnyPool {
    pub pool: Box<dyn Pool>,
    caching_strategy: CachingStrategy,
    /// When set to true, SQL statements sent to the [Query::query()] and [Query::execute()]
    /// functions will be parsed and if they will result in tables being edited and/or dropped,
    /// the cache will be maintained in accordance with the given [CachingStrategy].
    /// For further information, see [Query::set_cache_aware_query()].
    cache_aware_query: bool,
    meta_cache: MetaCache,
    memory_query_cache: MemoryQueryCache,
    memory_table_cache: MemoryTableCache,
}

/// Ways in which to edit a table.
#[allow(unused)]
#[derive(PartialEq, Eq)]
enum EditType {
    Insert,
    Update,
    #[allow(unused)]
    Upsert,
}

impl Display for EditType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EditType::Update => write!(f, "UPDATE"),
            EditType::Insert => write!(f, "INSERT"),
            EditType::Upsert => write!(f, "UPSERT"),
        }
    }
}

impl From<Box<dyn Pool>> for AnyPool {
    fn from(pool: Box<dyn Pool>) -> Self {
        AnyPool {
            pool,
            caching_strategy: CachingStrategy::None,
            cache_aware_query: false,
            meta_cache: MetaCache::default(),
            memory_query_cache: MemoryQueryCache::default(),
            memory_table_cache: MemoryTableCache::default(),
        }
    }
}

impl AnyPool {
    /// Returns a connection to the database located at the giveb URL.
    pub async fn connect(url: &str) -> Result<Self, Error> {
        let pool = connect(url).await?;
        Ok(AnyPool::from(pool))
    }

    /// syntax() for [AnyPool]
    pub fn syntax(&self) -> &dyn Syntax {
        self.pool.syntax()
    }

    /// columns() for [AnyPool]
    pub async fn columns(&self, table: &str) -> Result<IndexMap<String, String>, Error> {
        self.pool.columns(table).await
    }

    // TODO: Combine this with columns() if possible
    /// primary_keys() for [AnyPool]
    pub async fn primary_keys(&self, table: &str) -> Result<Vec<String>, Error> {
        self.pool.primary_keys(table).await
    }

    /// execute() for [AnyPool]
    pub async fn execute(&self, sql: &str, values: impl IntoValues) -> Result<(), Error> {
        let values = values.into_values()?.collect::<Vec<_>>();
        self.query(sql, values).await?;
        Ok(())
    }

    /// Note that all of the methods on AnyPool implicitly check if the cache needs to be cleaned.
    /// to use the raw "no-cache" method, call the underlying pool, self.pool.execute_batch()
    /// instead of self.execute_batch().
    #[allow(unused)]
    pub async fn execute_batch(&self, sql: &str) -> Result<(), Error> {
        self.pool.execute_batch(sql).await?;
        if self.get_cache_aware_query() {
            self.clear_cache_for_affected_tables(sql).await?;
        }
        Ok(())
    }

    /// Note that all of the methods on AnyPool implicitly check if the cache needs to be cleaned.
    /// to use the raw "no-cache" method, call the underlying pool, self.pool.query() instead
    /// of self.query().
    pub async fn query(&self, sql: &str, values: impl IntoValues) -> Result<Rows, Error> {
        let values = values.into_values()?.collect::<Vec<_>>();
        let rows = self.pool.query(sql, &values).await?;
        if self.get_cache_aware_query() {
            self.clear_cache_for_affected_tables(sql).await?;
        }
        Ok(rows)
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
        self.edit(
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

    /// Like insert(), but in addition this function also returns the data that was
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
        self.edit(
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
        self.edit(
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

    /// Like update(), but in addition this function also returns the data that was
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
        self.edit(
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

    /// Attempt to insert the given rows to the given table, similarly to insert().
    /// In case there is a conflict, update the table instead, similarly to update().
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
        self.edit(
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

    /// Like upsert(), but in addition this function also returns the data that was
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
        self.edit(
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

    /// TODO: Add docstring.
    pub async fn table_exists(&self, table: &str) -> Result<bool, Error> {
        Ok(self.which_are_tables(&[table]).await?.len() == 1)
    }

    #[allow(unused)]
    /// drop_table()
    pub async fn drop_table(&self, table: &str) -> Result<(), Error> {
        self.pool.drop_table(table).await?;
        self.clear_cache_for_dropped_tables(&[&table]).await?;
        Ok(())
    }

    pub async fn drop_view(&self, view: &str) -> Result<(), Error> {
        self.pool.drop_view(view).await?;
        self.clear_cache_for_dropped_tables(&[&view]).await?;
        Ok(())
    }

    /// TODO: Add docstring.
    pub async fn transaction(&self) -> Result<AnyTransaction, Error> {
        Ok(AnyTransaction::begin(self.pool.transaction().await?))
    }

    /// TODO: Add docstring.
    pub async fn cache(&self, sql: &str, values: impl IntoValues) -> Result<Rows, Error> {
        match self.get_caching_strategy() {
            CachingStrategy::None => self.cache_tables(&[], sql, values).await,
            _ => {
                let tables_read = get_accessed_tables(sql)?;
                let tables_read: Vec<_> = tables_read.iter().map(|s| s.as_str()).collect();
                match tables_read.is_empty() {
                    false => self.cache_tables(&tables_read, sql, values).await,
                    true => Err(Error::InputError(format!(
                        "No tables are read from in SQL: {sql}"
                    ))),
                }
            }
        }
    }

    /// TODO: Add docstring.
    pub fn set_caching_strategy(&mut self, strategy: &CachingStrategy) {
        self.caching_strategy = *strategy;
    }

    /// Implements
    pub fn get_caching_strategy(&self) -> CachingStrategy {
        self.caching_strategy
    }

    /// Implements
    pub fn set_cache_aware_query(&mut self, value: bool) {
        self.cache_aware_query = value;
    }

    /// Implements
    pub fn get_cache_aware_query(&self) -> bool {
        self.cache_aware_query
    }

    /// TODO: Add docstring.
    pub async fn count_query_cache_rows(&self) -> Result<u64, Error> {
        match self.caching_strategy {
            CachingStrategy::None => Ok(0),
            CachingStrategy::Memory(_) => {
                Ok(self.memory_query_cache.get_cache()?.keys().len() as u64)
            }
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
    pub async fn count_table_cache_rows(&self) -> Result<u64, Error> {
        match self.caching_strategy {
            CachingStrategy::None => Ok(0),
            CachingStrategy::Memory(_) => {
                Ok(self.memory_table_cache.get_cache()?.keys().len() as u64)
            }
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

    /// TODO: Add docstring.
    pub fn clear_meta_cache(&self) -> Result<(), Error> {
        let mut cache = self.meta_cache.get_cache()?;
        cache.clear();
        Ok(())
    }

    /// TODO: Add docstring.
    pub fn clear_memory_query_cache(&self) -> Result<(), Error> {
        let mut cache = self.memory_query_cache.get_cache()?;
        cache.clear();
        Ok(())
    }

    /// TODO: Add docstring.
    pub fn clear_memory_table_cache(&self) -> Result<(), Error> {
        let mut cache = self.memory_table_cache.get_cache()?;
        cache.clear();
        Ok(())
    }

    ////////// Private functions //////////

    /// Similar to cache(). This version accepts an explicit list of tables, which
    /// must correspond to the tables queried from in the given SQL command(s).
    async fn cache_tables(
        &self,
        tables: &[&str],
        sql: &str,
        params: impl IntoValues,
    ) -> Result<Rows, Error> {
        let params = params.into_values()?.collect::<Vec<_>>();
        match self.get_caching_strategy() {
            CachingStrategy::None => {
                let rows = self.pool.query(sql, &params).await?;
                Ok(rows)
            }
            CachingStrategy::TruncateAll | CachingStrategy::Truncate => {
                self.ensure_cache_tables_exist().await?;
                self.update_cached_views(tables).await?;
                let rows = self.db_cache(tables, sql, &params).await?;
                Ok(rows)
            }
            CachingStrategy::Trigger => {
                let views = self
                    .which_are_views(tables)
                    .await?
                    .into_iter()
                    .collect::<HashSet<_>>();
                for table in tables
                    .into_iter()
                    .cloned()
                    .collect::<HashSet<_>>()
                    .difference(&views.iter().map(|v| v.as_str()).collect::<HashSet<_>>())
                    .collect::<Vec<_>>()
                {
                    self.ensure_caching_triggers_exist_for_table(&table).await?;
                }
                for view in &views {
                    self.ensure_caching_triggers_exist_for_view(view).await?;
                }
                let rows = self.db_cache(&tables, sql, &params).await?;
                Ok(rows)
            }
            CachingStrategy::Memory(cache_size) => {
                self.update_cached_views(tables).await?;
                let rows = self.mem_cache(tables, sql, &params, cache_size).await?;
                Ok(rows)
            }
        }
    }

    async fn db_cache(&self, tables: &[&str], sql: &str, params: &[Value]) -> Result<Rows, Error> {
        // Look in the cache to see if there is an entry corresponding to the given SQL
        // string for the given tables and parameters. If so, return the data from the
        // cache, otherwise execute the given SQL statement on the actualy specified
        // tables.
        let prefix = self.syntax().param_prefix().to_string();
        let cache_sql = format!(
            r#"SELECT {prefix}1||rtrim(ltrim("value", '['), ']')||{prefix}2 AS "value"
               FROM "{QUERY_CACHE_TABLE}"
               WHERE "tables" = {prefix}3
               AND "statement" = {prefix}4
               AND "parameters" = {prefix}5
               LIMIT 1"#,
        );
        let tables_param = format!(
            "[{}]",
            tables
                .iter()
                .map(|table| format!("\"{table}\""))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let params_param = {
            format!(
                "[{}]",
                params
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let cache_params = values!["[", "]", &*tables_param, sql, &*params_param];

        let strings = {
            let rows = self.pool.query(&cache_sql, &cache_params).await?;
            let strings = rows
                .iter()
                .map(|row| match row.values().nth(0) {
                    Some(value) => Ok(value.into()),
                    None => Err(Error::DataError("Empty row".to_owned())),
                })
                .collect::<Vec<_>>();
            let strings: Result<Vec<String>, Error> = strings.into_iter().collect();
            strings?
        };
        match strings.first() {
            Some(values) => {
                let db_rows: Vec<Row> = serde_json::from_str(values).map_err(|err| {
                    Error::DataError(format!("Error serializing values '{values}': {err}"))
                })?;
                // Only views need to be verified every time they are accessed. Tables
                // do not because they do not have any dependencies.
                if self.which_are_views(tables).await?.len() > 0 {
                    self.update_last_verified(tables, sql, &params).await?;
                }

                Ok(Rows { rows: db_rows })
            }
            None => {
                let db_rows = self.pool.query(sql, params).await?;
                let rows_as_string = {
                    let mut rows_as_string = vec![];
                    for db_row in db_rows.iter() {
                        let db_row = serde_json::to_string(db_row).map_err(|err| {
                            Error::DataError(format!("Invalid data ({err}): {db_row:?}"))
                        })?;
                        rows_as_string.push(db_row);
                    }
                    format!("[{}]", rows_as_string.join(", "))
                };
                let insert_sql = format!(
                    r#"INSERT INTO "{QUERY_CACHE_TABLE}"
                           ("tables", "statement", "parameters", "value")
                           VALUES ({prefix}1, {prefix}2, {prefix}3, {prefix}4)"#,
                    prefix = self.syntax().param_prefix(),
                );
                let insert_params = values![&*tables_param, sql, &*params_param, &*rows_as_string];
                self.pool.execute(&insert_sql, &insert_params).await?;
                Ok(db_rows)
            }
        }
    }

    async fn mem_cache(
        &self,
        tables: &[&str],
        sql: &str,
        values: &[Value],
        cache_size: usize,
    ) -> Result<Rows, Error> {
        let mem_key = MemoryQueryCacheKey {
            tables: format!(
                "[{}]",
                tables
                    .iter()
                    .map(|table| format!("\"{table}\""))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            statement: sql.to_string(),
            parameters: format!("{values:?}"),
        };
        let cached_rows = {
            let cache = self.memory_query_cache.get_cache()?;
            match cache.get(&mem_key) {
                Some(mem_value) => Some(mem_value.content.to_vec()),
                None => None,
            }
        };
        match cached_rows {
            Some(db_rows) => {
                // Only views need to be verified every time they are accessed. Tables
                // do not because they do not have any dependencies.
                if self.which_are_views(tables).await?.len() > 0 {
                    self.update_last_verified(tables, sql, &values).await?;
                }
                Ok(Rows { rows: db_rows })
            }
            None => {
                let db_rows = self.pool.query(sql, values).await?;
                let mut cache = self.memory_query_cache.get_cache()?;
                // If the number of entries exceeds the allowed cache size, remove the oldest
                // keys first.
                // TODO: We may want to do something smarter here. E.g., we could record the
                // length of time a query takes and/or the number of times it was requested
                // in order to determine which entries to delete (and/or the size of the
                // result).
                while cache.len() > cache_size {
                    cache.shift_remove_index(0);
                }
                cache.insert(
                    mem_key,
                    MemoryQueryCacheValue {
                        content: db_rows.to_vec(),
                        last_verified: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map_err(|err| {
                                Error::DataError(format!("Error getting epoch time: {err}"))
                            })?
                            .as_millis(),
                    },
                );
                Ok(db_rows)
            }
        }
    }

    /// Return a list of the objects from the given list that are tables.
    async fn which_are_tables(self: &AnyPool, objects: &[&str]) -> Result<Vec<String>, Error> {
        let mut tables = vec![];
        let mut unknowns = vec![];
        // Start by looking for the given objects in the meta cache:
        for object in objects {
            if self.meta_cache.exists(&format!("{object}_TABLE"))? {
                tables.push(object.to_string());
            } else if !self.meta_cache.exists(&format!("{object}_VIEW"))? {
                unknowns.push(object.to_string());
            }
        }

        // Query the database for the status of any unknowns:
        if !unknowns.is_empty() {
            let (sql, params) = self
                .syntax()
                .which_are_tables_sql(&unknowns.iter().map(|s| s.as_str()).collect::<Vec<_>>());
            let rows = self.pool.query(&sql, &params).await?;
            for row in rows.iter() {
                let table = row
                    .get("table_name")
                    .ok_or(Error::DataError("No table_name found in row".to_string()))?
                    .to_string();
                self.meta_cache.insert(&format!("{table}_TABLE"))?;
                tables.push(table);
            }

            let (sql, params) = self
                .syntax()
                .which_are_views_sql(&unknowns.iter().map(|s| s.as_str()).collect::<Vec<_>>());
            let rows = self.pool.query(&sql, &params).await?;
            for row in rows.iter() {
                let view = row
                    .get("view_name")
                    .ok_or(Error::DataError("No view_name found in row".to_string()))?
                    .to_string();
                self.meta_cache.insert(&format!("{view}_VIEW"))?;
            }
        }
        Ok(tables)
    }

    /// Return a list of the objects from the given list that are views.
    async fn which_are_views(self: &AnyPool, objects: &[&str]) -> Result<Vec<String>, Error> {
        let mut views = vec![];
        let mut unknowns = vec![];
        // Start by looking for the given objects in the meta cache:
        for object in objects {
            if self.meta_cache.exists(&format!("{object}_VIEW"))? {
                views.push(object.to_string());
            } else if !self.meta_cache.exists(&format!("{object}_TABLE"))? {
                unknowns.push(object.to_string());
            }
        }

        // Query the database for the status of any unknowns:
        if !unknowns.is_empty() {
            let (sql, params) = self
                .syntax()
                .which_are_views_sql(&unknowns.iter().map(|s| s.as_str()).collect::<Vec<_>>());
            let rows = self.pool.query(&sql, &params).await?;
            for row in rows.iter() {
                let view = row
                    .get("view_name")
                    .ok_or(Error::DataError("No view_name found in row".to_string()))?
                    .to_string();
                self.meta_cache.insert(&format!("{view}_VIEW"))?;
                views.push(view);
            }

            let (sql, params) = self
                .syntax()
                .which_are_tables_sql(&unknowns.iter().map(|s| s.as_str()).collect::<Vec<_>>());
            let rows = self.pool.query(&sql, &params).await?;
            for row in rows.iter() {
                let table = row
                    .get("table_name")
                    .ok_or(Error::DataError("No table_name found in row".to_string()))?
                    .to_string();
                self.meta_cache.insert(&format!("{table}_TABLE"))?;
            }
        }
        Ok(views)
    }

    /// Create the query cache table in the database.
    async fn create_query_cache_table(&self) -> Result<(), Error> {
        let sql = self.syntax().create_query_cache_table_sql();
        match self.pool.execute(&sql, &[]).await {
            Ok(_) => Ok(()),
            Err(_) => {
                // Since we are not using transactions, a race condition could occur in
                // which two or more threads are trying to create the cache at the same
                // time, triggering a primary key violation in the metadata table. So if
                // there is an error creating the cache table we just check that it exists
                // and if it does we assume that all is ok.
                match self.table_exists(QUERY_CACHE_TABLE).await? {
                    false => Err(Error::DatabaseError(format!(
                        "The cache table '{QUERY_CACHE_TABLE}' could not be created"
                    ))),
                    true => Ok(()),
                }
            }
        }
    }

    /// Create the table cache table in the database.
    async fn create_table_cache_table(&self) -> Result<(), Error> {
        let sql = self.syntax().create_table_cache_table_sql();
        match self.pool.execute(&sql, &[]).await {
            Ok(_) => Ok(()),
            Err(_) => {
                // Since we are not using transactions, a race condition could occur in
                // which two or more threads are trying to create the cache at the same
                // time, triggering a primary key violation in the metadata table. So if
                // there is an error creating the cache table we just check that it exists
                // and if it does we assume that all is ok.
                match self.table_exists(TABLE_CACHE_TABLE).await? {
                    false => Err(Error::DatabaseError(format!(
                        "The cache table '{TABLE_CACHE_TABLE}' could not be created"
                    ))),
                    true => Ok(()),
                }
            }
        }
    }

    /// TODO: Add docstring.
    async fn create_table_caching_triggers_for_table(&self, table: &str) -> Result<(), Error> {
        let sql = self
            .syntax()
            .create_table_caching_triggers_for_table_sql(&table)?
            .join(";\n");
        self.execute_batch(&sql).await?;
        Ok(())
    }

    /// TODO: Add docstring.
    async fn create_table_caching_triggers_for_view(
        &self,
        table: &str,
        view: &str,
    ) -> Result<(), Error> {
        let sql = self
            .syntax()
            .create_table_caching_triggers_for_view_sql(table, view)?
            .join(";\n");
        self.execute_batch(&sql).await?;
        Ok(())
    }

    /// Get the SQL code that is used to define the given view.
    async fn get_view_sql(&self, view: &str) -> Result<String, Error> {
        let view_sql = {
            let rows = {
                let (sql, params) = self.syntax().view_sql_sql(view);
                self.pool.query(&sql, &params).await?
            };
            match rows.first() {
                Some(row) => row
                    .get("sql")
                    .ok_or(Error::DataError("No column 'sql' in row".to_string()))?
                    .to_string(),
                None => {
                    return Err(Error::DataError(format!(
                        "No view definition found for '{view}'"
                    )));
                }
            }
        };
        Ok(view_sql)
    }

    /// Edit the given rows in the given table using the given queryable and optional returning
    /// clause (set with_returning = false to turn this off). When generating the SQL statements
    /// used to edit the table, do not use more than max_params bound parameters at a time. If more
    /// than max_params are required, multiple SQL statements will be generated.
    async fn edit(
        &self,
        edit_type: &EditType,
        max_params: &usize,
        table: &str,
        columns: &[&str],
        rows: impl IntoIterator<Item = &Row>,
        with_returning: bool,
        returning: &[&str],
    ) -> Result<Rows, Error> {
        // Begin by verifying that the given table name is valid, which has the side-effect of
        // removing any enclosing double-quotes:
        let table = validate_table_name(table)?;

        // This is very unlikely but we check anyway to be sure:
        if columns.len() > *max_params {
            return Err(Error::InputError(format!(
                "Unable to {} table '{}', which has more columns ({}) than the \
                 maximum number of variables ({}) allowed in a SQL statement by {}.",
                edit_type,
                table,
                columns.len(),
                max_params,
                self.syntax().name(),
            )));
        }

        // Use the `returning` argument to restrict the RETURNING clause, defaulting
        // to '*' if `returning` is empty:
        let returning_clause = match with_returning {
            true => match returning.is_empty() {
                true => format!("\nRETURNING *"),
                false => format!(
                    "\nRETURNING {}",
                    returning
                        .iter()
                        .map(|column| format!(r#""{table}"."{column}""#))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            },
            false => String::new(),
        };

        let primary_keys = match edit_type {
            EditType::Update | EditType::Upsert => match self.primary_keys(&table).await? {
                primary_keys if primary_keys.is_empty() => {
                    return Err(Error::InputError(
                        "Primary keys must not be empty.".to_string(),
                    ));
                }
                primary_keys
                    if !primary_keys
                        .iter()
                        .all(|pkey| columns.contains(&pkey.as_str())) =>
                {
                    return Err(Error::InputError(format!(
                        "Not all of the table's primary keys: {primary_keys:?} are in {columns:?}"
                    )));
                }
                primary_keys => primary_keys,
            },
            // Since we don't need a list of the table's primary keys to do an insert, we save
            // the database access and just return an empty list here:
            EditType::Insert => vec![],
        };

        // We use the column_map to determine the SQL type of each parameter.
        let column_map = self.columns(&table).await?;
        let param_prefix = self.syntax().param_prefix();
        let mut rows_to_return = vec![];
        let mut lines_to_bind = Vec::new();
        let mut params_to_be_bound = Vec::new();
        let mut param_idx = 0;

        // Closure used to edit the table by executing a SQL statement using the given data and SQL
        // clauses. After executing the statement, the lines and parameters to bind / be bound
        // are cleared, and any rows returned by the query are returned to the caller.
        let execute_batch_edit_and_reset = async |lines_to_bind: &mut Vec<String>,
                                                  param_idx: &mut usize,
                                                  params_to_be_bound: &mut Vec<Value>|
               -> Result<Rows, Error> {
            let sql = match edit_type {
                EditType::Update => generate_update_statement(
                    &table,
                    columns,
                    primary_keys
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .as_slice(),
                    &returning_clause,
                    lines_to_bind
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .as_slice(),
                ),
                EditType::Insert => generate_insert_statement(
                    &table,
                    columns,
                    &returning_clause,
                    lines_to_bind
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .as_slice(),
                ),
                EditType::Upsert => generate_upsert_statement(
                    &table,
                    columns,
                    primary_keys
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .as_slice(),
                    &returning_clause,
                    lines_to_bind
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .as_slice(),
                ),
            };
            let rows = self.pool.query(&sql, &params_to_be_bound[..]).await?;
            lines_to_bind.clear();
            params_to_be_bound.clear();
            *param_idx = 0;
            Ok(rows)
        };

        for row in rows {
            // If we have reached the limit on the number of bound parameters, edit the rows that
            // we have processed so far and then reset all of the counters and collections:
            if param_idx + columns.len() > *max_params {
                rows_to_return.append(
                    &mut execute_batch_edit_and_reset(
                        &mut lines_to_bind,
                        &mut param_idx,
                        &mut params_to_be_bound,
                    )
                    .await?
                    .rows,
                );
            }

            // Optimization to avoid repeated heap allocations while processing a single given row:
            params_to_be_bound.reserve(columns.len());
            let mut cells: Vec<String> = Vec::with_capacity(columns.len());
            for column in columns {
                let sql_type = column_map.get(*column).ok_or(Error::InputError(format!(
                    "Column '{column}' does not exist in table '{table}'"
                )))?;
                param_idx += 1;
                // In the CTE we generate for UPDATE statements, tokio-postgres can't infer the
                // types of the VALUES, so we explicitly cast them.
                if *edit_type == EditType::Update
                    && self.syntax().name() == "postgres"
                // We only need to cast the first value row. The rest are inferred by Postgres:
                    && lines_to_bind.len() == 0
                {
                    cells.push(format!(
                        "{param_prefix}{param_idx}::{}",
                        sql_type.to_uppercase()
                    ));
                } else {
                    cells.push(format!("{param_prefix}{param_idx}"));
                }
                let param = match row.get(*column) {
                    Some(value) => value.clone(),
                    None => Value::Null,
                };
                params_to_be_bound.push(param);
            }
            let line_to_bind = format!("({})", cells.join(", "));
            lines_to_bind.push(line_to_bind);
        }

        // If there is anything left to edit, do it now:
        if lines_to_bind.len() > 0 {
            rows_to_return.append(
                &mut execute_batch_edit_and_reset(
                    &mut lines_to_bind,
                    &mut param_idx,
                    &mut params_to_be_bound,
                )
                .await?
                .rows,
            );
        }

        self.clear_cache_for_edited_tables(&[&table]).await?;

        Ok(Rows {
            rows: rows_to_return,
        })
    }

    /* TODO:
    /// Read data from the given file and insert it to the given table in the database.
    pub async fn batch_insert(
        pool: &AnyPool,
        table: &str,
        columns: &IndexMap<String, Column>,
        filename: &str,
    ) -> Result<(), Error> {
        let batch_size = 100;

        eprintln!(
            "Loading table '{table}' from '{filename}' using batch_insert() \
             with batch size {batch_size}"
        );

        // TODO: Change the signature of insert() and similar methods so that they take iterators
        // as arguments instead of impl IntoRows. Then we will not have to collect the contents
        // of the file into a vector but can keep it in the form of an iterator as we do in
        // TokioPostgreSQLPool::load_table().

        let delimiter = {
            if filename.to_lowercase().ends_with("tsv") {
                b'\t'
            } else if filename.to_lowercase().ends_with(".csv") {
                b','
            } else {
                return Err(Error::InputError(format!(
                    "Filename: '{filename}' must end with .tsv or .csv"
                )));
            }
        };

        // Read the rows from the given file into a vector.
        let rdr =
            ReaderBuilder::new()
                .has_headers(false)
                .delimiter(delimiter)
                .from_reader(File::open(filename).map_err(|err| {
                    Error::InputError(format!("Unable to open '{filename}': {err}"))
                })?);
        let mut records = rdr.into_records();

        // Extract the columns from the first line of the file:
        let headers = {
            let headers = match records.next() {
                None => return Err(Error::InputError(format!("'{filename}' is empty"))),
                Some(record) => match record {
                    Err(err) => {
                        return Err(Error::InputError(format!(
                            "Error reading from '{filename}': {err}"
                        )));
                    }
                    Ok(headers) => headers.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                },
            };
            for header in &headers {
                if header.trim().is_empty() {
                    return Err(Error::InputError(format!(
                        "One or more of the header fields is empty in file '{filename}'"
                    )));
                }
            }
            headers
        };

        // Collect all of the rows into vectors and insert them
        // (TODO: See comment at the beginning of this function).
        let mut db_rows = vec![];
        let str_columns = headers.iter().map(|s| s.as_str()).collect::<Vec<_>>();
        for row in records {
            let row_values = row
                .map_err(|err| {
                    Error::DataError(format!("Error reading from file '{filename}': {err}"))
                })?
                .into_iter()
                .map(|value| Value::from(value))
                .collect::<Vec<_>>();
            let db_row = Row {
                map: zip(headers.clone(), row_values).collect::<IndexMap<_, _>>(),
            };

            // Logically we should be able to call coerce for both SQLite and PostgreSQL, but in the
            // case of SQLite, since it is so liberal about types, this doesn't actually matter, and
            // it avoids having to worry about differences in the rounding of real numbers between
            // libsql and rusqlite.
            if pool.kind().name() != "SQLite" {
                db_rows.push(db_row.coerce(columns)?);
            } else {
                db_rows.push(db_row);
            }

            // We don't insert more than batch_size at a time:
            if db_rows.len() >= batch_size {
                pool.insert(table, &str_columns, db_rows.clone()).await?;
                db_rows.clear();
            }
        }
        // Insert anything that's left:
        if db_rows.len() > 0 {
            pool.insert(table, &str_columns, db_rows).await?;
        }
        Ok(())
    }
    */

    /// Ensure that caching triggers exist for the given table. Note that this function calls
    /// ensure_cache_tables_exist() implicitly.
    pub async fn ensure_caching_triggers_exist_for_table(&self, table: &str) -> Result<(), Error> {
        let table_triggers_name = format!("{table}_triggers");
        if !self.meta_cache.exists(&table_triggers_name)? {
            self.ensure_cache_tables_exist().await?;
            self.create_table_caching_triggers_for_table(&table).await?;
            // Indicate that triggers exist for `table` in the meta-cache:
            self.meta_cache.insert(&table_triggers_name)?;
        }
        Ok(())
    }

    /// Ensure that the query cache table and the table cache table exist (see
    /// [QUERY_CACHE_TABLE] and [TABLE_CACHE_TABLE]).
    pub async fn ensure_cache_tables_exist(&self) -> Result<(), Error> {
        if !self.meta_cache.exists(QUERY_CACHE_TABLE)? {
            self.create_query_cache_table().await?;
            self.meta_cache.insert(QUERY_CACHE_TABLE)?;
        }
        if !self.meta_cache.exists(TABLE_CACHE_TABLE)? {
            self.create_table_cache_table().await?;
            self.meta_cache.insert(TABLE_CACHE_TABLE)?;
        }
        Ok(())
    }

    /// Ensure that caching triggers exist for the source tables of the given view. Note that
    /// this function calls ensure_cache_tables_exist() implicitly.
    pub async fn ensure_caching_triggers_exist_for_view(&self, view: &str) -> Result<(), Error> {
        let view_triggers_name = format!("{view}_triggers");
        if !self.meta_cache.exists(&view_triggers_name)? {
            self.ensure_cache_tables_exist().await?;
            let view_sql = self.get_view_sql(&view).await?;
            let source_tables = sql_parse::get_view_tables(&view_sql)?;
            for source_table in source_tables.iter() {
                // Add a trigger to clean entries from the cache for the source table itself:
                self.create_table_caching_triggers_for_table(source_table)
                    .await?;
                // Add a trigger to clean entries from the cache for the view:
                self.create_table_caching_triggers_for_view(&source_table, &view)
                    .await?;
                // Add an entry for the source table triggers to the metacache. If there is another
                // entry for this source table it will be overwritten, which is desirable in
                // case it was not previously known if the table was the source table for a view.
                self.meta_cache
                    .insert(&format!("{source_table}_triggers"))?;
            }
            self.meta_cache.insert(&view_triggers_name)?;
        }
        Ok(())
    }

    /// Parse the given semi-colon-separated SQL commands and determine which tables will be
    /// affected (either edited or dropped) by the commands, then ensure that there are no
    /// entries for those tables in the cache in accordance with the current [CachingStrategy].
    pub async fn clear_cache_for_affected_tables(&self, sql: &str) -> Result<(), Error> {
        if self.get_caching_strategy() != CachingStrategy::None {
            let (edited_tables, dropped_tables): (Vec<_>, Vec<_>) = {
                let (edited_tables, dropped_tables) = sql_parse::get_affected_tables(sql)?;
                (
                    edited_tables.into_iter().collect(),
                    dropped_tables.into_iter().collect(),
                )
            };
            if !edited_tables.is_empty() {
                let edited_tables: Vec<_> = edited_tables.iter().map(|t| t.as_str()).collect();
                self.clear_cache_for_edited_tables(&edited_tables).await?;
            }
            if !dropped_tables.is_empty() {
                let dropped_tables: Vec<_> = dropped_tables.iter().map(|t| t.as_str()).collect();
                self.clear_cache_for_dropped_tables(&dropped_tables).await?;
            }
        }
        Ok(())
    }

    // Triggers cannot apply to DROP commands, only to INSERT, UPDATE, DELETE, or TRUNCATE.
    // See https://www.postgresql.org/docs/current/sql-createtrigger.html and
    // https://sqlite.org/lang_createtrigger.html. Note that PostgreSQL has the concept
    // of an "event trigger":
    // https://www.pgtutorial.com/postgresql-tutorial/postgresql-event-triggers/ which could
    // be used, but SQLite has no such capability. To workaround this limitation, we
    // define two clear_cache_() functions, one for edited tables, and one for dropped tables.
    // In the case of a dropped table, unlike an edit, we cannot rely on the caching trigger,
    // when we are using the [CachingStategy::Trigger] strategy, to automatically delete the
    // entries from the cache for those tables, since those triggers will have beeen dropped
    // along with the table.
    // Although strictly speaking, PostgreSQL (which has event triggers) is not subject to this
    // limitation, for simplicity we will not be creating a PostgreSQL event trigger and we will
    // use both functions below for both database types.

    /// Update the cache tables, for the given list of tables, using the current [CachingStrategy],
    /// under the assumption that the tables in the given list have all just been edited (i.e.,
    /// truncated, deleted from, inserted to, or updated).
    pub async fn clear_cache_for_edited_tables(&self, tables: &[&str]) -> Result<(), Error> {
        match self.get_caching_strategy() {
            CachingStrategy::None | CachingStrategy::Trigger => (),
            CachingStrategy::TruncateAll => {
                self.update_last_modified_times(tables).await?;
                self.delete_query_cache_entries(&[]).await?
            }
            CachingStrategy::Truncate => {
                self.update_last_modified_times(tables).await?;
                self.delete_query_cache_entries(tables).await?
            }
            CachingStrategy::Memory(_) => {
                self.update_last_modified_times(tables).await?;
                self.memory_query_cache.clear(tables)?;
            }
        };
        Ok(())
    }

    /// Update the cache tables for the given list of tables, using the current [CachingStrategy],
    /// under the assumption that the tables in the given list have all just been dropped.
    pub async fn clear_cache_for_dropped_tables(&self, tables: &[&str]) -> Result<(), Error> {
        if let CachingStrategy::Memory(_) = self.get_caching_strategy() {
            self.update_last_modified_times(tables).await?;
            self.memory_query_cache.clear(&tables)?;
        } else {
            // Do not clear the cache if the dropped tables include the cache tables themselves:
            if !tables
                .iter()
                .any(|table| [QUERY_CACHE_TABLE, TABLE_CACHE_TABLE].contains(table))
            {
                match self.get_caching_strategy() {
                    CachingStrategy::Memory(_) => unreachable!(),
                    CachingStrategy::None => (),
                    CachingStrategy::TruncateAll => {
                        self.update_last_modified_times(tables).await?;
                        self.delete_query_cache_entries(&[]).await?;
                    }
                    CachingStrategy::Trigger | CachingStrategy::Truncate => {
                        self.update_last_modified_times(tables).await?;
                        self.delete_query_cache_entries(tables).await?;
                    }
                }
            }
        }
        // Update the meta-cache to remove any entries associated with tables that no longer exist:
        let mut meta_cache = self.meta_cache.get_cache()?;
        for table in tables {
            if *table == QUERY_CACHE_TABLE {
                meta_cache.remove(QUERY_CACHE_TABLE);
            } else if *table == TABLE_CACHE_TABLE {
                meta_cache.remove(TABLE_CACHE_TABLE);
            } else {
                meta_cache.remove(&format!("{table}_triggers"));
                meta_cache.remove(&format!("{table}_VIEW"));
                meta_cache.remove(&format!("{table}_TABLE"));
            }
        }
        Ok(())
    }

    /// Update the last verified time of the query cache entry identified by the triple:
    /// (tables, statement, params).
    pub async fn update_last_verified(
        &self,
        tables: &[&str],
        statement: &str,
        params: &[Value],
    ) -> Result<(), Error> {
        match self.get_caching_strategy() {
            CachingStrategy::Memory(_) => {
                let mut cache = self.memory_query_cache.get_cache()?;
                let epoch_now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|err| Error::DataError(format!("Error getting epoch time: {err}")))?;
                let mem_key = MemoryQueryCacheKey {
                    tables: format!(
                        "[{}]",
                        tables
                            .iter()
                            .map(|table| format!("\"{table}\""))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                    statement: statement.to_string(),
                    parameters: format!("{params:?}"),
                };
                match cache.get_mut(&mem_key) {
                    Some(value) => value.last_verified = epoch_now.as_millis(),
                    None => (),
                };
            }
            _ => match self.table_exists(QUERY_CACHE_TABLE).await? {
                true => {
                    let tables_param = format!(
                        "[{}]",
                        tables
                            .iter()
                            .map(|table| format!("\"{table}\""))
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                    self.pool
                        .execute(
                            &format!(
                                r#"UPDATE "{QUERY_CACHE_TABLE}"
                               SET "last_verified" = {ts}
                               WHERE "tables" = {p}1
                               AND "statement" = {p}2
                               AND "parameters" = {p}3"#,
                                p = self.syntax().param_prefix(),
                                ts = self.syntax().get_epoch_time_sql(),
                            ),
                            &values![
                                &*format!("[{tables_param}]"),
                                &*statement,
                                &*format!("{params:?}"),
                            ][..],
                        )
                        .await?;
                }
                false => (),
            },
        };
        Ok(())
    }

    /// Update the last modified times of each of the given tables in the table cache.
    pub async fn update_last_modified_times(&self, tables: &[&str]) -> Result<(), Error> {
        match self.get_caching_strategy() {
            CachingStrategy::Memory(_) => {
                let mut cache = self.memory_table_cache.get_cache()?;
                let epoch_now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|err| Error::DataError(format!("Error getting epoch time: {err}")))?;
                for table in tables {
                    cache.insert(table.to_string(), epoch_now.as_millis());
                }
            }
            _ => {
                if self.table_exists(TABLE_CACHE_TABLE).await? {
                    for table in tables {
                        let sql = format!(
                            r#"INSERT INTO "{TABLE_CACHE_TABLE}" ("table", "last_modified")
                               VALUES ({prefix}1, {ts})
                               ON CONFLICT ("table") DO UPDATE SET "last_modified" = {ts}"#,
                            prefix = self.syntax().param_prefix(),
                            ts = self.syntax().get_epoch_time_sql(),
                        );
                        self.pool.execute(&sql, &values![*table]).await?;
                    }
                }
            }
        };
        Ok(())
    }

    /// Delete the entries for the tables in the given list (independently of the current
    /// caching strategy) from the query cache table, if it exists. If the given list is empty,
    /// clear the entire query cache table.
    pub async fn delete_query_cache_entries(&self, tables: &[&str]) -> Result<(), Error> {
        if self.table_exists(QUERY_CACHE_TABLE).await? {
            if tables.is_empty() {
                self.pool
                    .execute(&format!(r#"DELETE FROM "{QUERY_CACHE_TABLE}""#), &[])
                    .await?;
            } else {
                for table in tables {
                    let table_param = format!(r#"%"{table}"%"#);
                    self.pool
                        .execute(
                            &format!(
                                r#"DELETE FROM "{QUERY_CACHE_TABLE}" WHERE "tables" LIKE {}1"#,
                                self.syntax().param_prefix()
                            ),
                            &values![table_param],
                        )
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Uses the current caching strategy to clear the query cache for any of the given tables
    /// that (a) are views and (b) have source tables that have been modified more recently than
    /// the view. This function works both with database and memory cache strategies.
    pub async fn update_cached_views(&self, tables: &[&str]) -> Result<(), Error> {
        let views = self.which_are_views(tables).await?;
        match self.get_caching_strategy() {
            CachingStrategy::Memory(_) => {
                for view in &views {
                    let last_verified = {
                        let mut last_verified = 0;
                        for (key, value) in self.memory_query_cache.get_cache()?.iter() {
                            if key.tables.contains(view) && value.last_verified > last_verified {
                                last_verified = value.last_verified;
                            }
                        }
                        last_verified
                    };
                    let view_sql = self.get_view_sql(&view).await?;
                    let view_tables = sql_parse::get_view_tables(&view_sql)?;
                    let last_modified = {
                        let mut latest_last_modified = 0;
                        for view_table in &view_tables {
                            match self.memory_table_cache.get_cache()?.get(view_table) {
                                Some(lm) if *lm > latest_last_modified => {
                                    latest_last_modified = *lm;
                                }
                                _ => (),
                            };
                        }
                        latest_last_modified
                    };
                    if last_modified >= last_verified {
                        self.memory_query_cache.clear(&[view])?;
                    }
                }
            }
            _ => {
                for view in &views {
                    let last_verified = self.last_verified(&view).await?;
                    let view_sql = self.get_view_sql(&view).await?;
                    let view_tables = sql_parse::get_view_tables(&view_sql)?;
                    let last_modified = self
                        .get_latest_last_modified(
                            &view_tables.iter().map(|t| t.as_str()).collect::<Vec<_>>(),
                        )
                        .await?;
                    if last_modified >= last_verified {
                        self.delete_query_cache_entries(&[&view]).await?;
                    }
                }
            }
        };
        Ok(())
    }

    /// Returns the latest of the last modified times of the given tables in the table cache.
    pub async fn get_latest_last_modified(&self, tables: &[&str]) -> Result<u64, Error> {
        match self.table_exists(TABLE_CACHE_TABLE).await? {
            true => {
                let prefix = self.syntax().param_prefix().to_string();
                let mut placeholders = vec![];
                let mut parameters = vec![];
                for (i, table) in tables.iter().enumerate() {
                    let i = i + 1;
                    placeholders.push(format!("{prefix}{i}"));
                    parameters.push(Value::from(*table));
                }
                let placeholders = placeholders.join(",");

                let sql = format!(
                    r#"SELECT "last_modified"
                       FROM "{TABLE_CACHE_TABLE}"
                       WHERE "table" IN ({placeholders})
                       ORDER BY "last_modified" DESC
                       LIMIT 1"#,
                );
                let rows = self.pool.query(&sql, &parameters).await?;
                match rows.len() {
                    0 => Ok(0),
                    1 => {
                        let row = &rows[0];
                        match row.get("last_modified") {
                            Some(last_modified) => Ok(last_modified.try_into()?),
                            None => Err(Error::DataError(format!(
                                "No field 'last_modified' in row {row:?}"
                            ))),
                        }
                    }
                    too_many => Err(Error::DataError(format!(
                        "Too many rows returned: {too_many} from table {TABLE_CACHE_TABLE}"
                    ))),
                }
            }
            false => Ok(0),
        }
    }

    /// Gets the last time the given table was modified, as read from the table cache table.
    /// If there is no entry for the table in the table cache, or if the table cache does not
    /// exist, returns 0.
    pub async fn last_modified(&self, table: &str) -> Result<u64, Error> {
        self.get_latest_last_modified(&[table]).await
    }

    /// Gets the last time that the given table was verified, as read from the query cache table.
    /// If there is no entry involving the given table in the query cache, or if the query cache
    /// table doesn't exist, returns 0.
    pub async fn last_verified(&self, table: &str) -> Result<u64, Error> {
        match self.table_exists(QUERY_CACHE_TABLE).await? {
            true => {
                let sql = format!(
                    r#"SELECT MAX("last_verified") AS "last_verified"
                       FROM "{QUERY_CACHE_TABLE}"
                       WHERE "tables" LIKE {p}1"#,
                    p = self.syntax().param_prefix(),
                );
                let table_param = format!(r#"%"{table}"%"#);
                let rows = self
                    .pool
                    .query(&sql, &values![table_param.as_str()])
                    .await?;
                match rows.first() {
                    Some(row) => match row.get("last_verified") {
                        Some(value) if *value == Value::Null => Ok(0),
                        Some(value) => Ok(value.try_into()?),
                        None => Err(Error::DataError(format!(
                            "No 'last_verified' found in row: {row:?}"
                        ))),
                    },
                    None => Ok(0),
                }
            }
            false => Ok(0),
        }
    }
}

// Private helper functions:

/// Generate a SQL UPDATE statement for the given table and columns using the given clauses
/// and the given value lines.
#[allow(unused)]
fn generate_update_statement(
    table: &str,
    columns: &[&str],
    primary_keys: &[&str],
    returning_clause: &str,
    value_lines: &[&str],
) -> String {
    // Quote the column names to avoid potential clashes with database keywords:
    let quoted_columns = columns
        .iter()
        .map(|c| format!(r#""{c}""#))
        .collect::<Vec<_>>()
        .join(", ");

    let set_clause = columns
        .iter()
        .filter(|column| !primary_keys.contains(&column))
        .map(|column| format!(r#""{column}" = "source"."{column}""#))
        .collect::<Vec<_>>()
        .join(", ");

    let where_clause = primary_keys
        .iter()
        .map(|pk| format!(r#""{table}"."{pk}" = "source"."{pk}""#,))
        .collect::<Vec<_>>()
        .join(" AND ");

    format!(
        r#"WITH "source" ({quoted_columns}) AS (
             VALUES
             {}
           )
           UPDATE "{table}"
           SET {set_clause}
           FROM "source"
           WHERE {where_clause}{returning_clause}"#,
        value_lines.join(",\n")
    )
}

/// Generate a SQL INSERT statement for the given table and columns using the given clauses
/// and the given value lines.
#[allow(unused)]
fn generate_insert_statement(
    table: &str,
    columns: &[&str],
    returning_clause: &str,
    value_lines: &[&str],
) -> String {
    // Quote the column names to avoid potential clashes with database keywords:
    let quoted_columns = columns
        .iter()
        .map(|c| format!(r#""{c}""#))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        r#"INSERT INTO "{table}" ({quoted_columns})
           VALUES
           {}{returning_clause}"#,
        value_lines.join(",\n")
    )
}

/// Generate SQL statement of the form:
/// INSERT INTO <table> VALUES <tuples> ON CONFLICT (<primary key constraint>) DO UPDATE ...
#[allow(unused)]
fn generate_upsert_statement(
    table: &str,
    columns: &[&str],
    primary_keys: &[&str],
    returning_clause: &str,
    value_lines: &[&str],
) -> String {
    let quoted_columns = columns
        .iter()
        .map(|c| format!(r#""{c}""#))
        .collect::<Vec<_>>()
        .join(", ");

    let constraint_clause = primary_keys
        .iter()
        .map(|pk| format!(r#""{pk}""#))
        .collect::<Vec<_>>()
        .join(", ");

    let set_clause = columns
        .iter()
        .filter(|column| !primary_keys.contains(&column))
        .map(|column| format!(r#""{column}" = "excluded"."{column}""#))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        r#"INSERT INTO "{table}" ({quoted_columns})
           VALUES
           {}
           ON CONFLICT ({constraint_clause}) DO UPDATE SET {set_clause}{returning_clause}"#,
        value_lines.join(",\n"),
    )
}
