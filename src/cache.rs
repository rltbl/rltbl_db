//! Code for query caching.

use crate::{
    core::{DbError, DbQuery},
    db_value::{DbParams, DbRow, DbRows, DbValue, IntoDbParams},
    params,
    parse::{get_accessed_tables, get_affected_tables, get_view_tables},
};

use async_trait::async_trait;
use indexmap::IndexMap;
use lazy_static::lazy_static;
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    str::FromStr,
    sync::{Mutex, MutexGuard},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// The name of the database's query cache table.
pub static QUERY_CACHE_TABLE: &str = "rltbl_db_query_cache";

/// The name of the database's table cache table.
pub static TABLE_CACHE_TABLE: &str = "rltbl_db_table_cache";

/// Default size for the in-memory query cache
pub static DEFAULT_MEMORY_QUERY_CACHE_SIZE: usize = 1000;

// Maximum number of times to try to retrieve an in-memory cache (retrieval will fail when the
// cache is locked by another thread).
static MAX_RETRIEVAL_ATTEMPTS: usize = 20;

lazy_static! {
    /// The in-memory query cache, used by [CachingStrategy::Memory].
    pub static ref MEMORY_QUERY_CACHE: Mutex<IndexMap<MemoryQueryCacheKey, MemoryQueryCacheValue>>
        = Mutex::new(IndexMap::new());

    /// The in-memory table cache, used by [CachingStrategy::Memory].
    pub static ref MEMORY_TABLE_CACHE: Mutex<HashMap<String, u128>> = Mutex::new(HashMap::new());

    /// The in-memory meta cache. It holds a set of things known to exist.
    pub static ref MEMORY_META_CACHE: Mutex<HashSet<String>>
        = Mutex::new(HashSet::new());
}

/// Strategy to use when caching query results
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CachingStrategy {
    /// No Caching.
    None,
    /// Truncate the entire cache when it is dirty.
    TruncateAll,
    /// Truncate entries only for edited tables when the cache is dirty.
    Truncate,
    /// Truncate cache entries, for edited tables only, automatically whenever tables are edited.
    Trigger,
    /// Similar to Truncate, but use an in-memory cache.
    Memory(usize),
}

impl FromStr for CachingStrategy {
    type Err = DbError;

    fn from_str(strategy: &str) -> Result<Self, DbError> {
        match strategy.to_lowercase().as_str() {
            "none" => Ok(CachingStrategy::None),
            "truncate_all" => Ok(CachingStrategy::TruncateAll),
            "truncate" => Ok(CachingStrategy::Truncate),
            "trigger" => Ok(CachingStrategy::Trigger),
            strategy if strategy.starts_with("memory") => {
                let elems = strategy.split(":").collect::<Vec<_>>();
                let cache_size = {
                    if elems.len() < 2 {
                        DEFAULT_MEMORY_QUERY_CACHE_SIZE
                    } else {
                        let cache_size = elems[1];
                        match cache_size.parse::<usize>() {
                            Ok(0) => DEFAULT_MEMORY_QUERY_CACHE_SIZE,
                            Ok(size) => size,
                            Err(err) => return Err(DbError::InputError(format!(
                                "Error parsing memory cache size specification: '{cache_size}': \
                                 {err}"
                            ))
                            .into()),
                        }
                    }
                };
                Ok(CachingStrategy::Memory(cache_size))
            }
            _ => {
                return Err(
                    DbError::InputError(format!("Unrecognized strategy: {strategy}")).into(),
                );
            }
        }
    }
}

impl Display for CachingStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CachingStrategy::None => write!(f, "none"),
            CachingStrategy::TruncateAll => write!(f, "truncate_all"),
            CachingStrategy::Truncate => write!(f, "truncate"),
            CachingStrategy::Trigger => write!(f, "trigger"),
            CachingStrategy::Memory(size) => write!(f, "memory:{size}"),
        }
    }
}

/// The structure used to look up query results in the in-memory query cache.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MemoryQueryCacheKey {
    pub tables: String,
    pub statement: String,
    pub parameters: String,
}

/// Represents the value of an entry in the in-memory query cache.
#[derive(Clone, Debug)]
pub struct MemoryQueryCacheValue {
    pub content: Vec<DbRow>,
    pub last_verified: u128,
}

/// Returns true if the given object exists in the meta-cache.
pub fn exists_in_meta_cache(object: &str) -> Result<bool, DbError> {
    match get_meta_cache()?.get(object) {
        Some(_) => Ok(true),
        None => Ok(false),
    }
}

/// Retrieve the in-memory meta-cache [MEMORY_META_CACHE].
pub fn get_meta_cache<'a>() -> Result<MutexGuard<'a, HashSet<String>>, DbError> {
    let mut remaining_attempts = MAX_RETRIEVAL_ATTEMPTS;
    let mut meta_cache = MEMORY_META_CACHE.try_lock();
    while let Err(err) = meta_cache {
        meta_cache = MEMORY_META_CACHE.try_lock();
        if let Ok(_) = meta_cache {
            break;
        }
        remaining_attempts -= 1;
        if remaining_attempts == 0 {
            return Err(DbError::ConnectError(format!(
                "Error locking cache: {err} (retried {MAX_RETRIEVAL_ATTEMPTS} times)"
            )));
        } else {
            thread::sleep(Duration::from_millis(5));
        }
    }
    let meta_cache = meta_cache.unwrap();
    Ok(meta_cache)
}

/// Retrieve the in-memory query cache (see [MEMORY_QUERY_CACHE]).
pub fn get_memory_query_cache<'a>()
-> Result<MutexGuard<'a, IndexMap<MemoryQueryCacheKey, MemoryQueryCacheValue>>, DbError> {
    let mut remaining_attempts = MAX_RETRIEVAL_ATTEMPTS;
    let mut memory_cache = MEMORY_QUERY_CACHE.try_lock();
    while let Err(err) = memory_cache {
        memory_cache = MEMORY_QUERY_CACHE.try_lock();
        if let Ok(_) = memory_cache {
            break;
        }
        remaining_attempts -= 1;
        if remaining_attempts == 0 {
            return Err(DbError::ConnectError(format!(
                "Error locking cache: {err} (retried {MAX_RETRIEVAL_ATTEMPTS} times)"
            )));
        } else {
            thread::sleep(Duration::from_millis(5));
        }
    }
    let memory_cache = memory_cache.unwrap();
    Ok(memory_cache)
}

/// Retrieve the in-memory table cache (see [MEMORY_TABLE_CACHE]).
pub fn get_memory_table_cache<'a>() -> Result<MutexGuard<'a, HashMap<String, u128>>, DbError> {
    let mut remaining_attempts = MAX_RETRIEVAL_ATTEMPTS;
    let mut memory_cache = MEMORY_TABLE_CACHE.try_lock();
    while let Err(err) = memory_cache {
        memory_cache = MEMORY_TABLE_CACHE.try_lock();
        if let Ok(_) = memory_cache {
            break;
        }
        remaining_attempts -= 1;
        if remaining_attempts == 0 {
            return Err(DbError::ConnectError(format!(
                "Error locking cache: {err} (retried {MAX_RETRIEVAL_ATTEMPTS} times)"
            )));
        } else {
            thread::sleep(Duration::from_millis(5));
        }
    }
    let memory_cache = memory_cache.unwrap();
    Ok(memory_cache)
}

/// Retrieve a copy of the contents of the meta cache.
pub fn get_meta_cache_contents() -> Result<HashSet<String>, DbError> {
    let cache = get_meta_cache()?;
    Ok(cache.clone())
}

/// Retrieve a copy of the contents of the memory query cache.
pub fn get_memory_query_cache_contents()
-> Result<IndexMap<MemoryQueryCacheKey, MemoryQueryCacheValue>, DbError> {
    let cache = get_memory_query_cache()?;
    Ok(cache.clone())
}

/// Retrieve a copy of the contents of the memory table cache.
pub fn get_memory_table_cache_contents() -> Result<HashMap<String, u128>, DbError> {
    let cache = get_memory_table_cache()?;
    Ok(cache.clone())
}

/// Clear the meta cache.
pub fn clear_meta_cache() -> Result<(), DbError> {
    let mut cache = get_meta_cache()?;
    cache.clear();
    Ok(())
}

/// Clear the memory query cache.
pub fn clear_memory_query_cache(tables: &[&str]) -> Result<(), DbError> {
    let mut cache = get_memory_query_cache()?;
    if tables.is_empty() {
        cache.clear();
    }
    let keys = cache
        .keys()
        .map(|k| k)
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    for table in tables {
        for key in keys.iter() {
            if key.tables.contains(table) {
                cache.shift_remove(key);
            }
        }
    }
    Ok(())
}

/// Clear the memory table cache.
pub fn clear_memory_table_cache(tables: &[&str]) -> Result<(), DbError> {
    let mut cache = get_memory_table_cache()?;
    if tables.is_empty() {
        cache.clear();
    }
    for table in tables {
        cache.remove(&table.to_string());
    }
    Ok(())
}

/// The DbCache trait implements caching functionality for anything that also implements [DbQuery].
#[async_trait]
pub trait DbCache: DbQuery {
    /// Set the caching strategy.
    fn set_caching_strategy(&mut self, strategy: &CachingStrategy);

    /// Get the current caching strategy.
    fn get_caching_strategy(&self) -> CachingStrategy;

    /// When turned on, and the current [CachingStrategy] is not [None](CachingStrategy::None),
    /// SQL commands executed through the API will be automatically checked to see if they
    /// involve edits and/or drops of database tables. If they do then the cache will be
    /// automatically updated in accordance with the current [CachingStrategy].
    /// Note that setting this flag does not imply that the results of queries should be
    /// cached. Setting this flag only means that the current contents, if any, of the cache
    /// table should be kept up to date whenever the data in the database is edited
    /// via one of the query_* or execute() methods in [DbQuery]. To add new content to the cache
    /// that can be later be reused you must explicitly use the [DbCache::cache()] method.
    /// To explicitly skip the housekeeping implied by setting the cache-aware-query flag, even
    /// when it is set to on, use [DbQuery::execute_no_cache_clean()] or
    /// [DbQuery::query_no_cache_clean()].
    fn set_cache_aware_query(&mut self, value: bool);

    /// Returns true if the cache-aware-query option is currently on.
    fn get_cache_aware_query(&self) -> bool;

    /// Ensure that the query cache table and the table cache table exist (see
    /// [QUERY_CACHE_TABLE] and [TABLE_CACHE_TABLE]).
    async fn ensure_cache_tables_exist(&self) -> Result<(), DbError> {
        if !exists_in_meta_cache(QUERY_CACHE_TABLE)? || !exists_in_meta_cache(TABLE_CACHE_TABLE)? {
            for special_table in [QUERY_CACHE_TABLE, TABLE_CACHE_TABLE] {
                let sql = match special_table {
                    table if table == QUERY_CACHE_TABLE => {
                        self.kind().create_query_cache_table_sql()
                    }
                    table if table == TABLE_CACHE_TABLE => {
                        self.kind().create_table_cache_table_sql()
                    }
                    _ => unreachable!(),
                };
                match self.execute_no_cache_clean(&sql, ()).await {
                    Ok(_) => (),
                    Err(_) => {
                        // Since we are not using transactions, a race condition could occur in
                        // which two or more threads are trying to create the cache at the same
                        // time, triggering a primary key violation in the metadata table. So if
                        // there is an error creating the cache table we just check that it exists
                        // and if it does we assume that all is ok.
                        match self.table_exists(special_table).await? {
                            false => {
                                return Err(DbError::DatabaseError(format!(
                                    "The cache table '{special_table}' could not be created"
                                )));
                            }
                            true => (),
                        }
                    }
                };
                let mut cache = get_meta_cache()?;
                cache.insert(special_table.to_string());
            }
        }
        Ok(())
    }

    /// Ensure that caching triggers exist for the given table. Note that this function calls
    /// [DbCache::ensure_cache_tables_exist()] implicitly.
    async fn ensure_caching_triggers_exist_for_table(&self, table: &str) -> Result<(), DbError> {
        let table_triggers_name = format!("{table}_triggers");
        if !exists_in_meta_cache(&table_triggers_name)? {
            self.ensure_cache_tables_exist().await?;
            let sql = self
                .kind()
                .create_table_caching_triggers_for_table_sql(&table)?
                .join(";\n");
            self.execute_batch(&sql).await?;

            // Indicate that triggers exist for `table` in the meta-cache:
            let mut cache = get_meta_cache()?;
            cache.insert(table_triggers_name);
        }
        Ok(())
    }

    /// Ensure that caching triggers exist for the source tables of the given view. Note that
    /// this function calls [DbCache::ensure_cache_tables_exist()] implicitly.
    async fn ensure_caching_triggers_exist_for_view(&self, view: &str) -> Result<(), DbError> {
        let view_triggers_name = format!("{view}_triggers");
        if !exists_in_meta_cache(&view_triggers_name)? {
            self.ensure_cache_tables_exist().await?;
            let view_sql = self.get_view_sql(&view).await?;
            let source_tables = get_view_tables(&view_sql)?;
            for source_table in source_tables.iter() {
                // Add a trigger to clean entries from the cache for the source table itself:
                let sql = self
                    .kind()
                    .create_table_caching_triggers_for_table_sql(&source_table)?
                    .join(";\n");
                self.execute_batch(&sql).await?;
                // Add a trigger to clean entries from the cache for the view:
                let sql = self
                    .kind()
                    .create_table_caching_triggers_for_view_sql(&source_table, &view)?
                    .join(";\n");
                self.execute_batch(&sql).await?;
                // Add an entry for the source table triggers to the metacache. If there is another
                // entry for this source table it will be overwritten, which is desirable in
                // case it was not previously known if the table was the source table for a view.
                let mut cache = get_meta_cache()?;
                let source_triggers_name = format!("{source_table}_triggers");
                cache.insert(source_triggers_name);
            }
            let mut cache = get_meta_cache()?;
            cache.insert(view_triggers_name);
        }
        Ok(())
    }

    /// Parse the given semi-colon-separated SQL commands and determine which tables will be
    /// affected (either edited or dropped) by the commands, then ensure that there are no
    /// entries for those tables in the cache in accordance with the current [CachingStrategy].
    async fn clear_cache_for_affected_tables(&self, sql: &str) -> Result<(), DbError> {
        if self.get_caching_strategy() != CachingStrategy::None {
            let (edited_tables, dropped_tables): (Vec<_>, Vec<_>) = {
                let (edited_tables, dropped_tables) = get_affected_tables(sql)?;
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
    async fn clear_cache_for_edited_tables(&self, tables: &[&str]) -> Result<(), DbError> {
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
                clear_memory_query_cache(tables)?;
            }
        };
        Ok(())
    }

    /// Update the cache tables for the given list of tables, using the current [CachingStrategy],
    /// under the assumption that the tables in the given list have all just been dropped.
    async fn clear_cache_for_dropped_tables(&self, tables: &[&str]) -> Result<(), DbError> {
        if let CachingStrategy::Memory(_) = self.get_caching_strategy() {
            self.update_last_modified_times(tables).await?;
            clear_memory_query_cache(&tables)?;
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
        let mut meta_cache = get_meta_cache()?;
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
    async fn update_last_verified(
        &self,
        tables: &[&str],
        statement: &str,
        params: &DbParams,
    ) -> Result<(), DbError> {
        match self.get_caching_strategy() {
            CachingStrategy::Memory(_) => {
                let mut cache = get_memory_query_cache()?;
                let epoch_now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|err| {
                        DbError::DataError(format!("Error getting epoch time: {err}"))
                    })?;
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
                    self.execute(
                        &format!(
                            r#"UPDATE "{QUERY_CACHE_TABLE}"
                               SET "last_verified" = {ts}
                               WHERE "tables" = {p}1
                               AND "statement" = {p}2
                               AND "parameters" = {p}3"#,
                            p = self.kind().param_prefix(),
                            ts = self.kind().get_epoch_time_sql(),
                        ),
                        &[
                            &format!("[{tables_param}]"),
                            statement,
                            &format!("{params:?}"),
                        ],
                    )
                    .await?;
                }
                false => (),
            },
        };
        Ok(())
    }

    /// Update the last modified times of each of the given tables in the table cache.
    async fn update_last_modified_times(&self, tables: &[&str]) -> Result<(), DbError> {
        match self.get_caching_strategy() {
            CachingStrategy::Memory(_) => {
                let mut cache = get_memory_table_cache()?;
                let epoch_now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|err| {
                        DbError::DataError(format!("Error getting epoch time: {err}"))
                    })?;
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
                            prefix = self.kind().param_prefix(),
                            ts = self.kind().get_epoch_time_sql(),
                        );
                        self.execute_no_cache_clean(&sql, params![table]).await?;
                    }
                }
            }
        };
        Ok(())
    }

    /// Delete the entries for the tables in the given list (independently of the current
    /// caching strategy) from the query cache table, if it exists. If the given list is empty,
    /// clear the entire query cache table.
    async fn delete_query_cache_entries(&self, tables: &[&str]) -> Result<(), DbError> {
        if self.table_exists(QUERY_CACHE_TABLE).await? {
            if tables.is_empty() {
                self.execute_no_cache_clean(&format!(r#"DELETE FROM "{QUERY_CACHE_TABLE}""#), ())
                    .await?;
            } else {
                for table in tables {
                    let table_param = format!(r#"%"{table}"%"#);
                    self.execute_no_cache_clean(
                        &format!(
                            r#"DELETE FROM "{QUERY_CACHE_TABLE}" WHERE "tables" LIKE {}1"#,
                            self.kind().param_prefix()
                        ),
                        &[table_param],
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
    async fn update_cached_views(&self, tables: &[&str]) -> Result<(), DbError> {
        let views = self.which_are_views(tables).await?;
        match self.get_caching_strategy() {
            CachingStrategy::Memory(_) => {
                for view in &views {
                    let last_verified = {
                        let mut last_verified = 0;
                        for (key, value) in get_memory_query_cache()?.iter() {
                            if key.tables.contains(view) && value.last_verified > last_verified {
                                last_verified = value.last_verified;
                            }
                        }
                        last_verified
                    };
                    let view_sql = self.get_view_sql(&view).await?;
                    let view_tables = get_view_tables(&view_sql)?;
                    let last_modified = {
                        let mut latest_last_modified = 0;
                        for view_table in &view_tables {
                            match get_memory_table_cache()?.get(view_table) {
                                Some(lm) if *lm > latest_last_modified => {
                                    latest_last_modified = *lm;
                                }
                                _ => (),
                            };
                        }
                        latest_last_modified
                    };
                    if last_modified >= last_verified {
                        clear_memory_query_cache(&[view])?;
                    }
                }
            }
            _ => {
                for view in &views {
                    let last_verified = self.last_verified(&view).await?;
                    let view_sql = self.get_view_sql(&view).await?;
                    let view_tables = get_view_tables(&view_sql)?;
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
    async fn get_latest_last_modified(&self, tables: &[&str]) -> Result<u64, DbError> {
        match self.table_exists(TABLE_CACHE_TABLE).await? {
            true => {
                let prefix = self.kind().param_prefix().to_string();
                let mut placeholders = vec![];
                let mut parameters = vec![];
                for (i, table) in tables.iter().enumerate() {
                    let i = i + 1;
                    placeholders.push(format!("{prefix}{i}"));
                    parameters.push(DbValue::from(*table));
                }
                let placeholders = placeholders.join(",");

                let sql = format!(
                    r#"SELECT "last_modified"
                       FROM "{TABLE_CACHE_TABLE}"
                       WHERE "table" IN ({placeholders})
                       ORDER BY "last_modified" DESC
                       LIMIT 1"#,
                );
                let rows = self.query_no_cache_clean(&sql, parameters).await?;
                match rows.len() {
                    0 => Ok(0),
                    1 => {
                        let row = &rows[0];
                        match row.get("last_modified") {
                            Some(last_modified) => Ok(last_modified.try_into()?),
                            None => Err(DbError::DataError(format!(
                                "No field 'last_modified' in row {row:?}"
                            ))),
                        }
                    }
                    too_many => Err(DbError::DataError(format!(
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
    async fn last_modified(&self, table: &str) -> Result<u64, DbError> {
        self.get_latest_last_modified(&[table]).await
    }

    /// Gets the last time that the given table was verified, as read from the query cache table.
    /// If there is no entry involving the given table in the query cache, or if the query cache
    /// table doesn't exist, returns 0.
    async fn last_verified(&self, table: &str) -> Result<u64, DbError> {
        match self.table_exists(QUERY_CACHE_TABLE).await? {
            true => {
                let sql = format!(
                    r#"SELECT MAX("last_verified") AS "last_verified"
                       FROM "{QUERY_CACHE_TABLE}"
                       WHERE "tables" LIKE {p}1"#,
                    p = self.kind().param_prefix(),
                );
                let table_param = format!(r#"%"{table}"%"#);
                let rows = self.query_no_cache_clean(&sql, &[&table_param]).await?;
                match rows.first() {
                    Some(row) => match row.get("last_verified") {
                        Some(value) if value == DbValue::Null => Ok(0),
                        Some(value) => Ok(value.try_into()?),
                        None => Err(DbError::DataError(format!(
                            "No 'last_verified' found in row: {row:?}"
                        ))),
                    },
                    None => Ok(0),
                }
            }
            false => Ok(0),
        }
    }

    /// Execute the given SQL command with the given parameters, returning a vector of rows.
    /// If the result of the command exists in the query cache for the given tables, get the
    /// value from there instead of from the tables themselves, in accordance with the given
    /// [CachingStrategy].
    async fn cache(
        &self,
        sql: &str,
        params: impl IntoDbParams + Send + Copy + Sync,
    ) -> Result<DbRows, DbError> {
        match self.get_caching_strategy() {
            CachingStrategy::None => self.cache_tables(&[], sql, params).await,
            _ => {
                let tables_read = get_accessed_tables(sql)?;
                let tables_read: Vec<_> = tables_read.iter().map(|s| s.as_str()).collect();
                match tables_read.is_empty() {
                    false => self.cache_tables(&tables_read, sql, params).await,
                    true => Err(DbError::InputError(format!(
                        "No tables are read from in SQL: {sql}"
                    ))),
                }
            }
        }
    }

    /// Similar to [DbCache::cache()]. This version accepts an explicit list of tables, which
    /// must correspond to the tables queried from in the given SQL command(s).
    async fn cache_tables(
        &self,
        tables: &[&str],
        sql: &str,
        params: impl IntoDbParams + Send + Copy + Sync,
    ) -> Result<DbRows, DbError> {
        let db_cache = async |tables: &[&str],
                              sql: &str,
                              params: &DbParams|
               -> Result<DbRows, DbError> {
            // Look in the cache to see if there is an entry corresponding to the given SQL
            // string for the given tables and parameters. If so, return the data from the
            // cache, otherwise execute the given SQL statement on the actualy specified
            // tables.
            let prefix = self.kind().param_prefix().to_string();
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
            let params_param = match params {
                DbParams::None => "[]".to_string(),
                DbParams::Positional(params) => {
                    let params = params.iter().map(|p| p.into()).collect::<Vec<String>>();
                    format!("[{}]", params.join(", "))
                }
            };
            let cache_params = &["[", "]", &tables_param, sql, &params_param];

            let strings = {
                let rows = self.query_no_cache_clean(&cache_sql, cache_params).await?;
                let strings = rows
                    .iter()
                    .map(|row| match row.values().nth(0) {
                        Some(value) => Ok(value.into()),
                        None => Err(DbError::DataError("Empty row".to_owned())),
                    })
                    .collect::<Vec<_>>();
                let strings: Result<Vec<String>, DbError> = strings.into_iter().collect();
                strings?
            };
            match strings.first() {
                Some(values) => {
                    let db_rows: Vec<DbRow> = serde_json::from_str(values).map_err(|err| {
                        DbError::DataError(format!("Error serializing values '{values}': {err}"))
                    })?;
                    // Only views need to be verified every time they are accessed. Tables
                    // do not because they do not have any dependencies.
                    if self.which_are_views(tables).await?.len() > 0 {
                        self.update_last_verified(tables, sql, &params).await?;
                    }
                    Ok(DbRows { content: db_rows })
                }
                None => {
                    let db_rows = self.query_no_cache_clean(sql, params).await?;
                    let rows_as_string = {
                        let mut rows_as_string = vec![];
                        for db_row in db_rows.iter() {
                            let db_row = serde_json::to_string(db_row).map_err(|err| {
                                DbError::DataError(format!("Invalid data ({err}): {db_row:?}"))
                            })?;
                            rows_as_string.push(db_row);
                        }
                        format!("[{}]", rows_as_string.join(", "))
                    };
                    let insert_sql = format!(
                        r#"INSERT INTO "{QUERY_CACHE_TABLE}"
                           ("tables", "statement", "parameters", "value")
                           VALUES ({prefix}1, {prefix}2, {prefix}3, {prefix}4)"#,
                        prefix = self.kind().param_prefix(),
                    );
                    let insert_params = [&tables_param, sql, &params_param, &rows_as_string];
                    self.execute_no_cache_clean(&insert_sql, &insert_params)
                        .await?;
                    Ok(db_rows)
                }
            }
        };

        let mem_cache = async |tables: &[&str],
                               sql: &str,
                               params: &DbParams,
                               cache_size: usize|
               -> Result<DbRows, DbError> {
            let params = &params.into_db_params();
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
                parameters: format!("{params:?}"),
            };
            let cached_rows = {
                let cache = get_memory_query_cache()?;
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
                        self.update_last_verified(tables, sql, &params).await?;
                    }
                    Ok(DbRows { content: db_rows })
                }
                None => {
                    let db_rows = self.query_no_cache_clean(sql, params).await?;
                    let mut cache = get_memory_query_cache()?;
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
                                    DbError::DataError(format!("Error getting epoch time: {err}"))
                                })?
                                .as_millis(),
                        },
                    );
                    Ok(db_rows)
                }
            }
        };

        match self.get_caching_strategy() {
            CachingStrategy::None => {
                let rows = self.query_no_cache_clean(sql, params).await?;
                Ok(rows)
            }
            CachingStrategy::TruncateAll | CachingStrategy::Truncate => {
                self.ensure_cache_tables_exist().await?;
                self.update_cached_views(tables).await?;
                let rows = db_cache(tables, sql, &params.into_db_params()).await?;
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
                let rows = db_cache(&tables, sql, &params.into_db_params()).await?;
                Ok(rows)
            }
            CachingStrategy::Memory(cache_size) => {
                self.update_cached_views(tables).await?;
                let rows = mem_cache(tables, sql, &params.into_db_params(), cache_size).await?;
                Ok(rows)
            }
        }
    }
}
