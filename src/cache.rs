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
