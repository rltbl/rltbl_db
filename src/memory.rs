//! Code for the in-memory query, table, and meta caches.

use crate::{core::DbError, db_value::DbRow};
use indexmap::IndexMap;
use lazy_static::lazy_static;
use std::sync::{Mutex, MutexGuard};
use std::{
    collections::{HashMap, HashSet},
    thread,
    time::Duration,
};

/// Default size for the in-memory query cache
pub static DEFAULT_MEMORY_QUERY_CACHE_SIZE: usize = 1000;

// Maximum number of times to try to retrieve a cache (retrieval will fail when the cache is locked
// by another thread).
static MAX_RETRIEVAL_ATTEMPTS: usize = 20;

lazy_static! {
    /// The in-memory query cache, used by [CachingStrategy::Memory].
    pub static ref MEMORY_QUERY_CACHE: Mutex<IndexMap<MemoryQueryCacheKey, MemoryQueryCacheValue>>
        = Mutex::new(IndexMap::new());

    /// The in-memory table cache, used by [CachingStrategy::Memory].
    pub static ref MEMORY_TABLE_CACHE: Mutex<HashMap<String, u128>> = Mutex::new(HashMap::new());

    /// TODO: Add docstring
    pub static ref MEMORY_PARSE_CACHE: Mutex<HashMap<String, Vec<String>>> =
        Mutex::new(HashMap::new());

    /// The in-memory meta cache. It holds a set of things known to exist.
    pub static ref MEMORY_META_CACHE: Mutex<HashSet<String>>
        = Mutex::new(HashSet::new());
}

/// The structure used to look up query results in the in-memory cache.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MemoryQueryCacheKey {
    pub tables: String,
    pub statement: String,
    pub parameters: String,
}

/// Represents the value of an entry in the in-memory cache.
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

/// Returns true if the given object exists in the meta-cache.
pub fn get_entry_from_parse_cache(entry: &str) -> Result<Option<Vec<String>>, DbError> {
    Ok(get_parse_cache()?.get(entry).cloned())
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

/// TODO: Add docstring
pub fn get_parse_cache<'a>() -> Result<MutexGuard<'a, HashMap<String, Vec<String>>>, DbError> {
    let mut remaining_attempts = MAX_RETRIEVAL_ATTEMPTS;
    let mut parse_cache = MEMORY_PARSE_CACHE.try_lock();
    while let Err(err) = parse_cache {
        parse_cache = MEMORY_PARSE_CACHE.try_lock();
        if let Ok(_) = parse_cache {
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
    let parse_cache = parse_cache.unwrap();
    Ok(parse_cache)
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
