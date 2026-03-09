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

/// Retrieve the in-memory meta-cache [MEMORY_META_CACHE].
pub fn get_meta_cache<'a>() -> Result<MutexGuard<'a, HashSet<String>>, DbError> {
    let max_attempts = 20;
    let mut remaining_attempts = max_attempts;
    let mut meta_cache = MEMORY_META_CACHE.try_lock();
    while let Err(err) = meta_cache {
        meta_cache = MEMORY_META_CACHE.try_lock();
        if let Ok(_) = meta_cache {
            break;
        }
        remaining_attempts -= 1;
        if remaining_attempts == 0 {
            return Err(DbError::ConnectError(format!(
                "Error locking cache: {err} (retried {max_attempts} times)"
            )));
        } else {
            thread::sleep(Duration::from_millis(5));
        }
    }
    let meta_cache = meta_cache.unwrap();
    Ok(meta_cache)
}

/// Clear the meta cache.
pub fn clear_meta_cache() -> Result<(), DbError> {
    let mut cache = get_meta_cache()?;
    cache.clear();
    Ok(())
}

/// Returns true if the given object exists in the meta-cache.
pub fn exists_in_meta_cache(object: &str) -> Result<bool, DbError> {
    match get_meta_cache()?.get(object) {
        Some(_) => Ok(true),
        None => Ok(false),
    }
}

/// Retrieve the in-memory query cache (see [MEMORY_QUERY_CACHE]).
pub fn get_memory_query_cache<'a>()
-> Result<MutexGuard<'a, IndexMap<MemoryQueryCacheKey, MemoryQueryCacheValue>>, DbError> {
    let max_attempts = 20;
    let mut remaining_attempts = max_attempts;
    let mut memory_cache = MEMORY_QUERY_CACHE.try_lock();
    while let Err(err) = memory_cache {
        memory_cache = MEMORY_QUERY_CACHE.try_lock();
        if let Ok(_) = memory_cache {
            break;
        }
        remaining_attempts -= 1;
        if remaining_attempts == 0 {
            return Err(DbError::ConnectError(format!(
                "Error locking cache: {err} (retried {max_attempts} times)"
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
    let max_attempts = 20;
    let mut remaining_attempts = max_attempts;
    let mut memory_cache = MEMORY_TABLE_CACHE.try_lock();
    while let Err(err) = memory_cache {
        memory_cache = MEMORY_TABLE_CACHE.try_lock();
        if let Ok(_) = memory_cache {
            break;
        }
        remaining_attempts -= 1;
        if remaining_attempts == 0 {
            return Err(DbError::ConnectError(format!(
                "Error locking cache: {err} (retried {max_attempts} times)"
            )));
        } else {
            thread::sleep(Duration::from_millis(5));
        }
    }
    let memory_cache = memory_cache.unwrap();
    Ok(memory_cache)
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
