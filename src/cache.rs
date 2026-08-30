//! Caching

use indexmap::IndexMap;
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    str::FromStr,
    sync::{Mutex, MutexGuard},
    thread,
    time::Duration,
};

use crate::{Error, Row};

/// The name of the database's query cache table.
pub static QUERY_CACHE_TABLE: &str = "rltbl_db_query_cache";

/// The name of the database's table cache table.
pub static TABLE_CACHE_TABLE: &str = "rltbl_db_table_cache";

/// Default size for the in-memory query cache
pub static DEFAULT_MEMORY_QUERY_CACHE_SIZE: usize = 1000;

// Maximum number of times to try to retrieve an in-memory cache (retrieval will fail when the
// cache is locked by another thread).
static MAX_RETRIEVAL_ATTEMPTS: usize = 20;

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
    type Err = Error;

    fn from_str(strategy: &str) -> Result<Self, Error> {
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
                            Err(err) => return Err(Error::InputError(format!(
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
                return Err(Error::InputError(format!("Unrecognized strategy: {strategy}")).into());
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
    pub content: Vec<Row>,
    pub last_verified: u128,
}

////////////////////////
// The meta-cache
////////////////////////
#[derive(Debug, Default)]
pub struct MetaCache {
    cache: Mutex<HashSet<String>>,
}

impl MetaCache {
    /// TODO: Add docstring.
    pub fn get_cache<'a>(&'a self) -> Result<MutexGuard<'a, HashSet<String>>, Error> {
        let mut remaining_attempts = MAX_RETRIEVAL_ATTEMPTS;
        let mut meta_cache = self.cache.try_lock();
        while let Err(err) = meta_cache {
            meta_cache = self.cache.try_lock();
            if let Ok(_) = meta_cache {
                break;
            }
            remaining_attempts -= 1;
            if remaining_attempts == 0 {
                return Err(Error::ConnectError(format!(
                    "Error locking cache: {err} (retried {MAX_RETRIEVAL_ATTEMPTS} times)"
                )));
            } else {
                thread::sleep(Duration::from_millis(5));
            }
        }
        let meta_cache = meta_cache.unwrap();
        Ok(meta_cache)
    }

    /// TODO: Add docstring.
    pub fn exists(&self, object: &str) -> Result<bool, Error> {
        let cache = self.get_cache()?;
        match cache.get(object) {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    /// TODO: Add docstring.
    pub fn insert(&self, object: &str) -> Result<(), Error> {
        let mut cache = self.get_cache()?;
        cache.insert(object.to_string());
        Ok(())
    }

    /// Clear the meta cache.
    pub fn clear(&self) -> Result<(), Error> {
        let mut cache = self.get_cache()?;
        cache.clear();
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct MemoryQueryCache {
    pub cache: Mutex<IndexMap<MemoryQueryCacheKey, MemoryQueryCacheValue>>,
}

impl MemoryQueryCache {
    /// TODO: Add docstring.
    pub fn get_cache<'a>(
        &'a self,
    ) -> Result<MutexGuard<'a, IndexMap<MemoryQueryCacheKey, MemoryQueryCacheValue>>, Error> {
        let mut remaining_attempts = MAX_RETRIEVAL_ATTEMPTS;
        let mut memory_cache = self.cache.try_lock();
        while let Err(err) = memory_cache {
            memory_cache = self.cache.try_lock();
            if let Ok(_) = memory_cache {
                break;
            }
            remaining_attempts -= 1;
            if remaining_attempts == 0 {
                return Err(Error::ConnectError(format!(
                    "Error locking cache: {err} (retried {MAX_RETRIEVAL_ATTEMPTS} times)"
                )));
            } else {
                thread::sleep(Duration::from_millis(5));
            }
        }
        let memory_cache = memory_cache.unwrap();
        Ok(memory_cache)
    }

    /// Clear the memory query cache.
    pub fn clear(&self, tables: &[&str]) -> Result<(), Error> {
        let mut cache = self.get_cache()?;
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
}

#[derive(Debug, Default)]
pub struct MemoryTableCache {
    pub cache: Mutex<HashMap<String, u128>>,
}

impl MemoryTableCache {
    pub fn get_cache<'a>(&'a self) -> Result<MutexGuard<'a, HashMap<String, u128>>, Error> {
        let mut remaining_attempts = MAX_RETRIEVAL_ATTEMPTS;
        let mut memory_cache = self.cache.try_lock();
        while let Err(err) = memory_cache {
            memory_cache = self.cache.try_lock();
            if let Ok(_) = memory_cache {
                break;
            }
            remaining_attempts -= 1;
            if remaining_attempts == 0 {
                return Err(Error::ConnectError(format!(
                    "Error locking cache: {err} (retried {MAX_RETRIEVAL_ATTEMPTS} times)"
                )));
            } else {
                thread::sleep(Duration::from_millis(5));
            }
        }
        let memory_cache = memory_cache.unwrap();
        Ok(memory_cache)
    }

    /// Clear the memory table cache.
    pub fn clear(&self, tables: &[&str]) -> Result<(), Error> {
        let mut cache = self.get_cache()?;
        if tables.is_empty() {
            cache.clear();
        }
        for table in tables {
            cache.remove(&table.to_string());
        }
        Ok(())
    }
}
