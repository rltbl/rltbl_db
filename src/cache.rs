//! Caching

use std::{fmt::Display, str::FromStr};

use crate::Error;

/// The name of the database's query cache table.
pub static QUERY_CACHE_TABLE: &str = "rltbl_db_query_cache";

/// The name of the database's table cache table.
pub static TABLE_CACHE_TABLE: &str = "rltbl_db_table_cache";

/// Default size for the in-memory query cache
pub static DEFAULT_MEMORY_QUERY_CACHE_SIZE: usize = 1000;

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
