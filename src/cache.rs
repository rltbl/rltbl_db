//! Caching

use std::{fmt::Display, str::FromStr};

use crate::{AnyPool, Error, Row};

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
// Database cache code
////////////////////////

/// Ensure that the query cache table and the table cache table exist (see
/// [QUERY_CACHE_TABLE] and [TABLE_CACHE_TABLE]).
pub async fn ensure_cache_tables_exist(_pool: &AnyPool) -> Result<(), Error> {
    // if !exists_in_meta_cache(QUERY_CACHE_TABLE)? || !exists_in_meta_cache(TABLE_CACHE_TABLE)? {
    // for special_table in [QUERY_CACHE_TABLE, TABLE_CACHE_TABLE] {
    //     let sql = match special_table {
    //         table if table == QUERY_CACHE_TABLE => pool.kind().create_query_cache_table_sql(),
    //         table if table == TABLE_CACHE_TABLE => pool.kind().create_table_cache_table_sql(),
    //         _ => unreachable!(),
    //     };
    //     match pool.execute_no_cache_clean(&sql, ()).await {
    //         Ok(_) => (),
    //         Err(_) => {
    //             // Since we are not using transactions, a race condition could occur in
    //             // which two or more threads are trying to create the cache at the same
    //             // time, triggering a primary key violation in the metadata table. So if
    //             // there is an error creating the cache table we just check that it exists
    //             // and if it does we assume that all is ok.
    //             match pool.table_exists(special_table).await? {
    //                 false => {
    //                     return Err(Error::DatabaseError(format!(
    //                         "The cache table '{special_table}' could not be created"
    //                     )));
    //                 }
    //                 true => (),
    //             }
    //         }
    //     };
    //     let mut cache = get_meta_cache()?;
    //     cache.insert(special_table.to_string());
    // }
    // }
    todo!()
}

/// Uses the current caching strategy to clear the query cache for any of the given tables
/// that (a) are views and (b) have source tables that have been modified more recently than
/// the view. This function works both with database and memory cache strategies.
pub async fn update_cached_views(_pool: &AnyPool, _tables: &[&str]) -> Result<(), Error> {
    todo!()
}
