pub mod any;
pub mod core;
pub mod db_kind;
pub mod db_value;
pub mod memory;
pub mod parse;
pub mod serde;
pub mod shared;

#[cfg(feature = "rusqlite")]
pub mod rusqlite;

#[cfg(feature = "tokio-postgres")]
pub mod tokio_postgres;

#[cfg(feature = "libsql")]
pub mod libsql;

// Macro definitions

/// Converts a list of assorted types implementing [db_value::IntoDbValue] into [db_value::DbParams]
#[macro_export]
macro_rules! params {
    () => {
       ()
    };
    ($($value:expr),* $(,)?) => {{
        use $crate::db_value::IntoDbValue;
        [$($value.into_db_value()),*]

    }};
}

/// Converts a key value pair into a [db_value::DbRow]. The syntax of this macro is identical to
/// [indexmap]. For example: db_row! { key1 -> value1, key2 -> value2, ... }
/// The code for this function is adapted from the code for indexmap! (see
/// <https://docs.rs/indexmap/latest/src/indexmap/macros.rs.html#59-73>
#[macro_export]
macro_rules! db_row {
    ($($key:expr => $value:expr,)+) => {
        $crate::db_value::DbRow {
            map: indexmap::indexmap!($($key => $value),+)
        }
    };
    ($($key:expr => $value:expr),*) => {
        $crate::db_value::DbRow {
            map: {
                // Note: `stringify!($key)` is just here to consume the repetition,
                // but we throw away that string literal during constant evaluation.
                const CAP: usize = <[()]>::len(&[$({ stringify!($key); }),*]);
                let mut map = indexmap::IndexMap::with_capacity(CAP);
                $(
                    map.insert($key, $value);
                )*
                    map
            }
        }
    };
}

// TODO: Try this instead:
/// Converts a set of pairs into a [db_value::DbRow]
#[macro_export]
macro_rules! db_row_new {
    (@single $($x:tt)*) => (());
    (@count $($rest:expr),*) => (<[()]>::len(&[$(indexmap::indexmap!(@single $rest)),*]));

    ($($key:expr => $value:expr,)+) => {
        indexmap::indexmap!($($key.to_string() => $value.into()),+)
    };
    ($($key:expr => $value:expr),*) => {
        {
            let cap = indexmap::indexmap!(@count $($key),*);
            let mut map = indexmap::IndexMap::with_capacity(cap);
            $(
                let _ = map.insert($key.to_string(), $value.into());
            )*
                map
        }
    };
}
