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


// TODO: Remove this later.
/// Converts a key value pair into a [db_value::DbRow]. The syntax of this macro is identical to
/// [indexmap]. For example: db_row! { key1 -> value1, key2 -> value2, ... }
/// The code for this function is adapted from the code for indexmap! (see
/// <https://docs.rs/indexmap/latest/src/indexmap/macros.rs.html#59-73>
#[macro_export]
macro_rules! db_row_old {
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

/// Converts a set of pairs into a [db_value::DbRow]. Note that all entries must end in a comma,
/// even the final one. E.g.,
/// db_row_new! {
///   "table" => "data",
/// };
#[macro_export]
macro_rules! db_row {
    // TODO: What are these two lines for? This macro (like the one above) seems to be working
    // without them?
    // (@single $($x:tt)*) => (());
    // (@count $($rest:expr),*) => (<[()]>::len(&[$(indexmap::indexmap!(@single $rest)),*]));

    ($($key:expr => $value:expr,)+) => {
        DbRow {map: indexmap::indexmap!($($key.to_string() => $value.into()),+) }
    };
    ($($key:expr => $value:expr),*) => {
        {
            let cap = indexmap::indexmap!(@count $($key),*);
            let mut map = indexmap::IndexMap::with_capacity(cap);
            $(
                let _ = map.insert($key.to_string(), $value.into());
            )*
                DbRow { map }
        }
    };
}

#[cfg(test)]
mod tests {
    use crate::db_value::{DbRow, DbValue};

    #[test]
    fn test_macros() {
        let params = params![1_i32, "foo", 1.1_f64];
        assert_eq!(
            params,
            [
                DbValue::Integer(1),
                DbValue::Text("foo".to_string()),
                DbValue::BigReal(1.1_f64)
            ]
        );

        let mut expected_db_row = DbRow::new();
        expected_db_row
            .map
            .insert("foo".to_string(), DbValue::Boolean(true));
        expected_db_row
            .map
            .insert("bar".to_string(), DbValue::BigReal(1_f64));


        assert_eq!(
            expected_db_row,
            db_row_old! {
                "foo".into() => DbValue::from(true),
                "bar".into() => DbValue::from(1_f64),
            }
        );

        assert_eq!(
            expected_db_row,
            db_row! { "foo" => true, "bar" => 1_f64,}
        );
    }
}
