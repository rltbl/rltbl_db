//! Module declarations and macro definitions for rltbl_db.

pub mod any;
pub mod cache;
pub mod core;
pub mod db_kind;
pub mod db_value;
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

/// Converts a set of pairs into a [db_value::DbRow].
#[macro_export]
macro_rules! db_row {
    ($($key:expr => $value:expr,)+) => {
        DbRow {map: indexmap::indexmap!($($key.to_string() => $value.into()),+) }
    };
    ($($key:expr => $value:expr),*) => {
        {
            const CAP: usize = <[()]>::len(&[$({ stringify!($key); }),*]);
            let mut _map = indexmap::IndexMap::with_capacity(CAP);
            $(
                let _ = _map.insert($key.to_string(), $value.into());
            )*
                DbRow { map: _map }
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

        // Row with values:
        let mut expected_db_row = DbRow::new();
        expected_db_row
            .map
            .insert("foo".to_string(), DbValue::Boolean(true));
        expected_db_row
            .map
            .insert("bar".to_string(), DbValue::BigReal(1_f64));


        assert_eq!(
            expected_db_row,
            db_row! { "foo" => true, "bar" => 1_f64}
        );

        // Empty row:
        assert_eq!(db_row! { }, DbRow::new());
    }
}
