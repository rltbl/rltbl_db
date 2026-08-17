//! # Relatable DB
//!
//! `rltbl_db` provides an async API
//! that abstracts over differences between SQL databases,
//! letting you choose your database driver at runtime,
//! and work with database schemas that you don't know in advance.
//! Our goal is to provide easy access
//! to the core functionality of a SQL database.
//! If you need the specialized functionality of a specific SQL database,
//! this is not the crate for you.
//!
//! MC: While that is true as a general rule, it is still the case that there are certain
//!     database-specific features that we _do_ support, e.g., certain non-standard types in
//!     PostgreSQL, or direct CSV/TSV loading in the case of both PostgreSQL and SQLite.
//!     Additionally, we support basically *anything* at all, syntax-wise, as long as
//!     you write your own SQL and as long as the syntax is supported by the driver
//!     (rltbl_db will happily pass any string you like to the database).
//!     I suggest mentioning this here, since otherwise this comment makes it seem as
//!     though we only support ANSI SQL and nothing else.
//!
//! We provide structs to handle differences between SQL types and values,
//! and support `serde` for converting these to and from Rust structs.
//! The `Syntax` trait allows for multiple "flavours" of the SQL language.
//! A handful of other traits allow for different database "drivers"
//! to handle the details of the pooled database connection.
//! The `AnyPool` struct connects you to your database at runtime,
//! providing `execute`, `query`, `insert`, and similar methods,
//! plus a configurable caching system.
//!
//! ## Usage
//!
//! Here we connect to an in-memory SQLite database and do some basic operations.
//!
//! ```ignore
//! use rltbl_db::{AnyPool, row, Error as DbError, values};
//!
//! async fn basic_example() -> Result<(), DbError> {
//!     // Use a URL to connect to a SQLite in-memory database.
//!     let url = ":memory:";
//!     let pool = AnyPool::connect(url).await?;
//!
//!     // Get one value.
//!     let value: i64 = pool.query("SELECT 1", []).await?.try_into_value()?;
//!     assert_eq!(value, 1);
//!
//!     // Execute a statement.
//!     pool.execute("CREATE TABLE foo ( bar INT )", []).await?;
//!
//!     // Insert one row.
//!     let row = row!{ "bar" => 2 };
//!     pool.insert("foo", &["bar"], [&row]).await?;
//!
//!     // Retrieve the inserted row.
//!     let result = pool.query("SELECT bar FROM foo", []).await?.row()?;
//!     assert_eq!(result, row);
//!
//!     // Query with parameters.
//!     let result = pool.query("SELECT * FROM foo WHERE bar = $1", &values![2]).await?.row()?;
//!     assert_eq!(result, row);
//!
//!     Ok(())
//! }
//!
//! # #[tokio::main]
//! # async fn main() {
//! #     basic_example().await.unwrap();
//! # }
//! ```
//!
//! Given a struct with named fields that implements `serde` `Serialize` and `Deserialize`,
//! we can convert it to a row to insert it into the database,
//! and from a row back into a struct.
//!
//! ```ignore
//! use rltbl_db::{AnyPool, to_row, Error as DbError};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Deserialize, Serialize)]
//! struct Foo {
//!     bar: i64,
//!     baz: String,
//! }
//!
//! async fn serde_example() -> Result<(), DbError> {
//!     let pool = AnyPool::connect(":memory:").await?;
//!
//!     let test = Foo { bar: 1, baz: "b" };
//!     let row = to_row(test)?;
//!     pool.execute("CREATE TABLE foo ( bar INT, baz TEXT )", ()).await?;
//!     pool.insert("foo", &["bar", "baz"], [&row]).await?;
//!
//!     // Retrieve the inserted row as a Foo struct.
//!     let result: Foo = pool.query("SELECT bar FROM foo", ()).await?.row()?.try_into()?;
//!     assert_eq!(result, test);
//!
//!     Ok(())
//! }
//!
//! # #[tokio::main]
//! # async fn main() {
//! #     serde_example().await.unwrap();
//! # }
//! ```
//!
//! Given a CSV or TSV file,
//! we can determine a schema for the data,
//! and load it into a new table.
//!
//! ```ignore
//! use rltbl_db::{AnyPool, Error as DbError};
//!
//! async fn tsv_example() -> Result<(), DbError> {
//!     let pool = AnyPool::connect(":memory:").await?;
//!
//!     // determine table schema from TSV
//!     // create table
//!     // load TSV into table
//!     // query from table
//!
//!     Ok(())
//! }
//!
//! # #[tokio::main]
//! # async fn main() {
//! #     tsv_example().await.unwrap();
//! # }
//! ```
//!
//!
//! ## Limitations
//!
//! Different SQL databases use variations on the SQL language,
//! different datatypes,
//! and provide different functions and features.
//! The main goal of `rltbl_db` is to switch between databases at runtime,
//! which means that we support core SQL functionality
//! that's common to any SQL database.
//!
//! MC: See my comment above.
//!
//! You may need to write your queries differently to support multiple databases,
//! or provide different SQL strings for different cases.
//!
//! ## Extension
//!
//! We currently support PostgreSQL and SQLite syntaxes,
//! and
//! `tokio_posgtgres`,
//! `ruqslite`,
//! and `libsql` drivers.
//!
//! You can support a new SQL database by implementing these traits:
//!
//! 1. [Syntax] trait with your SQL language "flavour"
//! 2. [Query] trait to run a query
//! 3. [Pool] trait to connect to a database pool
//! 4. [Transaction] trait to support transactions
//!
//! Each of these traits has default implementations of most methods,
//! so only a few method implementations are required.

pub use self::column::Column;
pub use self::error::Error;
pub use self::pool::{AnyPool, Pool};
pub use self::query::Query;
pub use self::row::{Row, Rows};
pub use self::syntax::Syntax;
pub use self::table::Table;
pub use self::transaction::{AnyTransaction, Transaction};
pub use self::value::{Type, Value};

// all modules use error
pub mod error;

// types and values
pub mod value;

// database columns
pub mod column;

// rows of values
pub mod row;

// database tables
pub mod table;

// define syntax trait
pub mod syntax;

// syntax implementations
pub mod sqlite;
pub mod postgresql;

// define query trait
pub mod query;

// transaction extends query
pub mod transaction;

// pool extends query and returns a transaction
pub mod pool;

// driver implementations
//
#[cfg(feature = "rusqlite")]
pub mod rusqlite;

#[cfg(feature = "tokio-postgres")]
pub mod tokio_postgres;

// MC: I guess we are not dropping libsql support then?
// JO: Yes, we will support libsql, and hopefully more "drivers".
// I just didn't want to write stubs for it at this point.

// #[cfg(feature = "libsql")]
// pub mod libsql;

// macros

/// Convert a list of items that implement `Into<Value>` into a list of [Value]s.
#[macro_export]
macro_rules! values {
    () => {
       ()
    };
    ($($value:expr),* $(,)?) => {{
        use $crate::value::Value;
        [$(Into::<Value>::into($value)),*]
    }};
}

#[macro_export]
macro_rules! row {
    ($($key:expr => $value:expr,)+) => {
        $crate::row::Row {map: indexmap::indexmap!($($key.to_string() => $value.into()),+) }
    };
    ($($key:expr => $value:expr),*) => {
        {
            const CAP: usize = <[()]>::len(&[$({ stringify!($key); }),*]);
            let mut _map = indexmap::IndexMap::with_capacity(CAP);
            $(
                let _ = _map.insert($key.to_string(), $value.into());
            )*
                $crate::row::Row { map: _map }
        }
    };
}

// TODO: Remove this later.
///////////////////////////// OLD CODE /////////////////////////////////////////////////////////////

pub mod z_old_any;
pub mod z_old_cache;
pub mod z_old_core;
pub mod z_old_db_kind;
pub mod z_old_db_value;
pub mod z_old_parse;
pub mod z_old_serde;
pub mod z_old_shared;

#[cfg(feature = "rusqlite")]
pub mod z_old_rusqlite;

#[cfg(feature = "tokio-postgres")]
pub mod z_old_tokio_postgres;

#[cfg(feature = "libsql")]
pub mod z_old_libsql;

// Macro definitions

/// Converts a list of assorted types implementing [db_value::IntoDbValue] into [db_value::DbParams]
#[macro_export]
macro_rules! z_old_params {
    () => {
       ()
    };
    ($($value:expr),* $(,)?) => {{
        use $crate::z_old_db_value::IntoDbValue;
        [$($value.into_db_value()),*]

    }};
}

/// Converts a set of pairs into a [db_value::DbRow].
#[macro_export]
macro_rules! z_old_db_row {
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
    use crate::z_old_db_value::{DbRow, DbValue};

    #[test]
    fn test_macros() {
        let params = z_old_params![1_i32, "foo", 1.1_f64];
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
            z_old_db_row! { "foo" => true, "bar" => 1_f64}
        );

        // Empty row:
        assert_eq!(z_old_db_row! { }, DbRow::new());
    }
}
