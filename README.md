# rltbl_db

`rltbl_db` is the database layer for [`rltbl`](https://github.com/rltbl/relatable).
It provides an async abstraction over multiple database libraries, currently
[`tokio-postgres`](https://github.com/rust-postgres/rust-postgres)
and [`rusqlite`](https://github.com/rusqlite/rusqlite).
Our goal is to be able to switch databases at runtime,
and query arbitrary tables using dynamically generated SQL.

Only consider using this library if all three are true:

1. you need to support multiple database types at runtime
2. you need to handle tables that you don't know the structure of at compile time
3. other options such as
   [`sqlx`](https://github.com/launchbadge/sqlx)
   don't suit your needs.

# Install

Add to your `Cargo.toml` using this GitHub repo:

```sh
cargo add rltbl_db --git 'https://github.com/rltbl/rltbl_db'
```

# Usage

```rust
use rltbl_db::{any::AnyPool, core::{DbError, DbQuery}};

async fn example() -> Result<String, DbError> {
    let pool = AnyPool::connect("test.db").await?;
    pool.execute_batch(
        "DROP TABLE IF EXISTS test;\
         CREATE TABLE test ( value TEXT );\
         INSERT INTO test VALUES ('foo');",
    ).await?;
    let value = pool.query_string("SELECT value FROM test;", &[]).await?;
    Ok(value)
}
```

# Differences between PostgreSQL and SQLite

The [libsql](https://crates.io/crates/libsql) and [deadpool-sqlite](https://crates.io/crates/deadpool-sqlite) drivers do not fully support querying special floating point types such as "NaN", "-Infinity", "Infinity", etc. If one tries to query from a column that contains such values the results will be returned as TEXT. It is, possible, however, to insert these special values into a table by hard coding them into the submitted query text (rather than by using dynammic query parameters), by double quoting them. E.g., `INSERT INTO foo VALUES ("NaN")`.

There are no such issues with PostgreSQL. The following will work irrespective of whether the column `bar` is of floating point or text type (postgresql will be able to determine this implicitly): `INSERT INTO foo (bar) VALUES ('-Infinity')`. Note that one must use single- rather than double-quotes for postgresql. It is also possible to use these
special values as parameters, e.g.,

            let float_param = std::f64::from_str("-Infinity").unwrap();
            pool.execute(
                r#"INSERT INTO foo (bar) VALUES ($1)"#,
                params![float_param],
            )
            .await
            .unwrap();
