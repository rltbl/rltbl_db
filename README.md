# rltbl_db

`rltbl_db` is the database layer for [`rltbl`](https://github.com/rltbl/relatable).
It uses [`deadpool`](https://crates.io/crates/deadpool) to provide an
asynchronous abstraction over multiple database libraries. We provide built-in
support for the [`tokio-postgres`](https://crates.io/crates/deadpool-postgres),
[`rusqlite`](https://crates.io/crates/deadpool-sqlite), and
[`libsql`](https://crates.io/crates/deadpool-libsql) libraries, and the
capability for users to extend `rltbl_db` using custom libraries if desired.

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
cargo add tokio --features full
```

# Usage

```rust
use rltbl_db::{AnyPool, Error, Rows};

async fn example() -> Result<Rows, Error> {
    let pool = AnyPool::connect("test.db").await?;
    pool.execute_batch(
        "DROP TABLE IF EXISTS test;\
         CREATE TABLE test ( value TEXT );\
         INSERT INTO test VALUES ('foo');",
    ).await?;
    let value = pool.query("SELECT value FROM test;", ()).await?;
    Ok(value)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let rows = example().await?;
    println!("Rows: {rows:?}");
    Ok(())
}
```

# Differences between PostgreSQL and SQLite

The [libsql](https://crates.io/crates/libsql) and
[deadpool-sqlite](https://crates.io/crates/deadpool-sqlite) drivers do not
fully support querying special floating point types such as "NaN", "-Infinity",
"Infinity", etc. If one tries to query from a column that contains such values
the results will be returned as TEXT. It is, possible, however, to insert these
special values into a table by hard coding them into the submitted query text
(rather than by using dynammic query parameters), by single-quoting them. E.g.,
`INSERT INTO foo VALUES ('NaN')`. Notet that sqlite is permissive enough to
accept double-quotes for values instead.

There are no such issues with PostgreSQL. The following will work irrespective
of whether the column `bar` is of floating point or text type (postgresql will
be able to determine this implicitly): `INSERT INTO foo (bar) VALUES
('-Infinity')`. It is also possible to use these special values as parameters,
e.g.,

            let float_param = std::f64::from_str("-Infinity").unwrap();
            pool.execute(
                r#"INSERT INTO foo (bar) VALUES ($1)"#,
                params![float_param],
            )
            .await
            .unwrap();

# Incompatibility of `rusqlite` and `libsql`

Note that the two SQLite drivers, `rusqlite` and `libsql`, are not
compatible with one another and cannot both be activated simultaneously. Our
default SQLite implementation uses `rusqlite`. If you would like to use
`libsql` instead, **rltbl_db** must be compiled as follows:

    cargo build --no-default-features --features libsql,tokio-postgres

# Regression tests

To install the regression tests, clone the repository
[rltbl_db_benchmarks](https://github.com/rltbl/rltbl_db_benchmarks) into a
subdirectory of the root directory.

To run the regression tests, use

    make test_driver_perf
