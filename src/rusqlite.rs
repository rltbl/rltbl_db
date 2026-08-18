//! Driver using deadpool-sqlite (rusqlite).
use std::str::from_utf8;

use async_trait::async_trait;
use deadpool_sqlite::{
    Config, Connection, Runtime,
    rusqlite::{
        Statement,
        fallible_iterator::FallibleIterator,
        types::{Null, ValueRef},
    },
};
use indexmap::indexmap;

use crate::{
    Error, Pool, Query, Row, Rows, Syntax, Transaction, Value, shared::EditType, shared::edit,
    sqlite::MAX_PARAMS_SQLITE, sqlite::SqliteSyntax,
};

#[derive(Debug)]
pub struct RusqlitePool {
    syntax: SqliteSyntax,
    pool: deadpool_sqlite::Pool,
}

impl RusqlitePool {
    pub async fn connect(url: &str) -> Result<Self, Error> {
        let cfg = Config::new(url);
        let pool = match url {
            ":memory:" => cfg.builder(Runtime::Tokio1).unwrap().max_size(1).build()?,
            _ => cfg.create_pool(Runtime::Tokio1)?,
        };
        let syntax = SqliteSyntax;
        Ok(Self { syntax, pool })
    }
}

fn query_prepared(stmt: &mut Statement<'_>, params: &[Value]) -> Result<Vec<Row>, Error> {
    for (i, param) in params.iter().enumerate() {
        match param {
            Value::Text(text) => {
                stmt.raw_bind_parameter(i + 1, text)?;
            }
            // MC: Are these commented out because we are dropping support?
            // JO: No, I was just getting basic tests to compile.
            // We will have at least as many Value variants as `rlbtl_db` currently has.
            Value::SmallInteger(num) => {
                stmt.raw_bind_parameter(i + 1, num.to_string())?;
            }
            Value::Integer(num) => {
                stmt.raw_bind_parameter(i + 1, num.to_string())?;
            }
            Value::BigInteger(num) => {
                stmt.raw_bind_parameter(i + 1, num.to_string())?;
            }
            Value::Real(num) => {
                stmt.raw_bind_parameter(i + 1, num.to_string())?;
            }
            Value::BigReal(num) => {
                stmt.raw_bind_parameter(i + 1, num.to_string())?;
            }
            Value::Numeric(num) => {
                stmt.raw_bind_parameter(i + 1, num.to_string())?;
            }
            Value::Boolean(flag) => {
                // Note that SQLite's type affinity means that booleans are actually
                // implemented as numbers (see https://sqlite.org/datatype3.html).
                let num = match flag {
                    true => 1,
                    false => 0,
                };
                stmt.raw_bind_parameter(i + 1, num.to_string())?;
            }
            Value::Null => {
                stmt.raw_bind_parameter(i + 1, &Null)?;
            } // Value::Json(value) => {
              //     let value = match value {
              //         JsonValue::String(value) => value.to_string(),
              //         _ => value.to_string(),
              //     };
              //     stmt.raw_bind_parameter(i + 1, value)?;
              // }
              // Value::Other(type_name, bytes, string_opt) => {
              //     return Err(Error::InputError(format!(
              //         "Not supported for SQLite: \
              //                  Value::Other({type_name}, {bytes:?}, {string_opt:?})"
              //     )));
              // }
        };
    }

    // Define the struct that we will use to represent information about a given column:
    struct ColumnConfig {
        name: String,
        datatype: Option<String>,
    }

    // Collect the column information from the prepared statement:
    let columns = stmt
        .column_names()
        .iter()
        .map(|col| ColumnConfig {
            name: col.to_string(),
            datatype: None,
        })
        .collect::<Vec<_>>();

    // Execute the statement and send back the results
    let results = stmt
        .raw_query()
        .map(|row| {
            let mut db_row = Row::new();
            for column in &columns {
                let column_name = &column.name;
                let column_type = &column.datatype;
                let value = row.get_ref(column_name.as_str())?;
                let value = match value {
                    ValueRef::Null => Value::Null,
                    ValueRef::Integer(value) => match column_type {
                        Some(ctype) if ctype.to_lowercase() == "bool" => Value::Boolean(value != 0),
                        // The remaining cases are (a) the column's datatype is integer, and
                        // (b) the column is an expression. In the latter case it doesn't seem
                        // possible to get the datatype of the expression from the metadata.
                        // So the only thing to do here is just to convert the value
                        // using the default method, and since we already know that it
                        // is an integer, the result of the conversion will be a number.
                        _ => Value::from(value),
                    },
                    ValueRef::Real(value) => Value::BigReal(value),
                    ValueRef::Text(value) | ValueRef::Blob(value) => match column_type {
                        // MC: Are we dropping numeric support?
                        // Some(ctype) if ctype.to_lowercase() == "numeric" => {
                        //     let value = from_utf8(value).unwrap_or_default();
                        //     let value = value.parse::<Decimal>().unwrap();
                        //     Value::Numeric(value)
                        // }
                        _ => {
                            let value = from_utf8(value).unwrap_or_default();
                            Value::Text(value.to_string())
                        }
                    },
                };
                db_row.insert(column_name.to_string(), value);
            }
            Ok(db_row)
        })
        .collect::<Vec<_>>();
    results.map_err(|err| Error::DeadpoolRusqliteError(err))
}

#[async_trait]
impl Query for RusqlitePool {
    fn syntax(&self) -> &dyn Syntax {
        &self.syntax
    }

    /// Implements [Query::execute_batch()] for SQLite.
    async fn execute_batch(&self, sql: &str) -> Result<(), Error> {
        let conn = self.pool.get().await?;
        let sql_string = sql.to_string();
        match conn
            .interact(move |conn| match conn.execute_batch(&sql_string) {
                Err(err) => {
                    return Err(Error::DeadpoolRusqliteError(err));
                }
                Ok(_) => Ok(()),
            })
            .await
        {
            Err(err) => Err(Error::DeadpoolRusqliteInteractError(err)),
            Ok(_) => {
                // We need to drop conn here to ensure that any changes to the db are persisted.
                drop(conn);
                // TODO: handle cache
                Ok(())
            }
        }
    }

    async fn query(&self, sql: &str, params: &[&Value]) -> Result<Rows, Error> {
        let conn = self.pool.get().await?;
        let sql_string = sql.to_string();
        let params: Vec<Value> = params.iter().cloned().map(|v| v.clone()).collect();
        conn.interact(move |conn| {
            let mut stmt = conn
                .prepare(&sql_string)
                .map_err(|err| Error::DeadpoolRusqliteError(err))
                // TODO: Replace expect() with a proper error if possible.
                .expect("generate statement");
            for (i, param) in params.iter().enumerate() {
                stmt.raw_bind_parameter(i + 1, param.to_string())
                    .expect("bind parameter");
            }
            let rows: Vec<Row> = query_prepared(&mut stmt, &params)?
                .into_iter()
                .map(|row| {
                    row.map
                        .into_iter()
                        .map(|(key, val)| (key, Value::from(val)))
                        .collect()
                })
                .collect();

            Ok(Rows { rows })
        })
        .await?
    }

    /// Implements [DbQuery::insert()] for SQLite.
    async fn insert(
        &self,
        table: &str,
        columns: &[&str],
        // TODO: This should be an iterator.
        rows: &Rows,
    ) -> Result<(), Error> {
        edit(
            self,
            &EditType::Insert,
            &MAX_PARAMS_SQLITE,
            table,
            columns,
            rows,
            false,
            &[],
        )
        .await?;
        Ok(())
    }

    /// Implements [Query::drop_table()] for SQLite.
    async fn drop_table(&self, table: &str) -> Result<(), Error> {
        // TODO: Add this:
        // let table = validate_table_name(table)?;

        // TODO: Use the "no_cache_clean" version insteadL
        // Drop the table:
        self.execute(&format!(r#"DROP TABLE IF EXISTS "{table}""#), &[])
            .await?;

        // TODO: Delete dirty entries from the cache in accordance with our caching strategy:
        // clear_cache_for_dropped_tables(&self.pool(), &[&table]).await?;
        Ok(())
    }
}

#[async_trait]
impl Pool for RusqlitePool {
    async fn transaction(&self) -> Result<Box<dyn Transaction>, Error> {
        match RusqliteTransaction::begin(self.pool.clone()).await {
            Ok(tx) => Ok(Box::new(tx)),
            Err(err) => Err(err),
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct RusqliteTransaction {
    syntax: SqliteSyntax,
    pool: deadpool_sqlite::Pool,
    conn: Option<Connection>,
}

impl RusqliteTransaction {
    pub async fn begin(pool: deadpool_sqlite::Pool) -> Result<Self, Error> {
        let conn = pool.get().await.unwrap();
        conn.interact(move |conn| conn.execute("BEGIN TRANSACTION;", []).unwrap())
            .await
            .unwrap();
        let conn = Some(conn);
        let syntax = SqliteSyntax;
        Ok(RusqliteTransaction { syntax, pool, conn })
    }
}

// MC: I guess Drop is part of std::ops and does not need an explicit `use std::ops::Drop`?
// I hadn't actually come across `Drop` before. Maybe it is worth a comment to say
// explicitly what this trait is for.
// JO: It would be fine to fully-qualify this.
impl Drop for RusqliteTransaction {
    fn drop(&mut self) {
        match &self.conn {
            Some(conn) => match conn.lock() {
                Ok(guard) => {
                    println!("ROLLING BACK!");
                    let _ = guard.execute("ROLLBACK;", []);
                }
                Err(_) => (),
            },
            None => (),
        }
    }
}

#[async_trait]
impl Query for RusqliteTransaction {
    fn syntax(&self) -> &dyn Syntax {
        &self.syntax
    }

    #[allow(unused)]
    async fn execute_batch(&self, _sql: &str) -> Result<(), Error> {
        todo!()
    }

    async fn query(&self, sql: &str, params: &[&Value]) -> Result<Rows, Error> {
        match &self.conn {
            Some(conn) => {
                let sql_string = sql.to_string();
                let params: Vec<Value> = params.iter().cloned().map(|v| v.clone()).collect();
                conn.interact(move |conn| {
                    let mut stmt = conn
                        .prepare(&sql_string)
                        .map_err(|err| Error::DeadpoolRusqliteError(err))
                        // TODO: Replace expect() with a proper error if possible.
                        .expect("generate statement");
                    for (i, param) in params.iter().enumerate() {
                        stmt.raw_bind_parameter(i + 1, param.to_string())
                            .expect("bind parameter");
                    }
                    let rows: Vec<Row> = stmt
                        .raw_query()
                        .map(|row| {
                            let string: String = row.get_unwrap(0);
                            Ok(Row {
                                map: indexmap! { "a".to_string() => Value::from(string)},
                            })
                        })
                        .collect()
                        .unwrap();
                    Ok(Rows { rows })
                })
                .await?
            }
            None => Err(Error::DatabaseError(format!(
                "transaction already complete"
            ))),
        }
    }

    /// Implements [DbQuery::insert()] for PostgreSQL
    async fn insert(
        &self,
        _table: &str,
        _columns: &[&str],
        // TODO: This should be an iterator.
        _rows: &Rows,
    ) -> Result<(), Error> {
        todo!()
    }

    async fn drop_table(&self, _table: &str) -> Result<(), Error> {
        todo!()
    }
}

#[async_trait]
impl Transaction for RusqliteTransaction {
    async fn rollback(&mut self) -> Result<(), Error> {
        match &self.conn {
            Some(conn) => {
                conn.interact(move |conn| conn.execute("ROLLBACK;", []).unwrap())
                    .await
                    .unwrap();
                self.conn = None;
                Ok(())
            }
            None => Err(Error::DatabaseError(format!(
                "transaction already complete"
            ))),
        }
    }

    async fn commit(&mut self) -> Result<(), Error> {
        match &self.conn {
            Some(conn) => {
                conn.interact(move |conn| conn.execute("COMMIT;", []).unwrap())
                    .await
                    .unwrap();
                self.conn = None;
                Ok(())
            }
            None => Err(Error::DatabaseError(format!(
                "transaction already complete"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{AnyPool, Error, Pool, Query, Transaction, Value, rusqlite::RusqlitePool, values};

    async fn foo(tx: &Box<dyn Transaction>) {
        tx.query("SELECT 'baz'", &[]).await.unwrap();
    }

    #[tokio::test]
    async fn text_transaction() {
        let url = ":memory:";
        let pool = RusqlitePool::connect(url).await.expect("connect to sqlite");
        pool.query("DROP TABLE IF EXISTS foo", &[])
            .await
            .expect("create table");
        pool.query("CREATE TABLE foo ( bar TEXT )", &[])
            .await
            .expect("create table");
        pool.query("INSERT INTO foo VALUES ('A')", &[])
            .await
            .expect("insert values");
        let rows = pool
            .query("SELECT bar FROM foo", &[])
            .await
            .expect("count rows");
        assert_eq!(rows.rows.len(), 1, "count rows before");

        println!("before pool {:?}", pool.pool.status());

        let mut tx = pool.transaction().await.unwrap();
        tx.query("INSERT INTO foo VALUES (456)", &[])
            .await
            .expect("transaction insert");
        foo(&tx).await;
        let rows = tx
            .query("SELECT bar FROM foo", &[])
            .await
            .expect("count rows");
        assert_eq!(rows.rows.len(), 2, "count rows inside transaction");

        // WARN: Don't do this for :memory:!
        // Because we set max_size = 1
        // and the transaction holds one connection,
        // asking for another connection will await forever.
        if url != ":memory:" {
            let rows = pool
                .query("SELECT bar FROM foo", &[])
                .await
                .expect("count rows");
            assert_eq!(rows.rows.len(), 1, "count rows during");
        }

        tx.commit().await.unwrap();
        println!("after pool {:?}", pool.pool.status());
        let rows = pool
            .query("SELECT bar FROM foo", &[])
            .await
            .expect("count rows");
        assert_eq!(rows.rows.len(), 2, "count rows after");

        // assert!(false, "DONE");
    }

    #[tokio::test]
    async fn test_rusqlite_anypool() -> Result<(), Error> {
        let url = ":memory:";
        let pool = RusqlitePool::connect(url).await?;
        let pool: Box<dyn Pool> = Box::new(pool);
        let pool = AnyPool::from(pool);

        let sql = "SELECT $1, $2";
        // let values = [1, 2];
        // let values = vec![1, 2];
        let values = vec![Value::from(1), Value::from("foo")];
        let _values = values![1, "foo"];
        let _rows = pool.query(sql, &values).await?;

        Ok(())
    }
}
