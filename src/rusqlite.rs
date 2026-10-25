//! Driver using deadpool-sqlite (rusqlite).

use async_trait::async_trait;
use deadpool_sqlite::{
    self, Config, Hook, HookError, Runtime,
    rusqlite::{
        self, Statement,
        fallible_iterator::FallibleIterator,
        functions::FunctionFlags,
        types::{Null, ValueRef},
        vtab::csvtab,
    },
};
use regex::Regex;
use rust_decimal::Decimal;
use std::{env, str::from_utf8, sync::Arc};

use crate::{
    AnyTransaction, Connection, Error, JsonValue, Pool, Query, Row, Rows, Syntax, Value,
    error::DatabaseError, sql_parse::validate_table_name, sqlite::SqliteSyntax,
};

// DatabaseError implementations:

impl From<deadpool_sqlite::rusqlite::Error> for DatabaseError {
    fn from(err: deadpool_sqlite::rusqlite::Error) -> DatabaseError {
        DatabaseError::Error(err.to_string())
    }
}

impl From<deadpool_sqlite::BuildError> for DatabaseError {
    fn from(err: deadpool_sqlite::BuildError) -> DatabaseError {
        DatabaseError::BuildError(err.to_string())
    }
}

impl From<deadpool_sqlite::CreatePoolError> for DatabaseError {
    fn from(err: deadpool_sqlite::CreatePoolError) -> DatabaseError {
        DatabaseError::CreatePoolError(err.to_string())
    }
}

impl From<deadpool_sqlite::PoolError> for DatabaseError {
    fn from(err: deadpool_sqlite::PoolError) -> DatabaseError {
        DatabaseError::PoolError(err.to_string())
    }
}

impl From<deadpool_sqlite::InteractError> for DatabaseError {
    fn from(err: deadpool_sqlite::InteractError) -> DatabaseError {
        DatabaseError::InteractError(err.to_string())
    }
}

///////////////////////////////////////////////////////////////////////////////
// RusqlitePool
///////////////////////////////////////////////////////////////////////////////

/// Represents a deadpool-sqlite database connection pool.
#[derive(Debug)]
pub struct RusqlitePool {
    pub pool: deadpool_sqlite::Pool,
    syntax: SqliteSyntax,
}

// Query is implemented for RusqlitePool (mainly) as a wrapper around our implementation of
// Query for RusqliteConnection (below).
#[async_trait]
impl Query for RusqlitePool {
    /// Implements [Query::syntax()].
    fn syntax(&self) -> &dyn Syntax {
        &self.syntax
    }

    /// Implements [Query::execute_batch()].
    async fn execute_batch(&self, sql: &str) -> Result<(), Error> {
        let connection = self.pool.get().await?;
        let connection = RusqliteConnection {
            connection,
            syntax: self.syntax,
        };
        connection.execute_batch(sql).await?;
        Ok(())
    }

    /// Implements [Query::query()].
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Rows, Error> {
        let connection = self.pool.get().await?;
        let connection = RusqliteConnection {
            connection,
            syntax: self.syntax,
        };
        Ok(connection.query(sql, params).await?)
    }

    /// Implements [Query::can_copy_in()]. Returns true if the given filename ends
    /// (case-insensitively) with '.csv'.
    fn can_copy_in(&self, filename: &str) -> bool {
        can_copy_in(filename)
    }

    /// Implements [Query::copy_in()]
    async fn copy_in(&self, table: &str, filename: &str) -> Result<(), Error> {
        let connection = self.pool.get().await?;
        let connection = RusqliteConnection {
            connection,
            syntax: self.syntax,
        };
        Ok(connection.copy_in(table, filename).await?)
    }

    /// Implements [Query::drop_table()].
    async fn drop_table(&self, table: &str) -> Result<(), Error> {
        let connection = self.pool.get().await?;
        let connection = RusqliteConnection {
            connection,
            syntax: self.syntax,
        };
        Ok(connection.drop_table(table).await?)
    }

    /// Implements [Query::drop_view()]
    async fn drop_view(&self, view: &str) -> Result<(), Error> {
        let connection = self.pool.get().await?;
        let connection = RusqliteConnection {
            connection,
            syntax: self.syntax,
        };
        Ok(connection.drop_view(view).await?)
    }
}

#[async_trait]
impl Pool for RusqlitePool {
    /// Implements [Pool::connection()].
    async fn connection(&self) -> Result<Box<dyn Connection>, Error> {
        let connection = self.pool.get().await?;
        let connection = RusqliteConnection {
            connection,
            syntax: self.syntax,
        };
        Ok(Box::new(connection))
    }
}

impl RusqlitePool {
    /// Connect to the database at the given URL using rusqlite.
    pub async fn connect(url: &str) -> Result<Self, Error> {
        let cfg = Config::new(url);
        let pool =
            cfg.builder(Runtime::Tokio1)?
                .post_create(Hook::Fn(Box::new(|conn, _metrics| {
                    let guard = conn
                        .lock()
                        .map_err(|err| HookError::message(err.to_string()))?;
                    csvtab::load_module(&guard)
                        .map_err(|err| HookError::message(err.to_string()))?;
                    add_rusqlite_regexp_function(&guard)
                        .map_err(|err| HookError::message(err.to_string()))?;
                    Ok(())
                })));
        let pool = match url {
            ":memory:" => pool.max_size(1).build()?,
            _ => pool.build()?,
        };
        Ok(Self {
            syntax: SqliteSyntax,
            pool,
        })
    }
}

///////////////////////////////////////////////////////////////////////////////
// RusqliteConnection
///////////////////////////////////////////////////////////////////////////////

/// Represents a SQLite connection.
#[derive(Debug)]
pub struct RusqliteConnection {
    pub connection: deadpool_sqlite::Object,
    syntax: SqliteSyntax,
}

#[async_trait]
impl Query for RusqliteConnection {
    /// Implements [Query::syntax()]
    fn syntax(&self) -> &dyn Syntax {
        &self.syntax
    }

    /// Implements [Query::execute_batch()]
    async fn execute_batch(&self, sql: &str) -> Result<(), Error> {
        let sql_string = sql.to_string();
        self.connection
            .interact(move |conn| match conn.execute_batch(&sql_string) {
                Err(err) => {
                    return Err(DatabaseError::InteractError(err.to_string()));
                }
                Ok(_) => Ok(()),
            })
            .await??;
        Ok(())
    }

    /// Implements [Query::query()]
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Rows, Error> {
        let sql_string = sql.to_string();
        let params = params.to_vec();
        self.connection
            .interact(move |conn| {
                let mut stmt = conn.prepare(&sql_string)?;
                for (i, param) in params.iter().enumerate() {
                    stmt.raw_bind_parameter(i + 1, param.to_string())?;
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

    /// Implements [Query::can_copy_in()]
    fn can_copy_in(&self, filename: &str) -> bool {
        can_copy_in(filename)
    }

    /// Implements [Query::copy_in()]
    async fn copy_in(&self, table: &str, filename: &str) -> Result<(), Error> {
        if !self.can_copy_in(filename) {
            return Err(Error::InputError(format!(
                "Filename: '{filename}' must end with .csv"
            )));
        }

        eprintln!("Loading table '{table}' from '{filename}' using SQLite's CSV load extension.");
        let current_dir = env::current_dir()?;
        let current_dir = current_dir.display();
        let sql = format!(
            r#"CREATE VIRTUAL TABLE temp.t1
               USING CSV(filename='{current_dir}/{filename}', header=true)"#
        );
        self.execute(&sql, &[]).await?;
        let sql = format!("INSERT INTO {table} SELECT * FROM temp.t1");
        self.execute(&sql, &[]).await?;
        Ok(())
    }

    /// Implements [Query::drop_table()]
    async fn drop_table(&self, table: &str) -> Result<(), Error> {
        let table = validate_table_name(table)?;

        // Drop the table:
        self.execute(&format!(r#"DROP TABLE IF EXISTS "{table}""#), &[])
            .await?;

        Ok(())
    }

    /// Implements [Query::drop_view()]
    async fn drop_view(&self, view: &str) -> Result<(), Error> {
        let view = validate_table_name(view)?;

        // Drop the view:
        self.execute(&format!(r#"DROP VIEW IF EXISTS "{view}""#), &[])
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Connection for RusqliteConnection {
    /// Implements [Connection::transaction()]
    async fn transaction(self: Box<RusqliteConnection>) -> Result<AnyTransaction, Error> {
        AnyTransaction::begin(self).await
    }
}

///////////////////////////////////////////////////////////////////////////////
// Free functions
///////////////////////////////////////////////////////////////////////////////

/// Uses the rusqlite driver directly to query a database using the given prepared [Statement]
/// and parameters.
fn query_prepared(stmt: &mut Statement<'_>, params: &[Value]) -> Result<Vec<Row>, Error> {
    // Begin by binding all of the parameters to the statement:
    for (i, param) in params.iter().enumerate() {
        match param {
            Value::Text(text) => {
                stmt.raw_bind_parameter(i + 1, text)?;
            }
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
            }
            Value::Json(value) => {
                let value = match value {
                    JsonValue::String(value) => value.to_string(),
                    _ => value.to_string(),
                };
                stmt.raw_bind_parameter(i + 1, value)?;
            }
            Value::Timestamp(value) => {
                stmt.raw_bind_parameter(i + 1, value.to_string())?;
            }
            Value::Other(type_name, bytes, string_opt) => {
                return Err(Error::InputError(format!(
                    "Not supported for SQLite: \
                             Value::Other({type_name}, {bytes:?}, {string_opt:?})"
                )));
            }
        };
    }

    // Define the struct that we will use (internally to this function) to represent information
    // about a given column:
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
                        Some(ctype) if ctype.to_lowercase() == "numeric" => {
                            let value = match from_utf8(value) {
                                Ok(value) => value,
                                Err(_err) => {
                                    return Err(deadpool_sqlite::rusqlite::Error::InvalidQuery);
                                }
                            };
                            let value = match value.parse::<Decimal>() {
                                Ok(value) => value,
                                Err(_err) => {
                                    return Err(deadpool_sqlite::rusqlite::Error::InvalidQuery);
                                }
                            };
                            Value::Numeric(value)
                        }
                        _ => {
                            let value = match from_utf8(value) {
                                Ok(value) => value,
                                Err(_err) => {
                                    return Err(deadpool_sqlite::rusqlite::Error::InvalidQuery);
                                }
                            };
                            Value::Text(value.to_string())
                        }
                    },
                };
                db_row.insert(column_name.to_string(), value);
            }
            Ok(db_row)
        })
        .collect::<Vec<_>>();
    Ok(results?)
}

/// Define a regular expression matching function and add it to the database.
fn add_rusqlite_regexp_function(db: &rusqlite::Connection) -> Result<(), Error> {
    type BoxError = Box<dyn std::error::Error + Send + Sync>;

    // This function has been adapted from:
    // https://docs.rs/rusqlite/0.32.1/rusqlite/functions/index.html
    Ok(db.create_scalar_function(
        "regexp_match",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        move |ctx| {
            let num_args = ctx.len();
            if num_args != 2 {
                return Err(rusqlite::Error::UserFunctionError(
                    format!("Expected 2 arguments but got {num_args}").into(),
                ));
            }
            let text = ctx.get_raw(0);
            let regexp: Arc<Regex> = ctx.get_or_create_aux(1, |vr| -> Result<_, BoxError> {
                Ok(Regex::new(vr.as_str()?)?)
            })?;
            match text {
                // If the text to match is NULL then the condition is vacuously true:
                ValueRef::Null => Ok(true),
                _ => {
                    let text = text.as_str()?;
                    Ok(regexp.is_match(text))
                }
            }
        },
    )?)
}

/// Determines whether copy_in() is supported for the given filename.
fn can_copy_in(filename: &str) -> bool {
    filename.to_lowercase().ends_with(".csv")
}
