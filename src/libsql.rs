//! Driver using deadpool-sqlite (libsql).

use async_trait::async_trait;
use deadpool_libsql::{self, Manager, libsql, libsql::Builder};
use rust_decimal::prelude::ToPrimitive;
use std::env;

use crate::{
    Connection, Error, JsonValue, Pool, Query, Row, Rows, Syntax, Transaction, Value,
    error::DatabaseError, sql_parse::validate_table_name, sqlite::SqliteSyntax,
};

// DatabaseError implementations.
impl From<deadpool_libsql::libsql::Error> for DatabaseError {
    fn from(err: deadpool_libsql::libsql::Error) -> DatabaseError {
        DatabaseError::Error(err.to_string())
    }
}

impl From<deadpool_libsql::BuildError> for DatabaseError {
    fn from(err: deadpool_libsql::BuildError) -> DatabaseError {
        DatabaseError::BuildError(err.to_string())
    }
}

impl From<deadpool_libsql::CreatePoolError> for DatabaseError {
    fn from(err: deadpool_libsql::CreatePoolError) -> DatabaseError {
        DatabaseError::CreatePoolError(err.to_string())
    }
}

impl From<deadpool_libsql::PoolError> for DatabaseError {
    fn from(err: deadpool_libsql::PoolError) -> DatabaseError {
        DatabaseError::PoolError(err.to_string())
    }
}

// Conversions from Values to libsql::Values and vice versa.
impl TryFrom<libsql::Value> for Value {
    type Error = Error;

    fn try_from(item: libsql::Value) -> Result<Self, Error> {
        match &item {
            libsql::Value::Null => Ok(Self::Null),
            libsql::Value::Integer(number) => Ok(Self::from(*number)),
            libsql::Value::Real(number) => Ok(Self::from(*number)),
            libsql::Value::Text(string) => Ok(Self::Text(string.to_string())),
            libsql::Value::Blob(blob) => Ok(Self::Other("".to_string(), blob.to_vec(), None)),
        }
    }
}

impl TryInto<libsql::Value> for Value {
    type Error = Error;

    fn try_into(self) -> Result<libsql::Value, Error> {
        match self {
            Value::Null => Ok(libsql::Value::Null),
            // LibSQL does not support booleans.
            // See: https://docs.rs/libsql/0.9.29/libsql/enum.Value.html,
            Value::Boolean(val) => Ok(libsql::Value::Integer(val.into())),
            Value::BigInteger(val) => Ok(libsql::Value::Integer(val.into())),
            Value::Integer(val) => Ok(libsql::Value::Integer(val.into())),
            Value::SmallInteger(val) => Ok(libsql::Value::Integer(val.into())),
            Value::Real(val) => Ok(libsql::Value::Real(val.into())),
            Value::BigReal(val) => Ok(libsql::Value::Real(val.into())),
            Value::Numeric(val) => {
                let val = val.to_f64().ok_or(Error::DatatypeError(format!(
                    "Error converting value '{val}' to f64"
                )))?;
                Ok(libsql::Value::Real(val.into()))
            }
            Value::Text(val) => Ok(libsql::Value::Text(val)),
            Value::Json(val) => {
                let val = match val {
                    JsonValue::String(val) => val.to_string(),
                    _ => val.to_string(),
                };
                Ok(libsql::Value::Text(val))
            }
            Value::Other(_, val, _) => Ok(libsql::Value::Blob(val)),
        }
    }
}

/// Represents a deadpool-sqlite database connection pool.
#[derive(Debug)]
pub struct LibSQLPool {
    pub pool: deadpool_libsql::Pool,
    syntax: SqliteSyntax,
    csv_extension_enabled: bool,
}

#[async_trait]
impl Query for LibSQLPool {
    /// Implements [Query::syntax()].
    fn syntax(&self) -> &dyn Syntax {
        &self.syntax
    }

    /// Implements [Query::execute_batch()].
    async fn execute_batch(&self, sql: &str) -> Result<(), Error> {
        let connection = self.pool.get().await?;
        let connection = LibSQLConnection {
            connection,
            syntax: self.syntax,
            csv_extension_enabled: self.csv_extension_enabled,
        };
        connection.execute_batch(sql).await?;
        Ok(())
    }

    /// Implements [Query::query()].
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Rows, Error> {
        let connection = self.pool.get().await?;
        let connection = LibSQLConnection {
            connection,
            syntax: self.syntax,
            csv_extension_enabled: self.csv_extension_enabled,
        };
        Ok(connection.query(sql, params).await?)
    }

    /// Implements [Query::can_copy_in()]. Returns true if the CSV load extension is enabled for
    /// the connection pool and the given filename ends (case-insensitively) with '.csv'. The
    /// CSV load extension will be enabled if the shared object file `csv.so` exists in the
    /// current directory when the connection pool is created.
    fn can_copy_in(&self, filename: &str) -> bool {
        can_copy_in(self.csv_extension_enabled, filename)
    }

    /// Implements [Query::copy_in()]
    async fn copy_in(&self, table: &str, filename: &str) -> Result<(), Error> {
        let connection = self.pool.get().await?;
        let connection = LibSQLConnection {
            connection,
            syntax: self.syntax,
            csv_extension_enabled: self.csv_extension_enabled,
        };
        Ok(connection.copy_in(table, filename).await?)
    }

    /// Implements [Query::drop_table()].
    async fn drop_table(&self, table: &str) -> Result<(), Error> {
        let connection = self.pool.get().await?;
        let connection = LibSQLConnection {
            connection,
            syntax: self.syntax,
            csv_extension_enabled: self.csv_extension_enabled,
        };
        Ok(connection.drop_table(table).await?)
    }

    /// Implements [Query::drop_view()]
    async fn drop_view(&self, view: &str) -> Result<(), Error> {
        let connection = self.pool.get().await?;
        let connection = LibSQLConnection {
            connection,
            syntax: self.syntax,
            csv_extension_enabled: self.csv_extension_enabled,
        };
        Ok(connection.drop_view(view).await?)
    }
}

impl LibSQLPool {
    /// Connects to the database at the given URL. If the file shared object file `csv.so` is in
    /// the current directory, enables the CSV load extension.
    pub async fn connect(url: &str) -> Result<Self, Error> {
        let db = Builder::new_local(url).build().await?;
        let manager = Manager::from_libsql_database(db);
        let pool = deadpool_libsql::Pool::builder(manager).build()?;
        let conn = pool.get().await?;

        // Enable the CSV load extension if possible. This requires that the file `csv.so` is in
        // the current directory.
        conn.load_extension_enable()?;
        let current_dir = env::current_dir()?;
        let current_dir = current_dir.display();
        match conn.load_extension(&format!("{current_dir}/csv"), None) {
            Ok(_) => Ok(Self {
                pool: pool,
                syntax: SqliteSyntax,
                csv_extension_enabled: true,
            }),
            Err(err) => {
                eprintln!("INFO Unable to CSV load extension: '{err}'. Disabling.");
                conn.load_extension_disable()?;
                Ok(Self {
                    pool: pool,
                    syntax: SqliteSyntax,
                    csv_extension_enabled: false,
                })
            }
        }
    }
}

#[async_trait]
impl Pool for LibSQLPool {
    /// Begins a new [Transaction].
    async fn connection(&self) -> Result<Box<dyn Connection>, Error> {
        todo!()
    }
}

/// Represents a SQLite connection.
#[derive(Debug)]
pub struct LibSQLConnection {
    pub connection: deadpool_libsql::Object,
    syntax: SqliteSyntax,
    csv_extension_enabled: bool,
}

#[async_trait]
impl Query for LibSQLConnection {
    /// Implements [Query::syntax()] for [LibSQLTransaction]
    fn syntax(&self) -> &dyn Syntax {
        &self.syntax
    }

    /// Implements [Query::execute_batch()] for [LibSQLTransaction]
    async fn execute_batch(&self, sql: &str) -> Result<(), Error> {
        self.connection.execute_batch(sql).await?;
        Ok(())
    }

    /// Implements [Query::query()] for [LibSQLTransaction]
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Rows, Error> {
        let params = params.to_vec();
        let mut rows = self.connection.query(sql, params).await?;
        let mut db_rows = vec![];
        while let Some(row) = rows.next().await? {
            let mut db_row = Row::new();
            for i in 0..row.column_count() {
                let column = row.column_name(i).ok_or(Error::DatabaseError(
                    format!("Error getting name of column {i} of row.").into(),
                ))?;
                let value = row.get_value(i)?;
                db_row.insert(column.to_string(), value.try_into()?);
            }
            db_rows.push(db_row);
        }

        Ok(Rows { rows: db_rows })
    }

    fn can_copy_in(&self, filename: &str) -> bool {
        can_copy_in(self.csv_extension_enabled, filename)
    }

    async fn copy_in(&self, table: &str, filename: &str) -> Result<(), Error> {
        if !self.can_copy_in(filename) {
            return Err(Error::InputError(format!(
                "Filename: '{filename}' must end with .csv and load extensions must be enabled \
                 for direct loading."
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

    /// Implements [Query::drop_table()] for [LibSQLTransaction]
    async fn drop_table(&self, table: &str) -> Result<(), Error> {
        let table = validate_table_name(table)?;

        // Drop the table:
        self.execute(&format!(r#"DROP TABLE IF EXISTS "{table}""#), &[])
            .await?;

        Ok(())
    }

    async fn drop_view(&self, view: &str) -> Result<(), Error> {
        let view = validate_table_name(view)?;

        // Drop the view:
        self.execute(&format!(r#"DROP VIEW IF EXISTS "{view}""#), &[])
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Connection for LibSQLConnection {
    async fn transaction(&self) -> Result<Box<dyn Transaction>, Error> {
        todo!()
    }
}

/// Represents a SQLite transaction.
#[derive(Debug)]
pub struct LibSQLTransaction {
    /// The syntax used for this transaction.
    _syntax: SqliteSyntax,
    csv_extension_enabled: bool,
    _pool: deadpool_libsql::Pool,
    _conn: Option<deadpool_libsql::Connection>,
}

/// [Drop] implements the destruction operation for rust objects.
impl Drop for LibSQLTransaction {
    /// Called whenever the object representing the LibSQLTransaction is dropped.
    /// Executes a ROLLBACK of the transaction.
    fn drop(&mut self) {
        todo!()
    }
}

#[async_trait]
impl Query for LibSQLTransaction {
    /// Implements [Query::syntax()] for [LibSQLTransaction]
    fn syntax(&self) -> &dyn Syntax {
        todo!()
    }

    /// Implements [Query::execute_batch()] for [LibSQLTransaction]
    async fn execute_batch(&self, _sql: &str) -> Result<(), Error> {
        todo!()
    }

    /// Implements [Query::query()] for [LibSQLTransaction]
    async fn query(&self, _sql: &str, _params: &[Value]) -> Result<Rows, Error> {
        todo!()
    }

    /// Implements [Query::can_copy_in()] for [LibSQLTransaction]
    fn can_copy_in(&self, filename: &str) -> bool {
        can_copy_in(self.csv_extension_enabled, filename)
    }

    /// Implements [Query::copy_in()] for [LibSQLTransaction]
    async fn copy_in(&self, _table: &str, _filename: &str) -> Result<(), Error> {
        todo!()
    }

    /// Implements [Query::drop_table()] for [LibSQLTransaction]
    async fn drop_table(&self, _table: &str) -> Result<(), Error> {
        todo!()
    }

    async fn drop_view(&self, _view: &str) -> Result<(), Error> {
        todo!()
    }
}

#[async_trait]
impl Transaction for LibSQLTransaction {
    /// Rolls back this transaction.
    async fn rollback(&mut self) -> Result<(), Error> {
        todo!()
    }

    /// Commits this transaction.
    async fn commit(&mut self) -> Result<(), Error> {
        todo!()
    }
}

impl LibSQLTransaction {
    /// Creates a new [LibSQLTransaction].
    pub async fn _begin(_pool: deadpool_libsql::Pool) -> Result<Self, Error> {
        todo!()
    }
}

fn can_copy_in(csv_extension_enabled: bool, filename: &str) -> bool {
    csv_extension_enabled && filename.to_lowercase().ends_with(".csv")
}
