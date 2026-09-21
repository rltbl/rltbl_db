//! Driver using deadpool-sqlite (ilbsql).

use async_trait::async_trait;
use deadpool_libsql::{self, Manager, libsql, libsql::Builder};
use rust_decimal::prelude::ToPrimitive;
use std::env;

use crate::{
    Error, JsonValue, Pool, Query, Row, Rows, Syntax, Transaction, Value,
    sql_parse::validate_table_name, sqlite::SqliteSyntax,
};

// Conversions from Values to libsql::Values and vice versa:
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
            // Libsql does not support booleans.
            // See: https://docs.rs/libsql/0.9.29/libsql/enum.Value.html,
            Value::Boolean(val) => Ok(libsql::Value::Integer(val.into())),
            Value::BigInteger(val) => Ok(libsql::Value::Integer(val.into())),
            Value::Integer(val) => Ok(libsql::Value::Integer(val.into())),
            Value::SmallInteger(val) => Ok(libsql::Value::Integer(val.into())),
            Value::Real(val) => Ok(libsql::Value::Real(val.into())),
            Value::BigReal(val) => Ok(libsql::Value::Real(val.into())),
            // TODO: Add another error type.
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
    load_extensions_enabled: bool,
}

#[async_trait]
impl Query for LibSQLPool {
    /// Implements [Query::syntax()].
    fn syntax(&self) -> &dyn Syntax {
        &self.syntax
    }

    /// Implements [Query::execute_batch()].
    async fn execute_batch(&self, sql: &str) -> Result<(), Error> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(|err| Error::ConnectError(format!("Error getting from pool: {err}")))?;
        match conn.execute_batch(sql).await {
            Err(err) => {
                return Err(Error::DatabaseError(format!("Error during query: {err}")));
            }
            Ok(_) => Ok(()),
        }
    }

    /// Implements [Query::query()].
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Rows, Error> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(|err| Error::ConnectError(format!("Error getting from pool: {err}")))?;

        let params = params.to_vec();
        let mut rows = conn
            .query(sql, params)
            .await
            .map_err(|err| Error::ConnectError(format!("Query error: {err}")))?;

        let mut db_rows = vec![];
        while let Some(row) = rows
            .next()
            .await
            .map_err(|err| Error::DataError(err.to_string()))?
        {
            let mut db_row = Row::new();
            for i in 0..row.column_count() {
                let column = row.column_name(i).ok_or(Error::DataError(format!(
                    "Error getting name of column {i} of row."
                )))?;
                let value = row.get_value(i).map_err(|err| {
                    Error::DataError(format!("Error getting value of column {i} of row: {err}"))
                })?;
                db_row.insert(column.to_string(), value.try_into()?);
            }
            db_rows.push(db_row);
        }

        Ok(Rows { rows: db_rows })
    }

    /// Implements [Query::can_load()]
    fn can_load(&self, filename: &str) -> bool {
        self.load_extensions_enabled || filename.to_lowercase().ends_with(".csv")
    }

    /// Implements [Query::load_table()]
    async fn load_table(&self, table: &str, filename: &str) -> Result<(), Error> {
        if !self.can_load(filename) {
            return Err(Error::InputError(format!(
                "Filename: '{filename}' must end with .csv and load extensions must be enabled \
                 for direct loading."
            )));
        }

        eprintln!("Loading table '{table}' from '{filename}' using SQLite's CSV load extension.");
        let current_dir = env::current_dir().map_err(|err| {
            Error::ConnectError(format!("Error getting current directory: {err}"))
        })?;
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

    /// Implements [Query::drop_table()].
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
impl Pool for LibSQLPool {
    /// Begins a new [Transaction].
    async fn transaction(&self) -> Result<Box<dyn Transaction>, Error> {
        todo!()
    }
}

impl LibSQLPool {
    /// Connect to the database at the given URL.
    pub async fn connect(url: &str) -> Result<Self, Error> {
        let db = Builder::new_local(url).build().await.map_err(|err| {
            Error::ConnectError(format!("Error creating pool from URL: '{url}': {err}"))
        })?;
        let manager = Manager::from_libsql_database(db);
        let pool = deadpool_libsql::Pool::builder(manager)
            .build()
            .map_err(|err| {
                Error::ConnectError(format!("Error creating pool from URL: '{url}': {err}"))
            })?;

        let conn = pool
            .get()
            .await
            .map_err(|err| Error::ConnectError(format!("Error getting from pool: {err}")))?;

        // Enable the CSV load extension.
        // Note that this requires that csv.so be in the current directory.
        conn.load_extension_enable().map_err(|err| {
            Error::ConnectError(format!("Error creating pool from URL: '{url}': {err}"))
        })?;
        let current_dir = env::current_dir().map_err(|err| {
            Error::ConnectError(format!("Error creating pool from URL: '{url}': {err}"))
        })?;
        let current_dir = current_dir.display();
        match conn.load_extension(&format!("{current_dir}/csv"), None) {
            Ok(_) => Ok(Self {
                pool: pool,
                syntax: SqliteSyntax,
                load_extensions_enabled: true,
            }),
            Err(err) => {
                eprintln!("WARNING Unable to load extension 'csv': {err}");
                conn.load_extension_disable().map_err(|err| {
                    Error::ConnectError(format!("Error creating pool from URL: '{url}': {err}"))
                })?;
                Ok(Self {
                    pool: pool,
                    syntax: SqliteSyntax,
                    load_extensions_enabled: false,
                })
            }
        }
    }
}

/// Represents a SQLite transaction.
#[derive(Debug)]
pub struct LibSQLTransaction {
    /// The syntax used for this transaction.
    _syntax: SqliteSyntax,
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

    fn can_load(&self, _filename: &str) -> bool {
        todo!()
    }

    async fn load_table(&self, _table: &str, _filename: &str) -> Result<(), Error> {
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
