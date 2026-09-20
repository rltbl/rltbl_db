//! Driver using deadpool-sqlite (ilbsql).

use async_trait::async_trait;
use deadpool_libsql::{self};

use crate::{Error, Pool, Query, Rows, Syntax, Transaction, Value, sqlite::SqliteSyntax};

/// Represents a deadpool-sqlite database connection pool.
#[derive(Debug)]
pub struct LibSQLPool {
    _syntax: SqliteSyntax,
    pub pool: deadpool_libsql::Pool,
}

#[async_trait]
impl Query for LibSQLPool {
    /// Implements [Query::syntax()].
    fn syntax(&self) -> &dyn Syntax {
        todo!()
    }

    /// Implements [Query::execute_batch()].
    async fn execute_batch(&self, _sql: &str) -> Result<(), Error> {
        todo!()
    }

    /// Implements [Query::query()].
    async fn query(&self, _sql: &str, _params: &[Value]) -> Result<Rows, Error> {
        todo!()
    }

    /// Implements [Query::can_load()]
    fn can_load(&self, _filename: &str) -> bool {
        todo!()
    }

    /// Implements [Query::load_table()]
    async fn load_table(&self, _table: &str, _filename: &str) -> Result<(), Error> {
        todo!()
    }

    /// Implements [Query::drop_table()].
    async fn drop_table(&self, _table: &str) -> Result<(), Error> {
        todo!()
    }

    /// Implements [Query::drop_view()]
    async fn drop_view(&self, _view: &str) -> Result<(), Error> {
        todo!()
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
    pub async fn connect(_url: &str) -> Result<Self, Error> {
        todo!()
    }
}

/// Represents a SQLite transaction.
#[derive(Debug)]
struct LibSQLTransaction {
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

    #[allow(unused)]
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
