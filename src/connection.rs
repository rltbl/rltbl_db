//! Provides the [Connection] trait for using database connections, and the [AnyTransaction]
//! struct for handling transactions.

use async_drop::{AsyncDrop, AsyncDropFuture, Dropper};
use async_trait::async_trait;

use crate::{Error, Query, Rows, Syntax, Value};

////////////////////////////////////////////////////////////////////////////////
// The Connection trait
////////////////////////////////////////////////////////////////////////////////

/// An asynchronous trait for using a database connection
#[async_trait]
pub trait Connection: Query + std::fmt::Debug {
    /// Begin a transaction.
    async fn transaction(self: Box<Self>) -> Result<Dropper<AnyTransaction>, Error>;
}

////////////////////////////////////////////////////////////////////////////////
// The AnyTransaction struct
////////////////////////////////////////////////////////////////////////////////

/// An abstract representation of a transaction associated with any of the supported
/// [Connection] types.
#[derive(Debug)]
pub struct AnyTransaction {
    pub connection: Box<dyn Connection>,
    completed: bool,
}

// TODO: Consider what to do about implementing Drop. Although it seems to be possible to
// implement Drop synchronously (which is mandatory) in sqlite, it doesn't seem possible in
// postgres. That said, the behaviour of the database seems to be that it will not commit
// transactions that go out of scope.

impl AsyncDrop for AnyTransaction {
    fn async_drop(&mut self) -> AsyncDropFuture<'_> {
        Box::pin(async {
            if !self.completed {
                eprintln!("DROPPING ...");
                self.rollback().await.unwrap();
                eprintln!("DROPPED!");
            }
            Ok(())
        })
    }
}

impl AnyTransaction {
    /// Executes a `BEGIN TRANSACTION` statement via the given [Connection], which it then
    /// returns wrapped within an [AnyTransaction].
    pub async fn begin(connection: Box<dyn Connection>) -> Result<Self, Error> {
        connection.execute("BEGIN TRANSACTION", &[]).await?;
        Ok(Self {
            connection: connection,
            completed: false,
        })
    }

    // Private function used by commit() and rollback(). Executes the given SQL command and
    // then sets self.completed to true.
    async fn conclude_with_command(&mut self, cmd: &str) -> Result<(), Error> {
        if self.completed {
            return Err(Error::DatabaseError(
                "Transaction already completed.".into(),
            ));
        }
        if !["commit", "rollback"].contains(&cmd.to_lowercase().as_str()) {
            return Err(Error::InputError(format!("Invalid command: {cmd}")));
        }
        self.connection.execute(cmd, &[]).await?;
        self.completed = true;
        Ok(())
    }

    /// Executes a `ROLLBACK` statement using this connection.
    pub async fn rollback(&mut self) -> Result<(), Error> {
        self.conclude_with_command("ROLLBACK").await
    }

    /// Executes a `COMMIT` statement using this connection.
    pub async fn commit(&mut self) -> Result<(), Error> {
        self.conclude_with_command("COMMIT").await
    }

    /// Returns true if this transaction has already been committed or rolled back.
    pub async fn completed(&self) -> bool {
        self.completed
    }
}

// We implement Query as a wrapper around the Query-able inner connection.
#[async_trait]
impl Query for AnyTransaction {
    /// Implements [Query::syntax()]
    fn syntax(&self) -> &dyn Syntax {
        self.connection.syntax()
    }

    /// Implements [Query::execute_batch()]
    async fn execute_batch(&self, sql: &str) -> Result<(), Error> {
        self.connection.execute_batch(sql).await
    }

    /// Implements [Query::query()]
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Rows, Error> {
        self.connection.query(sql, params).await
    }

    /// Implements [Query::can_copy_in()]
    fn can_copy_in(&self, filename: &str) -> bool {
        self.connection.can_copy_in(filename)
    }

    /// Implements [Query::copy_in()]
    async fn copy_in(&self, table: &str, filename: &str) -> Result<(), Error> {
        self.connection.copy_in(table, filename).await
    }

    /// Implements [Query::drop_table()]
    async fn drop_table(&self, table: &str) -> Result<(), Error> {
        self.connection.drop_table(table).await
    }

    /// Implements [Query::drop_view()]
    async fn drop_view(&self, view: &str) -> Result<(), Error> {
        self.connection.drop_view(view).await
    }
}
