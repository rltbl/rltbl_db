//! Provides the [Connection] trait for using database connections, and the [AnyTransaction]
//! struct for handling transactions.

use async_trait::async_trait;
use std::mem::ManuallyDrop;

use crate::{Error, Query, Rows, Syntax, Value};

////////////////////////////////////////////////////////////////////////////////
// The Connection trait
////////////////////////////////////////////////////////////////////////////////

/// An asynchronous trait for using a database connection
#[async_trait]
pub trait Connection: Query + std::fmt::Debug {
    /// Begin a transaction.
    async fn transaction(self: Box<Self>) -> Result<AnyTransaction, Error>;
}

////////////////////////////////////////////////////////////////////////////////
// The AnyTransaction struct
////////////////////////////////////////////////////////////////////////////////

/// An abstract representation of a transaction associated with any of the supported
/// [Connection] types.
#[derive(Debug)]
pub struct AnyTransaction {
    pub connection: ManuallyDrop<Box<dyn Connection>>,
    completed: bool,
}

impl AnyTransaction {
    /// Executes a `BEGIN TRANSACTION` statement via the given [Connection], which it then
    /// returns wrapped within an [AnyTransaction].
    pub async fn begin(connection: Box<dyn Connection>) -> Result<Self, Error> {
        connection.execute("BEGIN TRANSACTION", &[]).await?;
        Ok(Self {
            connection: ManuallyDrop::new(connection),
            completed: false,
        })
    }

    /// Private function used by commit() and rollback(). Executes the given SQL command and
    /// then sets self.completed to true.
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

impl Drop for AnyTransaction {
    /// Drops the inner connection after executing a "ROLLBACK" statement, unless the
    /// transaction is already complete. This function panics on database errors.
    fn drop(&mut self) {
        unsafe {
            // Steal the sender out of ManuallyDrop.
            // This moves the value without running its Drop impl.
            let connection = ManuallyDrop::take(&mut self.connection);
            if !self.completed {
                // Spawn a task to perform async cleanup.
                // The task owns the sender and can await.
                tokio::spawn(async move {
                    connection
                        .execute("ROLLBACK", &[])
                        .await
                        .expect("Error dropping transaction to ROLLBACK");
                });
            }
        }
    }
}
