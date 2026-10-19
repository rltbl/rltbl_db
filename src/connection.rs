//! Provides the [Connection] trait for using database connections.

use async_trait::async_trait;

use crate::{Error, Query, Rows, Syntax, Transaction, Value};

////////////////////////////////////////////////////////////////////////////////
// The Connection trait
////////////////////////////////////////////////////////////////////////////////

/// An asynchronous trait for using a database connection
#[async_trait]
pub trait Connection: Query + std::fmt::Debug {
    /// Begin a transaction.
    async fn transaction(self: Box<Self>) -> Result<Box<dyn Transaction>, Error>;
}
