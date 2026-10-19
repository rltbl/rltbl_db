//! Provides the [Transaction] trait for using database transactions, and the [AnyTransaction]
//! struct, providing a handy generic wrapper around [Transaction] implementations.

use async_trait::async_trait;

use crate::{Connection, Error, Query, Rows, Syntax, Value};

/// An asynchronous trait for using database transactions
#[async_trait]
pub trait Transaction: Query + std::fmt::Debug {
    async fn commit(&mut self) -> Result<(), Error>;

    async fn rollback(&mut self) -> Result<(), Error>;
}
