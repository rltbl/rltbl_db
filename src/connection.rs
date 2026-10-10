//! Provides the [Connection] trait for using database connections, and the [AnyConnection]
//! struct, providing a handy generic wrapper around [Connection] implementations.

use async_trait::async_trait;

use crate::{Error, Query, Transaction};

/// An asynchronous trait for using database connections
#[async_trait]
pub trait Connection: Query + std::fmt::Debug {
    // Note: the 'life0 lifetime allows the returned Transaction to depend on the data
    // in this connection.
    /// Begin a transaction.
    async fn transaction(&mut self) -> Result<Box<dyn Transaction + 'life0>, Error>;
}

/// An abstraction over the supported connection types.
#[derive(Debug)]
pub struct AnyConnection {
    pub connection: Box<dyn Connection>,
}

impl From<Box<dyn Connection>> for AnyConnection {
    fn from(connection: Box<dyn Connection>) -> Self {
        AnyConnection { connection }
    }
}

impl AnyConnection {
    // TODO: Add methods here in a similar way as in AnyPool.
}
