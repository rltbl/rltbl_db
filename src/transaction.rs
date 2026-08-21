//! A trait for using database transactions

use async_trait::async_trait;

use crate::{error::Error, query::Query, row::Rows, value::IntoValues};

/// An asynchronous trait for using database transactions
#[async_trait]
pub trait Transaction: Query + std::fmt::Debug {
    async fn commit(&mut self) -> Result<(), Error>;

    async fn rollback(&mut self) -> Result<(), Error>;
}

/// An abstraction over the supported types of database [Transaction].
#[derive(Debug)]
pub struct AnyTransaction {
    tx: Box<dyn Transaction>,
    #[allow(dead_code)]
    modified_tables: Vec<String>,
}

impl AnyTransaction {
    /// Wraps a transaction type into an [AnyTransaction].
    pub fn begin(tx: Box<dyn Transaction>) -> Self {
        Self {
            tx,
            modified_tables: Vec::new(),
        }
    }

    /// Query the database.
    pub async fn query(&self, sql: &str, params: impl IntoValues) -> Result<Rows, Error> {
        // TODO: track modified tables
        let params = params.into_values()?.map(|val| val).collect::<Vec<_>>();
        self.tx.query(sql, &params).await
    }

    // TODO: execute
    // TODO: execute_batch
    // TODO: insert
    // TODO: insert_returning
    // TODO: update
    // TODO: update_returning
    // TODO: upsert
    // TODO: upsert_returning

    /// Commit the transaction.
    pub async fn commit(&mut self) -> Result<(), Error> {
        // TODO: handle cache for modified tables
        self.tx.commit().await
    }

    /// Roll back the transaction.
    pub async fn rollback(&mut self) -> Result<(), Error> {
        self.tx.rollback().await
    }
}
