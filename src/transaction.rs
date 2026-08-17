//! Trait for database transactions

use async_trait::async_trait;

use crate::{Value, error::Error, query::Query, row::Rows};

// MC: I guess this trait is actually for *using* transactions, and the trait for
// transactions in pool.rs is for *creating* things that implement the Transaction trait.
// To be honest this is a little convoluted and seems confusing to me. But that's not an
// objection, since I guess that it makes things simpler in general.
// But in any case, maybe reflect this difference between what these traits do by picking
// better names for them both?
// JO: The Transaction is the database transaction.
// It implements Query, and can also commit and rollback.

// MC: I find it confusing that the two traits used for transactions are in different files.
// Can we move the one named "Pool" to this file (and rename it)?

// MC: Why do you say it is synchronous when both methods are asynchronous?
// A synchronous trait for using database transactions
#[async_trait]
pub trait Transaction: Query + std::fmt::Debug {
    async fn commit(&mut self) -> Result<(), Error>;

    async fn rollback(&mut self) -> Result<(), Error>;
}

#[derive(Debug)]
pub struct AnyTransaction {
    tx: Box<dyn Transaction>,
    #[allow(dead_code)]
    modified_tables: Vec<String>,
}

// MC: This interface seems useful and pretty clean, though.
impl AnyTransaction {
    pub fn begin(tx: Box<dyn Transaction>) -> Self {
        Self {
            tx,
            modified_tables: Vec::new(),
        }
    }

    pub async fn query(
        &self,
        sql: &str,
        params: impl IntoIterator<Item = &Value>,
    ) -> Result<Rows, Error> {
        // TODO: track modified tables
        let refs: Vec<&Value> = params.into_iter().collect();
        self.tx.query(sql, &refs).await
    }

    // TODO: execute
    // TODO: execute_batch
    // TODO: insert
    // TODO: insert_returning
    // TODO: update
    // TODO: update_returning
    // TODO: upsert
    // TODO: upsert_returning

    pub async fn commit(&mut self) -> Result<(), Error> {
        // TODO: handle cache for modified tables
        self.tx.commit().await
    }

    pub async fn rollback(&mut self) -> Result<(), Error> {
        self.tx.rollback().await
    }
}
