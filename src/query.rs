//! Trait for querying a database pool or transaction.
//!
//! Our [Query] trait is

use async_trait::async_trait;

use crate::{error::Error, row::Rows, syntax::Syntax, value::Value};

// JO: The Query trait provides the main database methods,
// which are shared by both Pool and Transaction.
// JO: These trait all have concrete arguments (no traits, no impls)
// to ensure that theyre dyn-compatible.
// The "normal" AnyPool, AnyTransaction structs can use traits and impls as convenient,
// because the structs don't have to be dyn-compatible.

#[async_trait]
pub trait Query: std::fmt::Debug + Sync {
    fn syntax(&self) -> &dyn Syntax;

    /// Execute a query without returning any values.
    async fn execute(&self, sql: &str, params: &[&Value]) -> Result<(), Error> {
        self.query(sql, params).await?;
        Ok(())
    }

    /// Execute a query returning a collection of [Rows].
    async fn query(&self, _sql: &str, _params: &[&Value]) -> Result<Rows, Error> {
        todo!("default implementation of DbQuery::query")
    }

    // TODO: insert
    // TODO: insert_returning
    // TODO: update
    // TODO: update_returning
    // TODO: upsert
    // TODO: upsert_returning
    // TODO: drop
}
