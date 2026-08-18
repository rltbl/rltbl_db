//! Trait for querying a database pool or transaction.
//!
//! Our [Query] trait is

use async_trait::async_trait;
use indexmap::IndexMap;

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

    /// Given a table, return an [IndexMap] from column names to column SQL types.
    async fn columns(&self, table: &str) -> Result<IndexMap<String, String>, Error> {
        let mut columns = IndexMap::new();
        let (sql, params) = self.syntax().columns_sql(table);
        let params = &params.iter().map(|val| val).collect::<Vec<_>>()[..];
        // TODO: Use the "no_cache_clean" version instead here:
        let rows = self.query(&sql, params).await?;
        for row in rows.iter() {
            match (
                row.get("column_name")
                    .and_then(|value| Some::<String>(value.into())),
                row.get("data_type")
                    .and_then(|value| Some::<String>(value.into())),
            ) {
                (Some(column), Some(data_type)) => {
                    columns.insert(column.to_string(), data_type.to_lowercase().to_string())
                }
                _ => {
                    return Err(Error::DataError(format!(
                        "Error getting columns for table '{table}'"
                    )));
                }
            };
        }
        match columns.is_empty() {
            true => Err(Error::DataError(format!(
                "No information found for table '{table}'"
            ))),
            false => Ok(columns),
        }
    }

    // TODO: Combine this with columns() if possible
    /// Retrieve the primary key column names for a given table.
    async fn primary_keys(&self, table: &str) -> Result<Vec<String>, Error> {
        let (sql, params) = self.syntax().primary_keys_sql(table);
        let params = &params.iter().map(|val| val).collect::<Vec<_>>()[..];
        // TODO: Use the "no_cache_clean" version of query().
        let rows = self.query(&sql, params).await?;
        rows.rows
            .iter()
            .map(|row| {
                match row
                    .get("column_name")
                    .and_then(|name| Some::<String>(name.into()))
                {
                    Some(pk_col) => Ok(pk_col.to_string()),
                    None => Err(Error::DataError("Empty row".to_owned())),
                }
            })
            .collect()
    }

    /// Execute a query without returning any values.
    async fn execute(&self, sql: &str, params: &[&Value]) -> Result<(), Error> {
        self.query(sql, params).await?;
        Ok(())
    }

    /// Sequentially execute a semicolon-delimited list of statements, without parameters.
    async fn execute_batch(&self, sql: &str) -> Result<(), Error>;

    /// Execute a query returning a collection of [Rows].
    async fn query(&self, _sql: &str, _params: &[&Value]) -> Result<Rows, Error> {
        todo!("default implementation of Query::query")
    }

    /// Insert rows into the given columns of the given table. If an input row does not have a
    /// key corresponding to one of the given columns, use NULL as the value of that column when
    /// inserting the row to the table.
    async fn insert(
        &self,
        table: &str,
        columns: &[&str],
        // TODO: This should be an iterator.
        rows: &Rows,
    ) -> Result<(), Error>;

    // TODO: insert_returning
    // TODO: update
    // TODO: update_returning
    // TODO: upsert
    // TODO: upsert_returning

    /// Drop the given table from the database. Note that for PostgreSQL (see
    /// <https://www.postgresql.org/docs/current/sql-droptable.html>), if the dropped table,
    /// say table1, appears in a foreign key constraint for another table, say table2, then
    /// table2's foreign constraint will be removed, but table2 will not be dropped.
    async fn drop_table(&self, table: &str) -> Result<(), Error>;
}
