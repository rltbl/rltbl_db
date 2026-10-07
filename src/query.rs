//! The Query trait, for querying a database pool or transaction.

use async_trait::async_trait;
use indexmap::IndexMap;

use crate::{Error, Rows, Syntax, Value};

#[async_trait]
pub trait Query: std::fmt::Debug + Sync + Send {
    /// Returns the SQL syntax supported by this [Query]-able.
    fn syntax(&self) -> &dyn Syntax;

    /// Given a table, return an [IndexMap] from column names to the names of their datatypes.
    async fn columns(&self, table: &str) -> Result<IndexMap<String, String>, Error> {
        let mut columns = IndexMap::new();
        let (sql, params) = self.syntax().columns_sql(table);
        let rows = self.query(&sql, &params).await?;
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

    /// Retrieve the primary key column names for a given table.
    async fn primary_keys(&self, table: &str) -> Result<Vec<String>, Error> {
        let (sql, params) = self.syntax().primary_keys_sql(table);
        let rows = self.query(&sql, &params).await?;
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
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<(), Error> {
        self.query(sql, params).await?;
        Ok(())
    }

    /// Sequentially execute a semicolon-delimited list of statements, without parameters.
    async fn execute_batch(&self, sql: &str) -> Result<(), Error>;

    /// Execute a query returning a collection of [Rows].
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Rows, Error>;

    /// Returns true if this [Query]-able is capable of bulk loading this file.
    fn can_load(&self, _filename: &str) -> bool {
        // The default implementation is for bulk loading to be unsupported. Implementations
        // for specific drivers (libsql, tokio-postgresql, rusqlite, etc.) will have their
        // own criteria.
        false
    }

    /// Bulk-load the contents of the given file into a table with the given name. If the
    /// table already exists it will be dropped first and recreated.
    async fn load_table(&self, table: &str, filename: &str) -> Result<(), Error>;

    /// Drop the given table from the database.
    async fn drop_table(&self, table: &str) -> Result<(), Error>;

    /// Drop the given view from the database.
    async fn drop_view(&self, view: &str) -> Result<(), Error>;
}
