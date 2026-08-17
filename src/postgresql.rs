//! PostgreSQL syntax

use crate::{Error, Syntax, Type, value::Value};

#[derive(Debug)]
pub struct PostgresSyntax;

impl Syntax for PostgresSyntax {
    /// Get a SQL Type by its name in this SQL syntax.
    fn sql_type(&self, name: &str) -> Result<Type, Error> {
        match name.to_uppercase().as_str() {
            "TEXT" => Ok(Type::Text(name.to_string())),
            _ => Err(Error::DatatypeError(format!(
                "Unrecoganized type name: {name}"
            ))),
        }
    }

    /// Generate the SQL and parameters needed to query the database's metadata for the names and
    /// types of the columns of the given table.
    fn columns_sql(&self, _table: &str) -> (String, [Value; 1]) {
        todo!("write default implementation for columns_sql")
    }
}
