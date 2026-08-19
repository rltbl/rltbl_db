//! Trait for implementing various SQL language "flavours"
//!
//! Although there is an ANSI SQL standard, each SQL database implemntation uses somewhat
//! different syntax. Our [Syntax] trait abstracts over some of those differences, so we can
//! write our Rust code once and use different SQL databases at runtime.

use crate::{Error, Type, Value};

pub trait Syntax: std::fmt::Debug {
    // TODO: Do we really need this? Since SqliteSyntax and PostgresSyntax are unit structs,
    // can't we just match on those? I guess we will need to use dyn Syntax ... not sure if that
    // is easier or more efficient, though. Maybe not. Another option is an enum?
    /// Returns the name for this syntax.
    fn name(&self) -> &str;

    /// Get a SQL Type by its name in this SQL syntax.
    fn sql_type(&self, name: &str) -> Result<Type, Error> {
        match name.to_uppercase().as_str() {
            "TEXT" => Ok(Type::Text(name.to_string())),
            // TODO: Add more?
            _ => Err(Error::DatatypeError(format!(
                "Unrecoganized type name: {name}"
            ))),
        }
    }

    /// Generate the SQL and parameters needed to query the database's metadata for the names and
    /// types of the columns of the given table.
    fn columns_sql(&self, _table: &str) -> (String, [Value; 1]) {
        // MC: I don't see why we need a default implementation? What would it be applicable to?
        todo!("write default implementation for columns_sql")
    }

    /// Generate the SQL and parameters needed to query the database's metadata for the primary
    /// key columns of the given table.
    fn primary_keys_sql(&self, table: &str) -> (String, [Value; 1]);

    /// Get the prefix to use for parameters to queries that need to be bound.
    fn param_prefix(&self) -> &str;

    // TODO: Other methods ...
}
