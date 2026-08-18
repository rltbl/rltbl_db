//! Trait for SQL language "flavours"
//!
//! Although there is an ANSI SQL standard,
//! each SQL database implemntation uses somewhat different syntax.
//! Our [Syntax] trait abstracts over some of those differences,
//! so we can write our Rust code once
//! and use different SQL databases at runtime.

use crate::{
    error::Error,
    value::{Type, Value},
};

pub trait Syntax: std::fmt::Debug {
    /// TODO: Add docstring.
    fn name(&self) -> &str;

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
        // MC: I don't see why we need a default implementation? What would it be applicable to?
        todo!("write default implementation for columns_sql")
    }

    // TODO: Combine primary_keys_sql with columns_sql if possible.
    /// Generate the SQL and parameters needed to query the database's metadata for the primary
    /// key columns of the given table.
    fn primary_keys_sql(&self, table: &str) -> (String, [Value; 1]);

    /// Get the prefix to use for parameters to queries that need to be bound.
    fn param_prefix(&self) -> &str;

    // MC: In rltbl_db there is also a method (that we definitely need) called
    // primary_keys_sql(). There used to be a TODO comment above it (possibly removed now)
    // to the effect that columns_sql() should also return primary key info since we are
    // querying the metadata anyway.
    // I think it might make sense to do that here.

    // MC: did you intend for this trait to have so few methods in comparison to
    // DbKind? Or do you plan to add those other methods later?
    // JO: It will need a bunch of methods, similar to DbKind.
    // I just didn't need much for my stub implementation.
}
