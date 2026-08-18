//! SQLite syntax

// MC: There are *a lot* of files in this repository now. I'm not necessarily objecting, just
// making a comment about it. It is starting to remind me of a Java project ;-)
// JO: Yes, that's my preference. This one will be big enough once `Syntax` is filled out.

use crate::{Error, Syntax, Type, value::Value, values};

/// The [maximum number of parameters](https://www.sqlite.org/limits.html#max_variable_number)
/// that can be bound to a SQLite query
pub static MAX_PARAMS_SQLITE: usize = 32766;

#[derive(Debug)]
pub struct SqliteSyntax;

impl Syntax for SqliteSyntax {
    /// TODO: Add docstring.
    fn name(&self) -> &str {
        "sqlite"
    }

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
    fn columns_sql(&self, table: &str) -> (String, [Value; 1]) {
        (
            r#"SELECT "name" AS "column_name", "type" AS "data_type"
               FROM pragma_table_info(?1)
               ORDER BY "column_name""#
                .to_string(),
            values![table],
        )
    }

    /// Implements [DbKind::primary_keys_sql()] for SQLiteKind.
    fn primary_keys_sql(&self, table: &str) -> (String, [Value; 1]) {
        (
            r#"SELECT "name" AS "column_name"
               FROM pragma_table_info(?1)
               WHERE "pk" > 0
               ORDER BY "pk""#
                .to_string(),
            values![table],
        )
    }

    /// TODO: Add doctring.
    fn param_prefix(&self) -> &str {
        "?"
    }
}
