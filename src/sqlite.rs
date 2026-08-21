//! SQLite syntax

use crate::{Error, Syntax, Value, ValueType, values};

/// The [maximum number of parameters](https://www.sqlite.org/limits.html#max_variable_number)
/// that can be bound to a SQLite query
pub static MAX_PARAMS_SQLITE: usize = 32766;

/// The identifying name of [this syntax](SqliteSyntax).
pub static SQLITE_SYNTAX_NAME: &str = "sqlite";

#[derive(Debug)]
pub struct SqliteSyntax;

impl Syntax for SqliteSyntax {
    /// Implements [Syntax::name()] for SQLite.
    fn name(&self) -> &str {
        SQLITE_SYNTAX_NAME
    }

    /// Implements [Syntax::sql_type()] for SQLite.
    fn sql_type(&self, name: &str) -> Result<ValueType, Error> {
        match name.to_uppercase().as_str() {
            "TEXT" => Ok(ValueType::Text(name.to_string())),
            _ => Err(Error::DatatypeError(format!(
                "Unrecoganized type name: {name}"
            ))),
        }
    }

    /// Implements [Syntax::columns_sql()] for SQLite.
    fn columns_sql(&self, table: &str) -> (String, [Value; 1]) {
        (
            r#"SELECT "name" AS "column_name", "type" AS "data_type"
               FROM pragma_table_info(?1)
               ORDER BY "column_name""#
                .to_string(),
            values![table],
        )
    }

    /// Implements [Syntax::primary_keys_sql()] for SQLiteKind.
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

    /// Implements [Syntax::param_prefix()] for SQLite.
    fn param_prefix(&self) -> &str {
        "?"
    }
}
