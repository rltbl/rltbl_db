//! SQLite syntax

use indexmap::IndexMap;

use crate::{Column, Error, Syntax, Value, ValueType, values};

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
        let name = name.to_uppercase();
        match name.as_str() {
            "INTEGER" | "INT" | "TINYINT" | "SMALLINT" | "MEDIUMINT" | "BIGINT" | "INT2"
            | "INT4" | "INT8" | "BOOLEAN" | "BOOL" => Ok(ValueType::BigInteger(name.to_string())),
            "REAL" | "DOUBLE PRECISION" | "DOUBLE" | "FLOAT" => {
                Ok(ValueType::BigReal(name.to_string()))
            }
            "NUMERIC" => Ok(ValueType::Numeric(name.to_string())),
            "TEXT" | "clob" => Ok(ValueType::Text(name.to_string())),
            other if other.starts_with("DECIMAL") => Ok(ValueType::Numeric(name.to_string())),
            other
                if ["CHARACTER", "VARCHAR", "NCHAR", "NVARCHAR"]
                    .iter()
                    .any(|other_type| other.starts_with(other_type)) =>
            {
                Ok(ValueType::Text(name.to_string()))
            }
            other => Err(Error::InputError(format!(
                "Invalid or unsupported SQLite type: {other}"
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

    /// TODO: Add docstring here.
    fn get_epoch_time_sql(&self) -> &str {
        "strftime('%s', 'now')"
    }

    /// TODO: Add docstring
    fn which_are_tables_sql(&self, objects: &[&str]) -> (String, Vec<Value>) {
        let prefix = self.param_prefix().to_string();
        let mut placeholders = vec![];
        let mut parameters = vec![];
        for (i, object) in objects.iter().enumerate() {
            let i = i + 1;
            placeholders.push(format!("{prefix}{i}"));
            parameters.push(Value::from(object.to_string()));
        }
        let placeholders = placeholders.join(",");
        (
            format!(
                r#"SELECT "name" AS "table_name" FROM "sqlite_master"
                   WHERE "type" = 'table' AND "name" IN ({placeholders})"#,
            ),
            parameters.clone(),
        )
    }

    /// TODO: Add docstring
    fn which_are_views_sql(&self, objects: &[&str]) -> (String, Vec<Value>) {
        let prefix = self.param_prefix().to_string();
        let mut placeholders = vec![];
        let mut parameters = vec![];
        for (i, object) in objects.iter().enumerate() {
            let i = i + 1;
            placeholders.push(format!("{prefix}{i}"));
            parameters.push(Value::from(object.to_string()));
        }
        let placeholders = placeholders.join(",");
        (
            format!(
                r#"SELECT "name" AS "view_name" FROM "sqlite_master"
                   WHERE "type" = 'view' AND "name" IN ({placeholders})"#,
            ),
            parameters.clone(),
        )
    }

    fn create_table_sql(
        &self,
        table: &str,
        columns: &IndexMap<String, Column>,
    ) -> Result<String, Error> {
        let column_clauses = columns.iter().map(|(column_name, column)| {
            let mut clause = format!(r#""{column_name}""#);
            match column.sql_type {
                ValueType::Null(_) => {
                    return Err(Error::InputError(format!(
                        "Can't use a NULL column to create table '{table}'."
                    )));
                }
                ValueType::Boolean(_) => clause.push_str(" BOOL"),
                ValueType::SmallInteger(_) | ValueType::Integer(_) | ValueType::BigInteger(_) => {
                    clause.push_str(" INTEGER")
                }
                ValueType::Real(_) | ValueType::BigReal(_) => clause.push_str(" REAL"),
                ValueType::Numeric(_) => clause.push_str(" NUMERIC"),
                ValueType::Text(_) => clause.push_str(" TEXT"),
            };
            if column.unique {
                clause.push_str(" UNIQUE");
            }
            if column.not_null {
                clause.push_str(" NOT NULL");
            }
            Ok(clause)
        });
        let sql = format!(
            r#"CREATE TABLE "{table}" ({})"#,
            column_clauses
                .into_iter()
                .collect::<Result<Vec<String>, Error>>()?
                .join(", ")
        );
        Ok(sql)
    }

    fn view_sql_sql(&self, view: &str) -> (String, [Value; 1]) {
        (
            r#"SELECT "sql" FROM "sqlite_master"
               WHERE "type" = 'view' AND "name" = ?1"#
                .to_string(),
            values![view],
        )
    }

    /// TODO: Add docstring.
    fn wrap_trigger_content(
        &self,
        table: &str,
        trigger_basename: &str,
        trigger_content: &str,
    ) -> Result<Vec<String>, Error> {
        let ddl = vec![
            format!(r#"DROP TRIGGER IF EXISTS "{trigger_basename}_after_insert""#),
            format!(
                r#"CREATE TRIGGER "{trigger_basename}_after_insert"
                   AFTER INSERT ON "{table}"
                   BEGIN
                     {trigger_content}
                   END"#
            ),
            format!(r#"DROP TRIGGER IF EXISTS "{trigger_basename}_after_update""#),
            format!(
                r#"CREATE TRIGGER "{trigger_basename}_after_update"
                   AFTER UPDATE ON "{table}"
                   BEGIN
                     {trigger_content}
                   END"#
            ),
            format!(r#"DROP TRIGGER IF EXISTS "{trigger_basename}_after_delete""#),
            format!(
                r#"CREATE TRIGGER "{trigger_basename}_after_delete"
                   AFTER DELETE ON "{table}"
                   BEGIN
                     {trigger_content}
                   END"#
            ),
        ];
        Ok(ddl)
    }
}
