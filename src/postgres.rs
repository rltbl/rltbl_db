//! Postgres Syntax

use crate::{Error, Syntax, Value, ValueType, values};

/// The [maximum number of parameters](https://www.postgresql.org/docs/current/limits.html)
/// that can be bound to a Postgres query is 65535. This has been true since at least PostgreSQL
/// version 12. However, for some (unknown) reason, tokio-postgres limits the actual number of
/// parameters to just under half that number.
pub static MAX_PARAMS_POSTGRES: usize = 32765;

/// The identifying name of [this syntax](PostgresSyntax).
pub static POSTGRES_SYNTAX_NAME: &str = "postgres";

#[derive(Debug)]
pub struct PostgresSyntax;

impl Syntax for PostgresSyntax {
    /// Implements [Syntax::name()] for [PostgresSyntax]. Returns [POSTGRES_SYNTAX_NAME].
    fn name(&self) -> &str {
        POSTGRES_SYNTAX_NAME
    }

    /// Implements [Syntax::sql_type()] for [PostgresSyntax]
    fn sql_type(&self, name: &str) -> Result<ValueType, Error> {
        match name.to_uppercase().as_str() {
            "TEXT" => Ok(ValueType::Text(name.to_string())),
            // TODO: Add more.
            _ => Err(Error::DatatypeError(format!(
                "Unrecoganized type name: {name}"
            ))),
        }
    }

    /// Implements [Syntax::columns_sql()] for [PostgresSyntax]
    fn columns_sql(&self, table: &str) -> (String, [Value; 1]) {
        (
            r#"SELECT
                 "columns"."column_name"::TEXT,
                 "columns"."data_type"::TEXT
               FROM
                 "information_schema"."columns" "columns"
               WHERE
                 "columns"."table_schema" IN (
                   SELECT REGEXP_SPLIT_TO_TABLE("setting", ', ')
                   FROM "pg_settings"
                   WHERE "name" = 'search_path'
                 )
                 AND "columns"."table_name" = $1
               ORDER BY "columns"."ordinal_position""#
                .to_string(),
            values![table],
        )
    }

    /// Implements [Syntax::primary_keys_sql()] for [PostgresSyntax].
    fn primary_keys_sql(&self, table: &str) -> (String, [Value; 1]) {
        (
            r#"SELECT "kcu"."column_name"
               FROM "information_schema"."table_constraints" "tco"
               JOIN "information_schema"."key_column_usage" "kcu"
                 ON "kcu"."constraint_name" = "tco"."constraint_name"
                AND "kcu"."constraint_schema" = "tco"."constraint_schema"
                AND "kcu"."table_name" = $1
                AND "tco"."constraint_type" ILIKE 'primary key'
               WHERE "kcu"."table_schema" IN (
                 SELECT REGEXP_SPLIT_TO_TABLE("setting", ', ')
                 FROM "pg_settings"
                 WHERE "name" = 'search_path'
              )
              ORDER by "kcu"."ordinal_position""#
                .to_string(),
            values![table],
        )
    }

    /// Implements [Syntax::param_prefix()] for [PostgresSyntax]
    fn param_prefix(&self) -> &str {
        "$"
    }

    /// TODO: Add docstring here.
    fn get_epoch_time_sql(&self) -> &str {
        "extract(epoch from now())"
    }

    /// TODO: Add docstring.
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
                r#"SELECT "table_name"
                   FROM "information_schema"."tables"
                   WHERE "table_type" LIKE '%TABLE'
                   AND "table_name" IN ({placeholders})
                   AND "table_schema" IN (
                     SELECT REGEXP_SPLIT_TO_TABLE("setting", ', ')
                     FROM "pg_settings"
                     WHERE "name" = 'search_path'
                   )"#
            ),
            parameters.clone(),
        )
    }

    /// TODO: Add docstring.
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
                r#"SELECT "table_name" AS "view_name"
                   FROM "information_schema"."tables"
                   WHERE "table_type" LIKE '%VIEW'
                   AND "table_name" IN ({placeholders})
                   AND "table_schema" IN (
                     SELECT REGEXP_SPLIT_TO_TABLE("setting", ', ')
                     FROM "pg_settings"
                     WHERE "name" = 'search_path'
                   )"#
            ),
            parameters.clone(),
        )
    }

    /// Implements [Kind::view_sql_sql()] for PostgreSQLKind.
    fn view_sql_sql(&self, view: &str) -> (String, [Value; 1]) {
        (
            format!(
                r#"SELECT 'CREATE VIEW "{view}" AS '||"definition" AS "sql"
                   FROM "pg_views"
                   WHERE "viewname" = $1
                   AND "schemaname" IN (
                     SELECT REGEXP_SPLIT_TO_TABLE("setting", ', ')
                     FROM "pg_settings"
                     WHERE "name" = 'search_path'
                   )"#
            ),
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
        let function_name = format!("clean_{trigger_basename}");
        let ddl = vec![
            format!(
                r#"CREATE OR REPLACE FUNCTION "{function_name}"()
                   RETURNS TRIGGER
                   LANGUAGE PLPGSQL
                   AS
                   $$
                   BEGIN
                       {trigger_content}
                       RETURN NEW;
                   END;
                   $$"#
            ),
            format!(r#"DROP TRIGGER IF EXISTS "{trigger_basename}_after_insert" ON "{table}""#),
            format!(
                r#"CREATE TRIGGER "{trigger_basename}_after_insert"
                   AFTER INSERT ON "{table}"
                   EXECUTE FUNCTION "{function_name}"()"#
            ),
            format!(r#"DROP TRIGGER IF EXISTS "{trigger_basename}_after_update" ON "{table}""#),
            format!(
                r#"CREATE TRIGGER "{trigger_basename}_after_update"
                   AFTER UPDATE ON "{table}"
                   EXECUTE FUNCTION "{function_name}"()"#
            ),
            format!(r#"DROP TRIGGER IF EXISTS "{trigger_basename}_after_delete" ON "{table}""#),
            format!(
                r#"CREATE TRIGGER "{trigger_basename}_after_delete"
                   AFTER DELETE ON "{table}"
                   EXECUTE FUNCTION "{function_name}"()"#
            ),
        ];
        Ok(ddl)
    }
}
