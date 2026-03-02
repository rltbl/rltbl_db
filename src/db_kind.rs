use crate::{
    core::{DbError, ParamValue, QUERY_CACHE_TABLE, TABLE_CACHE_TABLE, validate_table_name},
    params,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// The [maximum number of parameters](https://www.sqlite.org/limits.html#max_variable_number)
/// that can be bound to a SQLite query
pub static MAX_PARAMS_SQLITE: usize = 32766;

/// The [maximum number of parameters](https://www.postgresql.org/docs/current/limits.html)
/// that can be bound to a Postgres query is 65535. This has been true since at least PostgreSQL
/// version 12. However, for some (unknown) reason, tokio-postgres limits the actual number of
/// parameters to just under half that number.
pub static MAX_PARAMS_POSTGRES: usize = 32765;

/// Defines the supported database kinds.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum DbKind {
    SQLite,
    PostgreSQL,
}

impl Display for DbKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbKind::SQLite => write!(f, "SQLite"),
            DbKind::PostgreSQL => write!(f, "PostgreSQL"),
        }
    }
}

impl DbKind {
    // Although SQLite allows '$' as a prefix, it is required to use '?' to represent integer
    // literals (see https://sqlite.org/c3ref/bind_blob.html) which is what we are using here.
    /// Get the prefix to use for parameters to queries that need to be bound.
    pub fn param_prefix(&self) -> &str {
        match self {
            DbKind::SQLite => "?",
            DbKind::PostgreSQL => "$",
        }
    }

    /// Given a SQL type for this database and a string, parse the string into the right
    /// ParamValue.
    pub fn parse(&self, sql_type: &str, value: &str) -> Result<ParamValue, DbError> {
        fn parse_sqlite(sql_type: &str, value: &str) -> Result<ParamValue, DbError> {
            let err = || {
                Err(DbError::ParseError(format!(
                    "Could not parse '{sql_type}' from '{value}'"
                )))
            };
            match sql_type.to_lowercase().as_str() {
                "text" => Ok(ParamValue::Text(value.to_string())),
                "bool" => match value.to_lowercase().as_str() {
                    "true" | "1" => Ok(ParamValue::Boolean(true)),
                    "false" | "0" => Ok(ParamValue::Boolean(false)),
                    _ => err(),
                },
                "int" | "integer" | "int8" | "bigint" => match value.parse::<i64>() {
                    Ok(int) => Ok(ParamValue::BigInteger(int)),
                    Err(_) => err(),
                },
                // NOTE: We are treating NUMERIC as an f64 here and for tokio-postgres.
                "real" | "numeric" => match value.parse::<f64>() {
                    Ok(float) => Ok(ParamValue::BigReal(float)),
                    Err(_) => err(),
                },
                _ => Err(DbError::DatatypeError(format!(
                    "Unhandled SQL type: {sql_type}"
                ))),
            }
        }

        fn parse_postgresql(sql_type: &str, value: &str) -> Result<ParamValue, DbError> {
            let err = || {
                Err(DbError::ParseError(format!(
                    "Could not parse '{sql_type}' from '{value}'"
                )))
            };
            match sql_type.to_lowercase().as_str() {
                "text" => Ok(ParamValue::Text(value.to_string())),
                "bool" | "boolean" => match value.to_lowercase().as_str() {
                    // TODO: improve this
                    "true" | "1" => Ok(ParamValue::Boolean(true)),
                    _ => Ok(ParamValue::Boolean(false)),
                },
                "smallint" | "smallinteger" => match value.parse::<i16>() {
                    Ok(int) => Ok(ParamValue::SmallInteger(int)),
                    Err(_) => err(),
                },
                "int" | "integer" => match value.parse::<i32>() {
                    Ok(int) => Ok(ParamValue::Integer(int)),
                    Err(_) => err(),
                },
                "bigint" | "biginteger" => match value.parse::<i64>() {
                    Ok(int) => Ok(ParamValue::BigInteger(int)),
                    Err(_) => err(),
                },
                "real" => match value.parse::<f32>() {
                    Ok(float) => Ok(ParamValue::Real(float)),
                    Err(_) => err(),
                },
                "bigreal" => match value.parse::<f64>() {
                    Ok(float) => Ok(ParamValue::BigReal(float)),
                    Err(_) => err(),
                },
                // WARN: Treat NUMERIC as an f64.
                "numeric" => match value.parse::<f64>() {
                    Ok(float) => Ok(ParamValue::Numeric(
                        Decimal::from_f64_retain(float).unwrap_or_default(),
                    )),
                    Err(_) => err(),
                },
                _ => Err(DbError::DatatypeError(format!(
                    "Unhandled SQL type: {sql_type}"
                ))),
            }
        }

        match self {
            DbKind::SQLite => parse_sqlite(sql_type, value),
            DbKind::PostgreSQL => parse_postgresql(sql_type, value),
        }
    }

    /// Generate the SQL and parameters needed to query the database's metadata for names and
    /// types of the columns of the given table.
    pub fn columns_sql(&self, table: &str) -> (String, [ParamValue; 1]) {
        match self {
            DbKind::SQLite => (
                r#"SELECT "name" AS "column_name", "type" AS "data_type"
                   FROM pragma_table_info(?1)
                   ORDER BY "column_name""#
                    .to_string(),
                params![table],
            ),
            DbKind::PostgreSQL => (
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
                params![table],
            ),
        }
    }

    /// Generate the SQL and parameters needed to query the database's metadata for the primary
    /// key columns of the given table.
    pub fn primary_keys_sql(&self, table: &str) -> (String, [ParamValue; 1]) {
        match self {
            DbKind::SQLite => (
                r#"SELECT "name" AS "column_name"
                   FROM pragma_table_info(?1)
                   WHERE "pk" > 0
                   ORDER BY "pk""#
                    .to_string(),
                params![table],
            ),
            DbKind::PostgreSQL => (
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
                params![table],
            ),
        }
    }

    /// Generate the SQL and parameters needed to determine whether the given table exists.
    pub fn table_exists_sql(self, table: &str) -> (String, [ParamValue; 1]) {
        match self {
            DbKind::SQLite => (
                r#"SELECT 1 FROM "sqlite_master"
                   WHERE "type" = 'table' AND "name" = ?1"#
                    .to_string(),
                params![table],
            ),
            DbKind::PostgreSQL => (
                r#"SELECT 1
                   FROM "information_schema"."tables"
                   WHERE "table_type" LIKE '%TABLE'
                   AND "table_name" = $1
                   AND "table_schema" IN (
                     SELECT REGEXP_SPLIT_TO_TABLE("setting", ', ')
                     FROM "pg_settings"
                     WHERE "name" = 'search_path'
                   )"#
                .to_string(),
                params![table],
            ),
        }
    }

    /// Generate the SQL and parameters needed to determine whether the given view exists.
    pub fn view_exists_sql(self, view: &str) -> (String, [ParamValue; 1]) {
        match self {
            DbKind::SQLite => (
                r#"SELECT 1 FROM "sqlite_master"
                   WHERE "type" = 'view' AND "name" = ?1"#
                    .to_string(),
                params![view],
            ),
            DbKind::PostgreSQL => (
                r#"SELECT 1
                   FROM "information_schema"."tables"
                   WHERE "table_type" LIKE '%VIEW'
                   AND "table_name" = $1
                   AND "table_schema" IN (
                     SELECT REGEXP_SPLIT_TO_TABLE("setting", ', ')
                     FROM "pg_settings"
                     WHERE "name" = 'search_path'
                   )"#
                .to_string(),
                params![view],
            ),
        }
    }

    /// Generate the SQL and parameters needed to retrieve the underlying SQL code for the
    /// given view.
    pub fn view_sql_sql(self, view: &str) -> (String, [ParamValue; 1]) {
        match self {
            DbKind::SQLite => (
                r#"SELECT "sql" FROM "sqlite_master"
                   WHERE "type" = 'view' AND "name" = ?1"#
                    .to_string(),
                params![view],
            ),
            DbKind::PostgreSQL => (
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
                params![view],
            ),
        }
    }

    /// Generate the SQL needed to create the cache table.
    pub fn create_query_cache_table_sql(&self) -> String {
        // TODO: Move this to a separate function:
        let get_epoch_now = match self {
            DbKind::SQLite => "(strftime('%s', 'now'))",
            DbKind::PostgreSQL => "extract(epoch from now())",
        };
        format!(
            r#"CREATE TABLE IF NOT EXISTS "{QUERY_CACHE_TABLE}" (
                 "tables" TEXT,
                 "statement" TEXT,
                 "parameters" TEXT,
                 "value" TEXT,
                 "last_accessed" BIGINT DEFAULT {get_epoch_now},
                 PRIMARY KEY ("tables", "statement", "parameters")
             )"#
        )
    }

    /// Generate the SQL needed to create a table table, which is needed for caching.
    pub fn create_table_cache_table_sql(&self) -> String {
        // TODO: Move this to a separate function:
        let get_epoch_now = match self {
            DbKind::SQLite => "(strftime('%s', 'now'))",
            DbKind::PostgreSQL => "extract(epoch from now())",
        };
        format!(
            r#"CREATE TABLE IF NOT EXISTS "{TABLE_CACHE_TABLE}" (
                 "table" TEXT PRIMARY KEY,
                 "last_modified" BIGINT DEFAULT {get_epoch_now}
               )"#
        )
    }

    /// Generate the SQL needed to verify whether the caching triggers for the given table exist.
    pub fn triggers_exist_sql(&self, table: &str) -> (String, [ParamValue; 3]) {
        let sql = match self {
            DbKind::SQLite => {
                let sql = r#"SELECT (COUNT(1) = 3) AS triggers_exist
                             FROM sqlite_master
                             WHERE type = 'trigger'
                               AND name IN (?1, ?2, ?3)"#;
                sql.to_string()
            }
            DbKind::PostgreSQL => {
                let sql = r#"SELECT (COUNT(1) = 3) AS triggers_exist
                             FROM information_schema.triggers
                             WHERE trigger_name IN ($1, $2, $3)
                             AND "trigger_schema" IN (
                               SELECT REGEXP_SPLIT_TO_TABLE("setting", ', ')
                               FROM "pg_settings"
                               WHERE "name" = 'search_path'
                             )"#;
                sql.to_string()
            }
        };
        (
            sql,
            params![
                format!("{table}_cache_after_insert"),
                format!("{table}_cache_after_update"),
                format!("{table}_cache_after_delete"),
            ],
        )
    }

    /// TODO: Add docstring
    pub fn create_table_caching_triggers_for_view_sql(
        &self,
        table: &str,
        view: &str,
    ) -> Result<Vec<String>, DbError> {
        let table = validate_table_name(table)?;
        let view = validate_table_name(view)?;

        // TODO: Move this to a separate function:
        let get_epoch_now = match self {
            DbKind::SQLite => "(strftime('%s', 'now'))",
            DbKind::PostgreSQL => "extract(epoch from now())",
        };

        let trigger_basename = format!("{table}_{view}");
        let function_name = format!("clean_{table}_{view}");
        let trigger_content = format!(
            r#"INSERT INTO "{TABLE_CACHE_TABLE}"
               ("table", "last_modified")
               VALUES ('{table}', {get_epoch_now})
               ON CONFLICT ("table")
                 DO UPDATE SET "last_modified" = {get_epoch_now};
               DELETE FROM "{QUERY_CACHE_TABLE}"
               WHERE "tables" LIKE '%{view}%'
               AND EXISTS (
                 SELECT 1
                 FROM "{TABLE_CACHE_TABLE}" t
                 WHERE t."table" = '{table}'
                   AND t."last_modified" >= "{QUERY_CACHE_TABLE}"."last_accessed"
               );"#
        );
        self.wrap_trigger_content(&table, &trigger_basename, &function_name, &trigger_content)
    }

    /// Generate the SQL statements needed to create the caching triggers for the given table.
    pub fn create_table_caching_triggers_for_table_sql(
        &self,
        table: &str,
    ) -> Result<Vec<String>, DbError> {
        let table = validate_table_name(table)?;

        // TODO: Move this to a separate function:
        let get_epoch_now = match self {
            DbKind::SQLite => "(strftime('%s', 'now'))",
            DbKind::PostgreSQL => "extract(epoch from now())",
        };
        let trigger_basename = format!("{table}");
        let function_name = format!("clean_{table}");
        let trigger_content = format!(
            r#"INSERT INTO "{TABLE_CACHE_TABLE}"
               ("table", "last_modified")
               VALUES ('{table}', {get_epoch_now})
               ON CONFLICT ("table")
                 DO UPDATE SET "last_modified" = {get_epoch_now};
               DELETE FROM "{QUERY_CACHE_TABLE}"
               WHERE "tables" LIKE '%{table}%';"#
        );
        self.wrap_trigger_content(&table, &trigger_basename, &function_name, &trigger_content)
    }

    /// TODO: Add docstring
    fn wrap_trigger_content(
        &self,
        table: &str,
        trigger_basename: &str,
        function_name: &str,
        trigger_content: &str,
    ) -> Result<Vec<String>, DbError> {
        match self {
            DbKind::SQLite => {
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
            DbKind::PostgreSQL => {
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
                    format!(
                        r#"DROP TRIGGER IF EXISTS "{trigger_basename}_after_insert" ON "{table}""#
                    ),
                    format!(
                        r#"CREATE TRIGGER "{trigger_basename}_after_insert"
                               AFTER INSERT ON "{table}"
                               EXECUTE FUNCTION "{function_name}"()"#
                    ),
                    format!(
                        r#"DROP TRIGGER IF EXISTS "{trigger_basename}_after_update" ON "{table}""#
                    ),
                    format!(
                        r#"CREATE TRIGGER "{trigger_basename}_after_update"
                               AFTER UPDATE ON "{table}"
                               EXECUTE FUNCTION "{function_name}"()"#
                    ),
                    format!(
                        r#"DROP TRIGGER IF EXISTS "{trigger_basename}_after_delete" ON "{table}""#
                    ),
                    format!(
                        r#"CREATE TRIGGER "{trigger_basename}_after_delete"
                               AFTER DELETE ON "{table}"
                               EXECUTE FUNCTION "{function_name}"()"#
                    ),
                ];
                Ok(ddl)
            }
        }
    }
}
