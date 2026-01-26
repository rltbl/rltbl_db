use crate::{
    core::{DbError, ParamValue},
    params,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

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

    /// Generate the SQL needed to create the cache table.
    pub fn create_cache_table_sql(&self) -> String {
        // The generated SQL is currently identical for SQLite and PostgreSQL but they could
        // come apart in the future.
        match self {
            DbKind::SQLite | DbKind::PostgreSQL => {
                let sql = r#"CREATE TABLE IF NOT EXISTS "cache" (
                                "tables" TEXT,
                                "statement" TEXT,
                                "parameters" TEXT,
                                "value" TEXT,
                                PRIMARY KEY ("tables", "statement", "parameters")
                             )"#
                .to_string();
                sql
            }
        }
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

    /// Generate the SQL statements needed to create the caching triggers for the given table.
    pub fn create_caching_triggers_sql(&self, table: &str) -> Vec<String> {
        match self {
            DbKind::SQLite => {
                vec![
                    format!(r#"DROP TRIGGER IF EXISTS "{table}_cache_after_insert""#),
                    format!(
                        r#"CREATE TRIGGER "{table}_cache_after_insert"
                           AFTER INSERT ON "{table}"
                           BEGIN
                               DELETE FROM "cache" WHERE "tables" LIKE '%{table}%';
                           END"#
                    ),
                    format!(r#"DROP TRIGGER IF EXISTS "{table}_cache_after_update""#),
                    format!(
                        r#"CREATE TRIGGER "{table}_cache_after_update"
                           AFTER UPDATE ON "{table}"
                           BEGIN
                             DELETE FROM "cache" WHERE "tables" LIKE '%{table}%';
                           END"#
                    ),
                    format!(r#"DROP TRIGGER IF EXISTS "{table}_cache_after_delete""#),
                    format!(
                        r#"CREATE TRIGGER "{table}_cache_after_delete"
                           AFTER DELETE ON "{table}"
                           BEGIN
                             DELETE FROM "cache" WHERE "tables" LIKE '%{table}%';
                           END"#
                    ),
                ]
            }
            DbKind::PostgreSQL => {
                vec![
                    format!(
                        r#"CREATE OR REPLACE FUNCTION "clean_cache_for_{table}"()
                               RETURNS TRIGGER
                               LANGUAGE PLPGSQL
                               AS
                               $$
                               BEGIN
                                   DELETE FROM "cache" WHERE "tables" LIKE '%{table}%';
                                   RETURN NEW;
                               END;
                               $$"#
                    ),
                    format!(r#"DROP TRIGGER IF EXISTS "{table}_cache_after_insert" ON "{table}""#),
                    format!(
                        r#"CREATE TRIGGER "{table}_cache_after_insert"
                               AFTER INSERT ON "{table}"
                               EXECUTE FUNCTION "clean_cache_for_{table}"()"#
                    ),
                    format!(r#"DROP TRIGGER IF EXISTS "{table}_cache_after_update" ON "{table}""#),
                    format!(
                        r#"CREATE TRIGGER "{table}_cache_after_update"
                               AFTER UPDATE ON "{table}"
                               EXECUTE FUNCTION "clean_cache_for_{table}"()"#
                    ),
                    format!(r#"DROP TRIGGER IF EXISTS "{table}_cache_after_delete" ON "{table}""#),
                    format!(
                        r#"CREATE TRIGGER "{table}_cache_after_delete"
                               AFTER DELETE ON "{table}"
                               EXECUTE FUNCTION "clean_cache_for_{table}"()"#
                    ),
                ]
            }
        }
    }
}
