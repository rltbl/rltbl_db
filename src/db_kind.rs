//! Code specific to supported database kinds.

use crate::{
    core::{DbError, QUERY_CACHE_TABLE, TABLE_CACHE_TABLE},
    db_value::{DbValue, IntoDbValue, JsonValue},
    params,
    parse::validate_table_name,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, str::FromStr};

/// The [maximum number of parameters](https://www.sqlite.org/limits.html#max_variable_number)
/// that can be bound to a SQLite query
pub static MAX_PARAMS_SQLITE: usize = 32766;

/// The [maximum number of parameters](https://www.postgresql.org/docs/current/limits.html)
/// that can be bound to a Postgres query is 65535. This has been true since at least PostgreSQL
/// version 12. However, for some (unknown) reason, tokio-postgres limits the actual number of
/// parameters to just under half that number.
pub static MAX_PARAMS_POSTGRES: usize = 32765;

//////////////////////////////////////////////////////////////////////
// Database kinds
//////////////////////////////////////////////////////////////////////

/// Defines the supported database kinds.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum DbKind {
    SQLite,
    PostgreSQL,
}

impl FromStr for DbKind {
    type Err = DbError;

    fn from_str(kind_name: &str) -> Result<Self, Self::Err> {
        match kind_name.trim().to_lowercase().as_str() {
            "sqlite" => Ok(DbKind::SQLite),
            "postgresql" | "postgres" => Ok(DbKind::PostgreSQL),
            invalid => Err(DbError::InputError(format!("Invalid kind name: {invalid}"))),
        }
    }
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
    /// Constructs a [DbType] instance using the name of the given sql_type.
    pub fn db_type(&self, sql_type: &str) -> Result<DbType, DbError> {
        match self {
            DbKind::SQLite => match sql_type.to_lowercase().as_str() {
                "integer" | "int" | "tinyint" | "smallint" | "mediumint" | "bigint" | "int2"
                | "int4" | "int8" | "boolean" | "bool" => {
                    Ok(DbType::BigInteger(sql_type.to_string()))
                }
                "real" | "double precision" | "double" | "float" => {
                    Ok(DbType::BigReal(sql_type.to_string()))
                }
                "numeric" => Ok(DbType::Numeric(sql_type.to_string())),
                "text" | "clob" => Ok(DbType::Text(sql_type.to_string())),
                other if other.starts_with("decimal") => Ok(DbType::Numeric(sql_type.to_string())),
                other
                    if ["character", "varchar", "nchar", "nvarchar"]
                        .iter()
                        .any(|other_type| other.starts_with(other_type)) =>
                {
                    Ok(DbType::Text(sql_type.to_string()))
                }
                other => Err(DbError::InputError(format!(
                    "Invalid or unsupported SQLite type: {other}"
                ))),
            },
            DbKind::PostgreSQL => match sql_type.to_lowercase().as_str() {
                "bool" | "boolean" => Ok(DbType::Boolean(sql_type.to_string())),
                "smallint" | "int2" | "smallserial" => {
                    Ok(DbType::SmallInteger(sql_type.to_string()))
                }
                "integer" | "int4" | "serial" => Ok(DbType::Integer(sql_type.to_string())),
                "bigint" | "int8" | "bigserial" => Ok(DbType::BigInteger(sql_type.to_string())),
                "decimal" | "numeric" => Ok(DbType::Numeric(sql_type.to_string())),
                "real" | "float" | "float4" => Ok(DbType::Real(sql_type.to_string())),
                "double precision" | "float8" => Ok(DbType::BigReal(sql_type.to_string())),
                "text" | "bpchar" => Ok(DbType::Text(sql_type.to_string())),
                other if other.starts_with("decimal") => Ok(DbType::Numeric(sql_type.to_string())),
                other
                    if ["character", "varchar", "char", "bpchar"]
                        .iter()
                        .any(|other_type| other.starts_with(other_type)) =>
                {
                    Ok(DbType::Text(sql_type.to_string()))
                }
                other => Err(DbError::InputError(format!(
                    "Invalid or unsupported PostgreSQL type: {other}"
                ))),
            },
        }
    }

    /// Get the prefix to use for parameters to queries that need to be bound.
    pub fn param_prefix(&self) -> &str {
        // Although SQLite allows '$' as a prefix, it is required to use '?' to represent integer
        // literals (see https://sqlite.org/c3ref/bind_blob.html) which is what we are using here.
        match self {
            DbKind::SQLite => "?",
            DbKind::PostgreSQL => "$",
        }
    }

    /// Get the code needed to retrieve the current epoch time from the database.
    pub fn get_epoch_time_sql(&self) -> &str {
        match self {
            DbKind::SQLite => "strftime('%s', 'now')",
            DbKind::PostgreSQL => "extract(epoch from now())",
        }
    }

    /// Generate the SQL and parameters needed to query the database's metadata for the names and
    /// types of the columns of the given table.
    pub fn columns_sql(&self, table: &str) -> (String, [DbValue; 1]) {
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
    pub fn primary_keys_sql(&self, table: &str) -> (String, [DbValue; 1]) {
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

    /// Generate the SQL and parameters needed to determine which of the given list of
    /// database names correspond to views in the database.
    pub fn which_are_views_sql(self, objects: &[&str]) -> (String, Vec<DbValue>) {
        let prefix = self.param_prefix().to_string();
        let mut placeholders = vec![];
        let mut parameters = vec![];
        for (i, object) in objects.iter().enumerate() {
            let i = i + 1;
            placeholders.push(format!("{prefix}{i}"));
            parameters.push(DbValue::from(object.to_string()));
        }
        let placeholders = placeholders.join(",");
        let (sql, params) = match self {
            DbKind::SQLite => (
                format!(
                    r#"SELECT "name" AS "view_name" FROM "sqlite_master"
                       WHERE "type" = 'view' AND "name" IN ({placeholders})"#,
                ),
                parameters.clone(),
            ),
            DbKind::PostgreSQL => (
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
            ),
        };
        (sql, params)
    }

    /// Generate the SQL and parameters needed to determine which of the given list of
    /// database names correspond to tables in the database.
    pub fn which_are_tables_sql(self, objects: &[&str]) -> (String, Vec<DbValue>) {
        let prefix = self.param_prefix().to_string();
        let mut placeholders = vec![];
        let mut parameters = vec![];
        for (i, object) in objects.iter().enumerate() {
            let i = i + 1;
            placeholders.push(format!("{prefix}{i}"));
            parameters.push(DbValue::from(object.to_string()));
        }
        let placeholders = placeholders.join(",");
        let (sql, params) = match self {
            DbKind::SQLite => (
                format!(
                    r#"SELECT "name" AS "table_name" FROM "sqlite_master"
                       WHERE "type" = 'table' AND "name" IN ({placeholders})"#,
                ),
                parameters.clone(),
            ),
            DbKind::PostgreSQL => (
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
            ),
        };
        (sql, params)
    }

    /// Generate the SQL and parameters needed to retrieve the underlying SQL code for the
    /// given view.
    pub fn view_sql_sql(self, view: &str) -> (String, [DbValue; 1]) {
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

    /// Generate the SQL needed to create the query cache.
    pub fn create_query_cache_table_sql(&self) -> String {
        let get_epoch_now = self.get_epoch_time_sql();
        format!(
            r#"CREATE TABLE IF NOT EXISTS "{QUERY_CACHE_TABLE}" (
                 "statement" TEXT,
                 "parameters" TEXT,
                 "tables" TEXT,
                 "value" TEXT,
                 "last_verified" BIGINT DEFAULT ({get_epoch_now}),
                 PRIMARY KEY ("statement", "parameters")
             )"#
        )
    }

    /// Generate the SQL needed to create the table cache.
    pub fn create_table_cache_table_sql(&self) -> String {
        let get_epoch_now = self.get_epoch_time_sql();
        format!(
            r#"CREATE TABLE IF NOT EXISTS "{TABLE_CACHE_TABLE}" (
                 "table" TEXT PRIMARY KEY,
                 "last_modified" BIGINT DEFAULT ({get_epoch_now})
               )"#
        )
    }

    /// Generate the SQL statements needed to create caching triggers for the given table.
    pub fn create_table_caching_triggers_for_table_sql(
        &self,
        table: &str,
    ) -> Result<Vec<String>, DbError> {
        let table = validate_table_name(table)?;
        let trigger_basename = format!("{table}");
        let trigger_content = format!(
            r#"DELETE FROM "{QUERY_CACHE_TABLE}"
               WHERE "tables" LIKE '%"{table}"%';"#
        );
        self.wrap_trigger_content(&table, &trigger_basename, &trigger_content)
    }

    /// Generate the SQL statements needed to create caching triggers for the given table, which
    /// is assumed to be a source table for the given view.
    pub fn create_table_caching_triggers_for_view_sql(
        &self,
        table: &str,
        view: &str,
    ) -> Result<Vec<String>, DbError> {
        let table = validate_table_name(table)?;
        let view = validate_table_name(view)?;
        let get_epoch_now = self.get_epoch_time_sql();
        let trigger_basename = format!("{table}_{view}");
        let trigger_content = format!(
            r#"INSERT INTO "{TABLE_CACHE_TABLE}"
               ("table", "last_modified")
               VALUES ('{table}', {get_epoch_now})
               ON CONFLICT ("table")
                 DO UPDATE SET "last_modified" = {get_epoch_now};
               DELETE FROM "{QUERY_CACHE_TABLE}"
               WHERE "tables" LIKE '%"{view}"%'
               AND EXISTS (
                 SELECT 1
                 FROM "{TABLE_CACHE_TABLE}" t
                 WHERE t."table" = '{table}'
                   AND t."last_modified" >= "{QUERY_CACHE_TABLE}"."last_verified"
               );"#
        );
        self.wrap_trigger_content(&table, &trigger_basename, &trigger_content)
    }

    /// Generate the SQL statements to create a caching function and triggers with the given
    /// trigger content for the given table using the given trigger basename.
    fn wrap_trigger_content(
        &self,
        table: &str,
        trigger_basename: &str,
        trigger_content: &str,
    ) -> Result<Vec<String>, DbError> {
        let function_name = format!("clean_{trigger_basename}");
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

//////////////////////////////////////////////////////////////////////
// Database types
//////////////////////////////////////////////////////////////////////

/// The supported database types, including information about the name
/// used to refer to the type in the underlying database.
#[derive(Clone, Debug, PartialEq)]
pub enum DbType {
    Null(String),
    Boolean(String),
    I16(String),
    SmallInteger(String),
    Integer(String),
    BigInteger(String),
    Real(String),
    BigReal(String),
    Numeric(String),
    Text(String),
}

impl DbType {
    /// Parses a given string representing the value of a database field into a [DbValue] of this
    /// type.
    pub fn parse_str(&self, value: &str) -> Result<DbValue, DbError> {
        match self {
            DbType::Null(_) => Ok(DbValue::Null),
            DbType::Boolean(_) => {
                let value = value
                    .parse::<bool>()
                    .map_err(|_| DbError::InputError(format!("Not a boolean: {value}")))?;
                Ok(DbValue::Boolean(value))
            }
            DbType::I16(_) | DbType::SmallInteger(_) => {
                let value = value
                    .parse::<i16>()
                    .map_err(|_| DbError::InputError(format!("Not an i16: {value}")))?;
                Ok(DbValue::SmallInteger(value))
            }
            DbType::Integer(_) => {
                let value = value
                    .parse::<i32>()
                    .map_err(|_| DbError::InputError(format!("Not an i32: {value}")))?;
                Ok(DbValue::Integer(value))
            }
            DbType::BigInteger(_) => {
                let value = value
                    .parse::<i64>()
                    .map_err(|_| DbError::InputError(format!("Not an i64: {value}")))?;
                Ok(DbValue::BigInteger(value))
            }
            DbType::Real(_) => {
                let value = value
                    .parse::<f32>()
                    .map_err(|_| DbError::InputError(format!("Not an f32: {value}")))?;
                Ok(DbValue::Real(value))
            }
            DbType::BigReal(_) => {
                let value = value
                    .parse::<f64>()
                    .map_err(|_| DbError::InputError(format!("Not an f64: {value}")))?;
                Ok(DbValue::BigReal(value))
            }
            DbType::Numeric(_) => {
                let value = value
                    .parse::<Decimal>()
                    .map_err(|_| DbError::InputError(format!("Not a Decimal: {value}")))?;
                Ok(DbValue::Numeric(value))
            }
            DbType::Text(_) => Ok(DbValue::Text(value.to_string())),
        }
    }

    /// Parses a given [JsonValue] representing the value of a database field into a [DbValue] of
    /// this type.
    pub fn parse_json(&self, value: &JsonValue) -> Result<DbValue, DbError> {
        Ok(DbValue::from(value))
    }

    /// Parses the given value into a [DbValue] of this type.
    pub fn parse(&self, value: impl IntoDbValue) -> Result<DbValue, DbError> {
        let value = value.into_db_value();
        self.convert(&value)
    }

    /// Converts the given [DbValue] into a [DbValue] of this type.
    pub fn convert(&self, value: &DbValue) -> Result<DbValue, DbError> {
        // First handle NULLs and Text types:
        match self {
            DbType::Null(_) => match value {
                DbValue::Null => return Ok(DbValue::Null),
                value => {
                    return Err(DbError::InputError(format!(
                        "Can't convert to {self:?} from {value:?}"
                    )));
                }
            },
            _ => {
                if let DbValue::Text(value) = value {
                    return Ok(self.parse_str(value)?);
                }
            }
        };

        // Then handle everything else:

        let err_template = |db_value: &DbValue| {
            DbError::InputError(format!("Can't convert to {self:?} from {db_value:?}"))
        };

        match self {
            DbType::Null(_) => unreachable!(), // Handled above.
            DbType::Boolean(_) => {
                let value = value.as_bool().ok_or(err_template(value))?;
                Ok(DbValue::Boolean(value))
            }
            DbType::I16(_) => {
                let value = value.as_i16().ok_or(err_template(value))?;
                Ok(DbValue::SmallInteger(value))
            }
            DbType::SmallInteger(_) => {
                let value = value.as_i16().ok_or(err_template(value))?;
                Ok(DbValue::SmallInteger(value))
            }
            DbType::Integer(_) => {
                let value = value.as_i32().ok_or(err_template(value))?;
                Ok(DbValue::Integer(value))
            }
            DbType::BigInteger(_) => {
                let value = value.as_i64().ok_or(err_template(value))?;
                Ok(DbValue::BigInteger(value))
            }
            DbType::Real(_) => {
                let value = value.as_f32().ok_or(err_template(value))?;
                Ok(DbValue::Real(value))
            }
            DbType::BigReal(_) => {
                let value = value.as_f64().ok_or(err_template(value))?;
                Ok(DbValue::BigReal(value))
            }
            DbType::Numeric(_) => {
                let value = value.as_decimal().ok_or(err_template(value))?;
                Ok(DbValue::Numeric(value))
            }
            DbType::Text(_) => Ok(DbValue::Text(value.to_string())),
        }
    }
}
