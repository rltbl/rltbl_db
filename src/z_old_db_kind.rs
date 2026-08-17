//! Code specific to supported database kinds.

use crate::{
    z_old_cache::{QUERY_CACHE_TABLE, TABLE_CACHE_TABLE},
    z_old_core::DbError,
    z_old_db_value::{DbColumn, DbValue, IntoDbValue, JsonValue},
    z_old_params,
    z_old_parse::validate_table_name,
};
use indexmap::IndexMap;
use rust_decimal::{Decimal, dec};
use std::{cmp::Ordering, fmt::Display};

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

/// Trait that must be implemented by supported database kinds.
pub trait DbKind: std::fmt::Debug + Display + Send {
    /// The name of this DbKind.
    fn name(&self) -> String;

    /// Constructs a [DbType] instance using the name of the given sql_type.
    fn db_type(&self, sql_type: &str) -> Result<DbType, DbError>;

    /// Get the prefix to use for parameters to queries that need to be bound.
    fn param_prefix(&self) -> &str;

    /// Get the code needed to retrieve the current epoch time from the database.
    fn get_epoch_time_sql(&self) -> &str;

    /// Generate the SQL and parameters needed to query the database's metadata for the names and
    /// types of the columns of the given table.
    fn columns_sql(&self, table: &str) -> (String, [DbValue; 1]);

    /// Generate the SQL and parameters needed to query the database's metadata for the primary
    /// key columns of the given table.
    fn primary_keys_sql(&self, table: &str) -> (String, [DbValue; 1]);

    /// Generate the SQL needed to create a table with the given name and given column definitions.
    fn create_table_sql(
        &self,
        table: &str,
        columns: &IndexMap<String, DbColumn>,
    ) -> Result<String, DbError>;

    /// Generate the SQL and parameters needed to drop the given table.
    fn drop_table_sql(&self, table: &str) -> String;

    /// Generate the SQL and parameters needed to drop the given view.
    fn drop_view_sql(&self, table: &str) -> String;

    /// Generate the SQL and parameters needed to determine which of the given list of
    /// database names correspond to views in the database.
    fn which_are_views_sql(&self, objects: &[&str]) -> (String, Vec<DbValue>);

    /// Generate the SQL and parameters needed to determine which of the given list of
    /// database names correspond to tables in the database.
    fn which_are_tables_sql(&self, objects: &[&str]) -> (String, Vec<DbValue>);

    /// Generate the SQL and parameters needed to retrieve the underlying SQL code for the
    /// given view.
    fn view_sql_sql(&self, view: &str) -> (String, [DbValue; 1]);

    /// Generate the SQL needed to create the query cache.
    fn create_query_cache_table_sql(&self) -> String {
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
    fn create_table_cache_table_sql(&self) -> String {
        let get_epoch_now = self.get_epoch_time_sql();
        format!(
            r#"CREATE TABLE IF NOT EXISTS "{TABLE_CACHE_TABLE}" (
                 "table" TEXT PRIMARY KEY,
                 "last_modified" BIGINT DEFAULT ({get_epoch_now})
               )"#
        )
    }

    /// Generate the SQL statements needed to create caching triggers for the given table.
    fn create_table_caching_triggers_for_table_sql(
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
    fn create_table_caching_triggers_for_view_sql(
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
    ) -> Result<Vec<String>, DbError>;
}

// Builtin database kind implementations.

#[derive(Debug)]
pub struct SQLiteKind;

#[derive(Debug)]
pub struct PostgreSQLKind;

impl Display for SQLiteKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sqlite")
    }
}

impl Display for PostgreSQLKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "postgresql")
    }
}

impl DbKind for SQLiteKind {
    /// Implements [DbKind::name()] for SQLiteKind.
    fn name(&self) -> String {
        "SQLite".to_string()
    }

    /// Implements [DbKind::db_type()] for SQLiteKind.
    fn db_type(&self, sql_type: &str) -> Result<DbType, DbError> {
        match sql_type.to_lowercase().as_str() {
            "integer" | "int" | "tinyint" | "smallint" | "mediumint" | "bigint" | "int2"
            | "int4" | "int8" | "boolean" | "bool" => Ok(DbType::BigInteger(sql_type.to_string())),
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
        }
    }

    /// Implements [DbKind::param_prefix()] for SQLiteKind.
    fn param_prefix(&self) -> &str {
        "?"
    }

    /// Implements [DbKind::get_epoch_time_sql()] for SQLiteKind.
    fn get_epoch_time_sql(&self) -> &str {
        "strftime('%s', 'now')"
    }

    /// Implements [DbKind::columns_sql()] for SQLiteKind.
    fn columns_sql(&self, table: &str) -> (String, [DbValue; 1]) {
        (
            r#"SELECT "name" AS "column_name", "type" AS "data_type"
               FROM pragma_table_info(?1)
               ORDER BY "column_name""#
                .to_string(),
            z_old_params![table],
        )
    }

    /// Implements [DbKind::primary_keys_sql()] for SQLiteKind.
    fn primary_keys_sql(&self, table: &str) -> (String, [DbValue; 1]) {
        (
            r#"SELECT "name" AS "column_name"
               FROM pragma_table_info(?1)
               WHERE "pk" > 0
               ORDER BY "pk""#
                .to_string(),
            z_old_params![table],
        )
    }

    /// Implements [DbKind::create_table_sql()] for SQLiteKind.
    fn create_table_sql(
        &self,
        table: &str,
        columns: &IndexMap<String, DbColumn>,
    ) -> Result<String, DbError> {
        let column_clauses = columns.iter().map(|(column_name, column)| {
            let mut clause = format!(r#""{column_name}""#);
            match column.db_type {
                DbType::Null(_) => {
                    return Err(DbError::InputError(format!(
                        "Can't use a NULL column to create table '{table}'."
                    )));
                }
                DbType::Boolean(_) => clause.push_str(" BOOL"),
                DbType::I16(_)
                | DbType::SmallInteger(_)
                | DbType::Integer(_)
                | DbType::BigInteger(_) => clause.push_str(" INTEGER"),
                DbType::Real(_) | DbType::BigReal(_) => clause.push_str(" REAL"),
                DbType::Numeric(_) => clause.push_str(" NUMERIC"),
                DbType::Text(_) => clause.push_str(" TEXT"),
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
                .collect::<Result<Vec<String>, DbError>>()?
                .join(", ")
        );
        Ok(sql)
    }

    /// Implements [DbKind::drop_table_sql()] for SQLiteKind.
    fn drop_table_sql(&self, table: &str) -> String {
        format!(r#"DROP TABLE IF EXISTS "{table}""#)
    }

    /// Implements [DbKind::drop_view_sql()] for SQLiteKind.
    fn drop_view_sql(&self, view: &str) -> String {
        format!(r#"DROP VIEW IF EXISTS "{view}""#)
    }

    /// Implements [DbKind::which_are_views_sql()] for SQLiteKind.
    fn which_are_views_sql(&self, objects: &[&str]) -> (String, Vec<DbValue>) {
        let prefix = self.param_prefix().to_string();
        let mut placeholders = vec![];
        let mut parameters = vec![];
        for (i, object) in objects.iter().enumerate() {
            let i = i + 1;
            placeholders.push(format!("{prefix}{i}"));
            parameters.push(DbValue::from(object.to_string()));
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

    /// Implements [DbKind::which_are_tables_sql()] for SQLiteKind.
    fn which_are_tables_sql(&self, objects: &[&str]) -> (String, Vec<DbValue>) {
        let prefix = self.param_prefix().to_string();
        let mut placeholders = vec![];
        let mut parameters = vec![];
        for (i, object) in objects.iter().enumerate() {
            let i = i + 1;
            placeholders.push(format!("{prefix}{i}"));
            parameters.push(DbValue::from(object.to_string()));
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

    /// Implements [DbKind::view_sql_sql()] for SQLiteKind.
    fn view_sql_sql(&self, view: &str) -> (String, [DbValue; 1]) {
        (
            r#"SELECT "sql" FROM "sqlite_master"
               WHERE "type" = 'view' AND "name" = ?1"#
                .to_string(),
            z_old_params![view],
        )
    }

    /// Implements [DbKind::wrap_trigger_content()] for SQLiteKind.
    fn wrap_trigger_content(
        &self,
        table: &str,
        trigger_basename: &str,
        trigger_content: &str,
    ) -> Result<Vec<String>, DbError> {
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

impl DbKind for PostgreSQLKind {
    /// Implements [DbKind::name()] for PostgreSQLKind.
    fn name(&self) -> String {
        "PostgreSQL".to_string()
    }

    /// Implements [DbKind::db_type()] for PostgreSQLKind.
    fn db_type(&self, sql_type: &str) -> Result<DbType, DbError> {
        match sql_type.to_lowercase().as_str() {
            "bool" | "boolean" => Ok(DbType::Boolean(sql_type.to_string())),
            "smallint" | "int2" | "smallserial" => Ok(DbType::SmallInteger(sql_type.to_string())),
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
        }
    }

    /// Implements [DbKind::param_prefix()] for PostgreSQLKind.
    fn param_prefix(&self) -> &str {
        "$"
    }

    /// Implements [DbKind::get_epoch_time_sql()] for PostgreSQLKind.
    fn get_epoch_time_sql(&self) -> &str {
        "extract(epoch from now())"
    }

    /// Implements [DbKind::columns_sql()] for PostgreSQLKind.
    fn columns_sql(&self, table: &str) -> (String, [DbValue; 1]) {
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
            z_old_params![table],
        )
    }

    /// Implements [DbKind::primary_keys_sql()] for PostgreSQLKind.
    fn primary_keys_sql(&self, table: &str) -> (String, [DbValue; 1]) {
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
            z_old_params![table],
        )
    }

    /// Implements [DbKind::create_table_sql()] for PostgreSQLKind.
    fn create_table_sql(
        &self,
        table: &str,
        columns: &IndexMap<String, DbColumn>,
    ) -> Result<String, DbError> {
        let column_clauses = columns.iter().map(|(column_name, column)| {
            let mut clause = format!(r#""{column_name}""#);
            match column.db_type {
                DbType::Null(_) => {
                    return Err(DbError::InputError(format!(
                        "Can't use a NULL column to create table '{table}'."
                    )));
                }
                DbType::Boolean(_) => clause.push_str(" BOOLEAN"),
                DbType::I16(_) | DbType::SmallInteger(_) => clause.push_str(" SMALLINT"),
                DbType::Integer(_) | DbType::BigInteger(_) => clause.push_str(" INTEGER"),
                DbType::Real(_) => clause.push_str(" REAL"),
                DbType::BigReal(_) => clause.push_str(" DOUBLE PRECISION"),
                DbType::Numeric(_) => clause.push_str(" NUMERIC"),
                DbType::Text(_) => clause.push_str(" TEXT"),
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
                .collect::<Result<Vec<String>, DbError>>()?
                .join(", ")
        );
        Ok(sql)
    }

    /// Implements [DbKind::drop_table_sql()] for PostgreSQLKind.
    fn drop_table_sql(&self, table: &str) -> String {
        format!(r#"DROP TABLE IF EXISTS "{table}" CASCADE"#)
    }

    /// Implements [DbKind::drop_view_sql()] for PostgreSQLKind.
    fn drop_view_sql(&self, view: &str) -> String {
        format!(r#"DROP VIEW IF EXISTS "{view}" CASCADE"#)
    }

    /// Implements [DbKind::which_are_views_sql()] for PostgreSQLKind.
    fn which_are_views_sql(&self, objects: &[&str]) -> (String, Vec<DbValue>) {
        let prefix = self.param_prefix().to_string();
        let mut placeholders = vec![];
        let mut parameters = vec![];
        for (i, object) in objects.iter().enumerate() {
            let i = i + 1;
            placeholders.push(format!("{prefix}{i}"));
            parameters.push(DbValue::from(object.to_string()));
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

    /// Implements [DbKind::which_are_tables_sql()] for PostgreSQLKind.
    fn which_are_tables_sql(&self, objects: &[&str]) -> (String, Vec<DbValue>) {
        let prefix = self.param_prefix().to_string();
        let mut placeholders = vec![];
        let mut parameters = vec![];
        for (i, object) in objects.iter().enumerate() {
            let i = i + 1;
            placeholders.push(format!("{prefix}{i}"));
            parameters.push(DbValue::from(object.to_string()));
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

    /// Implements [DbKind::view_sql_sql()] for PostgreSQLKind.
    fn view_sql_sql(&self, view: &str) -> (String, [DbValue; 1]) {
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
            z_old_params![view],
        )
    }

    /// Implements [DbKind::wrap_trigger_content()] for PostgreSQLKind.
    fn wrap_trigger_content(
        &self,
        table: &str,
        trigger_basename: &str,
        trigger_content: &str,
    ) -> Result<Vec<String>, DbError> {
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

//////////////////////////////////////////////////////////////////////
// Database types
//////////////////////////////////////////////////////////////////////

/// The supported database types, including information about the name
/// used to refer to the type in the underlying database.
#[derive(Clone, Debug, Hash)]
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

impl Default for DbType {
    fn default() -> DbType {
        DbType::sorted().next().expect("No types defined")
    }
}

impl PartialEq for DbType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DbType::Boolean(_), DbType::Boolean(_)) => true,
            (DbType::I16(_), DbType::I16(_)) => true,
            (DbType::SmallInteger(_), DbType::SmallInteger(_)) => true,
            (DbType::Integer(_), DbType::Integer(_)) => true,
            (DbType::BigInteger(_), DbType::BigInteger(_)) => true,
            (DbType::Real(_), DbType::Real(_)) => true,
            (DbType::BigReal(_), DbType::BigReal(_)) => true,
            (DbType::Numeric(_), DbType::Numeric(_)) => true,
            (DbType::Text(_), DbType::Text(_)) => true,
            _ => false,
        }
    }
}

impl Eq for DbType {}

impl PartialOrd for DbType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            // Nulls are special and are not assigned an order in the hierarchy.
            (DbType::Null(_), _) | (_, DbType::Null(_)) => None,

            (DbType::Boolean(_), DbType::Boolean(_)) => Some(Ordering::Equal),
            (DbType::Boolean(_), _) => Some(Ordering::Less),
            (_, DbType::Boolean(_)) => Some(Ordering::Greater),

            (DbType::I16(_), DbType::I16(_)) => Some(Ordering::Equal),
            (DbType::I16(_), _) => Some(Ordering::Less),
            (_, DbType::I16(_)) => Some(Ordering::Greater),

            (DbType::SmallInteger(_), DbType::SmallInteger(_)) => Some(Ordering::Equal),
            (DbType::SmallInteger(_), _) => Some(Ordering::Less),
            (_, DbType::SmallInteger(_)) => Some(Ordering::Greater),

            (DbType::Integer(_), DbType::Integer(_)) => Some(Ordering::Equal),
            (DbType::Integer(_), _) => Some(Ordering::Less),
            (_, DbType::Integer(_)) => Some(Ordering::Greater),

            (DbType::BigInteger(_), DbType::BigInteger(_)) => Some(Ordering::Equal),
            (DbType::BigInteger(_), _) => Some(Ordering::Less),
            (_, DbType::BigInteger(_)) => Some(Ordering::Greater),

            (DbType::Real(_), DbType::Real(_)) => Some(Ordering::Equal),
            (DbType::Real(_), _) => Some(Ordering::Less),
            (_, DbType::Real(_)) => Some(Ordering::Greater),

            (DbType::BigReal(_), DbType::BigReal(_)) => Some(Ordering::Equal),
            (DbType::BigReal(_), _) => Some(Ordering::Less),
            (_, DbType::BigReal(_)) => Some(Ordering::Greater),

            (DbType::Numeric(_), DbType::Numeric(_)) => Some(Ordering::Equal),
            (DbType::Numeric(_), _) => Some(Ordering::Less),
            (_, DbType::Numeric(_)) => Some(Ordering::Greater),

            (DbType::Text(_), DbType::Text(_)) => Some(Ordering::Equal),
        }
    }
}

impl DbType {
    /// Return an iterator over sortable database types.
    pub fn sorted() -> impl Iterator<Item = DbType> {
        [
            // Note that DbType::Null is not part of this hierarchy.
            DbType::Boolean("".to_string()),
            DbType::I16("".to_string()),
            DbType::SmallInteger("".to_string()),
            DbType::Integer("".to_string()),
            DbType::BigInteger("".to_string()),
            DbType::Real("".to_string()),
            DbType::BigReal("".to_string()),
            DbType::Numeric("".to_string()),
            DbType::Text("".to_string()),
        ]
        .into_iter()
    }

    /// Determine the minimum type (see [DbType::sorted()]) needed to support the given value,
    /// where the latter is given in the form of a string. The minimum type is the first type
    /// in the type hierarchy for which we can parse the given value string as an instance of that
    /// type.
    pub fn min_type(&self, value: &str) -> Result<DbType, DbError> {
        // If the value is an empty string, return a Null type and value.
        if value == "" {
            return Ok(DbType::Null("".to_string()));
        }

        // Otherwise, try to parse it using the available types in order from most to least specific.
        for db_type in DbType::sorted() {
            if db_type >= *self {
                match db_type.parse_str(value) {
                    Ok(_) => return Ok(db_type),
                    Err(err) => {
                        if let DbType::Text(_) = db_type {
                            return Err(DbError::InputError(format!(
                                "Could not determine most specific type for value: '{value}'. \
                                 Got error: {err}"
                            )));
                        }
                    }
                }
            }
        }
        Ok(self.clone())
    }

    /// Parses a given string representing the value of a database field into a [DbValue] of this
    /// type.
    pub fn parse_str(&self, value: &str) -> Result<DbValue, DbError> {
        // If the value is a NULL value, then we just return a Null:
        if value == "" {
            return Ok(DbValue::Null);
        }

        // When parsing from a TSV or CSV file we will often encounter strings like "true",
        // "false", "f", "t", "0", and "1", possibly in uppercase or mixed case. When the current
        // type is a boolean, these should be interpreted as booleans. Otherwise, if the current
        // type is a number type, then these should be interpreted as numbers of that type (with
        // true => 1 and false => 0), otherwise if the current type is text then *all* values
        // should be interpreted as text.
        match self {
            DbType::Null(_) => Ok(DbValue::Null),
            DbType::Boolean(_) => match value.to_lowercase().as_str() {
                "0" | "false" | "f" => Ok(DbValue::Boolean(false)),
                "1" | "true" | "t" => Ok(DbValue::Boolean(true)),
                _ => {
                    let value = value
                        .parse::<bool>()
                        .map_err(|_| DbError::InputError(format!("Not a boolean: '{value}'")))?;
                    Ok(DbValue::Boolean(value))
                }
            },
            DbType::I16(_) | DbType::SmallInteger(_) => match value.to_lowercase().as_str() {
                "0" | "false" | "f" => Ok(DbValue::SmallInteger(0)),
                "1" | "true" | "t" => Ok(DbValue::SmallInteger(1)),
                _ => {
                    let value = value
                        .parse::<i16>()
                        .map_err(|_| DbError::InputError(format!("Not an i16: '{value}'")))?;
                    Ok(DbValue::SmallInteger(value))
                }
            },
            DbType::Integer(_) => match value.to_lowercase().as_str() {
                "0" | "false" | "f" => Ok(DbValue::Integer(0)),
                "1" | "true" | "t" => Ok(DbValue::Integer(1)),
                _ => {
                    let value = value
                        .parse::<i32>()
                        .map_err(|_| DbError::InputError(format!("Not an i32: '{value}'")))?;
                    Ok(DbValue::Integer(value))
                }
            },
            DbType::BigInteger(_) => match value.to_lowercase().as_str() {
                "0" | "false" | "f" => Ok(DbValue::BigInteger(0)),
                "1" | "true" | "t" => Ok(DbValue::BigInteger(1)),
                _ => {
                    let value = value
                        .parse::<i64>()
                        .map_err(|_| DbError::InputError(format!("Not an i64: '{value}'")))?;
                    Ok(DbValue::BigInteger(value))
                }
            },
            DbType::Real(_) => match value.to_lowercase().as_str() {
                "0" | "false" | "f" => Ok(DbValue::Real(0_f32)),
                "1" | "true" | "t" => Ok(DbValue::Real(1_f32)),
                _ => match value
                    .parse::<f32>()
                    .map_err(|_| DbError::InputError(format!("Not an f32: '{value}'")))?
                {
                    f32::INFINITY => Err(DbError::InputError(format!("Not an f32: '{value}'"))),
                    value => Ok(DbValue::Real(value)),
                },
            },
            DbType::BigReal(_) => match value.to_lowercase().as_str() {
                "0" | "false" | "f" => Ok(DbValue::BigReal(0_f64)),
                "1" | "true" | "t" => Ok(DbValue::BigReal(1_f64)),
                _ => match value
                    .parse::<f64>()
                    .map_err(|_| DbError::InputError(format!("Not an f64: '{value}'")))?
                {
                    f64::INFINITY => Err(DbError::InputError(format!("Not an f64: '{value}'"))),
                    value => Ok(DbValue::BigReal(value)),
                },
            },
            DbType::Numeric(_) => match value.to_lowercase().as_str() {
                "0" | "false" | "f" => Ok(DbValue::Numeric(dec!(0))),
                "1" | "true" | "t" => Ok(DbValue::Numeric(dec!(1))),
                _ => {
                    let value = value
                        .parse::<Decimal>()
                        .map_err(|_| DbError::InputError(format!("Not a Decimal: '{value}'")))?;
                    Ok(DbValue::Numeric(value))
                }
            },
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
