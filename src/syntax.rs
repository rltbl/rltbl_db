//! Trait for implementing various SQL language "flavours"
//!
//! Although there is an ANSI SQL standard, each SQL database implemntation uses somewhat
//! different syntax. Our [Syntax] trait abstracts over some of those differences, so we can
//! write our Rust code once and use different SQL databases at runtime.

use crate::{
    Error, Value, ValueType,
    cache::{QUERY_CACHE_TABLE, TABLE_CACHE_TABLE},
    sql_parse,
};

pub trait Syntax: std::fmt::Debug {
    /// Returns the name for this syntax.
    fn name(&self) -> &str;

    /// Get a SQL ValueType by its name in this SQL syntax.
    fn sql_type(&self, name: &str) -> Result<ValueType, Error> {
        let name = name.to_uppercase();
        match name.as_str() {
            "TEXT" => Ok(ValueType::Text("TEXT".to_string())),
            "SMALLINT" => Ok(ValueType::SmallInteger("SMALLINT".to_string())),
            "INT" | "INTEGER" => Ok(ValueType::Integer("INT".to_string())),
            "BIGINT" => Ok(ValueType::BigInteger("BIGINT".to_string())),
            "FLOAT" | "REAL" => Ok(ValueType::Real("REAL".to_string())),
            "DOUBLE" | "DOUBLE PRECISION" => Ok(ValueType::BigReal("DOUBLE".to_string())),
            _ => Err(Error::DatatypeError(format!(
                "Unrecoganized type name: {name}"
            ))),
        }
    }

    /// Generate the SQL and parameters needed to query the database's metadata for the names and
    /// types of the columns of the given table.
    fn columns_sql(&self, _table: &str) -> (String, [Value; 1]) {
        // MC: I don't see why we need a default implementation of this function?
        // What would it be valid for?
        todo!("write default implementation for columns_sql")
    }

    /// Generate the SQL and parameters needed to query the database's metadata for the primary
    /// key columns of the given table.
    fn primary_keys_sql(&self, table: &str) -> (String, [Value; 1]);

    /// Get the prefix to use for parameters to queries that need to be bound.
    fn param_prefix(&self) -> &str;

    /// TODO: Add docstring here.
    fn get_epoch_time_sql(&self) -> &str;

    /// TODO: Add docstring.
    fn which_are_tables_sql(&self, objects: &[&str]) -> (String, Vec<Value>);

    /// TODO: Add docstring.
    fn which_are_views_sql(&self, objects: &[&str]) -> (String, Vec<Value>);

    /// TODO: Add docstring.
    fn view_sql_sql(&self, view: &str) -> (String, [Value; 1]);

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

    /// TODO: Add docstring.
    fn create_table_caching_triggers_for_table_sql(
        &self,
        table: &str,
    ) -> Result<Vec<String>, Error> {
        let table = sql_parse::validate_table_name(table)?;
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
    ) -> Result<Vec<String>, Error> {
        let table = sql_parse::validate_table_name(table)?;
        let view = sql_parse::validate_table_name(view)?;
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
    ) -> Result<Vec<String>, Error>;

    // TODO: Other methods ...
}
