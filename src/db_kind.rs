use crate::{
    core::{ColumnMap, DbError, DbQuery, DbRow},
    params,
};
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
    /// Given a table, return a map from column names to column SQL types.
    pub async fn columns(
        &self,
        pool: &(impl DbQuery + Sync),
        table: &str,
    ) -> Result<ColumnMap, DbError> {
        match self {
            DbKind::SQLite => self.columns_sqlite(pool, table).await,
            DbKind::PostgreSQL => self.columns_postgresql(pool, table).await,
        }
    }

    async fn columns_sqlite(
        &self,
        pool: &(impl DbQuery + Sync),
        table: &str,
    ) -> Result<ColumnMap, DbError> {
        let mut columns = ColumnMap::new();
        let sql = r#"SELECT "name", "type"
                     FROM pragma_table_info(?1)
                     ORDER BY "name""#
            .to_string();

        let rows: Vec<DbRow> = pool.query_no_cache(&sql, params![&table]).await?;
        for row in &rows {
            match (
                row.get("name").and_then(|name| Some::<String>(name.into())),
                row.get("type").and_then(|name| Some::<String>(name.into())),
            ) {
                (Some(column), Some(sql_type)) => {
                    columns.insert(column.to_string(), sql_type.to_lowercase().to_string())
                }
                _ => {
                    return Err(DbError::DataError(format!(
                        "Error getting columns for table '{table}'"
                    )));
                }
            };
        }

        match columns.is_empty() {
            true => Err(DbError::DataError(format!(
                "No information found for table '{table}'"
            ))),
            false => Ok(columns),
        }
    }

    async fn columns_postgresql(
        &self,
        pool: &(impl DbQuery + Sync),
        table: &str,
    ) -> Result<ColumnMap, DbError> {
        let mut columns = ColumnMap::new();
        let sql = format!(
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
        );

        let rows: Vec<DbRow> = pool.query_no_cache(&sql, params![&table]).await?;
        for row in &rows {
            match (
                row.get("column_name")
                    .and_then(|name| Some::<String>(name.into())),
                row.get("data_type")
                    .and_then(|name| Some::<String>(name.into())),
            ) {
                (Some(column), Some(sql_type)) => {
                    columns.insert(column.to_string(), sql_type.to_lowercase().to_string())
                }
                _ => {
                    return Err(DbError::DataError(format!(
                        "Error getting columns for table '{table}'"
                    )));
                }
            };
        }

        match columns.is_empty() {
            true => Err(DbError::DataError(format!(
                "No information found for table '{table}'"
            ))),
            false => Ok(columns),
        }
    }
}
