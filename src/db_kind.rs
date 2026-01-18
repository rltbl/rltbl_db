use crate::{
    core::{ColumnMap, DbError, DbQuery, DbRow, validate_table_name},
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
    // Although SQLite allows '$' as a prefix, it is required to use '?' to represent integer
    // literals (see https://sqlite.org/c3ref/bind_blob.html) which is what we are using here.
    /// Get the prefix to use for parameters to queries that need to be bound.
    pub fn param_prefix(&self) -> &str {
        match self {
            DbKind::SQLite => "?",
            DbKind::PostgreSQL => "$",
        }
    }

    /// Query the database's metadata using the given pool and return a map from column names
    /// to column SQL types for the given table.
    pub async fn columns(
        &self,
        pool: &(impl DbQuery + Sync),
        table: &str,
    ) -> Result<ColumnMap, DbError> {
        async fn columns_sqlite(
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
        match self {
            DbKind::SQLite => columns_sqlite(pool, table).await,
            DbKind::PostgreSQL => columns_postgresql(pool, table).await,
        }
    }

    // TODO: Consider combining this function with columns().
    /// Query the database's metadata using the given pool and return the primary key columns
    /// for the given table.
    pub async fn primary_keys(
        &self,
        pool: &(impl DbQuery + Sync),
        table: &str,
    ) -> Result<Vec<String>, DbError> {
        async fn primary_keys_sqlite(
            pool: &(impl DbQuery + Sync),
            table: &str,
        ) -> Result<Vec<String>, DbError> {
            let rows: Vec<DbRow> = pool
                .query_no_cache(
                    r#"SELECT "name"
                       FROM pragma_table_info(?1)
                       WHERE "pk" > 0
                       ORDER BY "pk""#,
                    params![&table],
                )
                .await?;
            rows.iter()
                .map(
                    |row| match row.get("name").and_then(|name| Some::<String>(name.into())) {
                        Some(pk_col) => Ok(pk_col.to_string()),
                        None => Err(DbError::DataError("Empty row".to_owned())),
                    },
                )
                .collect()
        }

        async fn primary_keys_postgresql(
            pool: &(impl DbQuery + Sync),
            table: &str,
        ) -> Result<Vec<String>, DbError> {
            let rows: Vec<DbRow> = pool
                .query_no_cache(
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
                      ORDER by "kcu"."ordinal_position""#,
                    params![&table],
                )
                .await?;

            rows.iter()
                .map(|row| {
                    match row
                        .get("column_name")
                        .and_then(|name| Some::<String>(name.into()))
                    {
                        Some(pk_col) => Ok(pk_col.to_string()),
                        None => Err(DbError::DataError("Empty row".to_owned())),
                    }
                })
                .collect()
        }

        match self {
            DbKind::SQLite => primary_keys_sqlite(pool, table).await,
            DbKind::PostgreSQL => primary_keys_postgresql(pool, table).await,
        }
    }

    /// Determine whether the given table exists.
    pub async fn table_exists(
        self,
        pool: &(impl DbQuery + Sync),
        table: &str,
    ) -> Result<bool, DbError> {
        async fn table_exists_sqlite(
            pool: &(impl DbQuery + Sync),
            table: &str,
        ) -> Result<bool, DbError> {
            let rows: Vec<DbRow> = pool
                .query_no_cache(
                    r#"SELECT 1 FROM "sqlite_master"
                       WHERE "type" = 'table' AND "name" = ?1"#,
                    &[table],
                )
                .await?;
            match rows.first() {
                None => Ok(false),
                Some(_) => Ok(true),
            }
        }

        async fn table_exists_postgresql(
            pool: &(impl DbQuery + Sync),
            table: &str,
        ) -> Result<bool, DbError> {
            let rows: Vec<DbRow> = pool
                .query_no_cache(
                    r#"SELECT 1
                       FROM "information_schema"."tables"
                       WHERE "table_type" LIKE '%TABLE'
                         AND "table_name" = $1
                         AND "table_schema" IN (
                           SELECT REGEXP_SPLIT_TO_TABLE("setting", ', ')
                           FROM "pg_settings"
                           WHERE "name" = 'search_path'
                         )"#,
                    &[table],
                )
                .await?;

            match rows.first() {
                None => Ok(false),
                Some(_) => Ok(true),
            }
        }

        match self {
            DbKind::SQLite => table_exists_sqlite(pool, table).await,
            DbKind::PostgreSQL => table_exists_postgresql(pool, table).await,
        }
    }

    /// Ensure that the cache table exists
    pub async fn ensure_cache_table_exists(
        &self,
        pool: &(impl DbQuery + Sync),
    ) -> Result<(), DbError> {
        async fn ensure_cache_table_exists_sqlite(
            pool: &(impl DbQuery + Sync),
        ) -> Result<(), DbError> {
            match pool
                .execute_no_cache(
                    r#"CREATE TABLE IF NOT EXISTS "cache" (
                         "tables" TEXT,
                         "statement" TEXT,
                         "parameters" TEXT,
                         "value" TEXT,
                         PRIMARY KEY ("tables", "statement", "parameters")
                       )"#,
                    (),
                )
                .await
            {
                Ok(_) => Ok(()),
                Err(_) => {
                    // Since we are not using transactions, a race condition could occur in
                    // which two or more threads are trying to create the cache at the same
                    // time, triggering a primary key violation in the metadata table. So if
                    // there is an error creating the cache table we just check that it exists
                    // and if it does we assume that all is ok.
                    match pool.table_exists("cache").await? {
                        false => Err(DbError::DatabaseError(
                            "The cache table could not be created".to_string(),
                        )),
                        true => Ok(()),
                    }
                }
            }
        }

        async fn ensure_cache_table_exists_postgresql(
            pool: &(impl DbQuery + Sync),
        ) -> Result<(), DbError> {
            match pool
                .execute_no_cache(
                    r#"CREATE TABLE IF NOT EXISTS "cache" (
                         "tables" TEXT,
                         "statement" TEXT,
                         "parameters" TEXT,
                         "value" TEXT,
                         PRIMARY KEY ("tables", "statement", "parameters")
                       )"#,
                    (),
                )
                .await
            {
                Ok(_) => Ok(()),
                Err(_) => {
                    // Since we are not using transactions, a race condition could occur in
                    // which two or more threads are trying to create the cache at the same
                    // time, triggering a primary key violation in the metadata table. So if
                    // there is an error creating the cache table we just check that it exists
                    // and if it does we assume that all is ok.
                    match pool.table_exists("cache").await? {
                        false => Err(DbError::DatabaseError(
                            "The cache table could not be created".to_string(),
                        )),
                        true => Ok(()),
                    }
                }
            }
        }

        match self {
            DbKind::SQLite => ensure_cache_table_exists_sqlite(pool).await,
            DbKind::PostgreSQL => ensure_cache_table_exists_postgresql(pool).await,
        }
    }

    /// Ensure that caching triggers exist for the given tables. Note that this function calls
    /// [DbKind::ensure_cache_table_exists()] implicitly.
    pub async fn ensure_caching_triggers_exist(
        &self,
        pool: &(impl DbQuery + Sync),
        tables: &[&str],
    ) -> Result<(), DbError> {
        async fn ensure_caching_triggers_exist_sqlite(
            pool: &(impl DbQuery + Sync),
            tables: &[&str],
        ) -> Result<(), DbError> {
            for table in tables {
                let rows: Vec<DbRow> = pool
                    .query_no_cache(
                        r#"SELECT 1
                           FROM sqlite_master
                           WHERE type = 'trigger'
                             AND name IN (?1, ?2, ?3)"#,
                        &[
                            &format!("{table}_cache_after_insert"),
                            &format!("{table}_cache_after_update"),
                            &format!("{table}_cache_after_delete"),
                        ],
                    )
                    .await?;

                // Only recreate the triggers if they don't all already exist:
                if rows.len() != 3 {
                    // Note that parameters are not allowed in trigger creation statements in SQLite.
                    pool.execute_batch(&format!(
                        r#"DROP TRIGGER IF EXISTS "{table}_cache_after_insert";
                           CREATE TRIGGER "{table}_cache_after_insert"
                           AFTER INSERT ON "{table}"
                           BEGIN
                             DELETE FROM "cache" WHERE "tables" LIKE '%{table}%';
                           END;
                           DROP TRIGGER IF EXISTS "{table}_cache_after_update";
                           CREATE TRIGGER "{table}_cache_after_update"
                           AFTER UPDATE ON "{table}"
                           BEGIN
                             DELETE FROM "cache" WHERE "tables" LIKE '%{table}%';
                           END;
                           DROP TRIGGER IF EXISTS "{table}_cache_after_delete";
                           CREATE TRIGGER "{table}_cache_after_delete"
                           AFTER DELETE ON "{table}"
                           BEGIN
                             DELETE FROM "cache" WHERE "tables" LIKE '%{table}%';
                           END"#,
                        table = validate_table_name(table)?,
                    ))
                    .await?;
                }
            }
            Ok(())
        }

        async fn ensure_caching_triggers_exist_postgresql(
            pool: &(impl DbQuery + Sync),
            tables: &[&str],
        ) -> Result<(), DbError> {
            for table in tables {
                let rows: Vec<DbRow> = pool
                    .query_no_cache(
                        r#"SELECT 1
                           FROM information_schema.triggers
                           WHERE trigger_name IN ($1, $2, $3)
                             AND "trigger_schema" IN (
                               SELECT REGEXP_SPLIT_TO_TABLE("setting", ', ')
                               FROM "pg_settings"
                               WHERE "name" = 'search_path'
                             )"#,
                        &[
                            &format!("{table}_cache_after_insert"),
                            &format!("{table}_cache_after_update"),
                            &format!("{table}_cache_after_delete"),
                        ],
                    )
                    .await?;

                // Only recreate the triggers if they don't all already exist:
                if rows.len() != 3 {
                    // Note that parameters are not allowed in trigger creation statements
                    // in PostgreSQL.
                    pool.execute_batch(&format!(
                        r#"CREATE OR REPLACE FUNCTION "clean_cache_for_{table}"()
                             RETURNS TRIGGER
                             LANGUAGE PLPGSQL
                            AS
                            $$
                            BEGIN
                              DELETE FROM "cache" WHERE "tables" LIKE '%{table}%';
                              RETURN NEW;
                            END;
                            $$;
                            DROP TRIGGER IF EXISTS "{table}_cache_after_insert" ON "{table}";
                            CREATE TRIGGER "{table}_cache_after_insert"
                              AFTER INSERT ON "{table}"
                              EXECUTE FUNCTION "clean_cache_for_{table}"();
                            DROP TRIGGER IF EXISTS "{table}_cache_after_update" ON "{table}";
                            CREATE TRIGGER "{table}_cache_after_update"
                              AFTER UPDATE ON "{table}"
                              EXECUTE FUNCTION "clean_cache_for_{table}"();
                            DROP TRIGGER IF EXISTS "{table}_cache_after_delete" ON "{table}";
                            CREATE TRIGGER "{table}_cache_after_delete"
                              AFTER DELETE ON "{table}"
                              EXECUTE FUNCTION "clean_cache_for_{table}"()"#,
                        table = validate_table_name(table)?,
                    ))
                    .await?;
                }
            }
            Ok(())
        }

        self.ensure_cache_table_exists(pool).await?;
        match self {
            DbKind::SQLite => {
                self.ensure_cache_table_exists(pool).await?;
                ensure_caching_triggers_exist_sqlite(pool, tables).await
            }
            DbKind::PostgreSQL => ensure_caching_triggers_exist_postgresql(pool, tables).await,
        }
    }
}
