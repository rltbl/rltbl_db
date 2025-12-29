//! rusqlite implementation for rltbl_db.

use crate::{
    core::{
        CachingStrategy, ColumnMap, DbError, DbKind, DbQuery, IntoParams, JsonRow, JsonValue,
        ParamValue, Params, validate_table_name,
    },
    params,
    shared::{EditType, edit},
};

use deadpool_sqlite::{
    Config, Pool, Runtime,
    rusqlite::{
        Statement,
        fallible_iterator::FallibleIterator,
        types::{Null, ValueRef},
    },
};
use serde_json::json;

/// The [maximum number of parameters](https://www.sqlite.org/limits.html#max_variable_number)
/// that can be bound to a SQLite query
static MAX_PARAMS_SQLITE: usize = 32766;

/// Query a database using the given prepared statement and parameters.
fn query_prepared(
    stmt: &mut Statement<'_>,
    params: impl IntoParams + Send,
) -> Result<Vec<JsonRow>, DbError> {
    match params.into_params() {
        Params::None => (),
        Params::Positional(params) => {
            for (i, param) in params.iter().enumerate() {
                match param {
                    ParamValue::Text(text) => {
                        stmt.raw_bind_parameter(i + 1, text).map_err(|err| {
                            DbError::InputError(format!(
                                "Error binding parameter '{param:?}': {err}"
                            ))
                        })?;
                    }
                    ParamValue::SmallInteger(num) => {
                        stmt.raw_bind_parameter(i + 1, num.to_string())
                            .map_err(|err| {
                                DbError::InputError(format!(
                                    "Error binding parameter '{param:?}': {err}"
                                ))
                            })?;
                    }
                    ParamValue::Integer(num) => {
                        stmt.raw_bind_parameter(i + 1, num.to_string())
                            .map_err(|err| {
                                DbError::InputError(format!(
                                    "Error binding parameter '{param:?}': {err}"
                                ))
                            })?;
                    }
                    ParamValue::BigInteger(num) => {
                        stmt.raw_bind_parameter(i + 1, num.to_string())
                            .map_err(|err| {
                                DbError::InputError(format!(
                                    "Error binding parameter '{param:?}': {err}"
                                ))
                            })?;
                    }
                    ParamValue::Real(num) => {
                        stmt.raw_bind_parameter(i + 1, num.to_string())
                            .map_err(|err| {
                                DbError::InputError(format!(
                                    "Error binding parameter '{param:?}': {err}"
                                ))
                            })?;
                    }
                    ParamValue::BigReal(num) => {
                        stmt.raw_bind_parameter(i + 1, num.to_string())
                            .map_err(|err| {
                                DbError::InputError(format!(
                                    "Error binding parameter '{param:?}': {err}"
                                ))
                            })?;
                    }
                    ParamValue::Numeric(num) => {
                        stmt.raw_bind_parameter(i + 1, num.to_string())
                            .map_err(|err| {
                                DbError::InputError(format!(
                                    "Error binding parameter '{param:?}': {err}"
                                ))
                            })?;
                    }
                    ParamValue::Boolean(flag) => {
                        // Note that SQLite's type affinity means that booleans are actually
                        // implemented as numbers (see https://sqlite.org/datatype3.html).
                        let num = match flag {
                            true => 1,
                            false => 0,
                        };
                        stmt.raw_bind_parameter(i + 1, num.to_string())
                            .map_err(|err| {
                                DbError::InputError(format!(
                                    "Error binding parameter '{param:?}': {err}"
                                ))
                            })?;
                    }
                    ParamValue::Null => {
                        stmt.raw_bind_parameter(i + 1, &Null).map_err(|err| {
                            DbError::InputError(format!(
                                "Error binding parameter '{param:?}': {err}"
                            ))
                        })?;
                    }
                };
            }
        }
    };

    // Define the struct that we will use to represent information about a given column:
    struct ColumnConfig {
        name: String,
        datatype: Option<String>,
    }

    // Collect the column information from the prepared statement:
    let columns = stmt
        .columns()
        .iter()
        .map(|col| {
            let name = col.name().to_string();
            let datatype = col.decl_type().and_then(|s| Some(s.to_string()));
            ColumnConfig { name, datatype }
        })
        .collect::<Vec<_>>();

    // Execute the statement and send back the results
    let results = stmt
        .raw_query()
        .map(|row| {
            let mut json_row = JsonRow::new();
            for column in &columns {
                let column_name = &column.name;
                let column_type = &column.datatype;
                let value = row.get_ref(column_name.as_str())?;
                let value = match value {
                    ValueRef::Null => JsonValue::Null,
                    ValueRef::Integer(value) => match column_type {
                        Some(ctype) if ctype.to_lowercase() == "bool" => {
                            JsonValue::Bool(value != 0)
                        }
                        // The remaining cases are (a) the column's datatype is integer, and
                        // (b) the column is an expression. In the latter case it doesn't seem
                        // possible to get the datatype of the expression from the metadata.
                        // So the only thing to do here is just to convert the value
                        // to JSON using the default method, and since we already know that it
                        // is an integer, the result of the conversion will be a JSON number.
                        _ => JsonValue::from(value),
                    },
                    ValueRef::Real(value) => JsonValue::from(value),
                    ValueRef::Text(value) | ValueRef::Blob(value) => match column_type {
                        Some(ctype) if ctype.to_lowercase() == "numeric" => {
                            json!(value)
                        }
                        _ => {
                            let value = std::str::from_utf8(value).unwrap_or_default();
                            JsonValue::String(value.to_string())
                        }
                    },
                };
                json_row.insert(column_name.to_string(), value);
            }
            Ok(json_row)
        })
        .collect::<Vec<_>>();
    results.map_err(|err| DbError::DatabaseError(err.to_string()))
}

/// Represents a SQLite database connection pool
#[derive(Debug)]
pub struct RusqlitePool {
    pool: Pool,
    caching_strategy: CachingStrategy,
}

impl RusqlitePool {
    /// Connect to a SQLite database using the given url.
    pub async fn connect(url: &str) -> Result<Self, DbError> {
        let cfg = Config::new(url);
        let pool = cfg
            .create_pool(Runtime::Tokio1)
            .map_err(|err| DbError::ConnectError(format!("Error creating pool: {err}")))?;
        Ok(Self {
            pool: pool,
            caching_strategy: CachingStrategy::None,
        })
    }
}

impl DbQuery for RusqlitePool {
    /// Implements [DbQuery::kind()] for SQLite.
    fn kind(&self) -> DbKind {
        DbKind::SQLite
    }

    /// Implements [DbQuery::set_caching_strategy()] for SQLite.
    fn set_caching_strategy(&mut self, strategy: &CachingStrategy) {
        self.caching_strategy = *strategy;
    }

    /// Implements [DbQuery::get_caching_strategy()] for SQLite.
    fn get_caching_strategy(&self) -> CachingStrategy {
        self.caching_strategy
    }

    /// Implements [DbQuery::ensure_cache_table_exists()] for SQLite.
    async fn ensure_cache_table_exists(&self) -> Result<(), DbError> {
        match self
            .execute(
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
                match self.table_exists("cache").await? {
                    false => Err(DbError::DatabaseError(
                        "The cache table could not be created".to_string(),
                    )),
                    true => Ok(()),
                }
            }
        }
    }

    /// Implements [DbQuery::ensure_caching_triggers_exist()] for SQLite.
    async fn ensure_caching_triggers_exist(&self, tables: &[&str]) -> Result<(), DbError> {
        self.ensure_cache_table_exists().await?;
        for table in tables {
            let num_triggers = self
                .query_u64(
                    r#"SELECT COUNT(1)
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
            if num_triggers != 3 {
                // Note that parameters are not allowed in trigger creation statements in SQLite.
                self.execute_batch(&format!(
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

    /// Implements [DbQuery::parse()] for SQLite.
    fn parse(&self, sql_type: &str, value: &str) -> Result<ParamValue, DbError> {
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

    /// Implements [DbQuery::columns()] for SQLite.
    async fn columns(&self, table: &str) -> Result<ColumnMap, DbError> {
        let mut columns = ColumnMap::new();
        let sql = r#"SELECT "name", "type"
                     FROM pragma_table_info($1)
                     ORDER BY "name""#
            .to_string();

        for row in self.query(&sql, params![&table]).await? {
            match (
                row.get("name")
                    .and_then(|name| name.as_str().and_then(|name| Some(name))),
                row.get("type")
                    .and_then(|name| name.as_str().and_then(|name| Some(name))),
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

        Ok(columns)
    }

    /// Implements [DbQuery::primary_keys()] for SQLite.
    async fn primary_keys(&self, table: &str) -> Result<Vec<String>, DbError> {
        self.query(
            r#"SELECT "name"
               FROM pragma_table_info($1)
               WHERE "pk" > 0
               ORDER BY "pk""#,
            params![&table],
        )
        .await?
        .iter()
        .map(|row| {
            match row
                .get("name")
                .and_then(|name| name.as_str().and_then(|name| Some(name)))
            {
                Some(pk_col) => Ok(pk_col.to_string()),
                None => Err(DbError::DataError("Empty row".to_owned())),
            }
        })
        .collect()
    }

    /// Implements [DbQuery::execute_batch()] for PostgreSQL
    async fn execute_batch(&self, sql: &str) -> Result<(), DbError> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(|err| DbError::ConnectError(format!("Unable to get pool: {err}")))?;
        let sql = sql.to_string();
        match conn
            .interact(move |conn| match conn.execute_batch(&sql) {
                Err(err) => {
                    return Err(DbError::DatabaseError(format!("Error during query: {err}")));
                }
                Ok(_) => Ok(()),
            })
            .await
        {
            Err(err) => Err(DbError::DatabaseError(format!("Error during query: {err}"))),
            Ok(_) => Ok(()),
        }
    }

    /// Implements [DbQuery::query()] for SQLite.
    async fn query(
        &self,
        sql: &str,
        params: impl IntoParams + Send,
    ) -> Result<Vec<JsonRow>, DbError> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(|err| DbError::ConnectError(format!("Error getting pool: {err}")))?;
        let sql = sql.to_string();
        let params: Params = params.into_params();
        let rows = conn
            .interact(move |conn| {
                let mut stmt = conn.prepare(&sql).map_err(|err| {
                    DbError::DatabaseError(format!("Error preparing statement: {err}"))
                })?;
                let rows = query_prepared(&mut stmt, params).map_err(|err| {
                    DbError::DatabaseError(format!("Error querying prepared statement: {err}"))
                })?;
                Ok::<Vec<JsonRow>, DbError>(rows)
            })
            .await
            .map_err(|err| DbError::DatabaseError(err.to_string()))?
            .map_err(|err| DbError::DatabaseError(err.to_string()))?;
        Ok(rows)
    }

    /// Implements [DbQuery::insert()] for SQLite.
    async fn insert(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[&JsonRow],
    ) -> Result<(), DbError> {
        edit(
            self,
            &EditType::Insert,
            &MAX_PARAMS_SQLITE,
            table,
            columns,
            rows,
            false,
            &[],
        )
        .await?;
        Ok(())
    }

    /// Implements [DbQuery::insert_returning()] for SQLite.
    async fn insert_returning(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[&JsonRow],
        returning: &[&str],
    ) -> Result<Vec<JsonRow>, DbError> {
        edit(
            self,
            &EditType::Insert,
            &MAX_PARAMS_SQLITE,
            table,
            columns,
            rows,
            true,
            returning,
        )
        .await
    }

    /// Implements [DbQuery::update()] for SQLite.
    async fn update(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[&JsonRow],
    ) -> Result<(), DbError> {
        edit(
            self,
            &EditType::Update,
            &MAX_PARAMS_SQLITE,
            table,
            columns,
            rows,
            false,
            &[],
        )
        .await?;
        Ok(())
    }

    /// Implements [DbQuery::update_returning()] for SQLite.
    async fn update_returning(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[&JsonRow],
        returning: &[&str],
    ) -> Result<Vec<JsonRow>, DbError> {
        edit(
            self,
            &EditType::Update,
            &MAX_PARAMS_SQLITE,
            table,
            columns,
            rows,
            true,
            returning,
        )
        .await
    }

    /// Implements [DbQuery::upsert()] for SQLite.
    async fn upsert(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[&JsonRow],
    ) -> Result<(), DbError> {
        edit(
            self,
            &EditType::Upsert,
            &MAX_PARAMS_SQLITE,
            table,
            columns,
            rows,
            false,
            &[],
        )
        .await?;
        Ok(())
    }

    /// Implements [DbQuery::upsert_returning()] for SQLite.
    async fn upsert_returning(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[&JsonRow],
        returning: &[&str],
    ) -> Result<Vec<JsonRow>, DbError> {
        edit(
            self,
            &EditType::Upsert,
            &MAX_PARAMS_SQLITE,
            table,
            columns,
            rows,
            true,
            returning,
        )
        .await
    }

    /// Implements [DbQuery::table_exists()] for SQLite.
    async fn table_exists(&self, table: &str) -> Result<bool, DbError> {
        match self
            .query(
                r#"SELECT 1 FROM "sqlite_master"
                   WHERE "type" = 'table' AND "name" = ?1"#,
                &[table],
            )
            .await?
            .first()
        {
            None => Ok(false),
            Some(_) => Ok(true),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::params;
    use serde_json::json;

    #[tokio::test]
    async fn test_aliases_and_builtin_functions() {
        let pool = RusqlitePool::connect(":memory:").await.unwrap();
        pool.execute_batch(
            "DROP TABLE IF EXISTS test_table_indirect;\
             CREATE TABLE test_table_indirect (\
                 text_value TEXT,\
                 alt_text_value TEXT,\
                 float_value FLOAT8,\
                 int_value INT8,\
                 bool_value BOOL\
             )",
        )
        .await
        .unwrap();
        pool.execute(
            r#"INSERT INTO test_table_indirect
               (text_value, alt_text_value, float_value, int_value, bool_value)
               VALUES ($1, $2, $3, $4, $5)"#,
            params!["foo", (), 1.05_f64, 1_i64, true],
        )
        .await
        .unwrap();

        // Test aggregate:
        let rows = pool
            .query("SELECT MAX(int_value) FROM test_table_indirect", ())
            .await
            .unwrap();
        assert_eq!(json!(rows), json!([{"MAX(int_value)": 1}]));

        // Test alias:
        let rows = pool
            .query(
                "SELECT bool_value AS bool_value_alias FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(json!(rows), json!([{"bool_value_alias": true}]));

        // Test aggregate with alias:
        let rows = pool
            .query(
                "SELECT MAX(int_value) AS max_int_value FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        // Note that the alias is not shown in the results:
        assert_eq!(json!(rows), json!([{"max_int_value": 1}]));

        // Test non-aggregate function:
        let rows = pool
            .query(
                "SELECT CAST(int_value AS TEXT) FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(json!(rows), json!([{"CAST(int_value AS TEXT)": "1"}]));

        // Test non-aggregate function with alias:
        let rows = pool
            .query(
                "SELECT CAST(int_value AS TEXT) AS int_value_cast FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(json!(rows), json!([{"int_value_cast": "1"}]));

        // Test functions over booleans:
        let rows = pool
            .query("SELECT MAX(bool_value) FROM test_table_indirect", ())
            .await
            .unwrap();
        // It is not possible to represent the boolean result of an aggregate function as a
        // boolean, since internally to sqlite it is stored as an integer, and we can't query
        // the metadata to get the datatype of an expression. If we want to represent it as a
        // boolean, we will need to parse the expression. Note that PostgreSQL does not support
        // MAX(bool_value) - it gives the error:
        //   ERROR: function max(boolean) does not exist\nHINT: No function matches the given
        //          name and argument types. You might need to add explicit type casts.
        // So, perhaps, this is tu quoque an argument that the behaviour below is acceptable for
        // sqlite.
        assert_eq!(json!(rows), json!([{"MAX(bool_value)": 1}]));
    }
}
