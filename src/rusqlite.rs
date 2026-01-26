//! rusqlite implementation for rltbl_db.

use crate::{
    core::{
        CachingStrategy, DbError, DbQuery, DbRow, FromDbRows, IntoDbRows, IntoParams, ParamValue,
        Params,
    },
    db_kind::DbKind,
    shared::{EditType, edit},
};
use deadpool_sqlite::{
    Config, Pool, Runtime,
    rusqlite::{
        Statement,
        types::{Null, ValueRef},
    },
};
use fallible_iterator::FallibleIterator;
use rust_decimal::Decimal;
use std::str::from_utf8;

/// The [maximum number of parameters](https://www.sqlite.org/limits.html#max_variable_number)
/// that can be bound to a SQLite query
static MAX_PARAMS_SQLITE: usize = 32766;

/// Query a database using the given prepared statement and parameters.
fn query_prepared(
    stmt: &mut Statement<'_>,
    params: impl IntoParams + Send,
) -> Result<Vec<DbRow>, DbError> {
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
            let mut db_row = DbRow::new();
            for column in &columns {
                let column_name = &column.name;
                let column_type = &column.datatype;
                let value = row.get_ref(column_name.as_str())?;
                let value = match value {
                    ValueRef::Null => ParamValue::Null,
                    ValueRef::Integer(value) => match column_type {
                        Some(ctype) if ctype.to_lowercase() == "bool" => {
                            ParamValue::Boolean(value != 0)
                        }
                        // The remaining cases are (a) the column's datatype is integer, and
                        // (b) the column is an expression. In the latter case it doesn't seem
                        // possible to get the datatype of the expression from the metadata.
                        // So the only thing to do here is just to convert the value
                        // to using the default method, and since we already know that it
                        // is an integer, the result of the conversion will be a number.
                        _ => ParamValue::from(value),
                    },
                    ValueRef::Real(value) => ParamValue::from(value),
                    ValueRef::Text(value) | ValueRef::Blob(value) => match column_type {
                        Some(ctype) if ctype.to_lowercase() == "numeric" => {
                            let value = from_utf8(value).unwrap_or_default();
                            let value = value.parse::<Decimal>().unwrap();
                            ParamValue::Numeric(value)
                        }
                        _ => {
                            let value = from_utf8(value).unwrap_or_default();
                            ParamValue::Text(value.to_string())
                        }
                    },
                };
                db_row.insert(column_name.to_string(), value);
            }
            Ok(db_row)
        })
        .collect::<Vec<_>>();
    results.map_err(|err| DbError::DatabaseError(err.to_string()))
}

/// Represents a SQLite database connection pool
#[derive(Debug)]
pub struct RusqlitePool {
    pool: Pool,
    caching_strategy: CachingStrategy,
    /// When set to true, SQL statements sent to the [DbQuery::query()] and [DbQuery::execute()]
    /// functions will be parsed and if they will result in tables being edited and/or dropped,
    /// the cache will be maintained in accordance with the given [CachingStrategy].
    cache_aware_query: bool,
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
            cache_aware_query: false,
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

    /// Implements [DbQuery::set_cache_aware_query()] for SQLite.
    fn set_cache_aware_query(&mut self, flag: bool) {
        self.cache_aware_query = flag;
    }

    /// Implements [DbQuery::get_cache_aware_query()] for SQLite.
    fn get_cache_aware_query(&self) -> bool {
        self.cache_aware_query
    }

    /// Implements [DbQuery::execute_batch()] for PostgreSQL
    async fn execute_batch(&self, sql: &str) -> Result<(), DbError> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(|err| DbError::ConnectError(format!("Unable to get from pool: {err}")))?;
        let sql_string = sql.to_string();
        match conn
            .interact(move |conn| match conn.execute_batch(&sql_string) {
                Err(err) => {
                    return Err(DbError::DatabaseError(format!("Error during query: {err}")));
                }
                Ok(_) => Ok(()),
            })
            .await
        {
            Err(err) => Err(DbError::DatabaseError(format!("Error during query: {err}"))),
            Ok(_) => {
                // We need to drop conn here to ensure that any changes to the db are persisted.
                drop(conn);
                self.clear_cache_for_affected_tables(sql).await?;
                Ok(())
            }
        }
    }

    /// Implements [DbQuery::query_no_cache()] for SQLite.
    async fn query_no_cache<T: FromDbRows>(
        &self,
        sql: &str,
        params: impl IntoParams + Send,
    ) -> Result<T, DbError> {
        let rows = {
            let conn =
                self.pool.get().await.map_err(|err| {
                    DbError::ConnectError(format!("Error getting from pool: {err}"))
                })?;
            let sql_string = sql.to_string();
            let params: Params = params.into_params();
            conn.interact(move |conn| {
                let mut stmt = conn.prepare(&sql_string).map_err(|err| {
                    DbError::DatabaseError(format!("Error preparing statement: {err}"))
                })?;
                let rows = query_prepared(&mut stmt, params)
                    .map_err(|err: DbError| {
                        DbError::DatabaseError(format!("Error querying prepared statement: {err}"))
                    })?
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|(key, val)| (key, ParamValue::from(val)))
                            .collect()
                    })
                    .collect();
                Ok(rows)
            })
            .await
            .map_err(|err| DbError::DatabaseError(err.to_string()))??
        };
        Ok(FromDbRows::from(rows))
    }

    /// Implements [DbQuery::insert()] for SQLite.
    async fn insert(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
    ) -> Result<(), DbError> {
        let _: Vec<DbRow> = edit(
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
    async fn insert_returning<T: FromDbRows>(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
        returning: &[&str],
    ) -> Result<T, DbError> {
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
        rows: impl IntoDbRows,
    ) -> Result<(), DbError> {
        let _: Vec<DbRow> = edit(
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
    async fn update_returning<T: FromDbRows>(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
        returning: &[&str],
    ) -> Result<T, DbError> {
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
        rows: impl IntoDbRows,
    ) -> Result<(), DbError> {
        let _: Vec<DbRow> = edit(
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
    async fn upsert_returning<T: FromDbRows>(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
        returning: &[&str],
    ) -> Result<T, DbError> {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::params;
    use indexmap::indexmap as db_row;

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
               VALUES (?1, ?2, ?3, ?4, ?5)"#,
            params!["foo", (), 1.05_f64, 1_i64, true],
        )
        .await
        .unwrap();

        // Test aggregate:
        let rows: Vec<DbRow> = pool
            .query("SELECT MAX(int_value) FROM test_table_indirect", ())
            .await
            .unwrap();
        assert_eq!(
            rows,
            [db_row! {"MAX(int_value)".into() => ParamValue::from(1_i64)}]
        );

        // Test alias:
        let rows: Vec<DbRow> = pool
            .query(
                "SELECT bool_value AS bool_value_alias FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows,
            [db_row! {"bool_value_alias".into() => ParamValue::from(true)}]
        );

        // Test aggregate with alias:
        let rows: Vec<DbRow> = pool
            .query(
                "SELECT MAX(int_value) AS max_int_value FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        // Note that the alias is not shown in the results:
        assert_eq!(
            rows,
            [db_row! {"max_int_value".into() => ParamValue::from(1_i64)}]
        );

        // Test non-aggregate function:
        let rows: Vec<DbRow> = pool
            .query(
                "SELECT CAST(int_value AS TEXT) FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows,
            [db_row! {"CAST(int_value AS TEXT)".into() => ParamValue::from("1")}]
        );

        // Test non-aggregate function with alias:
        let rows: Vec<DbRow> = pool
            .query(
                "SELECT CAST(int_value AS TEXT) AS int_value_cast FROM test_table_indirect",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows,
            [db_row! {"int_value_cast".into() => ParamValue::from("1")}]
        );

        // Test functions over booleans:
        let rows: Vec<DbRow> = pool
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
        assert_eq!(
            rows,
            [db_row! {"MAX(bool_value)".into() => ParamValue::from(1_i64)}]
        );
    }

    /// This test is resource intensive and therefore ignored by default. It verifies that
    /// using [MAX_PARAMS_SQLITE] parameters in a query is indeed supported.
    /// To run this and other ignored tests, use `cargo test -- --ignored` or
    /// `cargo test -- --include-ignored`
    #[tokio::test]
    #[ignore]
    async fn test_max_params() {
        let pool = RusqlitePool::connect(":memory:").await.unwrap();
        pool.execute_batch(
            "DROP TABLE IF EXISTS test_max_params;\
             CREATE TABLE test_max_params (\
                 column1 INT,\
                 column2 INT,\
                 column3 INT,\
                 column4 INT,\
                 column5 INT,\
                 column6 INT\
             )",
        )
        .await
        .unwrap();

        let mut sql = "INSERT INTO test_max_params VALUES ".to_string();
        let mut values = vec![];
        let mut params = vec![];
        let mut n = 1;
        while n <= MAX_PARAMS_SQLITE {
            values.push(format!(
                "(?{}, ?{}, ?{}, ?{}, ?{}, ?{})",
                n,
                n + 1,
                n + 2,
                n + 3,
                n + 4,
                n + 5
            ));
            params.push(1);
            params.push(1);
            params.push(1);
            params.push(1);
            params.push(1);
            params.push(1);
            n += 6;
        }
        sql.push_str(&values.join(", "));
        pool.execute(&sql, params).await.unwrap();
    }
}
