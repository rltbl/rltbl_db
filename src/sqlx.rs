//! sqlx implementation for rltbl_db.

use crate::{
    core::{
        CachingStrategy, DbError, DbQuery, DbRow, FromDbRows, IntoDbRows, IntoParams, ParamValue,
        Params,
    },
    db_kind::DbKind,
    shared::{EditType, edit},
};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use sqlx::{
    Column, Postgres, Row, Sqlite, TypeInfo,
    postgres::{PgPool, PgPoolOptions, PgRow},
    sqlite::{SqlitePool, SqliteRow},
};

/// The [maximum number of parameters](https://www.sqlite.org/limits.html#max_variable_number)
/// that can be bound to a SQLite query.
static MAX_PARAMS_SQLITE: usize = 32766;

/// The [maximum number of parameters](https://www.postgresql.org/docs/current/limits.html)
/// that can be bound to a PostgreSQL query.
static MAX_PARAMS_POSTGRES: usize = 65535;

fn pg_to_db_rows(pg_rows: &Vec<PgRow>) -> Result<Vec<DbRow>, DbError> {
    let mut db_rows = vec![];
    for pg_row in pg_rows {
        let mut db_row = DbRow::new();
        for (idx, column) in pg_row.columns().iter().enumerate() {
            let cname: &str = column.name();
            let ctype: &str = column.type_info().name();
            match ctype {
                "TEXT" | "VARCHAR" | "NAME" => match pg_row.try_get::<&str, usize>(idx) {
                    Ok(value) => db_row.insert(cname.to_string(), value.into()),
                    Err(_) => {
                        // TODO: Try to be more specific about the type of error accepted
                        // (UnexpectedNullError?)
                        db_row.insert(cname.to_string(), ParamValue::Null)
                    }
                },
                "INT2" => match pg_row.try_get::<i16, usize>(idx) {
                    Ok(value) => db_row.insert(cname.to_string(), value.into()),
                    Err(_) => {
                        // TODO: Try to be more specific about the type of error accepted
                        // (UnexpectedNullError?)
                        db_row.insert(cname.to_string(), ParamValue::Null)
                    }
                },
                "INT4" => match pg_row.try_get::<i32, usize>(idx) {
                    Ok(value) => db_row.insert(cname.to_string(), value.into()),
                    Err(_) => {
                        // TODO: Try to be more specific about the type of error accepted
                        // (UnexpectedNullError?)
                        db_row.insert(cname.to_string(), ParamValue::Null)
                    }
                },
                "INT8" => match pg_row.try_get::<i64, usize>(idx) {
                    Ok(value) => db_row.insert(cname.to_string(), value.into()),
                    Err(_) => {
                        // TODO: Try to be more specific about the type of error accepted
                        // (UnexpectedNullError?)
                        db_row.insert(cname.to_string(), ParamValue::Null)
                    }
                },
                "BOOL" => match pg_row.try_get::<bool, usize>(idx) {
                    Ok(value) => db_row.insert(cname.to_string(), value.into()),
                    Err(_) => {
                        // TODO: Try to be more specific about the type of error accepted
                        // (UnexpectedNullError?)
                        db_row.insert(cname.to_string(), ParamValue::Null)
                    }
                },
                "FLOAT4" => match pg_row.try_get::<f32, usize>(idx) {
                    Ok(value) => db_row.insert(cname.to_string(), value.into()),
                    Err(_) => {
                        // TODO: Try to be more specific about the type of error accepted
                        // (UnexpectedNullError?)
                        db_row.insert(cname.to_string(), ParamValue::Null)
                    }
                },
                "FLOAT8" => match pg_row.try_get::<f64, usize>(idx) {
                    Ok(value) => db_row.insert(cname.to_string(), value.into()),
                    Err(_) => {
                        // TODO: Try to be more specific about the type of error accepted
                        // (UnexpectedNullError?)
                        db_row.insert(cname.to_string(), ParamValue::Null)
                    }
                },
                "NUMERIC" => match pg_row.try_get::<Decimal, usize>(idx) {
                    Ok(value) => db_row.insert(cname.to_string(), value.into()),
                    Err(_) => {
                        // TODO: Try to be more specific about the type of error accepted
                        // (UnexpectedNullError?)
                        db_row.insert(cname.to_string(), ParamValue::Null)
                    }
                },
                _ => unimplemented!("Unimplemented column type: {column:?}"),
            };
        }
        db_rows.push(db_row);
    }
    Ok(db_rows)
}

fn sqlite_to_db_rows(sqlite_rows: &Vec<SqliteRow>) -> Result<Vec<DbRow>, DbError> {
    let mut db_rows = vec![];
    for sqlite_row in sqlite_rows {
        let mut db_row = DbRow::new();
        for (idx, column) in sqlite_row.columns().iter().enumerate() {
            let cname: &str = column.name();
            let ctype = column.type_info().name();
            match ctype {
                "TEXT" | "VARCHAR" => match sqlite_row.try_get::<&str, usize>(idx) {
                    Ok(value) => {
                        db_row.insert(cname.to_string(), value.into());
                    }
                    Err(_) => {
                        // TODO: Try to be more specific about the type of error accepted
                        // (UnexpectedNullError?)
                        db_row.insert(cname.to_string(), ParamValue::Null);
                    }
                },
                "INTEGER" => match sqlite_row.try_get::<i64, usize>(idx) {
                    Ok(value) => {
                        db_row.insert(cname.to_string(), value.into());
                    }
                    Err(_) => {
                        // TODO: Try to be more specific about the type of error accepted
                        // (UnexpectedNullError?)
                        db_row.insert(cname.to_string(), ParamValue::Null);
                    }
                },
                "BOOLEAN" => match sqlite_row.try_get::<bool, usize>(idx) {
                    Ok(value) => {
                        db_row.insert(cname.to_string(), value.into());
                    }
                    Err(_) => {
                        // TODO: Try to be more specific about the type of error accepted
                        // (UnexpectedNullError?)
                        db_row.insert(cname.to_string(), ParamValue::Null);
                    }
                },
                "REAL" => match sqlite_row.try_get::<f64, usize>(idx) {
                    Ok(value) => {
                        db_row.insert(cname.to_string(), value.into());
                    }
                    Err(_) => {
                        // TODO: Try to be more specific about the type of error accepted
                        // (UnexpectedNullError?)
                        db_row.insert(cname.to_string(), ParamValue::Null);
                    }
                },
                // Columns of numeric type are not reported correctly by column.type_info()
                // but are reported to be type "NULL" (TODO: Is this a bug in sqlx or by
                // design?) In that case we try to parse it as an f64. Other columns, such as
                // those that exist in the pragma tables, will also have type NULL. In that
                // case we will try parsing as a string, and who knows what else we might
                // encounter. We'll try to handle all the possible cases here.
                "NULL" => match sqlite_row.try_get::<i64, usize>(idx) {
                    Ok(value) => {
                        db_row.insert(cname.to_string(), value.into());
                    }
                    Err(_) => {
                        // TODO: Try to be more specific about the type of error accepted
                        // (UnexpectedNullError?)
                        match sqlite_row.try_get::<f64, usize>(idx) {
                            Ok(value) => {
                                db_row.insert(cname.to_string(), value.into());
                            }
                            Err(_) => match sqlite_row.try_get::<bool, usize>(idx) {
                                Ok(value) => {
                                    db_row.insert(cname.to_string(), value.into());
                                }
                                Err(_) => match sqlite_row.try_get::<&str, usize>(idx) {
                                    Ok(value) => {
                                        db_row.insert(cname.to_string(), value.into());
                                    }
                                    Err(_) => {
                                        db_row.insert(cname.to_string(), ParamValue::Null);
                                    }
                                },
                            },
                        }
                    }
                },
                _ => unimplemented!("Unsupported column type: '{ctype}'"),
            };
        }
        db_rows.push(db_row);
    }
    Ok(db_rows)
}

#[derive(Debug)]
pub enum Pool {
    SQLite(SqlitePool),
    PostgreSQL(PgPool),
}

/// Represents a Sqlx database connection pool
#[derive(Debug)]
pub struct SqlxPool {
    pool: Pool,
    caching_strategy: CachingStrategy,
    cache_aware_query: bool,
}

impl SqlxPool {
    /// TODO: Add docstring here.
    pub async fn connect(url: &str) -> Result<Self, DbError> {
        if url.starts_with("postgresql://") {
            let pool = PgPoolOptions::new().connect(url).await.unwrap();
            Ok(Self {
                pool: Pool::PostgreSQL(pool),
                caching_strategy: CachingStrategy::None,
                cache_aware_query: false,
            })
        } else {
            let url = {
                if url.starts_with("sqlite://") {
                    url.to_string()
                } else {
                    format!("sqlite://{url}?mode=rwc")
                }
            };
            let pool = SqlitePool::connect(&url).await.unwrap();
            Ok(Self {
                pool: Pool::SQLite(pool),
                caching_strategy: CachingStrategy::None,
                cache_aware_query: false,
            })
        }
    }
}

impl DbQuery for SqlxPool {
    /// Implements [DbQuery::kind()] for Sqlx.
    fn kind(&self) -> DbKind {
        match self.pool {
            Pool::SQLite(_) => DbKind::SQLite,
            Pool::PostgreSQL(_) => DbKind::PostgreSQL,
        }
    }

    /// Implements [DbQuery::set_caching_strategy()] for Sqlx.
    fn set_caching_strategy(&mut self, strategy: &CachingStrategy) {
        self.caching_strategy = *strategy;
    }

    /// Implements [DbQuery::get_caching_strategy()] for Sqlx.
    fn get_caching_strategy(&self) -> CachingStrategy {
        self.caching_strategy
    }

    /// Implements [DbQuery::set_cache_aware_query()] for Sqlx.
    fn set_cache_aware_query(&mut self, flag: bool) {
        self.cache_aware_query = flag;
    }

    /// Implements [DbQuery::get_cache_aware_query()] for Sqlx.
    fn get_cache_aware_query(&self) -> bool {
        self.cache_aware_query
    }

    /// Implements [DbQuery::execute_batch()] for Sqlx
    async fn execute_batch(&self, sql: &str) -> Result<(), DbError> {
        match &self.pool {
            Pool::SQLite(pool) => {
                sqlx::raw_sql(sql).execute(pool).await.unwrap();
            }
            Pool::PostgreSQL(pool) => {
                sqlx::raw_sql(sql).execute(pool).await.unwrap();
            }
        }
        Ok(())
    }

    /// Implements [DbQuery::query_no_cache()] for Sqlx.
    async fn query_no_cache<T: FromDbRows>(
        &self,
        sql: &str,
        params: impl IntoParams + Send,
    ) -> Result<T, DbError> {
        let params = params.into_params();
        match &self.pool {
            Pool::PostgreSQL(pool) => {
                let mut query = sqlx::query::<Postgres>(sql);
                match params {
                    Params::None => (),
                    Params::Positional(params) => {
                        for param in params {
                            match param {
                                // TODO: Get the correct type in case of a NULL:
                                ParamValue::Null => query = query.bind(None::<String>),
                                ParamValue::Boolean(value) => query = query.bind(value),
                                ParamValue::SmallInteger(value) => query = query.bind(value),
                                ParamValue::Integer(value) => query = query.bind(value),
                                ParamValue::BigInteger(value) => query = query.bind(value),
                                ParamValue::Real(value) => query = query.bind(value),
                                ParamValue::BigReal(value) => query = query.bind(value),
                                ParamValue::Numeric(value) => query = query.bind(value),
                                ParamValue::Text(string) => query = query.bind(string),
                            };
                        }
                    }
                };
                let rows = query.fetch_all(pool).await.unwrap();
                let rows = pg_to_db_rows(&rows)?;
                Ok(FromDbRows::from(rows))
            }
            Pool::SQLite(pool) => {
                let mut query = sqlx::query::<Sqlite>(sql);
                match params {
                    Params::None => (),
                    Params::Positional(ref params) => {
                        for param in params {
                            match param {
                                // TODO: Get the correct type in case of a NULL:
                                ParamValue::Null => query = query.bind(None::<String>),
                                ParamValue::Boolean(value) => query = query.bind(value),
                                ParamValue::SmallInteger(value) => query = query.bind(value),
                                ParamValue::Integer(value) => query = query.bind(value),
                                ParamValue::BigInteger(value) => query = query.bind(value),
                                ParamValue::Real(value) => query = query.bind(value),
                                ParamValue::BigReal(value) => query = query.bind(value),
                                ParamValue::Numeric(value) => {
                                    let value = value.to_f64().ok_or(DbError::DatatypeError(
                                        format!("Error converting value '{value}' to f64"),
                                    ))?;
                                    query = query.bind(value)
                                }
                                ParamValue::Text(string) => query = query.bind(string),
                            };
                        }
                    }
                };
                let rows = query.fetch_all(pool).await.unwrap();
                let rows = sqlite_to_db_rows(&rows)?;
                Ok(FromDbRows::from(rows))
            }
        }
    }

    /// Implements [DbQuery::insert()] for Sqlx
    async fn insert(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
    ) -> Result<(), DbError> {
        let max_params = match self.pool {
            Pool::SQLite(_) => MAX_PARAMS_SQLITE,
            Pool::PostgreSQL(_) => MAX_PARAMS_POSTGRES,
        };
        let _: Vec<DbRow> = edit(
            self,
            &EditType::Insert,
            &max_params,
            table,
            columns,
            rows,
            false,
            &[],
        )
        .await?;
        Ok(())
    }

    /// Implements [DbQuery::insert_returning()] for Sqlx
    async fn insert_returning<T: FromDbRows>(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
        returning: &[&str],
    ) -> Result<T, DbError> {
        let max_params = match self.pool {
            Pool::SQLite(_) => MAX_PARAMS_SQLITE,
            Pool::PostgreSQL(_) => MAX_PARAMS_POSTGRES,
        };
        edit(
            self,
            &EditType::Insert,
            &max_params,
            table,
            columns,
            rows,
            true,
            returning,
        )
        .await
    }

    /// Implements [DbQuery::update()] for Sqlx.
    async fn update(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
    ) -> Result<(), DbError> {
        let max_params = match self.pool {
            Pool::SQLite(_) => MAX_PARAMS_SQLITE,
            Pool::PostgreSQL(_) => MAX_PARAMS_POSTGRES,
        };
        let _: Vec<DbRow> = edit(
            self,
            &EditType::Update,
            &max_params,
            table,
            columns,
            rows,
            false,
            &[],
        )
        .await?;
        Ok(())
    }

    /// Implements [DbQuery::update_returning()] for Sqlx.
    async fn update_returning<T: FromDbRows>(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
        returning: &[&str],
    ) -> Result<T, DbError> {
        let max_params = match self.pool {
            Pool::SQLite(_) => MAX_PARAMS_SQLITE,
            Pool::PostgreSQL(_) => MAX_PARAMS_POSTGRES,
        };
        edit(
            self,
            &EditType::Update,
            &max_params,
            table,
            columns,
            rows,
            true,
            returning,
        )
        .await
    }

    /// Implements [DbQuery::upsert()] for Sqlx.
    async fn upsert(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
    ) -> Result<(), DbError> {
        let max_params = match self.pool {
            Pool::SQLite(_) => MAX_PARAMS_SQLITE,
            Pool::PostgreSQL(_) => MAX_PARAMS_POSTGRES,
        };
        let _: Vec<DbRow> = edit(
            self,
            &EditType::Upsert,
            &max_params,
            table,
            columns,
            rows,
            false,
            &[],
        )
        .await?;
        Ok(())
    }

    /// Implements [DbQuery::upsert_returning()] for Sqlx.
    async fn upsert_returning<T: FromDbRows>(
        &self,
        table: &str,
        columns: &[&str],
        rows: impl IntoDbRows,
        returning: &[&str],
    ) -> Result<T, DbError> {
        let max_params = match self.pool {
            Pool::SQLite(_) => MAX_PARAMS_SQLITE,
            Pool::PostgreSQL(_) => MAX_PARAMS_POSTGRES,
        };
        edit(
            self,
            &EditType::Upsert,
            &max_params,
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
    use indexmap::{indexmap as column_map, indexmap as db_row};

    // TODO: Remove all these tests later and replace them with the aliases_and_builtins test.
    #[tokio::test]
    async fn test_basic() {
        for url in [":memory:", "postgresql:///rltbl_db"] {
            basic(url).await;
        }
    }

    async fn basic(url: &str) {
        let sql = "DROP TABLE IF EXISTS foo; \
                   CREATE TABLE IF NOT EXISTS foo ( bar INT, jar INT, PRIMARY KEY (bar) ); \
                   INSERT INTO foo (bar) VALUES (1)";

        let pool = SqlxPool::connect(url).await.unwrap();
        pool.execute_batch(sql).await.unwrap();
        let rows: Vec<DbRow> = pool.query("SELECT * from foo", ()).await.unwrap();
        assert_eq!(
            rows,
            match pool.kind() {
                DbKind::SQLite => [db_row! {
                    "bar".into() => ParamValue::from(1_i64),
                    "jar".into() => ParamValue::from(0_i64),
                }],
                DbKind::PostgreSQL => [db_row! {
                    "bar".into() => ParamValue::from(1_i32),
                    "jar".into() => ParamValue::Null,
                }],
            }
        );
    }

    #[tokio::test]
    async fn test_columns() {
        for url in [":memory:", "postgresql:///rltbl_db"] {
            columns(url).await;
        }
    }

    async fn columns(url: &str) {
        let sql = "DROP TABLE IF EXISTS foo; \
                   CREATE TABLE IF NOT EXISTS foo ( bar INT, jar INT, PRIMARY KEY (bar) ); \
                   INSERT INTO foo (bar) VALUES (1)";

        let pool = SqlxPool::connect(url).await.unwrap();
        pool.execute_batch(sql).await.unwrap();

        if pool.kind() == DbKind::SQLite {
            let columns = pool.columns("foo").await.unwrap();
            assert_eq!(
                columns,
                column_map! {
                    "bar".to_string() => "int".to_string(),
                    "jar".to_string() => "int".to_string(),
                }
            );
            let primary_keys = pool.primary_keys("foo").await.unwrap();
            assert_eq!(primary_keys, ["bar"]);
        } else {
            let columns = pool.columns("foo").await.unwrap();
            assert_eq!(
                columns,
                column_map! {
                    "bar".to_string() => "integer".to_string(),
                    "jar".to_string() => "integer".to_string(),
                }
            );
            let primary_keys = pool.primary_keys("foo").await.unwrap();
            assert_eq!(primary_keys, ["bar"]);
        }
    }
}
