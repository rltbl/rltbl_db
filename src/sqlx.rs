//! sqlx implementation for rltbl_db.

use crate::{
    core::{
        CachingStrategy, DbError, DbQuery, DbRow, FromDbRows, IntoDbRows, IntoParams, ParamValue,
        Params,
    },
    db_kind::{DbKind, MAX_PARAMS_POSTGRES, MAX_PARAMS_SQLITE},
    shared::{EditType, edit},
};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use sqlx::{
    AnyPool, Column, Execute, Postgres, Row, Statement, TypeInfo,
    any::{Any, AnyRow, install_default_drivers},
    postgres::{PgArguments, PgPool, PgPoolOptions, PgRow},
    query::Query,
};

/// Convert the given PostgreSQL database rows to [DbRow]s
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
                    Err(_) => db_row.insert(cname.to_string(), ParamValue::Null),
                },
                "INT2" => match pg_row.try_get::<i16, usize>(idx) {
                    Ok(value) => db_row.insert(cname.to_string(), value.into()),
                    Err(_) => db_row.insert(cname.to_string(), ParamValue::Null),
                },
                "INT4" => match pg_row.try_get::<i32, usize>(idx) {
                    Ok(value) => db_row.insert(cname.to_string(), value.into()),
                    Err(_) => db_row.insert(cname.to_string(), ParamValue::Null),
                },
                "INT8" => match pg_row.try_get::<i64, usize>(idx) {
                    Ok(value) => db_row.insert(cname.to_string(), value.into()),
                    Err(_) => db_row.insert(cname.to_string(), ParamValue::Null),
                },
                "BOOL" => match pg_row.try_get::<bool, usize>(idx) {
                    Ok(value) => db_row.insert(cname.to_string(), value.into()),
                    Err(_) => db_row.insert(cname.to_string(), ParamValue::Null),
                },
                "FLOAT4" => match pg_row.try_get::<f32, usize>(idx) {
                    Ok(value) => db_row.insert(cname.to_string(), value.into()),
                    Err(_) => db_row.insert(cname.to_string(), ParamValue::Null),
                },
                "FLOAT8" => match pg_row.try_get::<f64, usize>(idx) {
                    Ok(value) => db_row.insert(cname.to_string(), value.into()),
                    Err(_) => db_row.insert(cname.to_string(), ParamValue::Null),
                },
                "NUMERIC" => match pg_row.try_get::<Decimal, usize>(idx) {
                    Ok(value) => db_row.insert(cname.to_string(), value.into()),
                    Err(_) => db_row.insert(cname.to_string(), ParamValue::Null),
                },
                _ => {
                    return Err(DbError::DataError(format!(
                        "Unimplemented column type: {column:?}"
                    )));
                }
            };
        }
        db_rows.push(db_row);
    }
    Ok(db_rows)
}

/// Convert the given SQLite database rows to [DbRow]s
fn sqlite_to_db_rows(sqlite_rows: &Vec<AnyRow>) -> Result<Vec<DbRow>, DbError> {
    let mut db_rows = vec![];
    for sqlite_row in sqlite_rows {
        let mut db_row = DbRow::new();
        for (idx, column) in sqlite_row.columns().iter().enumerate() {
            let cname: &str = column.name();
            let ctype: &str = column.type_info().name();
            match ctype {
                "TEXT" | "VARCHAR" => match sqlite_row.try_get::<&str, usize>(idx) {
                    Ok(value) => db_row.insert(cname.to_string(), value.into()),
                    Err(_) => db_row.insert(cname.to_string(), ParamValue::Null),
                },
                "BIGINT" | "INTEGER" => match sqlite_row.try_get::<i64, usize>(idx) {
                    Ok(value) => db_row.insert(cname.to_string(), value.into()),
                    Err(_) => db_row.insert(cname.to_string(), ParamValue::Null),
                },
                "FLOAT" | "REAL" => match sqlite_row.try_get::<f32, usize>(idx) {
                    Ok(value) => db_row.insert(cname.to_string(), value.into()),
                    Err(_) => db_row.insert(cname.to_string(), ParamValue::Null),
                },
                "DOUBLE" => match sqlite_row.try_get::<f64, usize>(idx) {
                    Ok(value) => db_row.insert(cname.to_string(), value.into()),
                    Err(_) => db_row.insert(cname.to_string(), ParamValue::Null),
                },
                _ => {
                    // We had problems getting a type for columns that are not in the schema,
                    // e.g. "SELECT COUNT() AS count".
                    // So now we start with Null and try INTEGER, NUMERIC/REAL, STRING, BOOL.
                    let mut value = ParamValue::Null;
                    if value == ParamValue::Null {
                        let x: Result<i64, sqlx::Error> = sqlite_row.try_get(column.ordinal());
                        if let Ok(x) = x {
                            value = ParamValue::from(x);
                        }
                    }
                    if value == ParamValue::Null {
                        let x: Result<f64, sqlx::Error> = sqlite_row.try_get(column.ordinal());
                        if let Ok(x) = x {
                            value = ParamValue::from(x);
                        }
                    }
                    if value == ParamValue::Null {
                        let x: Result<String, sqlx::Error> = sqlite_row.try_get(column.ordinal());
                        if let Ok(x) = x {
                            value = ParamValue::from(x);
                        }
                    }
                    db_row.insert(column.name().into(), value)
                }
            };
        }
        db_rows.push(db_row);
    }
    Ok(db_rows)
}

/// Wrapper around [AnyPool] and [PgPool].
#[derive(Debug)]
pub enum Pool {
    /// Wrapper for [AnyPool]
    SQLite(AnyPool),
    /// Wrapper for [PgPool]
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
    /// Connect to a database using the given url.
    pub async fn connect(url: &str) -> Result<Self, DbError> {
        if url.starts_with("postgresql://") {
            let pool = PgPoolOptions::new()
                .connect(url)
                .await
                .map_err(|err| DbError::ConnectError(err.to_string()))?;
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

            install_default_drivers();
            let pool = AnyPool::connect(&url)
                .await
                .map_err(|err| DbError::ConnectError(err.to_string()))?;
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
                sqlx::raw_sql(sql)
                    .execute(pool)
                    .await
                    .map_err(|err| DbError::DatabaseError(err.to_string()))?;
            }
            Pool::PostgreSQL(pool) => {
                sqlx::raw_sql(sql)
                    .execute(pool)
                    .await
                    .map_err(|err| DbError::DatabaseError(err.to_string()))?;
            }
        };
        Ok(())
    }

    /// Implements [DbQuery::query_no_cache()] for Sqlx.
    async fn query_no_cache<T: FromDbRows>(
        &self,
        sql: &str,
        params: impl IntoParams + Send,
    ) -> Result<T, DbError> {
        fn create_pg_query<'a>(
            sql: &'a str,
            params: &'a Params,
        ) -> Query<'a, Postgres, PgArguments> {
            let mut query = sqlx::query::<Postgres>(sql);
            match &params {
                Params::None => (),
                Params::Positional(params) => {
                    for (param_idx, param) in params.iter().enumerate() {
                        match param {
                            ParamValue::Null => {
                                let statement = query.statement();
                                match statement {
                                    Some(statement) => {
                                        let column = &statement.columns()[param_idx];
                                        let ctype: &str = column.type_info().name();
                                        match ctype {
                                            "TEXT" | "VARCHAR" | "NAME" => {
                                                query = query.bind(None::<String>)
                                            }
                                            "INT2" => query = query.bind(None::<i64>),
                                            "INT4" => query = query.bind(None::<i64>),
                                            "INT8" => query = query.bind(None::<i64>),
                                            "BOOL" => query = query.bind(None::<bool>),
                                            "FLOAT4" => query = query.bind(None::<f64>),
                                            "FLOAT8" => query = query.bind(None::<f64>),
                                            "NUMERIC" => query = query.bind(None::<Decimal>),
                                            _ => unimplemented!(
                                                "Unimplemented column type: {column:?}"
                                            ),
                                        }
                                    }
                                    None => {
                                        let match_val = rand::random_range(0..5);
                                        if match_val == 0 {
                                            query = query.bind(None::<String>)
                                        } else if match_val == 1 {
                                            query = query.bind(None::<i64>)
                                        } else if match_val == 2 {
                                            query = query.bind(None::<bool>)
                                        } else if match_val == 3 {
                                            query = query.bind(None::<f64>)
                                        } else if match_val == 4 {
                                            query = query.bind(None::<Decimal>)
                                        }
                                    }
                                }
                            }
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
            }
            query
        }

        async fn query_no_cache_pg(
            pool: &sqlx::Pool<Postgres>,
            sql: &str,
            params: &Params,
        ) -> Result<Vec<DbRow>, DbError> {
            let mut saved_err: Option<DbError> = None;
            // TODO: Think of a better solution ...
            let max_attempts = 1000;
            for _ in 0..max_attempts {
                let query = create_pg_query(sql, &params);
                match query.fetch_all(pool).await {
                    Ok(rows) => {
                        let rows = pg_to_db_rows(&rows)?;
                        return Ok(FromDbRows::from(rows));
                    }
                    Err(err) => {
                        saved_err = Some(DbError::DatabaseError(format!(
                            "Giving up after {max_attempts}. Last error was: '{err}', while \
                             executing SQL: '{sql}'"
                        )))
                    }
                }
            }
            Err(saved_err.unwrap())
        }

        async fn query_no_cache_sqlite(
            pool: &sqlx::Pool<Any>,
            sql: &str,
            params: &Params,
        ) -> Result<Vec<DbRow>, DbError> {
            let mut query = sqlx::query::<Any>(sql);
            match &params {
                Params::None => (),
                Params::Positional(params) => {
                    for param in params {
                        match param {
                            // It is alright to use None::<String> to represent a NULL value
                            // regardless of whether that is the actual type of the underlying
                            // column, because SQLite is permissive enough for this not to
                            // matter.
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
            let rows = query
                .fetch_all(pool)
                .await
                .map_err(|err| DbError::DatabaseError(err.to_string()))?;
            let rows = sqlite_to_db_rows(&rows)?;
            Ok(rows)
        }

        let params = params.into_params();
        match &self.pool {
            Pool::PostgreSQL(pool) => {
                let rows = query_no_cache_pg(pool, sql, &params).await?;
                Ok(FromDbRows::from(rows))
            }
            Pool::SQLite(pool) => {
                let rows = query_no_cache_sqlite(pool, sql, &params).await?;
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

    /// This test is resource intensive and therefore ignored by default. It verifies that
    /// using [MAX_PARAMS_SQLITE] parameters in a query is indeed supported.
    /// To run this and other ignored tests, use `cargo test -- --ignored` or
    /// `cargo test -- --include-ignored`
    #[tokio::test]
    #[ignore]
    async fn test_max_params() {
        let pool = SqlxPool::connect("test.db").await.unwrap();
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

        let pool = SqlxPool::connect("postgresql:///rltbl_db").await.unwrap();
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
        while n <= MAX_PARAMS_POSTGRES {
            values.push(format!(
                "(${}, ${}, ${}, ${}, ${}, ${})",
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
