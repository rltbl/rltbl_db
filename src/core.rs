use rust_decimal::Decimal;
use serde_json::Map as JsonMap;
use std::future::Future;

pub type JsonValue = serde_json::Value;
pub type JsonRow = JsonMap<String, JsonValue>;

pub enum DbKind {
    SQLite,
    PostgreSQL,
}

#[derive(Clone, Debug)]
#[non_exhaustive] // We may add more specific error types in the future.
pub enum DbError {
    /// An error that occurred while connecting to a database.
    ConnectError(String),
    /// An error in the arguments to a function that accessed the database.
    InputError(String),
    /// An error in the data retrieved from the database.
    DataError(String),
    /// An error that originated from the database.
    DatabaseError(String),
}

impl std::error::Error for DbError {}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DbError::ConnectError(err)
            | DbError::DataError(err)
            | DbError::InputError(err)
            | DbError::DatabaseError(err) => write!(f, "{err}"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ParamValue {
    Null,
    Boolean(bool),
    SmallInteger(i16),
    Integer(i32),
    BigInteger(i64),
    Real(f32),
    BigReal(f64),
    Numeric(Decimal),
    Text(String),
}

impl TryFrom<&str> for ParamValue {
    type Error = DbError;

    fn try_from(item: &str) -> Result<Self, DbError> {
        Ok(ParamValue::Text(item.to_string()))
    }
}

impl TryFrom<i16> for ParamValue {
    type Error = DbError;

    fn try_from(item: i16) -> Result<Self, DbError> {
        Ok(ParamValue::SmallInteger(item))
    }
}

impl TryFrom<i32> for ParamValue {
    type Error = DbError;

    fn try_from(item: i32) -> Result<Self, DbError> {
        Ok(ParamValue::Integer(item.into()))
    }
}

impl TryFrom<i64> for ParamValue {
    type Error = DbError;

    fn try_from(item: i64) -> Result<Self, DbError> {
        Ok(ParamValue::BigInteger(item))
    }
}

impl TryFrom<f32> for ParamValue {
    type Error = DbError;

    fn try_from(item: f32) -> Result<Self, DbError> {
        Ok(ParamValue::Real(item))
    }
}

impl TryFrom<f64> for ParamValue {
    type Error = DbError;

    fn try_from(item: f64) -> Result<Self, DbError> {
        Ok(ParamValue::BigReal(item))
    }
}

impl TryFrom<Decimal> for ParamValue {
    type Error = DbError;

    fn try_from(item: Decimal) -> Result<Self, DbError> {
        Ok(ParamValue::Numeric(item))
    }
}

impl TryFrom<bool> for ParamValue {
    type Error = DbError;

    fn try_from(item: bool) -> Result<Self, DbError> {
        Ok(ParamValue::Boolean(item))
    }
}

#[derive(Debug, Clone)]
pub enum Params {
    None,
    Positional(Vec<ParamValue>),
    Named(Vec<(String, ParamValue)>),
}

pub trait IntoParamValue {
    fn into_param_value(self) -> Result<ParamValue, DbError>;
}

impl<T> IntoParamValue for T
where
    T: TryInto<ParamValue>,
    T::Error: Into<DbError>,
{
    fn into_param_value(self) -> Result<ParamValue, DbError> {
        self.try_into()
            .map_err(|e| DbError::DataError(e.into().to_string()))
    }
}

impl IntoParamValue for Result<ParamValue, DbError> {
    fn into_param_value(self) -> Result<ParamValue, DbError> {
        self
    }
}

pub trait IntoParams {
    /// TODO: Add docstring here.
    fn into_params(self) -> Result<Params, DbError>;
}

impl IntoParams for () {
    fn into_params(self) -> Result<Params, DbError> {
        Ok(Params::None)
    }
}

impl IntoParams for Params {
    fn into_params(self) -> Result<Params, DbError> {
        Ok(self)
    }
}

impl IntoParams for &Params {
    fn into_params(self) -> Result<Params, DbError> {
        Ok(self.clone())
    }
}

impl<T: IntoParamValue, const N: usize> IntoParams for [T; N] {
    fn into_params(self) -> Result<Params, DbError> {
        self.into_iter().collect::<Vec<_>>().into_params()
    }
}

impl<T: IntoParamValue + Clone, const N: usize> IntoParams for &[T; N] {
    fn into_params(self) -> Result<Params, DbError> {
        self.iter().cloned().collect::<Vec<_>>().into_params()
    }
}

impl<T: IntoParamValue> IntoParams for Vec<T> {
    fn into_params(self) -> Result<Params, DbError> {
        let values = self
            .into_iter()
            .map(|i| i.into_param_value())
            .collect::<Result<Vec<_>, DbError>>()?;
        Ok(Params::Positional(values))
    }
}

#[macro_export]
macro_rules! params {
    () => {
       ()
    };
    ($($value:expr),* $(,)?) => {{
        use $crate::core::IntoParamValue;
        [$($value.into_param_value()),*]

    }};
}

pub trait DbQuery {
    /// Execute a SQL command, without a return value.
    fn execute(
        &self,
        sql: &str,
        params: &[JsonValue],
    ) -> impl Future<Output = Result<(), DbError>> + Send;
    /// Execute a SQL command, without a return value.
    fn execute_new(
        &self,
        sql: &str,
        params: impl IntoParams + Send + Clone + 'static,
    ) -> impl Future<Output = Result<(), DbError>> + Send;
    /// Sequentially execute a semicolon-delimited list of statements, without parameters.
    fn execute_batch(&self, sql: &str) -> impl Future<Output = Result<(), DbError>> + Send;
    /// Execute a SQL command, returning a vector of JSON rows.
    fn query(
        &self,
        sql: &str,
        params: &[JsonValue],
    ) -> impl Future<Output = Result<Vec<JsonRow>, DbError>> + Send;
    /// Execute a SQL command, returning a vector of JSON rows.
    fn query_new(
        &self,
        sql: &str,
        params: impl IntoParams + Send + 'static,
    ) -> impl Future<Output = Result<Vec<JsonRow>, DbError>> + Send;
    /// Execute a SQL command, returning a single JSON row.
    fn query_row(
        &self,
        sql: &str,
        params: &[JsonValue],
    ) -> impl Future<Output = Result<JsonRow, DbError>> + Send;
    /// Execute a SQL command, returning a single JSON value.
    fn query_value(
        &self,
        sql: &str,
        params: &[JsonValue],
    ) -> impl Future<Output = Result<JsonValue, DbError>> + Send;
    /// Execute a SQL command, returning a single string.
    fn query_string(
        &self,
        sql: &str,
        params: &[JsonValue],
    ) -> impl Future<Output = Result<String, DbError>> + Send;
    /// Execute a SQL command, returning a single unsigned integer.
    fn query_u64(
        &self,
        sql: &str,
        params: &[JsonValue],
    ) -> impl Future<Output = Result<u64, DbError>> + Send;
    /// Execute a SQL command, returning a single signed integer.
    fn query_i64(
        &self,
        sql: &str,
        params: &[JsonValue],
    ) -> impl Future<Output = Result<i64, DbError>> + Send;
    /// Execute a SQL command, returning a single float.
    fn query_f64(
        &self,
        sql: &str,
        params: &[JsonValue],
    ) -> impl Future<Output = Result<f64, DbError>> + Send;
}
