use crate::core::DbError;
use indexmap::IndexMap;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, json};
use std::{
    fmt::Display,
    hash::{Hash, Hasher},
};

pub type JsonValue = serde_json::Value;
pub type JsonRow = JsonMap<String, JsonValue>;
pub type DbRow = IndexMap<String, DbValue>;
pub type StringRow = IndexMap<String, String>;
pub type ColumnMap = IndexMap<String, String>;

/// Value types for [query parameters](Params)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum DbValue {
    /// Represents a NULL value. Can be used with any column type.
    Null,
    /// Use with BOOL column types or equivalent.
    Boolean(bool),
    /// Use with INT2 column types or equivalent.
    SmallInteger(i16),
    /// Use with INT4 column types or equivalent.
    Integer(i32),
    /// Use with INT8 column types or equivalent.
    BigInteger(i64),
    /// Use with FLOAT4 column types or equivalent.
    Real(f32),
    /// Use with FLOAT8 column types or equivalent.
    BigReal(f64),
    /// Use with NUMERIC column types or equivalent.
    Numeric(Decimal),
    /// Use with TEXT and VARCHAR column types or equivalent.
    Text(String),
}

impl Hash for DbValue {
    fn hash<H: Hasher>(&self, h: &mut H) {
        match self {
            DbValue::Null => ().hash(h),
            DbValue::Text(txt) => txt.hash(h),
            DbValue::Boolean(num) => num.hash(h),
            DbValue::SmallInteger(num) => num.hash(h),
            DbValue::Integer(num) => num.hash(h),
            DbValue::BigInteger(num) => num.hash(h),
            DbValue::Real(num) => {
                if *num == 0.0f32 {
                    // There are 2 zero representations, +0 and -0, which
                    // compare equal but have different bits. We use the +0 hash
                    // for both so that hash(+0) == hash(-0).
                    0.0f32.to_bits().hash(h)
                } else {
                    num.to_bits().hash(h)
                }
            }
            DbValue::BigReal(num) => {
                if *num == 0.0f64 {
                    // There are 2 zero representations, +0 and -0, which
                    // compare equal but have different bits. We use the +0 hash
                    // for both so that hash(+0) == hash(-0).
                    0.0f64.to_bits().hash(h)
                } else {
                    num.to_bits().hash(h)
                }
            }
            DbValue::Numeric(num) => num.hash(h),
        }
    }
}

impl Display for DbValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let string_value: String = self.into();
        write!(f, "{}", string_value)
    }
}

// Implementations of attempted conversion of DbValues into various types:

impl Into<JsonValue> for DbValue {
    fn into(self) -> JsonValue {
        match self {
            DbValue::Null => JsonValue::Null,
            DbValue::Boolean(value) => JsonValue::Bool(value),
            DbValue::SmallInteger(value) => JsonValue::Number(value.into()),
            DbValue::Integer(value) => JsonValue::Number(value.into()),
            DbValue::BigInteger(value) => JsonValue::Number(value.into()),
            DbValue::Real(value) => json!(value),
            DbValue::BigReal(value) => json!(value),
            DbValue::Numeric(value) => json!(value),
            DbValue::Text(value) => JsonValue::String(value),
        }
    }
}

impl Into<JsonValue> for &DbValue {
    fn into(self) -> JsonValue {
        self.clone().into()
    }
}

impl Into<String> for DbValue {
    fn into(self) -> String {
        match self {
            DbValue::Null => String::new(),
            DbValue::Boolean(val) => val.to_string(),
            DbValue::SmallInteger(number) => number.to_string(),
            DbValue::Integer(number) => number.to_string(),
            DbValue::BigInteger(number) => number.to_string(),
            DbValue::Real(number) => number.to_string(),
            DbValue::BigReal(number) => number.to_string(),
            DbValue::Numeric(decimal) => decimal.to_string(),
            DbValue::Text(string) => string.to_string(),
        }
    }
}

impl Into<String> for &DbValue {
    fn into(self) -> String {
        self.clone().into()
    }
}

impl TryInto<u64> for DbValue {
    type Error = DbError;

    fn try_into(self) -> Result<u64, DbError> {
        match self {
            DbValue::SmallInteger(number) => {
                Ok(u64::try_from(number).map_err(|err| DbError::InputError(err.to_string()))?)
            }
            DbValue::Integer(number) => {
                Ok(u64::try_from(number).map_err(|err| DbError::InputError(err.to_string()))?)
            }
            DbValue::BigInteger(number) => {
                Ok(u64::try_from(number).map_err(|err| DbError::InputError(err.to_string()))?)
            }
            _ => Err(DbError::InputError(format!(
                "Not an unsigned integer: {self:?}"
            ))),
        }
    }
}

impl TryInto<u64> for &DbValue {
    type Error = DbError;

    fn try_into(self) -> Result<u64, DbError> {
        self.clone().try_into()
    }
}

impl TryInto<i64> for DbValue {
    type Error = DbError;

    fn try_into(self) -> Result<i64, DbError> {
        match self {
            DbValue::SmallInteger(number) => {
                Ok(i64::try_from(number).map_err(|err| DbError::InputError(err.to_string()))?)
            }
            DbValue::Integer(number) => {
                Ok(i64::try_from(number).map_err(|err| DbError::InputError(err.to_string()))?)
            }
            DbValue::BigInteger(number) => {
                Ok(i64::try_from(number).map_err(|err| DbError::InputError(err.to_string()))?)
            }
            _ => Err(DbError::InputError(format!("Not an integer: {self:?}"))),
        }
    }
}

impl TryInto<i64> for &DbValue {
    type Error = DbError;

    fn try_into(self) -> Result<i64, DbError> {
        self.clone().try_into()
    }
}

impl TryInto<f64> for DbValue {
    type Error = DbError;

    fn try_into(self) -> Result<f64, DbError> {
        match self {
            DbValue::Real(number) => {
                Ok(f64::try_from(number).map_err(|err| DbError::InputError(err.to_string()))?)
            }
            DbValue::BigReal(number) => {
                Ok(f64::try_from(number).map_err(|err| DbError::InputError(err.to_string()))?)
            }
            DbValue::Numeric(number) => {
                Ok(f64::try_from(number).map_err(|err| DbError::InputError(err.to_string()))?)
            }
            _ => Err(DbError::InputError(format!("Not an integer: {self:?}"))),
        }
    }
}

impl TryInto<f64> for &DbValue {
    type Error = DbError;

    fn try_into(self) -> Result<f64, DbError> {
        self.clone().try_into()
    }
}

impl TryInto<f32> for DbValue {
    type Error = DbError;

    fn try_into(self) -> Result<f32, DbError> {
        match self {
            DbValue::Real(number) => {
                Ok(f32::try_from(number).map_err(|err| DbError::InputError(err.to_string()))?)
            }
            DbValue::BigReal(number) => Ok(number as f32),
            DbValue::Numeric(number) => {
                Ok(f32::try_from(number).map_err(|err| DbError::InputError(err.to_string()))?)
            }
            _ => Err(DbError::InputError(format!("Not an integer: {self:?}"))),
        }
    }
}

impl TryInto<f32> for &DbValue {
    type Error = DbError;

    fn try_into(self) -> Result<f32, DbError> {
        self.clone().try_into()
    }
}

impl TryInto<bool> for DbValue {
    type Error = DbError;

    fn try_into(self) -> Result<bool, DbError> {
        match self {
            DbValue::Boolean(value) => Ok(value),
            DbValue::Integer(value) => Ok(value == 1),
            _ => Err(DbError::InputError(format!("Not a boolean: {self:?}"))),
        }
    }
}

impl TryInto<bool> for &DbValue {
    type Error = DbError;

    fn try_into(self) -> Result<bool, DbError> {
        self.clone().try_into()
    }
}

// Implementations of attempted conversions of various types into DbValues:

impl From<&str> for DbValue {
    fn from(item: &str) -> Self {
        DbValue::Text(item.to_string())
    }
}

impl From<String> for DbValue {
    fn from(item: String) -> Self {
        DbValue::Text(item)
    }
}

impl From<&String> for DbValue {
    fn from(item: &String) -> Self {
        DbValue::Text(item.clone())
    }
}

impl From<i16> for DbValue {
    fn from(item: i16) -> Self {
        DbValue::SmallInteger(item)
    }
}

impl From<i32> for DbValue {
    fn from(item: i32) -> Self {
        DbValue::Integer(item.into())
    }
}

impl From<i64> for DbValue {
    fn from(item: i64) -> Self {
        DbValue::BigInteger(item)
    }
}

impl From<u16> for DbValue {
    fn from(item: u16) -> Self {
        DbValue::Integer(item.into())
    }
}

impl From<u32> for DbValue {
    fn from(item: u32) -> Self {
        if usize::BITS <= 31 {
            DbValue::Integer(item as i32)
        } else {
            DbValue::BigInteger(item as i64)
        }
    }
}

impl From<u64> for DbValue {
    fn from(item: u64) -> Self {
        if item <= i64::MAX as u64 {
            DbValue::BigInteger(item as i64)
        } else {
            DbValue::Numeric(Decimal::from(item))
        }
    }
}

impl From<isize> for DbValue {
    fn from(item: isize) -> Self {
        if isize::BITS <= 32 {
            DbValue::Integer(item as i32)
        } else if isize::BITS <= 64 {
            DbValue::BigInteger(item as i64)
        } else {
            DbValue::Numeric(Decimal::from(item))
        }
    }
}

impl From<usize> for DbValue {
    fn from(item: usize) -> Self {
        if usize::BITS <= 31 {
            DbValue::Integer(item as i32)
        } else if usize::BITS <= 63 {
            DbValue::BigInteger(item as i64)
        } else {
            DbValue::Numeric(Decimal::from(item))
        }
    }
}

impl From<f32> for DbValue {
    fn from(item: f32) -> Self {
        DbValue::Real(item)
    }
}

impl From<f64> for DbValue {
    fn from(item: f64) -> Self {
        DbValue::BigReal(item)
    }
}

impl From<Decimal> for DbValue {
    fn from(item: Decimal) -> Self {
        DbValue::Numeric(item)
    }
}

impl From<bool> for DbValue {
    fn from(item: bool) -> Self {
        DbValue::Boolean(item)
    }
}

impl From<JsonValue> for DbValue {
    fn from(item: JsonValue) -> Self {
        match &item {
            JsonValue::Null => Self::Null,
            JsonValue::Bool(val) => Self::Boolean(*val),
            JsonValue::Number(number) => {
                if number.is_u64() {
                    Self::from(number.as_u64().unwrap())
                } else if number.is_i64() {
                    Self::from(number.as_i64().unwrap())
                } else if number.is_f64() {
                    Self::BigReal(number.as_f64().unwrap())
                } else {
                    Self::Text(item.to_string())
                }
            }
            JsonValue::String(string) => Self::Text(string.to_string()),
            JsonValue::Array(_) => Self::Text(item.to_string()),
            JsonValue::Object(_) => Self::Text(item.to_string()),
        }
    }
}

impl From<()> for DbValue {
    fn from(_: ()) -> Self {
        DbValue::Null
    }
}

// f32 and f64 don't implement PartialEq, so we have to do it ourselves.
impl PartialEq for DbValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DbValue::Null, DbValue::Null) => true,
            (DbValue::Boolean(a), DbValue::Boolean(b)) => a == b,
            (DbValue::SmallInteger(a), DbValue::SmallInteger(b)) => a == b,
            (DbValue::Integer(a), DbValue::Integer(b)) => a == b,
            (DbValue::BigInteger(a), DbValue::BigInteger(b)) => a == b,
            (DbValue::Real(a), DbValue::Real(b)) => {
                if a.is_finite() && b.is_finite() {
                    a == b
                } else {
                    false
                }
            }
            (DbValue::BigReal(a), DbValue::BigReal(b)) => {
                if a.is_finite() && b.is_finite() {
                    a == b
                } else {
                    false
                }
            }
            (DbValue::Numeric(a), DbValue::Numeric(b)) => a == b,
            (DbValue::Text(a), DbValue::Text(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for DbValue {}

/// Types that implement this trait can be converted into a [DbValue].
pub trait IntoDbValue {
    fn into_param_value(self) -> DbValue;
}

/// Implements [IntoDbValue] for types that implement [TryFrom] for [DbValue].
impl<T: Into<DbValue>> IntoDbValue for T {
    fn into_param_value(self) -> DbValue {
        self.into()
    }
}

/// Query parameters
#[derive(Debug, Clone)]
pub enum Params {
    None,
    Positional(Vec<DbValue>),
}

/// Types that implement this trait can be converted into [Params]
pub trait IntoParams {
    fn into_params(self) -> Params;
}

/// (Trivially) implements [IntoParams] for [Params]
impl IntoParams for Params {
    fn into_params(self) -> Params {
        self
    }
}

/// Implements [IntoParams] for references to [Params]
impl IntoParams for &Params {
    fn into_params(self) -> Params {
        self.clone()
    }
}

/// Implements [IntoParams] for an empty tuple. Always returns [Params::None].
impl IntoParams for () {
    fn into_params(self) -> Params {
        Params::None
    }
}

/// Implements [IntoParams] for fixed-length arrays of types that implement [IntoDbValue]
impl<T: IntoDbValue, const N: usize> IntoParams for [T; N] {
    fn into_params(self) -> Params {
        self.into_iter().collect::<Vec<_>>().into_params()
    }
}

/// Implements [IntoParams] for references to fixed-length arrays of types that implement
/// [IntoDbValue]
impl<T: IntoDbValue + Clone, const N: usize> IntoParams for &[T; N] {
    fn into_params(self) -> Params {
        self.iter().cloned().collect::<Vec<_>>().into_params()
    }
}

/// Implements [IntoParams] for vectors of types that implement [IntoDbValue]
impl<T: IntoDbValue> IntoParams for Vec<T> {
    fn into_params(self) -> Params {
        let values = self
            .into_iter()
            .map(|i| i.into_param_value())
            .collect::<Vec<_>>();
        Params::Positional(values)
    }
}

// Traits for converting to and from vectors of DbRows:

/// Enables conversion from something into a vector of [DbRow]s
pub trait IntoDbRows {
    fn into_db_rows(self) -> Vec<DbRow>;
}

impl IntoDbRows for Vec<DbRow> {
    fn into_db_rows(self) -> Vec<DbRow> {
        self
    }
}

impl IntoDbRows for &Vec<DbRow> {
    fn into_db_rows(self) -> Vec<DbRow> {
        self.clone()
    }
}

impl IntoDbRows for &[DbRow] {
    fn into_db_rows(self) -> Vec<DbRow> {
        self.to_vec()
    }
}

impl IntoDbRows for &[&DbRow] {
    fn into_db_rows(self) -> Vec<DbRow> {
        self.into_iter()
            .cloned()
            .map(|row| row.clone())
            .collect::<Vec<_>>()
    }
}

impl<const N: usize> IntoDbRows for &[&DbRow; N] {
    fn into_db_rows(self) -> Vec<DbRow> {
        self.into_iter()
            .cloned()
            .map(|row| row.clone())
            .collect::<Vec<_>>()
    }
}

impl IntoDbRows for Vec<JsonRow> {
    fn into_db_rows(self) -> Vec<DbRow> {
        self.into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|(key, val)| (key, DbValue::from(val)))
                    .collect()
            })
            .collect::<Vec<_>>()
    }
}

impl IntoDbRows for &Vec<JsonRow> {
    fn into_db_rows(self) -> Vec<DbRow> {
        self.clone().into_db_rows()
    }
}

/// Enables conversion from a vector of [DbRow]s into something.
pub trait FromDbRows {
    fn from(rows: Vec<DbRow>) -> Self;
}

impl FromDbRows for Vec<StringRow> {
    fn from(rows: Vec<DbRow>) -> Self {
        rows.iter()
            .map(|row| {
                row.iter()
                    .map(|(key, value)| (key.clone(), value.into()))
                    .collect()
            })
            .collect()
    }
}

impl FromDbRows for Vec<JsonRow> {
    fn from(rows: Vec<DbRow>) -> Self {
        rows.into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|(key, val)| (key, val.into()))
                    .collect()
            })
            .collect::<Vec<_>>()
    }
}

impl FromDbRows for Vec<DbRow> {
    fn from(rows: Vec<DbRow>) -> Self {
        rows
    }
}

/// Enables conversion from something into a [DbRow]
pub trait IntoDbRow {
    fn into_db_row(self) -> DbRow;
}

impl IntoDbRow for DbRow {
    fn into_db_row(self) -> DbRow {
        self
    }
}

impl IntoDbRow for JsonRow {
    fn into_db_row(self) -> DbRow {
        self.into_iter()
            .map(|(key, val)| (key, val.into()))
            .collect()
    }
}

/// Enables conversion from a [DbRow] into something.
pub trait FromDbRow {
    fn from(row: DbRow) -> Self;
}

impl FromDbRow for DbRow {
    fn from(row: DbRow) -> Self {
        row
    }
}

impl FromDbRow for JsonRow {
    fn from(row: DbRow) -> Self {
        row.into_iter()
            .map(|(key, val)| (key, val.into()))
            .collect()
    }
}

/// Converts a list of assorted types implementing [IntoDbValue] into [Params]
#[macro_export]
macro_rules! params {
    () => {
       ()
    };
    ($($value:expr),* $(,)?) => {{
        use $crate::db_value::IntoDbValue;
        [$($value.into_param_value()),*]

    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::dec;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_hashing() {
        let mut test_map = HashMap::new();
        for (i, value) in [
            DbValue::Null,
            DbValue::Text("NULL".to_string()),
            DbValue::Boolean(true),
            DbValue::SmallInteger(1),
            DbValue::BigInteger(1),
            DbValue::Real(0.0f32),
            DbValue::Real(0.456f32),
            DbValue::BigReal(0.123f64),
            DbValue::BigReal(0.0f64),
            DbValue::Numeric(dec!(1)),
        ]
        .iter()
        .enumerate()
        {
            test_map.insert(value.clone(), i);
            assert_eq!(*test_map.get(&value).unwrap(), i);
        }
    }
}
