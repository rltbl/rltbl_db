//! Database values.
//!
//! Our goal with rltbl_db is to provide core functionality of SQL databases. Each SQL database
//! uses different types -- SQLite has very few, while Postgres has many. So we need to abstract
//! over differences in these types.
//!
//! We do this by defining a [Value] enum, which includes null, wraps Rust primitives:
//! booleans, integers, floats, and text, as well as a flexbile JSON type, and an "Other" type
//! as a catchall.
//!
//! We can parse a `Value` from a string, and compare and convert between `Value`s of different
//! types. The `values!` macro is a convenient way to create a list of `Value`s, such as when
//! you specify parameters for a query, especially when the values have a mix of types.
//!
//! ```
//! use rltbl_db::{values, Value};
//!
//! let values1 = [
//!      Value::from(2_i64),
//!      Value::from("b"),
//! ];
//! let values2 = values![2_i64, "b"];
//! assert_eq!(values1, values2);
//! ```
//!
//! We implement `From<PRIMITIVE>` into `Value`, and `TryFrom<Value>` into PRIMITIVE,
//! for all the Rust primitives.
//!
//! We implement [serde::Serialize] and [serde::Deserialize] for `Value`. For Rust primitives
//! the serlialization is trivial. We represent complex cases as JSON using `serde_json`.

use rust_decimal::{Decimal, dec};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    cmp::Ordering,
    fmt::Display,
    hash::{Hash, Hasher},
};

use crate::Error;

// A useful alias:
pub type JsonValue = serde_json::Value;

///////////////////////////////////////////////////////////////////////////////
// Value and ValueType
///////////////////////////////////////////////////////////////////////////////

/// The type of a [Value], including the name of the type according to the underlying database,
/// as a [String].
#[derive(Clone, Debug, Hash)]
pub enum ValueType {
    Null(String),
    Boolean(String),
    SmallInteger(String),
    Integer(String),
    BigInteger(String),
    Real(String),
    BigReal(String),
    Numeric(String),
    Text(String),
}

impl Default for ValueType {
    fn default() -> ValueType {
        ValueType::sorted().next().expect("No types defined")
    }
}

impl PartialEq for ValueType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            // Note that Null is a special type and is not equal to any other type,
            // including itself.
            (ValueType::Boolean(_), ValueType::Boolean(_)) => true,
            (ValueType::SmallInteger(_), ValueType::SmallInteger(_)) => true,
            (ValueType::Integer(_), ValueType::Integer(_)) => true,
            (ValueType::BigInteger(_), ValueType::BigInteger(_)) => true,
            (ValueType::Real(_), ValueType::Real(_)) => true,
            (ValueType::BigReal(_), ValueType::BigReal(_)) => true,
            (ValueType::Numeric(_), ValueType::Numeric(_)) => true,
            (ValueType::Text(_), ValueType::Text(_)) => true,
            _ => false,
        }
    }
}

impl Eq for ValueType {}

impl PartialOrd for ValueType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            // Nulls are special and are not assigned an order in the hierarchy.
            (ValueType::Null(_), _) | (_, ValueType::Null(_)) => None,

            (ValueType::Boolean(_), ValueType::Boolean(_)) => Some(Ordering::Equal),
            (ValueType::Boolean(_), _) => Some(Ordering::Less),
            (_, ValueType::Boolean(_)) => Some(Ordering::Greater),

            (ValueType::SmallInteger(_), ValueType::SmallInteger(_)) => Some(Ordering::Equal),
            (ValueType::SmallInteger(_), _) => Some(Ordering::Less),
            (_, ValueType::SmallInteger(_)) => Some(Ordering::Greater),

            (ValueType::Integer(_), ValueType::Integer(_)) => Some(Ordering::Equal),
            (ValueType::Integer(_), _) => Some(Ordering::Less),
            (_, ValueType::Integer(_)) => Some(Ordering::Greater),

            (ValueType::BigInteger(_), ValueType::BigInteger(_)) => Some(Ordering::Equal),
            (ValueType::BigInteger(_), _) => Some(Ordering::Less),
            (_, ValueType::BigInteger(_)) => Some(Ordering::Greater),

            (ValueType::Real(_), ValueType::Real(_)) => Some(Ordering::Equal),
            (ValueType::Real(_), _) => Some(Ordering::Less),
            (_, ValueType::Real(_)) => Some(Ordering::Greater),

            (ValueType::BigReal(_), ValueType::BigReal(_)) => Some(Ordering::Equal),
            (ValueType::BigReal(_), _) => Some(Ordering::Less),
            (_, ValueType::BigReal(_)) => Some(Ordering::Greater),

            (ValueType::Numeric(_), ValueType::Numeric(_)) => Some(Ordering::Equal),
            (ValueType::Numeric(_), _) => Some(Ordering::Less),
            (_, ValueType::Numeric(_)) => Some(Ordering::Greater),

            (ValueType::Text(_), ValueType::Text(_)) => Some(Ordering::Equal),
        }
    }
}

impl ValueType {
    /// Return an iterator over sortable database types.
    pub fn sorted() -> impl Iterator<Item = ValueType> {
        [
            // Note that Null is a special type and is not part of this hierarchy.
            ValueType::Boolean("".to_string()),
            ValueType::SmallInteger("".to_string()),
            ValueType::Integer("".to_string()),
            ValueType::BigInteger("".to_string()),
            ValueType::Real("".to_string()),
            ValueType::BigReal("".to_string()),
            ValueType::Numeric("".to_string()),
            ValueType::Text("".to_string()),
        ]
        .into_iter()
    }

    /// Determine the minimum type (see [ValueType::sorted()]) needed to support the given value,
    /// where the latter is given in the form of a string. The minimum type is the first type
    /// in the type hierarchy for which we can parse the given value string as an instance of that
    /// type.
    pub fn min_type(&self, value: &str) -> Result<ValueType, Error> {
        // If the value is an empty string, return a Null type and value.
        if value == "" {
            return Ok(ValueType::Null("".to_string()));
        }

        // Otherwise, try to parse it using the available types in order from most to least
        // specific.
        for value_type in ValueType::sorted() {
            if value_type >= *self {
                match value_type.parse_str(value) {
                    Ok(_) => return Ok(value_type),
                    Err(err) => {
                        if let ValueType::Text(_) = value_type {
                            return Err(Error::InputError(format!(
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

    /// Parses a given string representing the value of a database field into a [Value] of this
    /// type.
    pub fn parse_str(&self, value: &str) -> Result<Value, Error> {
        // If the value is a NULL value, then we just return a Null:
        if value == "" {
            return Ok(Value::Null);
        }

        // When parsing from a TSV or CSV file we will often encounter strings like "true",
        // "false", "f", "t", "0", and "1", possibly in uppercase or mixed case. When the current
        // type is a boolean, these should be interpreted as booleans. Otherwise, if the current
        // type is a number type, then these should be interpreted as numbers of that type (with
        // true => 1 and false => 0), otherwise if the current type is text then *all* values
        // should be interpreted as text.
        match self {
            ValueType::Null(_) => Ok(Value::Null),
            ValueType::Boolean(_) => match value.to_lowercase().as_str() {
                "0" | "false" | "f" => Ok(Value::Boolean(false)),
                "1" | "true" | "t" => Ok(Value::Boolean(true)),
                _ => {
                    let value = value
                        .parse::<bool>()
                        .map_err(|_| Error::InputError(format!("Not a boolean: '{value}'")))?;
                    Ok(Value::Boolean(value))
                }
            },
            ValueType::SmallInteger(_) => match value.to_lowercase().as_str() {
                "0" | "false" | "f" => Ok(Value::SmallInteger(0)),
                "1" | "true" | "t" => Ok(Value::SmallInteger(1)),
                _ => {
                    let value = value
                        .parse::<i16>()
                        .map_err(|_| Error::InputError(format!("Not an i16: '{value}'")))?;
                    Ok(Value::SmallInteger(value))
                }
            },
            ValueType::Integer(_) => match value.to_lowercase().as_str() {
                "0" | "false" | "f" => Ok(Value::Integer(0)),
                "1" | "true" | "t" => Ok(Value::Integer(1)),
                _ => {
                    let value = value
                        .parse::<i32>()
                        .map_err(|_| Error::InputError(format!("Not an i32: '{value}'")))?;
                    Ok(Value::Integer(value))
                }
            },
            ValueType::BigInteger(_) => match value.to_lowercase().as_str() {
                "0" | "false" | "f" => Ok(Value::BigInteger(0)),
                "1" | "true" | "t" => Ok(Value::BigInteger(1)),
                _ => {
                    let value = value
                        .parse::<i64>()
                        .map_err(|_| Error::InputError(format!("Not an i64: '{value}'")))?;
                    Ok(Value::BigInteger(value))
                }
            },
            ValueType::Real(_) => match value.to_lowercase().as_str() {
                "0" | "false" | "f" => Ok(Value::Real(0_f32)),
                "1" | "true" | "t" => Ok(Value::Real(1_f32)),
                _ => match value
                    .parse::<f32>()
                    .map_err(|_| Error::InputError(format!("Not an f32: '{value}'")))?
                {
                    f32::INFINITY => Err(Error::InputError(format!("Not an f32: '{value}'"))),
                    value => Ok(Value::Real(value)),
                },
            },
            ValueType::BigReal(_) => match value.to_lowercase().as_str() {
                "0" | "false" | "f" => Ok(Value::BigReal(0_f64)),
                "1" | "true" | "t" => Ok(Value::BigReal(1_f64)),
                _ => match value
                    .parse::<f64>()
                    .map_err(|_| Error::InputError(format!("Not an f64: '{value}'")))?
                {
                    f64::INFINITY => Err(Error::InputError(format!("Not an f64: '{value}'"))),
                    value => Ok(Value::BigReal(value)),
                },
            },
            ValueType::Numeric(_) => match value.to_lowercase().as_str() {
                "0" | "false" | "f" => Ok(Value::Numeric(dec!(0))),
                "1" | "true" | "t" => Ok(Value::Numeric(dec!(1))),
                _ => {
                    let value = value
                        .parse::<Decimal>()
                        .map_err(|_| Error::InputError(format!("Not a Decimal: '{value}'")))?;
                    Ok(Value::Numeric(value))
                }
            },
            ValueType::Text(_) => Ok(Value::Text(value.to_string())),
        }
    }

    /// Parses a given [JsonValue] representing the value of a database field into a [Value] of
    /// this type.
    pub fn parse_json(&self, value: &JsonValue) -> Result<Value, Error> {
        Ok(Value::from(value))
    }

    /// Parses the given value into a [Value] of this type.
    pub fn parse(&self, value: impl IntoValue) -> Result<Value, Error> {
        let value = value.into_value();
        self.convert(&value)
    }

    /// Converts the given [Value] into a [Value] of this type.
    pub fn convert(&self, value: &Value) -> Result<Value, Error> {
        // First handle NULLs and Text types:
        match self {
            ValueType::Null(_) => match value {
                Value::Null => return Ok(Value::Null),
                value => {
                    return Err(Error::InputError(format!(
                        "Can't convert to {self:?} from {value:?}"
                    )));
                }
            },
            _ => {
                if let Value::Text(value) = value {
                    return Ok(self.parse_str(value)?);
                }
            }
        };

        // Then handle everything else:

        let err_template = |db_value: &Value| {
            Error::InputError(format!("Can't convert to {self:?} from {db_value:?}"))
        };

        match self {
            ValueType::Null(_) => unreachable!(), // Handled above.
            ValueType::Boolean(_) => {
                let value = value.as_bool().ok_or(err_template(value))?;
                Ok(Value::Boolean(value))
            }
            ValueType::SmallInteger(_) => {
                let value = value.as_i16().ok_or(err_template(value))?;
                Ok(Value::SmallInteger(value))
            }
            ValueType::Integer(_) => {
                let value = value.as_i32().ok_or(err_template(value))?;
                Ok(Value::Integer(value))
            }
            ValueType::BigInteger(_) => {
                let value = value.as_i64().ok_or(err_template(value))?;
                Ok(Value::BigInteger(value))
            }
            ValueType::Real(_) => {
                let value = value.as_f32().ok_or(err_template(value))?;
                Ok(Value::Real(value))
            }
            ValueType::BigReal(_) => {
                let value = value.as_f64().ok_or(err_template(value))?;
                Ok(Value::BigReal(value))
            }
            ValueType::Numeric(_) => {
                let value = value.as_decimal().ok_or(err_template(value))?;
                Ok(Value::Numeric(value))
            }
            ValueType::Text(_) => Ok(Value::Text(value.to_string())),
        }
    }
}

// MC: This is much more minimal than what we currently have in rltbl_db. Is this because
// you wanted to start with these basic types and add the others later, or is the
// intention to simplify?
// JO: We should have at least as many types as we currently have in rltbl_db.
// I just used fewer types here as a stub implementation.
// JO: Now I'm thinking that we should handle all the Rust primitives that Serde handles.
// If we do that, we should use the Rust names.
// bool, i8, i16, i32, i64, u8, u16, u32, u64, f32, f64, str -> String, char -> String
// In addition to these: Null, Json, and Other.
// Eventually we want to try Timestamp.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Value {
    /// Represents a NULL value. Can be used with any column type.
    Null,
    /// Use with BOOL column types or equivalent.
    Boolean(bool),
    /// Use with INT2 column types or equivalent.
    BigInteger(i64),
    /// Use with INT4 column types or equivalent.
    Integer(i32),
    /// Use with INT8 column types or equivalent.
    SmallInteger(i16),
    /// Use with FLOAT4 column types or equivalent.
    Real(f32),
    /// Use with FLOAT8 column types or equivalent.
    BigReal(f64),
    /// Use with NUMERIC column types or equivalent.
    Numeric(Decimal),
    /// Use with TEXT and VARCHAR column types or equivalent.
    Text(String),
    /// Use with JSON or JSONB column types or equivalent.
    Json(JsonValue),
    /// Other types that are not explicitly supported, represented by the triple
    /// (other type, raw representation, optional string representation)
    Other(String, Vec<u8>, Option<String>),
}

impl Default for Value {
    fn default() -> Self {
        Self::Text(String::new())
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let string_value: String = self.into();
        write!(f, "{}", string_value)
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, h: &mut H) {
        match self {
            Value::Null => ().hash(h),
            Value::Text(txt) => txt.hash(h),
            Value::Boolean(num) => num.hash(h),
            Value::SmallInteger(num) => num.hash(h),
            Value::Integer(num) => num.hash(h),
            Value::BigInteger(num) => num.hash(h),
            Value::Real(num) => {
                if *num == 0.0f32 {
                    // There are 2 zero representations, +0 and -0, which
                    // compare equal but have different bits. We use the +0 hash
                    // for both so that hash(+0) == hash(-0).
                    0.0f32.to_bits().hash(h)
                } else {
                    num.to_bits().hash(h)
                }
            }
            Value::BigReal(num) => {
                if *num == 0.0f64 {
                    // There are 2 zero representations, +0 and -0, which
                    // compare equal but have different bits. We use the +0 hash
                    // for both so that hash(+0) == hash(-0).
                    0.0f64.to_bits().hash(h)
                } else {
                    num.to_bits().hash(h)
                }
            }
            Value::Numeric(num) => num.hash(h),
            Value::Json(value) => value.hash(h),
            Value::Other(_, _, _) => format!("{self:?}").hash(h),
        }
    }
}

// f32 and f64 don't implement PartialEq, so we have to do it ourselves.
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::SmallInteger(a), Value::SmallInteger(b)) => a == b,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::BigInteger(a), Value::BigInteger(b)) => a == b,
            (Value::Real(a), Value::Real(b)) => {
                if a.is_finite() && b.is_finite() {
                    a == b
                } else {
                    false
                }
            }
            (Value::BigReal(a), Value::BigReal(b)) => {
                if a.is_finite() && b.is_finite() {
                    a == b
                } else {
                    false
                }
            }
            (Value::Numeric(a), Value::Numeric(b)) => a == b,
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Json(a), Value::Json(b)) => a == b,
            (Value::Other(a, b, c), Value::Other(d, e, f)) => a == d && b == e && c == f,
            _ => false,
        }
    }
}

impl Eq for Value {}

// is_*() and as_*() methods for converting a Value to a primitive type.
impl Value {
    // is_*() methods

    pub fn is_null(&self) -> bool {
        self.as_null().is_some()
    }

    pub fn is_bool(&self) -> bool {
        self.as_bool().is_some()
    }

    pub fn is_i8(&self) -> bool {
        self.as_i8().is_some()
    }

    pub fn is_i16(&self) -> bool {
        self.as_i16().is_some()
    }

    pub fn is_i32(&self) -> bool {
        self.as_i32().is_some()
    }

    pub fn is_i64(&self) -> bool {
        self.as_i64().is_some()
    }

    pub fn is_u8(&self) -> bool {
        self.as_u8().is_some()
    }

    pub fn is_u16(&self) -> bool {
        self.as_u16().is_some()
    }

    pub fn is_u32(&self) -> bool {
        self.as_u32().is_some()
    }

    pub fn is_u64(&self) -> bool {
        self.as_u64().is_some()
    }

    pub fn is_f32(&self) -> bool {
        self.as_f32().is_some()
    }

    pub fn is_f64(&self) -> bool {
        self.as_f64().is_some()
    }

    pub fn is_decimal(&self) -> bool {
        self.as_decimal().is_some()
    }

    pub fn is_string(&self) -> bool {
        self.as_str().is_some()
    }

    pub fn is_json(&self) -> bool {
        self.as_json().is_some()
    }

    // as_*() methods

    pub fn as_null(&self) -> Option<()> {
        self.try_into().ok()
    }

    pub fn as_bool(&self) -> Option<bool> {
        self.try_into().ok()
    }

    pub fn as_i8(&self) -> Option<i8> {
        self.try_into().ok()
    }

    pub fn as_i16(&self) -> Option<i16> {
        self.try_into().ok()
    }

    pub fn as_i32(&self) -> Option<i32> {
        self.try_into().ok()
    }

    pub fn as_i64(&self) -> Option<i64> {
        self.try_into().ok()
    }

    pub fn as_u8(&self) -> Option<u8> {
        self.try_into().ok()
    }

    pub fn as_u16(&self) -> Option<u16> {
        self.try_into().ok()
    }

    pub fn as_u32(&self) -> Option<u32> {
        self.try_into().ok()
    }

    pub fn as_u64(&self) -> Option<u64> {
        self.try_into().ok()
    }

    pub fn as_f32(&self) -> Option<f32> {
        self.try_into().ok()
    }

    pub fn as_f64(&self) -> Option<f64> {
        self.try_into().ok()
    }

    pub fn as_decimal(&self) -> Option<Decimal> {
        self.try_into().ok()
    }

    /// Note that db_value.as_str() and db_value.to_string() differ in more than just their
    /// return type. The latter will format a [Value] as a string regardless of its type.
    /// This method returns a string slice only if the underlying type is [Value::Text].
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Text(txt) => Some(txt),
            _ => None,
        }
    }

    /// Note that: db_value.as_json() gives a different result from into().
    /// The latter will format a [Value] as a [JsonValue] regardless of its type.
    /// The as_json() method returns a JsonValue only if the underlying type is
    // /// [Value::Json].
    pub fn as_json(&self) -> Option<JsonValue> {
        match self {
            Value::Json(value) => Some(value.clone()),
            _ => None,
        }
    }
}

// Self-conversions:
impl Into<Value> for &Value {
    fn into(self) -> Value {
        self.clone()
    }
}

// NULL conversions:
impl TryInto<()> for Value {
    type Error = Error;

    fn try_into(self) -> Result<(), Error> {
        match self {
            Value::Null => Ok(()),
            _ => Err(Error::InputError(format!("Not a Null: {self:?}"))),
        }
    }
}

impl TryInto<()> for &Value {
    type Error = Error;

    fn try_into(self) -> Result<(), Error> {
        self.clone().try_into()
    }
}

// String and &str conversions:
impl Into<String> for Value {
    fn into(self) -> String {
        match self {
            Value::BigInteger(val) => val.to_string(),
            Value::Integer(val) => val.to_string(),
            Value::SmallInteger(val) => val.to_string(),
            Value::Text(val) => val.to_string(),
            Value::Null => String::new(),
            Value::Boolean(val) => val.to_string(),
            Value::Real(val) => val.to_string(),
            Value::BigReal(val) => val.to_string(),
            Value::Numeric(val) => val.to_string(),
            Value::Json(value) => value.to_string(),
            Value::Other(_, _, _) => {
                format!("{self:?}")
            }
        }
    }
}

impl Into<String> for &Value {
    fn into(self) -> String {
        self.clone().into()
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::Text(value.to_string())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

// JSON conversions.
impl From<JsonValue> for Value {
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
            JsonValue::Array(_) => Self::Json(item),
            JsonValue::Object(_) => Self::Json(item),
        }
    }
}

impl From<&JsonValue> for Value {
    fn from(item: &JsonValue) -> Self {
        item.clone().into()
    }
}

impl Into<JsonValue> for Value {
    fn into(self) -> JsonValue {
        match self {
            Value::Null => JsonValue::Null,
            Value::Boolean(value) => JsonValue::Bool(value),
            Value::SmallInteger(value) => JsonValue::Number(value.into()),
            Value::Integer(value) => JsonValue::Number(value.into()),
            Value::BigInteger(value) => JsonValue::Number(value.into()),
            Value::Real(value) => json!(value),
            Value::BigReal(value) => json!(value),
            Value::Numeric(value) => json!(value),
            Value::Text(value) => JsonValue::String(value),
            Value::Json(value) => value,
            Value::Other(_, _, _) => JsonValue::String(format!("{self:?}")),
        }
    }
}

impl Into<JsonValue> for &Value {
    fn into(self) -> JsonValue {
        self.clone().into()
    }
}

// Primitive type conversions.

// TODO: Add more for all of the remaining rust primitive types, including isize and usize

impl From<bool> for Value {
    fn from(item: bool) -> Self {
        Value::Boolean(item)
    }
}

impl From<i16> for Value {
    fn from(value: i16) -> Self {
        Self::SmallInteger(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Self::Integer(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::BigInteger(value)
    }
}

impl From<u32> for Value {
    fn from(item: u32) -> Self {
        if item <= i32::MAX as u32 {
            Value::Integer(item as i32)
        } else {
            Value::BigInteger(item as i64)
        }
    }
}

impl From<u64> for Value {
    fn from(item: u64) -> Self {
        if item <= i64::MAX as u64 {
            Value::BigInteger(item as i64)
        } else {
            Value::Numeric(Decimal::from(item))
        }
    }
}

impl From<f32> for Value {
    fn from(item: f32) -> Self {
        Value::Real(item)
    }
}

impl From<f64> for Value {
    fn from(item: f64) -> Self {
        Value::BigReal(item)
    }
}

impl From<Decimal> for Value {
    fn from(item: Decimal) -> Self {
        Value::Numeric(item)
    }
}

impl TryFrom<Value> for bool {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Boolean(value) => Ok(value),
            _ => Err(Error::InputError(format!("Not a boolean: {value:?}"))),
        }
    }
}

impl TryFrom<Value> for i8 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::SmallInteger(number) => Ok(i8::try_from(number)?),
            Value::Integer(number) => Ok(i8::try_from(number)?),
            Value::BigInteger(number) => Ok(i8::try_from(number)?),
            _ => Err(Error::InputError(format!("Not an integer: {value:?}"))),
        }
    }
}

impl TryFrom<Value> for i16 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::SmallInteger(number) => Ok(i16::try_from(number)?),
            Value::Integer(number) => Ok(i16::try_from(number)?),
            Value::BigInteger(number) => Ok(i16::try_from(number)?),
            _ => Err(Error::InputError(format!("Not an integer: {value:?}"))),
        }
    }
}

impl TryFrom<Value> for i32 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::SmallInteger(number) => Ok(i32::try_from(number)?),
            Value::Integer(number) => Ok(i32::try_from(number)?),
            Value::BigInteger(number) => Ok(i32::try_from(number)?),
            _ => Err(Error::InputError(format!("Not an integer: {value:?}"))),
        }
    }
}

impl TryFrom<Value> for i64 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::SmallInteger(number) => Ok(i64::try_from(number)?),
            Value::Integer(number) => Ok(i64::try_from(number)?),
            Value::BigInteger(number) => Ok(i64::try_from(number)?),
            _ => Err(Error::InputError(format!("Not an integer: {value:?}"))),
        }
    }
}

impl TryFrom<Value> for u8 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::SmallInteger(number) => Ok(u8::try_from(number)?),
            Value::Integer(number) => Ok(u8::try_from(number)?),
            Value::BigInteger(number) => Ok(u8::try_from(number)?),
            _ => Err(Error::InputError(format!("Not an integer: {value:?}"))),
        }
    }
}

impl TryFrom<Value> for u16 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::SmallInteger(number) => Ok(u16::try_from(number)?),
            Value::Integer(number) => Ok(u16::try_from(number)?),
            Value::BigInteger(number) => Ok(u16::try_from(number)?),
            _ => Err(Error::InputError(format!("Not an integer: {value:?}"))),
        }
    }
}

impl TryFrom<Value> for u32 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::SmallInteger(number) => Ok(u32::try_from(number)?),
            Value::Integer(number) => Ok(u32::try_from(number)?),
            Value::BigInteger(number) => Ok(u32::try_from(number)?),
            _ => Err(Error::InputError(format!("Not an integer: {value:?}"))),
        }
    }
}

impl TryFrom<Value> for u64 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::SmallInteger(number) => Ok(u64::try_from(number)?),
            Value::Integer(number) => Ok(u64::try_from(number)?),
            Value::BigInteger(number) => Ok(u64::try_from(number)?),
            _ => Err(Error::InputError(format!("Not an integer: {value:?}"))),
        }
    }
}

impl TryFrom<Value> for f32 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Real(number) => Ok(f32::try_from(number)?),
            Value::BigReal(number) => Ok(number as f32),
            Value::Numeric(number) => Ok(f32::try_from(number)?),
            Value::SmallInteger(number) => Ok(f32::try_from(number)?),
            _ => Err(Error::InputError(format!("Not an f32: {value:?}"))),
        }
    }
}

impl TryFrom<Value> for f64 {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Real(number) => Ok(f64::try_from(number)?),
            Value::BigReal(number) => Ok(f64::try_from(number)?),
            Value::Numeric(number) => Ok(f64::try_from(number)?),
            Value::SmallInteger(number) => Ok(f64::try_from(number)?),
            Value::Integer(number) => Ok(f64::try_from(number)?),
            _ => Err(Error::InputError(format!("Not an f64: {value:?}"))),
        }
    }
}

impl TryFrom<Value> for Decimal {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Real(number) => Ok(Decimal::try_from(number)?),
            Value::BigReal(number) => Ok(Decimal::try_from(number)?),
            Value::Numeric(number) => Ok(Decimal::try_from(number)?),
            Value::SmallInteger(number) => Ok(Decimal::try_from(number)?),
            Value::Integer(number) => Ok(Decimal::try_from(number)?),
            Value::BigInteger(number) => Ok(Decimal::try_from(number)?),
            _ => Err(Error::InputError(format!("Not a decimal: {value:?}"))),
        }
    }
}

impl TryFrom<&Value> for bool {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        value.clone().try_into()
    }
}

impl TryFrom<&Value> for i8 {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        value.clone().try_into()
    }
}

impl TryFrom<&Value> for i16 {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        value.clone().try_into()
    }
}

impl TryFrom<&Value> for i32 {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        value.clone().try_into()
    }
}

impl TryFrom<&Value> for i64 {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        value.clone().try_into()
    }
}

impl TryFrom<&Value> for u8 {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        value.clone().try_into()
    }
}

impl TryFrom<&Value> for u16 {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        value.clone().try_into()
    }
}

impl TryFrom<&Value> for u32 {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        value.clone().try_into()
    }
}

impl TryFrom<&Value> for u64 {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        value.clone().try_into()
    }
}

impl TryFrom<&Value> for f32 {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        value.clone().try_into()
    }
}

impl TryFrom<&Value> for f64 {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        value.clone().try_into()
    }
}

impl TryFrom<&Value> for Decimal {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        value.clone().try_into()
    }
}

///////////////////////////////////////////////////////////////////////////////
// IntoValue
///////////////////////////////////////////////////////////////////////////////

/// Types that implement this trait can be converted into a [Value].
pub trait IntoValue: Clone {
    fn into_value(self) -> Value;
}

/// Implements [IntoValue] for types that implement [Into] for [Value].
impl<T: Into<Value> + Clone> IntoValue for T {
    fn into_value(self) -> Value {
        self.into()
    }
}

///////////////////////////////////////////////////////////////////////////////
// IntoValues
///////////////////////////////////////////////////////////////////////////////

/// Any type that implements this trait can be converted into an [Iterator] of [Value]s.
pub trait IntoValues {
    fn into_values(self) -> Result<impl Iterator<Item = Value>, Error>;
}

/// Implements [IntoValues] for an empty tuple.
impl IntoValues for () {
    fn into_values(self) -> Result<impl Iterator<Item = Value>, Error> {
        Ok(vec![].into_iter())
    }
}

impl<T: IntoValue, const N: usize> IntoValues for [T; N] {
    fn into_values(self) -> Result<impl Iterator<Item = Value>, Error> {
        Ok(self.into_iter().map(|value| value.into_value()))
    }
}

impl<T: IntoValue + Clone, const N: usize> IntoValues for &[T; N] {
    fn into_values(self) -> Result<impl Iterator<Item = Value>, Error> {
        Ok(self.clone().into_iter().map(|value| value.into_value()))
    }
}

impl<T: IntoValue> IntoValues for Vec<T> {
    fn into_values(self) -> Result<impl Iterator<Item = Value>, Error> {
        Ok(self.into_iter().map(|value| value.into_value()))
    }
}

impl<T: IntoValue> IntoValues for &Vec<T> {
    fn into_values(self) -> Result<impl Iterator<Item = Value>, Error> {
        Ok(self.clone().into_iter().map(|value| value.into_value()))
    }
}

impl IntoValues for &[Value] {
    fn into_values(self) -> Result<impl Iterator<Item = Value>, Error> {
        Ok(self.into_iter().map(|value| value.clone().into_value()))
    }
}
