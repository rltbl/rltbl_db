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

use rust_decimal::Decimal;
use std::fmt::Display;

use crate::Error;

// MC: I noticed that there is nothing corresponding to DbParams in this repository. In
// rltbl_db the main convenience of DbParams was (if I remember right) to be able to easily
// represent the case of no parameters. If I remember right, there were compilation issues
// when trying to handle this without a wrapper struct, but I could be wrong. Have you tested
// that case, or is the state of the code still too preliminary to think about this?

// MC: I'm confused about the difference between this struct and column::Type. I noticed
// that this one defines a Null type but the other one does not. What are these each
// intended to be used for?
// JO: I think we want to distinguish between the type of a value (which might be NULL)
// and the type of a column (which cannot be NULL).
// Using two different enums is one way to do that, but maybe not the best way.
// TODO: Maybe this should be ColumnType, which would exclude Null.
pub enum Type {
    Null(String),
    Boolean(String),
    BigInteger(String),
    BigReal(String),
    Text(String),
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
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Value {
    Null,
    Boolean(bool),
    BigInteger(i64),
    Integer(i32),
    SmallInteger(i16),
    Real(f32),
    BigReal(f64),
    Numeric(Decimal),
    Text(String),
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
            // (Value::Numeric(a), Value::Numeric(b)) => a == b,
            (Value::Text(a), Value::Text(b)) => a == b,
            // (Value::Json(a), Value::Json(b)) => a == b,
            // (Value::Other(a, b, c), Value::Other(d, e, f)) => a == d && b == e && c == f,
            _ => false,
        }
    }
}

impl Eq for Value {}

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

// TODO: Add more From<..> and TryFrom<Value> blocks for all of the other rust primitive types.

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

impl From<u64> for Value {
    fn from(item: u64) -> Self {
        if item <= i64::MAX as u64 {
            Value::BigInteger(item as i64)
        } else {
            todo!()
            // Value::Numeric(Decimal::from(item))
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

// TODO: I have been trying to use these to make it possible to simply pass argument lists
// such as: &["foo", 1, 3.2] to execute(), but this still needs work.
// These do not seem to be needed for anything else so I'll leave all of this commented
// out for now.
// ///////////////////////////////////////////////////////////////////////////////
// // IntoValue
// ///////////////////////////////////////////////////////////////////////////////
//
// /// Types that implement this trait can be converted into a [Value].
// pub trait IntoValue {
//     fn into_value(self) -> Value;
// }
//
// /// Implements [IntoValue] for types that implement [TryFrom] for [Value].
// impl<T: Into<Value>> IntoValue for T {
//     fn into_value(self) -> Value {
//         self.into()
//     }
// }
//
// /////////////////////////////////
//
// /// Types that implement this trait can be converted into [Params]
// pub trait IntoParams {
//     fn into_params(self) -> Vec<Value>;
// }
//
// /// Implements [IntoParams] for references to [Params]
// impl IntoParams for &Vec<Value> {
//     fn into_params(self) -> Vec<Value> {
//         self.clone()
//     }
// }
//
// /// Implements [IntoParams] for an empty tuple. Always returns [Params::None].
// impl IntoParams for () {
//     fn into_params(self) -> Vec<Value> {
//         vec![]
//     }
// }
//
// /// Implements [IntoParams] for fixed-length arrays of types that implement [IntoValue]
// impl<T: IntoValue, const N: usize> IntoParams for [T; N] {
//     fn into_params(self) -> Vec<Value> {
//         self.into_iter().collect::<Vec<_>>().into_params()
//     }
// }
//
// /// Implements [IntoParams] for references to fixed-length arrays of types that implement
// /// [IntoValue]
// impl<T: IntoValue + Clone, const N: usize> IntoParams for &[T; N] {
//     fn into_params(self) -> Vec<Value> {
//         self.iter().cloned().collect::<Vec<_>>().into_params()
//     }
// }
//
// /// Implements [IntoParams] for vectors of types that implement [IntoValue]
// impl<T: IntoValue> IntoParams for Vec<T> {
//     fn into_params(self) -> Vec<Value> {
//         let values = self.into_iter().map(|i| i.into_value()).collect::<Vec<_>>();
//         values
//     }
// }
//
