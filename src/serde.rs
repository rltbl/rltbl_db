//! Implement serialization/deserialization to/from a [Value].

use crate::{Error, JsonValue, Value, ValueType};
use serde::{Deserialize, Serialize};
use serde_json::value::Serializer;

/// Serialize the given item into a [Value].
pub fn to_value<T>(item: &T) -> Result<Value, Error>
where
    T: Serialize,
{
    let serializer = Serializer;
    let json_value = item.serialize(serializer)?;
    match json_value {
        JsonValue::Null => Ok(Value::Null),
        JsonValue::Bool(val) => Ok(Value::Boolean(val)),
        JsonValue::Number(ref val) => match val.as_u64() {
            Some(val) => {
                let val_type = ValueType::min_integer().min_type(&val.to_string())?;
                match val_type {
                    ValueType::SmallInteger(_) => Ok(Value::SmallInteger(val as i16)),
                    ValueType::Integer(_) => Ok(Value::Integer(val as i32)),
                    ValueType::BigInteger(_) => Ok(Value::BigInteger(val as i64)),
                    _ => {
                        return Err(Error::ValueError(format!(
                            "Unexpected integer type: {val_type:?}"
                        )));
                    }
                }
            }
            None => match val.as_f64() {
                Some(val) => {
                    let val_type = ValueType::min_real().min_type(&val.to_string())?;
                    match val_type {
                        ValueType::Real(_) => Ok(Value::Real(val as f32)),
                        ValueType::BigReal(_) => Ok(Value::BigReal(val as f64)),
                        _ => {
                            return Err(Error::ValueError(format!(
                                "Unexpected real type: {val_type:?}"
                            )));
                        }
                    }
                }
                None => {
                    return Err(Error::ValueError(format!("Not a number: {json_value}")));
                }
            },
        },
        JsonValue::String(val) => Ok(Value::Text(val)),
        JsonValue::Array(_) => Ok(Value::Json(json_value)),
        JsonValue::Object(_) => Ok(Value::Json(json_value)),
    }
}

/// Deserialize the given value.
pub fn from_value<T>(value: &Value) -> Result<T, Error>
where
    T: for<'de> Deserialize<'de>,
{
    match value {
        Value::Json(json_value) => Ok(serde_json::from_str(&json_value.to_string())?),
        Value::Null => Ok(serde_json::from_str("null")?),
        Value::Text(text) => Ok(serde_json::from_str(&text.as_str().to_string())?),
        _ => {
            let value = value.to_string();
            Ok(serde_json::from_str(&value)?)
        }
    }
}
