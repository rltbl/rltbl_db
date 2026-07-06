//! Code specific to supported database kinds.

use crate::{
    core::DbError,
    db_value::{DbValue, IntoDbValue, JsonValue},
};

use rust_decimal::Decimal;
use std::{cmp::Ordering, str::FromStr};
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

/// The supported database types, including information about the name
/// used to refer to the type in the underlying database.
#[derive(Clone, Debug, EnumIter)]
pub enum DbType {
    Null(String),
    Boolean(String),
    I16(String),
    SmallInteger(String),
    Integer(String),
    BigInteger(String),
    Real(String),
    BigReal(String),
    Numeric(String),
    Text(String),
}

impl PartialEq for DbType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DbType::Null(_), DbType::Null(_)) => true,
            (DbType::Boolean(_), DbType::Boolean(_)) => true,
            (DbType::I16(_), DbType::I16(_)) => true,
            (DbType::SmallInteger(_), DbType::SmallInteger(_)) => true,
            (DbType::Integer(_), DbType::Integer(_)) => true,
            (DbType::BigInteger(_), DbType::BigInteger(_)) => true,
            (DbType::Real(_), DbType::Real(_)) => true,
            (DbType::BigReal(_), DbType::BigReal(_)) => true,
            (DbType::Numeric(_), DbType::Numeric(_)) => true,
            (DbType::Text(_), DbType::Text(_)) => true,
            _ => false,
        }
    }
}

impl Eq for DbType {}

impl Ord for DbType {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (DbType::Null(_), DbType::Null(_)) => Ordering::Equal,
            (DbType::Null(_), _) => Ordering::Less,
            (_, DbType::Null(_)) => Ordering::Greater,

            (DbType::Boolean(_), DbType::Boolean(_)) => Ordering::Equal,
            (DbType::Boolean(_), _) => Ordering::Less,
            (_, DbType::Boolean(_)) => Ordering::Greater,

            (DbType::I16(_), DbType::I16(_)) => Ordering::Equal,
            (DbType::I16(_), _) => Ordering::Less,
            (_, DbType::I16(_)) => Ordering::Greater,

            (DbType::SmallInteger(_), DbType::SmallInteger(_)) => Ordering::Equal,
            (DbType::SmallInteger(_), _) => Ordering::Less,
            (_, DbType::SmallInteger(_)) => Ordering::Greater,

            (DbType::Integer(_), DbType::Integer(_)) => Ordering::Equal,
            (DbType::Integer(_), _) => Ordering::Less,
            (_, DbType::Integer(_)) => Ordering::Greater,

            (DbType::BigInteger(_), DbType::BigInteger(_)) => Ordering::Equal,
            (DbType::BigInteger(_), _) => Ordering::Less,
            (_, DbType::BigInteger(_)) => Ordering::Greater,

            (DbType::Real(_), DbType::Real(_)) => Ordering::Equal,
            (DbType::Real(_), _) => Ordering::Less,
            (_, DbType::Real(_)) => Ordering::Greater,

            (DbType::BigReal(_), DbType::BigReal(_)) => Ordering::Equal,
            (DbType::BigReal(_), _) => Ordering::Less,
            (_, DbType::BigReal(_)) => Ordering::Greater,

            (DbType::Numeric(_), DbType::Numeric(_)) => Ordering::Equal,
            (DbType::Numeric(_), _) => Ordering::Less,
            (_, DbType::Numeric(_)) => Ordering::Greater,

            (DbType::Text(_), DbType::Text(_)) => Ordering::Equal,
        }
    }
}

impl PartialOrd for DbType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl FromStr for DbType {
    type Err = DbError;

    fn from_str(value: &str) -> Result<Self, DbError> {
        if 1 == 1 {
            let mut db_types = DbType::iter().collect::<Vec<_>>();
            db_types.sort();
            for db_type in db_types {
                println!("DB TYPE: {db_type:?}");
            }
        }
        todo!()
    }
}

impl DbType {
    /// Parses a given string representing the value of a database field into a [DbValue] of this
    /// type.
    pub fn parse_str(&self, value: &str) -> Result<DbValue, DbError> {
        match self {
            DbType::Null(_) => Ok(DbValue::Null),
            DbType::Boolean(_) => {
                let value = value
                    .parse::<bool>()
                    .map_err(|_| DbError::InputError(format!("Not a boolean: {value}")))?;
                Ok(DbValue::Boolean(value))
            }
            DbType::I16(_) | DbType::SmallInteger(_) => {
                let value = value
                    .parse::<i16>()
                    .map_err(|_| DbError::InputError(format!("Not an i16: {value}")))?;
                Ok(DbValue::SmallInteger(value))
            }
            DbType::Integer(_) => {
                let value = value
                    .parse::<i32>()
                    .map_err(|_| DbError::InputError(format!("Not an i32: {value}")))?;
                Ok(DbValue::Integer(value))
            }
            DbType::BigInteger(_) => {
                let value = value
                    .parse::<i64>()
                    .map_err(|_| DbError::InputError(format!("Not an i64: {value}")))?;
                Ok(DbValue::BigInteger(value))
            }
            DbType::Real(_) => {
                let value = value
                    .parse::<f32>()
                    .map_err(|_| DbError::InputError(format!("Not an f32: {value}")))?;
                Ok(DbValue::Real(value))
            }
            DbType::BigReal(_) => {
                let value = value
                    .parse::<f64>()
                    .map_err(|_| DbError::InputError(format!("Not an f64: {value}")))?;
                Ok(DbValue::BigReal(value))
            }
            DbType::Numeric(_) => {
                let value = value
                    .parse::<Decimal>()
                    .map_err(|_| DbError::InputError(format!("Not a Decimal: {value}")))?;
                Ok(DbValue::Numeric(value))
            }
            DbType::Text(_) => Ok(DbValue::Text(value.to_string())),
        }
    }

    /// Parses a given [JsonValue] representing the value of a database field into a [DbValue] of
    /// this type.
    pub fn parse_json(&self, value: &JsonValue) -> Result<DbValue, DbError> {
        Ok(DbValue::from(value))
    }

    /// Parses the given value into a [DbValue] of this type.
    pub fn parse(&self, value: impl IntoDbValue) -> Result<DbValue, DbError> {
        let value = value.into_db_value();
        self.convert(&value)
    }

    /// Converts the given [DbValue] into a [DbValue] of this type.
    pub fn convert(&self, value: &DbValue) -> Result<DbValue, DbError> {
        // First handle NULLs and Text types:
        match self {
            DbType::Null(_) => match value {
                DbValue::Null => return Ok(DbValue::Null),
                value => {
                    return Err(DbError::InputError(format!(
                        "Can't convert to {self:?} from {value:?}"
                    )));
                }
            },
            _ => {
                if let DbValue::Text(value) = value {
                    return Ok(self.parse_str(value)?);
                }
            }
        };

        // Then handle everything else:

        let err_template = |db_value: &DbValue| {
            DbError::InputError(format!("Can't convert to {self:?} from {db_value:?}"))
        };

        match self {
            DbType::Null(_) => unreachable!(), // Handled above.
            DbType::Boolean(_) => {
                let value = value.as_bool().ok_or(err_template(value))?;
                Ok(DbValue::Boolean(value))
            }
            DbType::I16(_) => {
                let value = value.as_i16().ok_or(err_template(value))?;
                Ok(DbValue::SmallInteger(value))
            }
            DbType::SmallInteger(_) => {
                let value = value.as_i16().ok_or(err_template(value))?;
                Ok(DbValue::SmallInteger(value))
            }
            DbType::Integer(_) => {
                let value = value.as_i32().ok_or(err_template(value))?;
                Ok(DbValue::Integer(value))
            }
            DbType::BigInteger(_) => {
                let value = value.as_i64().ok_or(err_template(value))?;
                Ok(DbValue::BigInteger(value))
            }
            DbType::Real(_) => {
                let value = value.as_f32().ok_or(err_template(value))?;
                Ok(DbValue::Real(value))
            }
            DbType::BigReal(_) => {
                let value = value.as_f64().ok_or(err_template(value))?;
                Ok(DbValue::BigReal(value))
            }
            DbType::Numeric(_) => {
                let value = value.as_decimal().ok_or(err_template(value))?;
                Ok(DbValue::Numeric(value))
            }
            DbType::Text(_) => Ok(DbValue::Text(value.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str() {
        let foo = DbType::from_str("0").unwrap();
    }
}
