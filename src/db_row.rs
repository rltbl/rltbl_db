//! Code related to database values.

use crate::{
    core::DbError,
    db_type::DbType,
    db_value::{DbValue, JsonValue},
};
use indexmap::{self, IndexMap};
use serde::{Deserialize, Serialize};
use serde_json::Map as JsonMap;
use std::{
    cmp::Ordering,
    ops::{Deref, DerefMut},
};

pub type JsonRow = JsonMap<String, JsonValue>;
pub type StringRow = IndexMap<String, String>;

// TODO: Maybe replace this with DbColumn:
pub type ColumnMap = IndexMap<String, String>;

/// A row of database values indexed by column name.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)] // See https://serde.rs/container-attrs.html#transparent
pub struct DbRow {
    pub map: IndexMap<String, DbValue>,
}

#[derive(Debug, Default, Clone)]
pub struct DbRows {
    pub content: Vec<DbRow>,
}

/// TODO: Add docstring.
#[derive(Clone, Debug, PartialEq, Eq)]
struct DbColumn {
    name: String,
    db_type: DbType,
    not_null: bool,
    unique: bool,
}

/// Enables conversion from something into a vector of [DbRow]s
pub trait IntoDbRows {
    fn into_db_rows(self) -> DbRows;
}

impl Deref for DbRow {
    type Target = IndexMap<String, DbValue>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl DerefMut for DbRow {
    fn deref_mut(&mut self) -> &mut IndexMap<String, DbValue> {
        &mut self.map
    }
}

impl Deref for DbRows {
    type Target = Vec<DbRow>;

    fn deref(&self) -> &Self::Target {
        &self.content
    }
}

impl DerefMut for DbRows {
    fn deref_mut(&mut self) -> &mut Vec<DbRow> {
        &mut self.content
    }
}

impl Ord for DbColumn {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.db_type.cmp(&other.db_type) {
            Ordering::Equal => {
                if self.not_null != other.not_null {
                    if self.not_null {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                } else if self.unique != other.unique {
                    if self.unique {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                } else {
                    Ordering::Equal
                }
            }
            ordering => ordering,
        }
    }
}

impl PartialOrd for DbColumn {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl DbRow {
    pub fn new() -> Self {
        DbRow {
            map: IndexMap::new(),
        }
    }

    pub fn insert(&mut self, key: String, value: DbValue) {
        self.map.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<DbValue> {
        self.map.get(key).cloned()
    }

    pub fn remove_nulls(mut self) -> Self {
        self.map = self
            .map
            .into_iter()
            .filter(|(_key, value)| !value.is_null())
            .collect();
        self
    }

    pub fn try_into<T>(&self) -> Result<T, DbError>
    where
        T: for<'de> Deserialize<'de>,
    {
        let mut deserializer = crate::serde::DbRowDeserializer::from_db_row(self);
        let t = T::deserialize(&mut deserializer)
            .map_err(|err| DbError::SerdeError(format!("{err} while deserializing {self:?}")));
        t
    }
}

impl DbRows {
    pub fn row(&self) -> Result<&DbRow, DbError> {
        if self.len() != 1 {
            return Err(DbError::DataError(format!(
                "Wrong number of rows: {}",
                self.len()
            )));
        }
        Ok(self.first().unwrap())
    }

    pub fn value(&self) -> Result<&DbValue, DbError> {
        if self.len() != 1 {
            return Err(DbError::DataError(format!(
                "Wrong number of rows: {}",
                self.len()
            )));
        }
        let row = self.row()?;
        if row.len() != 1 {
            return Err(DbError::DataError(format!(
                "Wrong number of values: {}",
                row.len()
            )));
        }
        let (_key, value) = row.first().unwrap();
        Ok(value)
    }

    pub fn remove_nulls(mut self) -> Self {
        self.content = self
            .content
            .into_iter()
            .map(|row| row.remove_nulls())
            .collect();
        self
    }

    pub fn to_strings(&self) -> Result<Vec<String>, DbError> {
        self.content
            .iter()
            .map(|row| match row.first() {
                Some((_key, value)) => Ok(value.to_string()),
                None => Err(DbError::DataError(format!("Missing value"))),
            })
            .collect()
    }

    pub fn try_into_value<T>(&self) -> Result<T, DbError>
    where
        T: TryFrom<DbValue, Error = DbError>,
    {
        self.value()?.clone().try_into()
    }

    pub fn try_into_vec<T>(&self) -> Result<Vec<T>, DbError>
    where
        T: for<'de> Deserialize<'de>,
    {
        self.content.iter().map(|row| row.try_into()).collect()
    }
}

impl Into<StringRow> for DbRow {
    fn into(self) -> StringRow {
        self.iter()
            .map(|(key, value)| (key.clone(), value.into()))
            .collect()
    }
}

impl Into<StringRow> for &DbRow {
    fn into(self) -> StringRow {
        self.clone().into()
    }
}

impl Into<Vec<StringRow>> for DbRows {
    fn into(self) -> Vec<StringRow> {
        self.content.iter().map(|row| row.into()).collect()
    }
}

impl Into<Vec<StringRow>> for &DbRows {
    fn into(self) -> Vec<StringRow> {
        self.clone().into()
    }
}

impl Into<JsonRow> for DbRow {
    fn into(self) -> JsonRow {
        self.map
            .into_iter()
            .map(|(key, value)| (key, value.into()))
            .collect()
    }
}

impl TryInto<JsonValue> for DbRows {
    type Error = DbError;

    fn try_into(self) -> Result<JsonValue, Self::Error> {
        let value = self.value()?;
        match value.as_json() {
            Some(json_value) => Ok(json_value),
            None => Err(DbError::InputError(format!("Not a JSON value: {value}"))),
        }
    }
}

impl TryInto<JsonValue> for &DbRows {
    type Error = DbError;

    fn try_into(self) -> Result<JsonValue, Self::Error> {
        Ok(self.clone().value()?.into())
    }
}

impl TryInto<String> for DbRows {
    type Error = DbError;

    fn try_into(self) -> Result<String, Self::Error> {
        Ok(self.value()?.to_string())
    }
}

impl TryInto<String> for &DbRows {
    type Error = DbError;

    fn try_into(self) -> Result<String, Self::Error> {
        Ok(self.clone().value()?.into())
    }
}

impl TryInto<u64> for DbRows {
    type Error = DbError;

    fn try_into(self) -> Result<u64, Self::Error> {
        Ok(self.value()?.try_into()?)
    }
}

impl TryInto<u64> for &DbRows {
    type Error = DbError;

    fn try_into(self) -> Result<u64, Self::Error> {
        Ok(self.value()?.try_into()?)
    }
}

impl TryInto<i64> for DbRows {
    type Error = DbError;

    fn try_into(self) -> Result<i64, Self::Error> {
        Ok(self.value()?.try_into()?)
    }
}

impl TryInto<i64> for &DbRows {
    type Error = DbError;

    fn try_into(self) -> Result<i64, Self::Error> {
        Ok(self.value()?.try_into()?)
    }
}

impl TryInto<i32> for DbRows {
    type Error = DbError;

    fn try_into(self) -> Result<i32, Self::Error> {
        Ok(self.value()?.try_into()?)
    }
}

impl TryInto<f64> for DbRows {
    type Error = DbError;

    fn try_into(self) -> Result<f64, Self::Error> {
        Ok(self.value()?.try_into()?)
    }
}

impl TryInto<f64> for &DbRows {
    type Error = DbError;

    fn try_into(self) -> Result<f64, Self::Error> {
        Ok(self.value()?.try_into()?)
    }
}

impl IntoIterator for DbRow {
    type Item = (String, DbValue);
    type IntoIter = indexmap::map::IntoIter<String, DbValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}

impl FromIterator<(String, DbValue)> for DbRow {
    fn from_iter<I: IntoIterator<Item = (String, DbValue)>>(iter: I) -> Self {
        DbRow {
            map: iter.into_iter().collect(),
        }
    }
}

impl IntoDbRows for DbRows {
    fn into_db_rows(self) -> DbRows {
        self
    }
}

impl IntoDbRows for &DbRows {
    fn into_db_rows(self) -> DbRows {
        self.clone()
    }
}

impl IntoDbRows for Vec<DbRow> {
    fn into_db_rows(self) -> DbRows {
        DbRows { content: self }
    }
}

impl IntoDbRows for &Vec<DbRow> {
    fn into_db_rows(self) -> DbRows {
        DbRows {
            content: self.clone(),
        }
    }
}

impl IntoDbRows for &[DbRow] {
    fn into_db_rows(self) -> DbRows {
        DbRows {
            content: self.to_vec(),
        }
    }
}

impl IntoDbRows for &[&DbRow] {
    fn into_db_rows(self) -> DbRows {
        DbRows {
            content: self
                .into_iter()
                .cloned()
                .map(|row| row.clone())
                .collect::<Vec<_>>(),
        }
    }
}

impl<const N: usize> IntoDbRows for &[&DbRow; N] {
    fn into_db_rows(self) -> DbRows {
        DbRows {
            content: self
                .into_iter()
                .cloned()
                .map(|row| row.clone())
                .collect::<Vec<_>>(),
        }
    }
}

impl IntoDbRows for Vec<JsonRow> {
    fn into_db_rows(self) -> DbRows {
        DbRows {
            content: self
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|(key, val)| (key, DbValue::from(val)))
                        .collect()
                })
                .collect::<Vec<_>>(),
        }
    }
}

impl IntoDbRows for &Vec<JsonRow> {
    fn into_db_rows(self) -> DbRows {
        DbRows {
            content: self.clone().into_db_rows().content,
        }
    }
}

impl DbColumn {
    fn guess<'a, I>(values: I) -> Self
    where
        I: Iterator<Item = &'a DbValue>,
    {
        for value in values {}
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::dec;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn test_guessing() {
        let db_values = vec![DbValue::SmallInteger(1), DbValue::Real(1.23)];
        let column = DbColumn::guess(db_values.iter());
        println!("COLUMN: {column:?}");
    }

    #[test]
    fn test_json() {
        // Test is_json(), as_json() methods:
        let db_val = DbValue::Json(json!([]));
        assert_eq!(db_val.is_json(), true);
        assert_eq!(db_val.as_json(), Some(json!([])));

        let db_val = DbValue::Text(json!([]).to_string());
        assert_eq!(db_val.is_json(), false);
        assert_eq!(db_val.as_json(), None);

        // Test into() method:
        let db_val = DbValue::Json(json!([]));
        let json_val: JsonValue = db_val.into();
        assert_eq!(json_val, json!([]));

        let db_val = DbValue::Text(json!([]).to_string());
        let json_val: JsonValue = db_val.into();
        assert_eq!(json_val, JsonValue::String("[]".into()));
    }

    #[test]
    fn test_hashing() {
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
            DbValue::Json(json!({"foo":1})),
            DbValue::Other("bpchar".to_string(), vec![97], Some("a".to_string())),
        ]
        .iter()
        .enumerate()
        {
            test_map.insert(value.clone(), i);
            assert_eq!(*test_map.get(&value).unwrap(), i);
        }
    }
}
