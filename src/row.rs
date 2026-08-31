//! Database rows.
//!
//! We represent a database [Row] as an `IndexMap` from the column name as a [String] to the
//! [Value] in that column. The `IndexMap` preserves the order of columns, allowing us to
//! iterate over them in the same order we see the columns in the table.
//!
//! We represent [Rows] as a vector of `Row`s.
//!
//! We also provide an alias, `StringRow`, for `IndexMap<String, String>`, which represents
//! the row with its column values converted into strings.
//!
//! For both `Row` and `Rows` we provide a number of convenience methods, including for
//! conversion into `StringRow`.
//!
//! The [row!](crate::row!) macro is a convenient way to create a `Row`:
//!
//! ```
//! use indexmap::IndexMap;
//! use rltbl_db::{row, Row, Value};
//!
//! let row1 = Row {
//!     map: IndexMap::from([
//!         (String::from("bar"), Value::from(2_i64)),
//!         (String::from("baz"), Value::from("b")),
//!     ])
//! };
//! let row2 = row! {
//!     "bar" => 2_i64,
//!     "baz" => "b",
//! };
//! assert_eq!(row1, row2);
//! ```
//!
//! A "normal" Rust struct with named fields can be converted back and forth from a `Row`
//! if it implements [serde::Serialize] and [serde::Deserialize].
//!
//! MC: Do we want to mention the subtleties involved in handling JSON values in these
//! comments?
//! JO: Yes, eventually.

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};

use crate::{Error, Value};

/// Represents a database row.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Row {
    /// A map from column names to column values.
    pub map: IndexMap<String, Value>,
}

impl Row {
    // Create an empty row.
    pub fn new() -> Self {
        Row {
            map: IndexMap::new(),
        }
    }

    // Returns the first value of the first column of this row.
    pub fn value(mut self) -> Result<Value, Error> {
        match self.map.shift_remove_index(0) {
            Some((_, value)) => Ok(value),
            None => Err(Error::DataError("No values returned".to_string())),
        }
    }
}

impl Deref for Row {
    type Target = IndexMap<String, Value>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl DerefMut for Row {
    fn deref_mut(&mut self) -> &mut IndexMap<String, Value> {
        &mut self.map
    }
}

impl IntoIterator for Row {
    type Item = (String, Value);
    type IntoIter = indexmap::map::IntoIter<String, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}

impl FromIterator<(String, Value)> for Row {
    fn from_iter<I: IntoIterator<Item = (String, Value)>>(iter: I) -> Self {
        Row {
            map: iter.into_iter().collect(),
        }
    }
}

/// A stringified version of [Row], where each column value is represented as a [String].
pub type StringRow = IndexMap<String, String>;

impl Into<StringRow> for Row {
    fn into(self) -> StringRow {
        self.iter()
            .map(|(key, value)| (key.clone(), value.into()))
            .collect()
    }
}

impl Into<StringRow> for &Row {
    fn into(self) -> StringRow {
        self.clone().into()
    }
}

impl From<StringRow> for Row {
    fn from(row: StringRow) -> Self {
        row.iter()
            .map(|(key, value)| (key.clone(), Value::from(value.as_str())))
            .collect()
    }
}

impl From<&StringRow> for Row {
    fn from(row: &StringRow) -> Self {
        row.clone().into()
    }
}

/// Represents a vector of rows.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Rows {
    pub rows: Vec<Row>,
}

impl Deref for Rows {
    type Target = Vec<Row>;

    fn deref(&self) -> &Self::Target {
        &self.rows
    }
}

impl DerefMut for Rows {
    fn deref_mut(&mut self) -> &mut Vec<Row> {
        &mut self.rows
    }
}

impl Into<Vec<StringRow>> for Rows {
    fn into(self) -> Vec<StringRow> {
        self.rows.iter().map(|row| row.into()).collect()
    }
}

impl Into<Vec<StringRow>> for &Rows {
    fn into(self) -> Vec<StringRow> {
        self.clone().into()
    }
}

impl Rows {
    /// Returns the first of these rows.
    pub fn row(mut self) -> Result<Row, Error> {
        match self.rows.len() {
            0 => Err(Error::DataError("No rows returned".to_string())),
            _ => Ok(self.rows.remove(0)),
        }
    }

    /// Returns the first value from the first of these rows.
    pub fn try_into_value<T: TryFrom<Value>>(self) -> Result<T, Error> {
        match self.row()?.value()?.try_into() {
            Ok(value) => Ok(value),
            Err(_) => Err(Error::ValueError(format!("Could not convert value"))),
        }
    }

    /// Returns the first value from each of these rows as a vector of Strings.
    pub fn to_strings(&self) -> Result<Vec<String>, Error> {
        self.rows
            .iter()
            .map(|row| match row.first() {
                Some((_key, value)) => Ok(value.to_string()),
                None => Err(Error::DataError(format!("Missing value"))),
            })
            .collect()
    }
}
