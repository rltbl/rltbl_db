//! Database rows.
//!
//! We represent a database [Row] as an [IndexMap]
//! from the column name as a [String] to the [Value].
//! The `IndexMap` preserves the order of columns,
//! allowing us to iterate over them
//! in the same order we see the columns in the table.
//! We represent [Rows] as a vector of `Row`s.
//!
//! MC: Is Rows really useful? In the old API I thought it made things a bit more
//! confusing and I couldn't see what the real advantage is in using this. I realise it
//! is useful insofar as it functions as a shorthand, but at least for me it comes at
//! the cost of extra (unneeded?) complication.
//! JO: I thing I do want `Rows` because we can attach some
//! convenience methods and (Try)From/Into methods to it.
//!
//! For both `Row` and `Rows` we provide a number of convenience methods.
//! The `row!` macro is a convenient way to create a `Row`:
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
//! A "normal" Rust struct with named fields
//! can be converted back and forth from a `Row`
//! if it implements [serde::Serialize] and [serde::Deserialize].
//!
//! MC: Do we want to mention the subtleties involved in handling JSON values in these
//! comments?
//! JO: Yes, eventually.

use std::ops::{Deref, DerefMut};

use indexmap::IndexMap;

use crate::{Error, Value};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Row {
    pub map: IndexMap<String, Value>,
}

impl Row {
    pub fn new() -> Self {
        Row {
            map: IndexMap::new(),
        }
    }

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

impl Rows {
    pub fn row(mut self) -> Result<Row, Error> {
        match self.rows.len() {
            0 => Err(Error::DataError("No rows returned".to_string())),
            _ => Ok(self.rows.remove(0)),
        }
    }

    // pub fn try_into_value<T>(self) -> Result<T, Error>
    // where
    //     T: TryFrom<Value, Error = crate::error::Error>,
    // {
    pub fn try_into_value<T: TryFrom<Value>>(self) -> Result<T, Error> {
        match self.row()?.value()?.try_into() {
            Ok(value) => Ok(value),
            Err(_) => Err(Error::ValueError(format!("Could not convert value"))),
        }
    }

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
