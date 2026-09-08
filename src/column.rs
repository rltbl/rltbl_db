use csv::ReaderBuilder;
use indexmap::{IndexMap, IndexSet};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    fs::File,
    iter::zip,
};

use crate::{Error, Row, Value, ValueType};

// MC: We had DbType::Null in the old API. Was it unused? (I think it might have been
// - I don't recall actually using it anywhere - but I'm not sure.)
// JO: I think we want to distinguish between the type of a value (which might be NULL)
// and the type of a column (which cannot be NULL).
// Using two different enums is one way to do that, but maybe not the best way.
// MC: Ok that's clear. I'm keeping the enum for now but I'll also keep these comments around
// until it's time to merge the PR, in case we want to revisit this before then.

// MC: Update: I'm not going to use this for now as I'm not quite sure what the best way to
// divide up the methods between ValueType and ColumnType is. TODO: Come back to this.

/// The type of a [Value], including the name of the type according to the
/// underlying database. Note that this type is similar to [ValueType],
/// but excludes NULL, which is not a valid type for a column.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ColumnType {
    Boolean(String),
    BigInteger(String),
    BigReal(String),
    Text(String),
}

/// TODO: Add docstring.
#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    // TODO: We will probably want to use ColumnType instead of ValueType here:
    pub sql_type: ValueType,
    pub not_null: bool,
    pub unique: bool,
}

impl PartialOrd for Column {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.sql_type.partial_cmp(&other.sql_type) {
            None => None,
            Some(Ordering::Equal) => {
                if self.not_null != other.not_null {
                    if self.not_null {
                        Some(Ordering::Less)
                    } else {
                        Some(Ordering::Greater)
                    }
                } else if self.unique != other.unique {
                    if self.unique {
                        Some(Ordering::Less)
                    } else {
                        Some(Ordering::Greater)
                    }
                } else {
                    Some(Ordering::Equal)
                }
            }
            Some(ordering) => Some(ordering),
        }
    }
}

impl Column {
    /// Return a blank database column of the minumum type.
    pub fn new() -> Self {
        Column {
            name: "".to_string(),
            // This should never fail unless there are actually no ValueType variants defined:
            sql_type: ValueType::sorted().next().expect("No types defined"),
            not_null: true,
            unique: true,
        }
    }

    /// Determine the database columns needed for each column of data in the given file.
    pub fn from_table_file(filename: &str) -> Result<IndexMap<String, Self>, Error> {
        let delimiter = {
            if filename.to_lowercase().ends_with("tsv") {
                b'\t'
            } else if filename.to_lowercase().ends_with(".csv") {
                b','
            } else {
                return Err(Error::InputError(format!(
                    "Filename: '{filename}' must end with .tsv or .csv"
                )));
            }
        };

        // Read the rows from the given file:
        let mut rdr =
            ReaderBuilder::new()
                .has_headers(false)
                .delimiter(delimiter)
                .from_reader(File::open(filename).map_err(|err| {
                    Error::InputError(format!("Unable to open '{filename}': {err}"))
                })?);
        let mut records = rdr.records();

        // Extract the headers from the first line of the file:
        let headers = {
            let headers = match records.next() {
                None => return Err(Error::InputError(format!("'{filename}' is empty"))),
                Some(record) => match record {
                    Err(err) => {
                        return Err(Error::InputError(format!(
                            "Error reading from '{filename}': {err}"
                        )));
                    }
                    Ok(headers) => headers.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                },
            };
            for header in &headers {
                if header.trim().is_empty() {
                    return Err(Error::InputError(format!(
                        "One or more of the header fields is empty in file '{filename}'"
                    )));
                }
            }
            headers
        };

        // Determine the columns required:
        let columns = {
            let mut columns = vec![];
            let mut values_seen = HashMap::new();
            for row in records {
                let row = row.map_err(|err| {
                    Error::InputError(format!("Error reading from '{filename}': {err}"))
                })?;

                // Determine the columns for the first row if this hasn't been done already:
                if columns.is_empty() {
                    for value in &row {
                        columns.push(
                            Column::new()
                                .min_column_from_strings(vec![value.to_string()].into_iter())?,
                        );
                    }
                }

                // Sanity checks:
                if row.len() != headers.len() {
                    return Err(Error::InputError(format!(
                        "Number of row values ({}) != number of headers ({})",
                        row.len(),
                        headers.len()
                    )));
                }
                if row.len() != columns.len() {
                    return Err(Error::InputError(format!(
                        "Number of row values ({}) != number of columns ({})",
                        row.len(),
                        columns.len()
                    )));
                }

                // Each new row will be used to refine the column types that were determined on the
                // basis of the previous N rows.
                for i in 0..row.len() {
                    if &row[i] == "" {
                        if columns[i].not_null {
                            columns[i].not_null = false;
                        }
                    } else {
                        columns[i] = columns[i]
                            .min_column_from_strings(vec![row[i].to_string()].into_iter())?;
                        if let None = values_seen.get_mut(&headers[i]) {
                            values_seen.insert(headers[i].to_string(), HashSet::new());
                        }
                        let this_column_seen = values_seen.get_mut(&headers[i]).unwrap();
                        if !this_column_seen.insert(row[i].to_string()) && columns[i].unique {
                            columns[i].unique = false;
                        }
                    }
                }
            }

            // Zip everything up into an IndexMap:
            zip(headers, columns)
                .map(|(key, mut column)| {
                    // Don't forget to copy the column name:
                    column.name = key.to_string();
                    (key, column)
                })
                .collect::<IndexMap<_, _>>()
        };

        Ok(columns)
    }

    /// Return the minimum column required to contain the given database values.
    pub fn min_column<I>(&mut self, values: I) -> Result<Column, Error>
    where
        I: Iterator<Item = Value>,
    {
        self.min_column_from_strings(values.map(|value| value.to_string()))
    }

    /// Return the minimum column required to contain the given string values.
    pub fn min_column_from_strings<I>(&mut self, values: I) -> Result<Column, Error>
    where
        I: Iterator<Item = String>,
    {
        // Iterate over the given values and determine the most specific type that is compatible
        // with them all:
        let mut values_seen = HashSet::new();
        let mut types_seen = HashSet::new();
        for value in values {
            // First check whether this is a null value or a duplicate:
            if value == "" {
                if self.not_null {
                    self.not_null = false;
                }
                continue;
            } else if values_seen.contains(&value) {
                if self.unique {
                    self.unique = false;
                }
                continue;
            }

            // Get the most specific type for this value and adjust the overall column type
            // accordingly.
            let sql_type = self.sql_type.min_type(&value)?;
            self.sql_type = sql_type.clone();

            // Add to the sets of values and types seen:
            types_seen.insert(sql_type);
            values_seen.insert(value);
        }

        Ok(self.clone())
    }

    /// Return the minimum column types corresponding to the given vectors of column values.
    pub fn min_columns_from_column_values<I>(column_values: I) -> Result<Vec<Column>, Error>
    where
        I: Iterator<Item = Vec<Value>>,
    {
        column_values
            .map(|values| Column::new().min_column(values.into_iter()))
            .collect::<Result<Vec<Column>, _>>()
    }

    /// Return the minimum columns needed to contain the given vectors of database values.
    pub fn min_columns_from_anonymous_rows<I>(rows: I) -> Result<Vec<Column>, Error>
    where
        I: Iterator<Item = Vec<Value>>,
    {
        let mut column_data = IndexMap::new();
        let mut not_unique = vec![];
        rows.for_each(|row| {
            for i in 0..row.len() {
                match column_data.get_mut(&i) {
                    None => {
                        let mut data = HashSet::new();
                        data.insert(row[i].clone());
                        column_data.insert(i, data);
                    }
                    Some(data) => {
                        if !data.insert(row[i].clone()) {
                            not_unique.push(i);
                        }
                    }
                };
            }
        });

        let mut db_columns = vec![];
        for (index, data) in column_data.into_iter() {
            let mut column = Column::new().min_column(data.into_iter())?;
            if not_unique.contains(&index) {
                column.unique = false;
            }
            db_columns.push(column)
        }

        Ok(db_columns)
    }

    /// Return the minimum columns needed to contain the given database rows.
    pub fn min_row_from_db_rows<I>(db_rows: I) -> Result<IndexMap<String, Column>, Error>
    where
        I: Iterator<Item = Row>,
    {
        let mut keys = IndexSet::new();
        let columns = Column::min_columns_from_anonymous_rows(db_rows.map(|row| {
            row.into_iter()
                .map(|(key, val)| {
                    // Use the values, saving the keys for later:
                    keys.insert(key.to_string());
                    val
                })
                .collect::<Vec<_>>()
        }))?;

        // Zip everything up into an IndexMap and return it:
        Ok(zip(keys, columns)
            .map(|(key, mut column)| {
                // Don't forget to copy the column name:
                column.name = key.to_string();
                (key, column)
            })
            .collect::<IndexMap<_, _>>())
    }
}
