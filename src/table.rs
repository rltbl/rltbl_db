use indexmap::IndexMap;

use crate::Column;

#[allow(dead_code)]
pub struct Table {
    name: String,
    columns: IndexMap<String, Column>,
    primary_keys: Vec<String>,
}
