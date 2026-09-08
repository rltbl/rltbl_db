use indexmap::IndexMap;

use crate::Column;

// MC: What is this going to be used for?

#[allow(dead_code)]
pub struct Table {
    name: String,
    columns: IndexMap<String, Column>,
    primary_keys: Vec<String>,
}
