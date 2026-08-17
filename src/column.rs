// MC: We had DbType::Null in the old API. Was it unused? (I think it might have been
// - I don't recall actually using it anywhere - but I'm not sure.)
// JO: I think we want to distinguish between the type of a value (which might be NULL)
// and the type of a column (which cannot be NULL).
// Using two different enums is one way to do that, but maybe not the best way.

// All the same types as Value, excluding NULL.
pub enum ColumnType {
    Boolean(String),
    BigInteger(String),
    BigReal(String),
    Text(String),
}

#[allow(dead_code)]
pub struct Column {
    name: String,
    sql_type: ColumnType,
    not_null: bool,
    unique: bool,
}
