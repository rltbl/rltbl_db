// MC: We had DbType::Null in the old API. Was it unused? (I think it might have been
// - I don't recall actually using it anywhere - but I'm not sure.)
// JO: I think we want to distinguish between the type of a value (which might be NULL)
// and the type of a column (which cannot be NULL).
// Using two different enums is one way to do that, but maybe not the best way.
// MC: Ok that's clear. I'm keeping the enum for now but I'll also keep these comments around
// until it's time to merge the PR, in case we want to revisit this before then.

/// The type of a [Value](crate::Value), including the name of the type according to the
/// underlying database. Note that this type is similar to [ValueType](crate::ValueType),
/// but excludes NULL, which is not a valid type for a column.
pub enum ColumnType {
    Boolean(String),
    BigInteger(String),
    BigReal(String),
    Text(String),
}

/// TODO: Add docstring.
#[allow(dead_code)]
pub struct Column {
    name: String,
    sql_type: ColumnType,
    not_null: bool,
    unique: bool,
}
