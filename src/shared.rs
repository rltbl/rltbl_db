use std::fmt::Display;

/// Ways in which to edit a table.
#[allow(unused)]
#[derive(PartialEq, Eq)]
pub enum EditType {
    Insert,
    Update,
    #[allow(unused)]
    Upsert,
}

impl Display for EditType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EditType::Update => write!(f, "UPDATE"),
            EditType::Insert => write!(f, "INSERT"),
            EditType::Upsert => write!(f, "UPSERT"),
        }
    }
}

/// Generate a SQL UPDATE statement for the given table and columns using the given clauses
/// and the given value lines.
#[allow(unused)]
pub(crate) fn generate_update_statement(
    table: &str,
    columns: &[&str],
    primary_keys: &[&str],
    returning_clause: &str,
    value_lines: &[&str],
) -> String {
    // Quote the column names to avoid potential clashes with database keywords:
    let quoted_columns = columns
        .iter()
        .map(|c| format!(r#""{c}""#))
        .collect::<Vec<_>>()
        .join(", ");

    let set_clause = columns
        .iter()
        .filter(|column| !primary_keys.contains(&column))
        .map(|column| format!(r#""{column}" = "source"."{column}""#))
        .collect::<Vec<_>>()
        .join(", ");

    let where_clause = primary_keys
        .iter()
        .map(|pk| format!(r#""{table}"."{pk}" = "source"."{pk}""#,))
        .collect::<Vec<_>>()
        .join(" AND ");

    format!(
        r#"WITH "source" ({quoted_columns}) AS (
  VALUES
  {}
)
UPDATE "{table}"
SET {set_clause}
FROM "source"
WHERE {where_clause}{returning_clause}"#,
        value_lines.join(",\n")
    )
}

/// Generate a SQL INSERT statement for the given table and columns using the given clauses
/// and the given value lines.
#[allow(unused)]
pub(crate) fn generate_insert_statement(
    table: &str,
    columns: &[&str],
    returning_clause: &str,
    value_lines: &[&str],
) -> String {
    // Quote the column names to avoid potential clashes with database keywords:
    let quoted_columns = columns
        .iter()
        .map(|c| format!(r#""{c}""#))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        r#"INSERT INTO "{table}" ({quoted_columns})
VALUES
{}{returning_clause}"#,
        value_lines.join(",\n")
    )
}

/// Generate SQL statement of the form:
/// INSERT INTO <table> VALUES <tuples> ON CONFLICT (<primary key constraint>) DO UPDATE ...
#[allow(unused)]
pub(crate) fn generate_upsert_statement(
    table: &str,
    columns: &[&str],
    primary_keys: &[&str],
    returning_clause: &str,
    value_lines: &[&str],
) -> String {
    let quoted_columns = columns
        .iter()
        .map(|c| format!(r#""{c}""#))
        .collect::<Vec<_>>()
        .join(", ");

    let constraint_clause = primary_keys
        .iter()
        .map(|pk| format!(r#""{pk}""#))
        .collect::<Vec<_>>()
        .join(", ");

    let set_clause = columns
        .iter()
        .filter(|column| !primary_keys.contains(&column))
        .map(|column| format!(r#""{column}" = "excluded"."{column}""#))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        r#"INSERT INTO "{table}" ({quoted_columns})
VALUES
{}
ON CONFLICT ({constraint_clause}) DO UPDATE SET {set_clause}{returning_clause}"#,
        value_lines.join(",\n"),
    )
}
