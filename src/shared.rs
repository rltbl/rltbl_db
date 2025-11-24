use crate::core::{DbError, DbKind, DbQuery, JsonRow, ParamValue, validate_table_name};
use std::fmt::Display;

pub(crate) enum EditType {
    Insert,
    Update,
}

impl Display for EditType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EditType::Update => write!(f, "UPDATE"),
            EditType::Insert => write!(f, "INSERT"),
        }
    }
}

// Generate a SQL UPDATE statement for the given table and columns using the given clauses
// and the given value lines.
fn generate_update_statement(
    table: &str,
    columns: &[&str],
    set_clause: &str,
    where_clause: &str,
    returning_clause: &str,
    value_lines: &Vec<String>,
) -> String {
    // Quote the column names to avoid potential clashes with database keywords:
    let quoted_columns = columns
        .iter()
        .map(|c| format!(r#""{c}""#))
        .collect::<Vec<_>>()
        .join(", ");

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

// Generate a SQL UPDATE statement for the given table and columns using the given clauses
// and the given value lines.
fn generate_insert_statement(
    table: &str,
    columns: &[&str],
    returning_clause: &str,
    value_lines: &Vec<String>,
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

/// Edit the given rows in the given table using the given queryable pool and optional returning
/// clause (set with_returning = false to turn this off). When generating the SQL statements
/// used to edit the table, do not use more than max_params bound parameters at a time. If more
/// than max_params are required, multiple SQL statements will be generated.
pub(crate) async fn edit(
    pool: &impl DbQuery,
    edit_type: &EditType,
    max_params: &usize,
    table: &str,
    columns: &[&str],
    rows: &[&JsonRow],
    with_returning: bool,
    returning: &[&str],
) -> Result<Vec<JsonRow>, DbError> {
    // Begin by verifying that the given table name is valid, which has the side-effect of
    // removing any enclosing double-quotes:
    let table = validate_table_name(table)?;

    // This is very unlikely but we check anyway to be sure:
    if columns.len() > *max_params {
        return Err(DbError::InputError(format!(
            "Unable to {} table '{}', which has more columns ({}) than the \
             maximum number of variables ({}) allowed in a SQL statement by {}.",
            edit_type,
            table,
            columns.len(),
            max_params,
            pool.kind()
        )));
    }

    // Use the `returning` argument to restrict the RETURNING clause, defaulting
    // to '*' if `returning` is empty:
    let returning_clause = match with_returning {
        true => match returning.is_empty() {
            true => format!("\nRETURNING *"),
            false => format!(
                "\nRETURNING {}",
                returning
                    .iter()
                    .map(|column| format!(r#""{table}"."{column}""#))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        },
        false => String::new(),
    };

    let (set_clause, where_clause) = match edit_type {
        EditType::Update => {
            let primary_keys = match pool.primary_keys(&table).await? {
                primary_keys if primary_keys.is_empty() => {
                    return Err(DbError::InputError(
                        "Primary keys must not be empty.".to_string(),
                    ));
                }
                primary_keys
                    if !primary_keys
                        .iter()
                        .all(|pkey| columns.contains(&pkey.as_str())) =>
                {
                    return Err(DbError::InputError(format!(
                        "Not all of the table's primary keys: {primary_keys:?} are in {columns:?}"
                    )));
                }
                primary_keys => primary_keys,
            };
            let set_clause = columns
                .iter()
                .filter(|column| !primary_keys.contains(&column.to_string()))
                .map(|column| format!(r#""{column}" = "source"."{column}""#))
                .collect::<Vec<_>>()
                .join(", ");
            let where_clause = primary_keys
                .iter()
                .map(|pk| format!(r#""{table}"."{pk}" = "source"."{pk}""#,))
                .collect::<Vec<_>>()
                .join(" AND ");
            (set_clause, where_clause)
        }
        // The SET and WHERE clauses are only applicable to updates so we just return
        // empty strings in the case of an insert:
        EditType::Insert => (String::new(), String::new()),
    };

    // We use the column_map to determine the SQL type of each parameter.
    let column_map = pool.columns(&table).await?;
    // Although SQLite allows '$' as a prefix, it is required to use '?' to represent integer
    // literals (see https://sqlite.org/c3ref/bind_blob.html) which is what we are using here.
    let param_prefix = match pool.kind() {
        DbKind::SQLite => "?",
        DbKind::PostgreSQL => "$",
    };
    let mut rows_to_return = vec![];
    let mut lines_to_bind = Vec::new();
    let mut params_to_be_bound = Vec::new();
    let mut param_idx = 0;
    for row in rows {
        // If we have reached the limit on the number of bound parameters, edit the rows that
        // we have processed so far and then reset all of the counters and collections:
        if param_idx + columns.len() > *max_params {
            let sql = match edit_type {
                EditType::Update => generate_update_statement(
                    &table,
                    columns,
                    &set_clause,
                    &where_clause,
                    &returning_clause,
                    &lines_to_bind,
                ),
                EditType::Insert => {
                    generate_insert_statement(&table, columns, &returning_clause, &lines_to_bind)
                }
            };
            rows_to_return.append(&mut pool.query(&sql, params_to_be_bound.clone()).await?);
            lines_to_bind.clear();
            params_to_be_bound.clear();
            param_idx = 0;
        }

        // Optimization to avoid repeated heap allocations while processing a single given row:
        params_to_be_bound.reserve(columns.len());
        let mut cells: Vec<String> = Vec::with_capacity(columns.len());
        for column in columns {
            let sql_type = column_map.get(*column).ok_or(DbError::InputError(format!(
                "Column '{column}' does not exist in table '{table}'"
            )))?;
            param_idx += 1;
            match pool.kind() {
                DbKind::SQLite => cells.push(format!("{param_prefix}{param_idx}")),
                DbKind::PostgreSQL => cells.push(format!(
                    "{param_prefix}{param_idx}::{}",
                    sql_type.to_uppercase()
                )),
            };
            let param = match row.get(*column) {
                Some(value) => pool.convert_json(sql_type, value)?,
                None => ParamValue::Null,
            };
            params_to_be_bound.push(param);
        }
        let line_to_bind = format!("({})", cells.join(", "));
        lines_to_bind.push(line_to_bind);
    }

    // If there is anything left to insert, insert it now:
    if lines_to_bind.len() > 0 {
        let sql = match edit_type {
            EditType::Update => generate_update_statement(
                &table,
                columns,
                &set_clause,
                &where_clause,
                &returning_clause,
                &lines_to_bind,
            ),
            EditType::Insert => {
                generate_insert_statement(&table, columns, &returning_clause, &lines_to_bind)
            }
        };

        rows_to_return.append(&mut pool.query(&sql, params_to_be_bound).await?);
    }
    Ok(rows_to_return)
}
