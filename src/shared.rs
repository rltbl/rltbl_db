use crate::core::{DbError, DbQuery, JsonRow, ParamValue, validate_table_name};

pub(crate) async fn insert(
    pool: &impl DbQuery,
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

    let column_map = pool.columns(&table).await?;
    let column_names = columns
        .iter()
        .map(|c| format!(r#""{c}""#))
        .collect::<Vec<_>>()
        .join(", ");
    if columns.len() > *max_params {
        return Err(DbError::InputError(format!(
            "Unable to insert to table '{}', which has more columns ({}) than the \
                 maximum number of variables ({}) allowed in a SQL statement by {}.",
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
            false => format!("\nRETURNING {}", returning.join(", ")),
        },
        false => String::new(),
    };

    let mut rows_to_return = vec![];
    let mut lines_to_bind: Vec<String> = Vec::new();
    let mut params_to_be_bound: Vec<ParamValue> = Vec::new();
    let mut param_idx = 0;
    for row in rows {
        // If we have reached SQLite's limit on the number of bound parameters, insert what
        // we have so far and then reset all of the counters and collections:
        if param_idx + columns.len() > *max_params {
            let sql = format!(
                r#"INSERT INTO "{table}"({column_names}) VALUES
                   {}{returning_clause}"#,
                lines_to_bind.join(",\n")
            );
            rows_to_return.append(&mut pool.query(&sql, params_to_be_bound.clone()).await?);
            lines_to_bind.clear();
            params_to_be_bound.clear();
            param_idx = 0;
        }

        // Optimization to avoid repeated heap allocations while processing a single given row:
        params_to_be_bound.reserve(columns.len());
        let mut cells: Vec<String> = Vec::with_capacity(columns.len());
        for column in columns {
            param_idx += 1;
            cells.push(format!("${param_idx}"));
            let param = match row.get(*column) {
                Some(value) => {
                    let sql_type = column_map.get(*column).ok_or(DbError::InputError(format!(
                        "Column '{column}' does not exist in table '{table}'"
                    )))?;
                    pool.convert_json(sql_type, value)?
                }
                None => ParamValue::Null,
            };
            params_to_be_bound.push(param);
        }
        let line_to_bind = format!("({})", cells.join(", "));
        lines_to_bind.push(line_to_bind);
    }

    // If there is anything left to insert, insert it now:
    if lines_to_bind.len() > 0 {
        let sql = format!(
            r#"INSERT INTO "{table}"({column_names}) VALUES
               {}{returning_clause}"#,
            lines_to_bind.join(",\n")
        );
        rows_to_return.append(&mut pool.query(&sql, params_to_be_bound).await?);
    }
    Ok(rows_to_return)
}
