use crate::core::{DbError, DbQuery, JsonRow, ParamValue, validate_table_name};
use indexmap::IndexMap;

/// TODO: Add docstring here.
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

/// TODO: Add docstring here.
pub(crate) async fn update(
    pool: &impl DbQuery,
    max_params: &usize,
    table: &str,
    keys: &[&str],
    rows: &[&JsonRow],
    with_returning: bool,
    returning: &[&str],
) -> Result<Vec<JsonRow>, DbError> {
    // The goal is to generate update statements that look something like the following, where
    // "foo" and "bar", below, are the primary key columns for "my_table":
    //
    // UPDATE "my_table"
    // SET "column_1" = CASE WHEN foo = _ AND bar = _ THEN _
    //                       WHEN foo = _ AND bar = _ THEN _
    //                       WHEN foo = _ AND bar = _ THEN _
    //                       ELSE "column_1"
    //                  END,
    //     "column_2" = CASE WHEN foo = _ AND bar = _ THEN _
    //                       WHEN foo = _ AND bar = _ THEN _
    //                       WHEN foo = _ AND bar = _ THEN _
    //                       ELSE "column_2"
    //                  END;

    if keys.is_empty() {
        return Err(DbError::InputError("Keys must not be empty.".to_string()));
    }

    // We use the column_map to determine the SQL type of each parameter.
    let column_map = pool.columns(&table).await?;

    let mut when_clauses: IndexMap<String, Vec<String>> = IndexMap::new();
    for row in rows {
        let all_columns = row.keys().map(|key| key.as_str()).collect::<Vec<_>>();
        if !keys.iter().all(|key| all_columns.contains(key)) {
            return Err(DbError::InputError(
                "All input rows must have all primary key columns defined".to_string(),
            ));
        }

        // Filter out the key columns from the columns to update:
        let columns_to_update = all_columns
            .iter()
            .filter(|column| !keys.contains(column))
            .collect::<Vec<_>>();

        // Generate a when clause for the key_combination, and push it to the entry for this
        // column in the when_clauses map:
        for column in columns_to_update {
            let when_clause = {
                let key_clause = keys
                    .iter()
                    .map(|key| {
                        let val = row.get(&key.to_string()).unwrap();
                        // TODO: Use params.
                        format!(r#"{key} = {val}"#)
                    })
                    .collect::<Vec<_>>()
                    .join(" AND ");
                let updated_value = row.get(&column.to_string()).unwrap();
                format!("WHEN {key_clause} THEN {updated_value}")
            };
            match when_clauses.get_mut(&column.to_string()) {
                Some(clauses) => {
                    clauses.push(when_clause);
                }
                None => {
                    when_clauses.insert(column.to_string(), vec![when_clause]);
                }
            };
        }
    }

    let cases = {
        let mut cases = vec![];
        for (column, clauses) in when_clauses.iter() {
            cases.push(format!(
                r#"
"{column}" = CASE
{}
ELSE "{column}"
END"#,
                clauses.join("\n")
            ));
        }
        cases
    };

    let returning_clause = match with_returning {
        true => match returning.is_empty() {
            true => format!("\nRETURNING *"),
            false => format!("\nRETURNING {}", returning.join(", ")),
        },
        false => String::new(),
    };

    let sql = format!(
        r#"
UPDATE "{table}"
SET {}{returning_clause}"#,
        cases.join(",")
    );

    let rows = pool.query(&sql, ()).await?;
    Ok(rows)
}
