use crate::core::{DbError, DbKind, DbQuery, JsonRow, ParamValue, validate_table_name};
use indexmap::IndexMap;

/// Insert the given rows, which have the given columns, to the given table using the given
/// queryable pool and optional returning clause (set with_returning = false to turn this off).
/// When generating the insert statements, do not use more than max_params bound parameters at
/// one time.
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

// Private function for generating an UPDATE statement for the given table using the given
// when clauses and returning vector.
fn generate_update_statement(
    table: &str,
    pkey_clauses: &Vec<String>,
    when_clauses: &IndexMap<String, Vec<String>>,
    with_returning: bool,
    returning: &[&str],
) -> String {
    let case_clause = {
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
        cases.join(",")
    };

    // A where clause is required to restrict the rows that are updated to only those that
    // correspond to the given primary key combinations.
    let where_clause = pkey_clauses
        .iter()
        .map(|pkey_clause| format!("({pkey_clause})"))
        .collect::<Vec<_>>()
        .join(" OR ");
    let where_clause = format!("\nWHERE {where_clause}");

    // The returning clause is optional.
    let returning_clause = match with_returning {
        true => match returning.is_empty() {
            true => format!("\nRETURNING *"),
            false => format!("\nRETURNING {}", returning.join(", ")),
        },
        false => String::new(),
    };

    // The SQL to be returned:
    format!(
        r#"
UPDATE "{table}"
SET {case_clause}{where_clause}{returning_clause}"#,
    )
}

/// Update the given rows in the given table, which has the given primary keys, using the given
/// queryable pool and optional returning clause (set with_returning = false to turn this off).
/// When generating the update statements, do not use more than max_params bound parameters at
/// one time.
pub(crate) async fn update(
    pool: &impl DbQuery,
    max_params: &usize,
    table: &str,
    primary_keys: &[&str],
    rows: &[&JsonRow],
    with_returning: bool,
    returning: &[&str],
) -> Result<Vec<JsonRow>, DbError> {
    // This function generates update statements that look something like the following, where
    // "pk1" and "pk2", below, are the primary key columns for "my_table":
    //
    // UPDATE my_table
    // SET column1 = CASE WHEN pk1 = $1 AND pk2 = $2 THEN $3
    //                    WHEN pk1 = $6 AND pk2 = $7 THEN $8
    //                    WHEN pk1 = $11 AND pk2 = $12 THEN $13
    //                    ELSE column1
    //               END,
    //     column2 = CASE WHEN pk1 = $1 AND pk2 = $2 THEN $4
    //                    WHEN pk1 = $6 AND pk2 = $7 THEN $9
    //                    WHEN pk1 = $11 AND pk2 = $12 THEN $14
    //                    ELSE column2
    //               END,
    //     column3 = CASE WHEN pk1 = $1 AND pk2 = $2 THEN $5
    //                    WHEN pk1 = $6 AND pk2 = $7 THEN $10
    //                    WHEN pk1 = $11 AND pk2 = $12 THEN $15
    //                    ELSE column3
    //               END
    //     WHERE (pk1 = $1 AND pk2 = $2) OR (pk1 = $6 AND pk2 = $7) OR (pk1 = $11 AND pk2 = $12)
    //     RETURNING column1, column2, column3
    if primary_keys.is_empty() {
        return Err(DbError::InputError(
            "Primary keys must not be empty.".to_string(),
        ));
    }

    // We use the column_map to determine the SQL type of each parameter.
    let column_map = pool.columns(&table).await?;
    if !primary_keys.iter().all(|pkey| {
        column_map
            .keys()
            .map(|key| key.as_str())
            .collect::<Vec<_>>()
            .contains(pkey)
    }) {
        return Err(DbError::InputError(format!(
            "The provided primary keys: {primary_keys:?} are not all in the table '{table}'"
        )));
    }

    // Although SQLite allows '$' as a prefix, it is required to use '?' to represent integer
    // literals (see https://sqlite.org/c3ref/bind_blob.html) which is what we need to be able
    // to generate them out of order as in the above example.
    let param_prefix = match pool.kind() {
        DbKind::SQLite => "?",
        DbKind::PostgreSQL => "$",
    };
    let mut rows_to_return = vec![];
    let mut params = vec![];
    let mut param_idx = 0;
    let mut pkey_clauses = vec![];
    let mut when_clauses: IndexMap<String, Vec<String>> = IndexMap::new();
    for row in rows {
        let all_row_columns = row.keys().map(|key| key.as_str()).collect::<Vec<_>>();
        if !primary_keys
            .iter()
            .all(|pkey| all_row_columns.contains(pkey))
        {
            return Err(DbError::InputError(
                "All input rows must have all primary key columns defined".to_string(),
            ));
        }

        if param_idx + all_row_columns.len() > *max_params {
            // It is extremely unlikely but possible in principle that a single row requires
            // more parameters than are allowed, but we check to be sure:
            if pkey_clauses.is_empty() {
                return Err(DbError::InputError(format!(
                    "Number of parameters required to update row exceeds the maximum allowable"
                )));
            }
            // Generate a bulk update statement for all of the rows we have processed so far
            // and append any returned rows to the rows to return:
            let sql = generate_update_statement(
                table,
                &pkey_clauses,
                &when_clauses,
                with_returning,
                returning,
            );
            rows_to_return.append(&mut pool.query(&sql, params.clone()).await?);
            // Clear all of the counters, maps, and parameter vectors we have been using to
            // generate the UPDATE sql:
            param_idx = 0;
            params.clear();
            pkey_clauses.clear();
            when_clauses.clear();
        }

        // The same primary key clause, e.g., "pk_1 = $N AND pk_2 = $M" (see the example above)
        // will be used for every column that needs to be updated in this row.
        let pkey_clause: Result<Vec<String>, DbError> = primary_keys
            .iter()
            .map(|key| {
                // These two unwraps are safe because of the checks that have been
                // performed above:
                let sql_type = column_map.get(*key).unwrap();
                let value = row.get(&key.to_string()).unwrap();
                match pool.convert_json(sql_type, value) {
                    Ok(value) => {
                        params.push(value);
                        param_idx += 1;
                        Ok(format!(r#"{key} = {param_prefix}{param_idx}"#))
                    }
                    Err(err) => Err(err),
                }
            })
            .collect();
        let pkey_clause = pkey_clause?.join(" AND ");
        pkey_clauses.push(pkey_clause.to_string());

        // Filter out the key columns from the row columns that need to be updated:
        let row_columns_to_update = all_row_columns
            .iter()
            .filter(|column| !primary_keys.contains(column))
            .collect::<Vec<_>>();

        // Generate a when clause corresponding to the primary key combination, for each column,
        // and add the generated when_clauses to the when_clauses map:
        for column in row_columns_to_update {
            let when_clause = {
                let sql_type = column_map.get(*column).ok_or(DbError::InputError(format!(
                    "Column '{column}' does not exist in table '{table}'"
                )))?;
                // This unwrap is safe: we are iterating over row columns of which this is one.
                let updated_value = row.get(*column).unwrap();
                let updated_value = pool.convert_json(sql_type, updated_value)?;
                params.push(updated_value);
                param_idx += 1;
                format!("WHEN {pkey_clause} THEN {param_prefix}{param_idx}")
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

    // Generate the final update statement, append any returned rows to rows_to_return,
    // then return everything.
    let sql = generate_update_statement(
        table,
        &pkey_clauses,
        &when_clauses,
        with_returning,
        returning,
    );
    rows_to_return.append(&mut pool.query(&sql, params).await?);
    Ok(rows_to_return)
}
