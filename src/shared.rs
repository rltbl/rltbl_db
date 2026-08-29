use std::fmt::Display;

use crate::{AnyPool, Error, Row, Rows, Value};

/// Ways in which to edit a table.
#[allow(unused)]
#[derive(PartialEq, Eq)]
pub(crate) enum EditType {
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

/// Edit the given rows in the given table using the given queryable and optional returning
/// clause (set with_returning = false to turn this off). When generating the SQL statements
/// used to edit the table, do not use more than max_params bound parameters at a time. If more
/// than max_params are required, multiple SQL statements will be generated.
#[allow(unused)]
pub(crate) async fn edit(
    pool: &AnyPool,
    edit_type: &EditType,
    max_params: &usize,
    table: &str,
    columns: &[&str],
    rows: impl IntoIterator<Item = &Row>,
    with_returning: bool,
    returning: &[&str],
) -> Result<Rows, Error> {
    // TODO: Begin by verifying that the given table name is valid, which has the side-effect of
    // removing any enclosing double-quotes:
    // let table = validate_table_name(table)?;

    // This is very unlikely but we check anyway to be sure:
    if columns.len() > *max_params {
        return Err(Error::InputError(format!(
            "Unable to {} table '{}', which has more columns ({}) than the \
             maximum number of variables ({}) allowed in a SQL statement by {}.",
            edit_type,
            table,
            columns.len(),
            max_params,
            pool.syntax().name(),
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

    let primary_keys = match edit_type {
        EditType::Update | EditType::Upsert => match pool.primary_keys(&table).await? {
            primary_keys if primary_keys.is_empty() => {
                return Err(Error::InputError(
                    "Primary keys must not be empty.".to_string(),
                ));
            }
            primary_keys
                if !primary_keys
                    .iter()
                    .all(|pkey| columns.contains(&pkey.as_str())) =>
            {
                return Err(Error::InputError(format!(
                    "Not all of the table's primary keys: {primary_keys:?} are in {columns:?}"
                )));
            }
            primary_keys => primary_keys,
        },
        // Since we don't need a list of the table's primary keys to do an insert, we save
        // the database access and just return an empty list here:
        EditType::Insert => vec![],
    };

    // We use the column_map to determine the SQL type of each parameter.
    let column_map = pool.columns(&table).await?;
    let param_prefix = pool.syntax().param_prefix();
    let mut rows_to_return = vec![];
    let mut lines_to_bind = Vec::new();
    let mut params_to_be_bound = Vec::new();
    let mut param_idx = 0;

    // Closure used to edit the table by executing a SQL statement using the given data and SQL
    // clauses. After executing the statement, the lines and parameters to bind / be bound
    // are cleared, and any rows returned by the query are returned to the caller.
    let execute_batch_edit_and_reset = async |lines_to_bind: &mut Vec<String>,
                                              param_idx: &mut usize,
                                              params_to_be_bound: &mut Vec<Value>|
           -> Result<Rows, Error> {
        let sql = match edit_type {
            EditType::Update => generate_update_statement(
                &table,
                columns,
                primary_keys
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .as_slice(),
                &returning_clause,
                lines_to_bind
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .as_slice(),
            ),
            EditType::Insert => generate_insert_statement(
                &table,
                columns,
                &returning_clause,
                lines_to_bind
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .as_slice(),
            ),
            EditType::Upsert => generate_upsert_statement(
                &table,
                columns,
                primary_keys
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .as_slice(),
                &returning_clause,
                lines_to_bind
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .as_slice(),
            ),
        };
        // TODO: Use the "no_cache_clean" version of query().
        let rows = pool.query(&sql, &params_to_be_bound[..]).await?;
        lines_to_bind.clear();
        params_to_be_bound.clear();
        *param_idx = 0;
        Ok(rows)
    };

    for row in rows {
        // If we have reached the limit on the number of bound parameters, edit the rows that
        // we have processed so far and then reset all of the counters and collections:
        if param_idx + columns.len() > *max_params {
            rows_to_return.append(
                &mut execute_batch_edit_and_reset(
                    &mut lines_to_bind,
                    &mut param_idx,
                    &mut params_to_be_bound,
                )
                .await?
                .rows,
            );
        }

        // Optimization to avoid repeated heap allocations while processing a single given row:
        params_to_be_bound.reserve(columns.len());
        let mut cells: Vec<String> = Vec::with_capacity(columns.len());
        for column in columns {
            let sql_type = column_map.get(*column).ok_or(Error::InputError(format!(
                "Column '{column}' does not exist in table '{table}'"
            )))?;
            param_idx += 1;
            // In the CTE we generate for UPDATE statements, tokio-postgres can't infer the types
            // of the VALUES, so we explicitly cast them.
            if *edit_type == EditType::Update
                && pool.syntax().name() == "postgres"
                // We only need to cast the first value row. The rest are inferred by Postgres:
                && lines_to_bind.len() == 0
            {
                cells.push(format!(
                    "{param_prefix}{param_idx}::{}",
                    sql_type.to_uppercase()
                ));
            } else {
                cells.push(format!("{param_prefix}{param_idx}"));
            }
            let param = match row.get(*column) {
                Some(value) => value.clone(),
                None => Value::Null,
            };
            params_to_be_bound.push(param);
        }
        let line_to_bind = format!("({})", cells.join(", "));
        lines_to_bind.push(line_to_bind);
    }

    // If there is anything left to edit, do it now:
    if lines_to_bind.len() > 0 {
        rows_to_return.append(
            &mut execute_batch_edit_and_reset(
                &mut lines_to_bind,
                &mut param_idx,
                &mut params_to_be_bound,
            )
            .await?
            .rows,
        );
    }

    // TODO: Delete dirty entries from the cache in accordance with our caching strategy:
    // clear_cache_for_edited_tables(pool, &[&table]).await?;

    Ok(Rows {
        rows: rows_to_return,
    })
}

/* TODO:
/// Read data from the given file and insert it to the given table in the database.
pub async fn batch_insert(
    pool: &AnyPool,
    table: &str,
    columns: &IndexMap<String, Column>,
    filename: &str,
) -> Result<(), Error> {
    let batch_size = 100;

    eprintln!(
        "Loading table '{table}' from '{filename}' using batch_insert() \
         with batch size {batch_size}"
    );

    // TODO: Change the signature of insert() and similar methods so that they take iterators
    // as arguments instead of impl IntoRows. Then we will not have to collect the contents
    // of the file into a vector but can keep it in the form of an iterator as we do in
    // TokioPostgreSQLPool::load_table().

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

    // Read the rows from the given file into a vector.
    let rdr =
        ReaderBuilder::new()
            .has_headers(false)
            .delimiter(delimiter)
            .from_reader(File::open(filename).map_err(|err| {
                Error::InputError(format!("Unable to open '{filename}': {err}"))
            })?);
    let mut records = rdr.into_records();

    // Extract the columns from the first line of the file:
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

    // Collect all of the rows into vectors and insert them
    // (TODO: See comment at the beginning of this function).
    let mut db_rows = vec![];
    let str_columns = headers.iter().map(|s| s.as_str()).collect::<Vec<_>>();
    for row in records {
        let row_values = row
            .map_err(|err| {
                Error::DataError(format!("Error reading from file '{filename}': {err}"))
            })?
            .into_iter()
            .map(|value| Value::from(value))
            .collect::<Vec<_>>();
        let db_row = Row {
            map: zip(headers.clone(), row_values).collect::<IndexMap<_, _>>(),
        };

        // Logically we should be able to call coerce for both SQLite and PostgreSQL, but in the
        // case of SQLite, since it is so liberal about types, this doesn't actually matter, and
        // it avoids having to worry about differences in the rounding of real numbers between
        // libsql and rusqlite.
        if pool.kind().name() != "SQLite" {
            db_rows.push(db_row.coerce(columns)?);
        } else {
            db_rows.push(db_row);
        }

        // We don't insert more than batch_size at a time:
        if db_rows.len() >= batch_size {
            pool.insert(table, &str_columns, db_rows.clone()).await?;
            db_rows.clear();
        }
    }
    // Insert anything that's left:
    if db_rows.len() > 0 {
        pool.insert(table, &str_columns, db_rows).await?;
    }
    Ok(())
}

*/
