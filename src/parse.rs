//! Code for parsing SQL expressions.

use crate::core::{DbError, QUERY_CACHE_TABLE, TABLE_CACHE_TABLE};
use lazy_static::lazy_static;
use regex::Regex;
use std::collections::HashSet;
use tree_sitter::{Node, Parser};
use tree_sitter_sequel::LANGUAGE as SQL_LANGUAGE;

/// Validates that a given [Node] is not an error node:
fn validate_node(node: &Node<'_>, sql: &str) -> Result<(), DbError> {
    if node.is_error() {
        return Err(DbError::ParseError(format!(
            "Error parsing '{sql}': {node}"
        )));
    }
    Ok(())
}

/// Validates that the given list of [Node]s is of the expected length:
fn validate_list_len(node_list: &Vec<Node<'_>>, len: usize) -> Result<(), DbError> {
    if node_list.len() != len {
        return Err(DbError::ParseError(format!(
            "Wrong number of values: {}. Expected: {}",
            node_list.len(),
            len
        )));
    }
    Ok(())
}

/// Represents a valid database table name.
static VALID_TABLE_NAME_MATCH_STR: &str = r"^[A-Za-z_][0-9A-Za-z_]*$";

lazy_static! {
    /// The regex used to match [valid database table names](VALID_TABLE_NAME_MATCH_STR).
    static ref VALID_TABLE_NAME_REGEX: Regex = Regex::new(VALID_TABLE_NAME_MATCH_STR).unwrap();
}

/// Determines whether the given table name is a valid database table name. Valid database table
/// names must match the regular expression: `^[A-Za-z_\]\[0-9A-Za-z_]*$`. For convenience, a
/// double-quoted valid table name is also accepted as valid. The function returns the table name,
/// if valid, with the surrounding double-quotes (if any) removed, or an error if the table name is
/// invalid.
pub fn validate_table_name(table_name: &str) -> Result<String, DbError> {
    let error_msg = format!(
        "Not a valid table name: \"{table_name}\". Valid table names must match \
         the regular expression: '{VALID_TABLE_NAME_MATCH_STR}' and may possibly begin and \
         end with double-quotes."
    );
    let table_name = match table_name.strip_prefix("\"") {
        Some(table_name) => match table_name.strip_suffix("\"") {
            Some(table_name) => table_name,
            None => return Err(DbError::InputError(error_msg)),
        },
        None => match table_name.strip_suffix("\"") {
            Some(_) => return Err(DbError::InputError(error_msg)),
            None => table_name,
        },
    };
    match VALID_TABLE_NAME_REGEX.is_match(table_name) {
        true => Ok(table_name.to_string()),
        false => Err(DbError::InputError(error_msg)),
    }
}

/// Given `view_sql`, which is the SQL code that will result in the creation of a view, parse
/// it, determine what the view's source tables are, and return the list.
pub fn get_view_tables(view_sql: &str) -> Result<Vec<String>, DbError> {
    // Instantiate the parser and try to parse the code:
    let mut parser = Parser::new();
    parser
        .set_language(&SQL_LANGUAGE.into())
        .map_err(|err| DbError::ParseError(format!("Error setting language to SQL: {err}")))?;
    let tree = match parser.parse(&view_sql, None) {
        Some(tree) => tree,
        None => return Err(DbError::ParseError(format!("Could not parse '{view_sql}'"))),
    };

    // Collect the top-level statements:
    let statements = {
        let root_node = tree.root_node();
        validate_node(&root_node, &view_sql)?;
        if root_node.kind().to_lowercase() != "program" {
            return Err(DbError::ParseError(format!(
                "Unexpected root node kind: {}",
                root_node.kind()
            )));
        }
        root_node
            .children(&mut root_node.walk())
            .filter(|child| child.kind().to_lowercase() == "statement")
            .collect::<Vec<_>>()
    };

    let mut view_tables = vec![];
    for statement in &statements {
        validate_node(&statement, &view_sql)?;
        for instruction in statement.children(&mut tree.walk()) {
            validate_node(&instruction, &view_sql)?;
            match instruction.kind().to_lowercase().as_str() {
                "create_view" => {
                    let create_query = instruction
                        .children(&mut instruction.walk())
                        .filter(|child| child.kind().to_lowercase() == "create_query")
                        .collect::<Vec<_>>();
                    validate_list_len(&create_query, 1)?;
                    let create_query = create_query[0];
                    validate_node(&create_query, &view_sql)?;

                    let from = create_query
                        .children(&mut create_query.walk())
                        .filter(|child| child.kind().to_lowercase() == "from")
                        .collect::<Vec<_>>();
                    validate_list_len(&from, 1)?;
                    let from = from[0];
                    validate_node(&from, &view_sql)?;

                    let relations = from
                        .children(&mut from.walk())
                        .filter(|child| child.kind().to_lowercase() == "relation")
                        .collect::<Vec<_>>();
                    for relation in relations {
                        validate_node(&relation, &view_sql)?;
                        let object_ref = relation
                            .children(&mut relation.walk())
                            .filter(|child| child.kind().to_lowercase() == "object_reference")
                            .collect::<Vec<_>>();
                        validate_list_len(&object_ref, 1)?;
                        let object_ref = object_ref[0];
                        validate_node(&object_ref, &view_sql)?;

                        let source_table = view_sql.to_string()
                            [object_ref.start_byte()..object_ref.end_byte()]
                            .to_string();
                        view_tables.push(source_table);
                    }
                    break;
                }
                _ => (),
            }
        }
    }
    Ok(view_tables)
}

/// Parse the given string, representing a series of (semi-colon-separated) SQL commands,
/// into their constituents and determine the tables and views that will be accessed when
/// the commands are executed, if any. Three sets are returned. The first contains tables
/// and views that are going to be edited (targets of commands like INSERT, UPDATE, DELETE,
/// ALTER, and TRUNCATE), the second contains tables and views that are going to be dropped
/// (targets of a DROP TABLE or DROP VIEW command). The third contains tables and views that
/// are only going to be read (targets of most other commands). Note table modifications that
/// occur _within_ a common table expression (CTE) are not recognized by this function. Such
/// table-modifying CTEs are supported in principle by PostgreSQL (see
/// <https://www.postgresql.org/docs/current/queries-with.html#QUERIES-WITH-MODIFYING>) but
/// seemingly not by SQLite (see <https://sqlite.org/lang_with.html>).
pub fn get_accessed_tables(
    sql: &str,
) -> Result<(HashSet<String>, HashSet<String>, HashSet<String>), DbError> {
    // Instantiate the parser and read in the given sql string:
    let mut parser = Parser::new();
    parser
        .set_language(&SQL_LANGUAGE.into())
        .map_err(|err| DbError::ParseError(format!("Error setting language to SQL: {err}")))?;
    let tree = match parser.parse(sql, None) {
        Some(tree) => tree,
        None => return Err(DbError::ParseError(format!("Could not parse '{sql}'"))),
    };

    // Collect the top-level statements:
    let statements = {
        let root_node = tree.root_node();
        validate_node(&root_node, sql)?;
        if root_node.kind().to_lowercase() != "program" {
            return Err(DbError::ParseError(format!(
                "Unexpected root node kind: {}",
                root_node.kind()
            )));
        }
        let children = root_node
            .children(&mut root_node.walk())
            .collect::<Vec<_>>();
        if children.len() > 0 && children.first().unwrap().kind().to_lowercase() == "transaction" {
            children
                .first()
                .unwrap()
                .children(&mut root_node.walk())
                .filter(|child| child.kind().to_lowercase() == "statement")
                .collect::<Vec<_>>()
        } else {
            root_node
                .children(&mut root_node.walk())
                .filter(|child| child.kind().to_lowercase() == "statement")
                .collect::<Vec<_>>()
        }
    };

    // Determine the tables that will be modified:
    let mut edited_tables = HashSet::new();
    let mut dropped_tables = HashSet::new();
    let mut read_tables = HashSet::new();
    for statement in &statements {
        validate_node(&statement, sql)?;
        for instruction in statement.children(&mut tree.walk()) {
            validate_node(&instruction, sql)?;
            match instruction.kind().to_lowercase().as_str() {
                "select" => {
                    let from = instruction
                        .next_sibling()
                        .ok_or(DbError::InputError(format!("Invalid SQL: {sql}")))?;

                    let relations = from
                        .children(&mut from.walk())
                        .filter(|child| child.kind().to_lowercase() == "relation")
                        .collect::<Vec<_>>();
                    for relation in relations {
                        validate_node(&relation, &sql)?;
                        let object_ref = relation
                            .children(&mut relation.walk())
                            .filter(|child| child.kind().to_lowercase() == "object_reference")
                            .collect::<Vec<_>>();
                        validate_list_len(&object_ref, 1)?;
                        let object_ref = object_ref[0];
                        validate_node(&object_ref, &sql)?;

                        let source_table = validate_table_name(
                            &sql.to_string()[object_ref.start_byte()..object_ref.end_byte()],
                        )?;
                        read_tables.insert(source_table.to_string());
                    }
                }
                "insert" => {
                    let table_name = {
                        let object_ref = instruction
                            .children(&mut instruction.walk())
                            .filter(|child| child.kind().to_lowercase() == "object_reference")
                            .collect::<Vec<_>>();
                        validate_list_len(&object_ref, 1)?;
                        let object_ref = object_ref[0];

                        let identifier = object_ref
                            .children(&mut object_ref.walk())
                            .filter(|child| child.kind().to_lowercase() == "identifier")
                            .collect::<Vec<_>>();
                        validate_list_len(&identifier, 1)?;
                        let identifier = identifier[0];

                        validate_table_name(
                            &sql.to_string()[identifier.start_byte()..identifier.end_byte()],
                        )?
                    };
                    edited_tables.insert(table_name);
                }
                "update" => {
                    let table_name = {
                        let relation = instruction
                            .children(&mut instruction.walk())
                            .filter(|child| child.kind().to_lowercase() == "relation")
                            .collect::<Vec<_>>();
                        validate_list_len(&relation, 1)?;
                        let relation = relation[0];

                        let object_ref = relation
                            .children(&mut relation.walk())
                            .filter(|child| child.kind().to_lowercase() == "object_reference")
                            .collect::<Vec<_>>();
                        validate_list_len(&object_ref, 1)?;
                        let object_ref = object_ref[0];

                        let identifier = object_ref
                            .children(&mut object_ref.walk())
                            .filter(|child| child.kind().to_lowercase() == "identifier")
                            .collect::<Vec<_>>();
                        validate_list_len(&identifier, 1)?;
                        let identifier = identifier[0];

                        validate_table_name(
                            &sql.to_string()[identifier.start_byte()..identifier.end_byte()],
                        )?
                    };
                    edited_tables.insert(table_name);
                }
                "delete" => {
                    let table_name = {
                        let details =
                            instruction
                                .next_sibling()
                                .ok_or(DbError::ParseError(format!(
                                    "No details found for '{}'",
                                    instruction.kind()
                                )))?;
                        let object_ref = details
                            .children(&mut details.walk())
                            .filter(|child| child.kind().to_lowercase() == "object_reference")
                            .collect::<Vec<_>>();
                        validate_list_len(&object_ref, 1)?;
                        let object_ref = object_ref[0];

                        let identifier = object_ref
                            .children(&mut object_ref.walk())
                            .filter(|child| child.kind().to_lowercase() == "identifier")
                            .collect::<Vec<_>>();
                        validate_list_len(&identifier, 1)?;
                        let identifier = identifier[0];

                        validate_table_name(
                            &sql.to_string()[identifier.start_byte()..identifier.end_byte()],
                        )?
                    };
                    edited_tables.insert(table_name);
                }
                "keyword_truncate" => {
                    let mut possible_next_word = instruction.next_sibling();
                    while let Some(next_word) = possible_next_word {
                        if next_word.kind().to_lowercase() == "object_reference" {
                            let identifier = next_word
                                .children(&mut next_word.walk())
                                .filter(|child| child.kind().to_lowercase() == "identifier")
                                .collect::<Vec<_>>();
                            validate_list_len(&identifier, 1)?;
                            let identifier = identifier[0];

                            let table = validate_table_name(
                                &sql.to_string()[identifier.start_byte()..identifier.end_byte()],
                            )?;
                            edited_tables.insert(table);
                        }
                        possible_next_word = next_word.next_sibling();
                    }
                }
                "drop_table" => {
                    let table_name = {
                        let object_ref = instruction
                            .children(&mut instruction.walk())
                            .filter(|child| child.kind().to_lowercase() == "object_reference")
                            .collect::<Vec<_>>();
                        validate_list_len(&object_ref, 1)?;
                        let object_ref = object_ref[0];

                        let identifier = object_ref
                            .children(&mut object_ref.walk())
                            .filter(|child| child.kind().to_lowercase() == "identifier")
                            .collect::<Vec<_>>();
                        validate_list_len(&identifier, 1)?;
                        let identifier = identifier[0];

                        validate_table_name(
                            &sql.to_string()[identifier.start_byte()..identifier.end_byte()],
                        )?
                    };
                    dropped_tables.insert(table_name);
                }
                "drop_view" => {
                    let view_name = {
                        let object_ref = instruction
                            .children(&mut instruction.walk())
                            .filter(|child| child.kind().to_lowercase() == "object_reference")
                            .collect::<Vec<_>>();
                        validate_list_len(&object_ref, 1)?;
                        let object_ref = object_ref[0];

                        let identifier = object_ref
                            .children(&mut object_ref.walk())
                            .filter(|child| child.kind().to_lowercase() == "identifier")
                            .collect::<Vec<_>>();
                        validate_list_len(&identifier, 1)?;
                        let identifier = identifier[0];

                        validate_table_name(
                            &sql.to_string()[identifier.start_byte()..identifier.end_byte()],
                        )?
                    };
                    dropped_tables.insert(view_name);
                }
                "alter_table" => {
                    let table_name = {
                        let object_ref = instruction
                            .children(&mut instruction.walk())
                            .filter(|child| child.kind().to_lowercase() == "object_reference")
                            .collect::<Vec<_>>();
                        validate_list_len(&object_ref, 1)?;
                        let object_ref = object_ref[0];

                        let identifier = object_ref
                            .children(&mut object_ref.walk())
                            .filter(|child| child.kind().to_lowercase() == "identifier")
                            .collect::<Vec<_>>();
                        validate_list_len(&identifier, 1)?;
                        let identifier = identifier[0];

                        validate_table_name(
                            &sql.to_string()[identifier.start_byte()..identifier.end_byte()],
                        )?
                    };
                    edited_tables.insert(table_name);
                }
                // Silently ignore all other kinds of instructions.
                _ => (),
            };
        }
    }

    // Edits of the cache tables themeselves are never cached so we do not need to report them.
    // Dropping one of the cache tables, by contrast, is always reported.
    let edited_tables = edited_tables
        .into_iter()
        .filter(|table| ![QUERY_CACHE_TABLE, TABLE_CACHE_TABLE].contains(&table.as_str()))
        .collect::<HashSet<_>>();

    Ok((
        edited_tables.clone(),
        dropped_tables.clone(),
        read_tables.clone(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_table_names() {
        // Valid table names:
        assert_eq!(
            validate_table_name(r#"table"#).expect("Expected table name to be valid"),
            "table"
        );
        assert_eq!(
            validate_table_name(r#"my_table"#).expect("Expected table name to be valid"),
            "my_table"
        );
        assert_eq!(
            validate_table_name(r#"my_2nd_table"#).expect("Expected table name to be valid"),
            "my_2nd_table"
        );
        assert_eq!(
            validate_table_name(r#"my_table_2"#).expect("Expected table name to be valid"),
            "my_table_2"
        );
        assert_eq!(
            validate_table_name(r#"my_table2"#).expect("Expected table name to be valid"),
            "my_table2"
        );
        assert_eq!(
            validate_table_name(r#"My_Table_2"#).expect("Expected table name to be valid"),
            "My_Table_2"
        );

        // Valid table name surrounded by quotes:
        assert_eq!(
            validate_table_name(r#""table""#).expect("Expected table name to be valid"),
            "table"
        );

        // Invalid first character:
        if let Ok(_) = validate_table_name(r#"1table"#) {
            panic!("Expected an error");
        };
        if let Ok(_) = validate_table_name(r#""1table""#) {
            panic!("Expected an error");
        }

        // Beginning or trailing double-quote is missing:
        if let Ok(_) = validate_table_name(r#"table""#) {
            panic!("Expected an error");
        }
        if let Ok(_) = validate_table_name(r#""table"#) {
            panic!("Expected an error");
        }

        // Table name with spaces:
        if let Ok(_) = validate_table_name(r#"my table"#) {
            panic!("Expected an error");
        }
    }

    #[tokio::test]
    async fn test_sql_parsing() {
        // Single statements, possibly with parameters:

        // TODO: We should be able to support more complex selects, including UNIONs, etc.
        let (edited_tables, dropped_tables, read_tables) = get_accessed_tables(&format!(
            r#"SELECT * FROM "alpha", "beta"
                   WHERE "alpha".foo = "beta".foo""#
        ))
        .unwrap();
        let mut read_tables: Vec<_> = read_tables.into_iter().collect();
        read_tables.sort();
        assert_eq!(edited_tables, [].into());
        assert_eq!(dropped_tables, [].into());
        assert_eq!(read_tables, ["alpha", "beta"]);

        let (edited_tables, dropped_tables, read_tables) =
            get_accessed_tables(&format!(r#"INSERT INTO "alpha" VALUES ($1, $2, $3)"#)).unwrap();
        let edited_tables: Vec<_> = edited_tables.into_iter().collect();
        assert_eq!(edited_tables, ["alpha"]);
        assert_eq!(dropped_tables, [].into());
        assert_eq!(read_tables, [].into());

        let (edited_tables, dropped_tables, read_tables) = get_accessed_tables(
            r#"WITH bar AS (SELECT * FROM alpha),
                                mar AS (SELECT * FROM beta)
                           INSERT INTO gamma
                           SELECT alpha.*
                           FROM alpha, beta
                           WHERE alpha.value = beta.value"#,
        )
        .unwrap();
        let edited_tables: Vec<_> = edited_tables.into_iter().collect();
        assert_eq!(edited_tables, ["gamma"]);
        assert_eq!(dropped_tables, [].into());
        assert_eq!(read_tables, [].into());

        let (edited_tables, dropped_tables, read_tables) =
            get_accessed_tables(&format!(r#"UPDATE "delta" set bar = $1 WHERE bar = $2"#)).unwrap();
        let edited_tables: Vec<_> = edited_tables.into_iter().collect();
        assert_eq!(edited_tables, ["delta"]);
        assert_eq!(dropped_tables, [].into());
        assert_eq!(read_tables, [].into());

        let (edited_tables, dropped_tables, read_tables) = get_accessed_tables(&format!(
            r#"WITH bar AS (SELECT * FROM test),
                                mar AS (SELECT * FROM test)
                           UPDATE delta
                           SET value = bar.value
                           FROM bar, mar
                           WHERE bar.value = $1 AND bar.value = mar.value"#,
        ))
        .unwrap();
        let edited_tables: Vec<_> = edited_tables.into_iter().collect();
        assert_eq!(edited_tables, ["delta"]);
        assert_eq!(dropped_tables, [].into());
        assert_eq!(read_tables, [].into());

        let (edited_tables, dropped_tables, read_tables) =
            get_accessed_tables(&format!(r#"DELETE FROM "epsilon" WHERE bar >= $1"#)).unwrap();
        let edited_tables: Vec<_> = edited_tables.into_iter().collect();
        assert_eq!(edited_tables, ["epsilon"]);
        assert_eq!(dropped_tables, [].into());
        assert_eq!(read_tables, [].into());

        let (edited_tables, dropped_tables, read_tables) = get_accessed_tables(
            r#"WITH bar AS (SELECT * FROM test),
                                mar AS (SELECT * FROM test)
                           DELETE FROM lambda WHERE value IN (SELECT value FROM bar)"#,
        )
        .unwrap();
        let edited_tables: Vec<_> = edited_tables.into_iter().collect();
        assert_eq!(edited_tables, ["lambda"]);
        assert_eq!(dropped_tables, [].into());
        assert_eq!(read_tables, [].into());

        let (edited_tables, dropped_tables, read_tables) =
            get_accessed_tables(r#"DROP TABLE "rho""#).unwrap();
        let dropped_tables: Vec<_> = dropped_tables.into_iter().collect();
        assert_eq!(dropped_tables, ["rho"]);
        assert_eq!(edited_tables, [].into());
        assert_eq!(read_tables, [].into());

        let (edited_tables, dropped_tables, read_tables) =
            get_accessed_tables(r#"DROP TABLE IF EXISTS "phi" CASCADE"#).unwrap();
        let dropped_tables: Vec<_> = dropped_tables.into_iter().collect();
        assert_eq!(dropped_tables, ["phi"]);
        assert_eq!(edited_tables, [].into());
        assert_eq!(read_tables, [].into());

        let (edited_tables, dropped_tables, read_tables) =
            get_accessed_tables("TRUNCATE TABLE mu, nu CASCADE").unwrap();
        let mut edited_tables: Vec<_> = edited_tables.into_iter().collect();
        edited_tables.sort();
        assert_eq!(edited_tables, ["mu", "nu"]);
        assert_eq!(dropped_tables, [].into());
        assert_eq!(read_tables, [].into());

        let (edited_tables, dropped_tables, read_tables) =
            get_accessed_tables("ALTER TABLE phi ADD COLUMN varphi INT").unwrap();
        let mut edited_tables: Vec<_> = edited_tables.into_iter().collect();
        edited_tables.sort();
        assert_eq!(edited_tables, ["phi"]);
        assert_eq!(dropped_tables, [].into());
        assert_eq!(read_tables, [].into());

        let (edited_tables, dropped_tables, read_tables) =
            get_accessed_tables("DROP VIEW theta").unwrap();
        let mut dropped_tables: Vec<_> = dropped_tables.into_iter().collect();
        dropped_tables.sort();
        assert_eq!(edited_tables, [].into());
        assert_eq!(dropped_tables, ["theta"]);
        assert_eq!(read_tables, [].into());

        let (edited_tables, dropped_tables, read_tables) = get_accessed_tables(
            r#"UPDATE epsilon
                       SET alpha = new_beta
                       FROM (
                         SELECT 9 AS new_alpha, 3 AS new_beta, 2 AS new_gamma, 1 AS new_delta
                         UNION ALL
                         SELECT 4 AS new_alpha, 3 as new_beta, 2 AS new_gamma, 1 AS new_delta
                       ) foo_alias
                       WHERE alpha = new_alpha"#,
        )
        .unwrap();
        let edited_tables: Vec<_> = edited_tables.into_iter().collect();
        assert_eq!(edited_tables, ["epsilon"]);
        assert_eq!(dropped_tables, [].into());
        assert_eq!(read_tables, [].into());

        // Multiple statements, no parameters:

        let sql = r#"BEGIN TRANSACTION;

                     INSERT INTO "alpha" VALUES (1, 2, 3), (4, 5, 6);

                     INSERT INTO gamma
                     SELECT alpha.*
                     FROM alpha, beta
                     WHERE alpha.value = beta.value;

                     WITH t AS (
                       SELECT * from delta_base ORDER BY quality LIMIT 1
                     )
                     UPDATE delta SET price = t.price * 1.05;

                     WITH t AS (
                       SELECT * FROM phi_base
                       WHERE
                         "date" >= '2010-10-01' AND
                         "date" < '2010-11-01'
                     )
                     INSERT INTO phi
                     SELECT * FROM t;

                     DELETE FROM "psi" WHERE bar >= 10;

                     WITH RECURSIVE included_lambda(sub_lambda, lambda) AS (
                         SELECT sub_lambda, lambda FROM lambda WHERE lambda = 'our_product'
                       UNION ALL
                         SELECT p.sub_lambda, p.lambda
                         FROM included_lambda pr, lambda p
                         WHERE p.lambda = pr.sub_lambda
                     )
                     DELETE FROM lambda
                       WHERE lambda IN (SELECT lambda FROM included_lambda);

                     DROP TABLE "rho";

                     DROP TABLE "sigma" CASCADE;

                     COMMIT"#;

        let (edited_tables, dropped_tables, read_tables) = get_accessed_tables(&sql).unwrap();
        let mut edited_tables: Vec<_> = edited_tables.into_iter().collect();
        let mut dropped_tables: Vec<_> = dropped_tables.into_iter().collect();
        edited_tables.sort();
        dropped_tables.sort();
        assert_eq!(
            edited_tables,
            ["alpha", "delta", "gamma", "lambda", "phi", "psi",]
        );
        assert_eq!(dropped_tables, ["rho", "sigma",]);
        assert_eq!(read_tables, [].into());
    }
}
