//! Code for parsing SQL expressions.

use crate::{
    Error,
    cache::{QUERY_CACHE_TABLE, TABLE_CACHE_TABLE},
};
use lazy_static::lazy_static;
use regex::Regex;
use std::collections::BTreeSet;
use tree_sitter::{Node, Parser, Tree};
use tree_sitter_sequel::LANGUAGE as SQL_LANGUAGE;

/// Validates that a given [Node] is not an error node:
fn validate_node(node: &Node<'_>, sql: &str) -> Result<(), Error> {
    if node.is_error() {
        return Err(Error::ParseError(format!("Error parsing '{sql}': {node}")));
    }
    Ok(())
}

/// Validates that the given list of [Node]s is of the expected length:
fn validate_list_len(node_list: &Vec<Node<'_>>, len: usize) -> Result<(), Error> {
    if node_list.len() != len {
        return Err(Error::ParseError(format!(
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
pub fn validate_table_name(table_name: &str) -> Result<String, Error> {
    let error_msg = format!(
        "Not a valid table name: \"{table_name}\". Valid table names must match \
         the regular expression: '{VALID_TABLE_NAME_MATCH_STR}' and may possibly begin and \
         end with double-quotes."
    );
    let table_name = match table_name.strip_prefix("\"") {
        Some(table_name) => match table_name.strip_suffix("\"") {
            Some(table_name) => table_name,
            None => return Err(Error::InputError(error_msg)),
        },
        None => match table_name.strip_suffix("\"") {
            Some(_) => return Err(Error::InputError(error_msg)),
            None => table_name,
        },
    };
    match VALID_TABLE_NAME_REGEX.is_match(table_name) {
        true => Ok(table_name.to_string()),
        false => Err(Error::InputError(error_msg)),
    }
}

/// Given `view_sql`, which is the SQL code that will result in the creation of a view, parse
/// it, determine what the view's source tables are, and return the list.
pub fn get_view_tables(view_sql: &str) -> Result<Vec<String>, Error> {
    // Instantiate the parser and try to parse the code:
    let mut parser = Parser::new();
    parser
        .set_language(&SQL_LANGUAGE.into())
        .map_err(|err| Error::ParseError(format!("Error setting language to SQL: {err}")))?;
    let tree = match parser.parse(&view_sql, None) {
        Some(tree) => tree,
        None => return Err(Error::ParseError(format!("Could not parse '{view_sql}'"))),
    };

    // Collect the top-level statements:
    let statements = {
        let root_node = tree.root_node();
        validate_node(&root_node, &view_sql)?;
        if root_node.kind().to_lowercase() != "program" {
            return Err(Error::ParseError(format!(
                "Unexpected root node kind: {}",
                root_node.kind()
            )));
        }
        root_node
            .children(&mut root_node.walk())
            .filter(|child| child.kind().to_lowercase() == "statement")
            .collect::<Vec<_>>()
    };

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
                    let create_sql = view_sql.to_string()
                        [create_query.start_byte()..create_query.end_byte()]
                        .to_string();
                    let view_tables = get_accessed_tables(&create_sql)?
                        .into_iter()
                        .collect::<Vec<_>>();
                    return Ok(view_tables);
                }
                _ => (),
            }
        }
    }
    Err(Error::InputError(format!(
        "Not a valid CREATE VIEW statement: {view_sql}"
    )))
}

/// Parse the given string, representing a series of (semi-colon-separated) SQL commands,
/// into their constituents and determine the tables and views that will be read from when
/// the commands are executed, if any.
pub fn get_accessed_tables(sql: &str) -> Result<BTreeSet<String>, Error> {
    fn process_select_clause(
        sql: &str,
        select_clause: Node<'_>,
        tables_read: &mut BTreeSet<String>,
    ) -> Result<(), Error> {
        let from = select_clause
            .next_sibling()
            .ok_or(Error::InputError(format!("Invalid SQL: {sql}")))?;

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

            let table = validate_table_name(
                &sql.to_string()[object_ref.start_byte()..object_ref.end_byte()],
            )?;
            tables_read.insert(table.to_string());
        }

        let join_clause = from
            .children(&mut from.walk())
            .filter(|child| child.kind().to_lowercase() == "join")
            .collect::<Vec<_>>();
        if join_clause.len() > 0 {
            validate_list_len(&join_clause, 1)?;
            let join_clause = join_clause[0];
            process_join_clause(sql, join_clause, tables_read)?;
        }

        let where_clause = from
            .children(&mut from.walk())
            .filter(|child| child.kind().to_lowercase() == "where")
            .collect::<Vec<_>>();
        if where_clause.len() > 0 {
            validate_list_len(&where_clause, 1)?;
            let where_clause = where_clause[0];
            process_where_clause(sql, where_clause, tables_read)?;
        }
        Ok(())
    }

    fn process_where_clause(
        sql: &str,
        where_clause: Node<'_>,
        tables_read: &mut BTreeSet<String>,
    ) -> Result<(), Error> {
        let expressions = where_clause
            .children(&mut where_clause.walk())
            .filter(|child| {
                [
                    "unary_expression",
                    "binary_expression",
                    "parenthesized_expression",
                ]
                .contains(&child.kind().to_lowercase().as_str())
            })
            .collect::<Vec<_>>();

        for expression in expressions {
            process_expression(sql, expression, tables_read)?;
        }
        Ok(())
    }

    fn process_join_clause(
        sql: &str,
        join_clause: Node<'_>,
        tables_read: &mut BTreeSet<String>,
    ) -> Result<(), Error> {
        let table = {
            let relation = join_clause
                .children(&mut join_clause.walk())
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

            validate_table_name(&sql.to_string()[identifier.start_byte()..identifier.end_byte()])?
        };
        tables_read.insert(table);

        let expressions = join_clause
            .children(&mut join_clause.walk())
            .filter(|child| {
                [
                    "unary_expression",
                    "binary_expression",
                    "parenthesized_expression",
                ]
                .contains(&child.kind().to_lowercase().as_str())
            })
            .collect::<Vec<_>>();

        for expression in expressions {
            process_expression(sql, expression, tables_read)?;
        }
        Ok(())
    }

    fn process_expression(
        sql: &str,
        expression: Node<'_>,
        tables_read: &mut BTreeSet<String>,
    ) -> Result<(), Error> {
        let children = expression
            .children(&mut expression.walk())
            .collect::<Vec<_>>();
        for child in children {
            if [
                "unary_expression",
                "binary_expression",
                "parenthesized_expression",
                "subquery",
            ]
            .contains(&child.kind().to_lowercase().as_str())
            {
                process_expression(sql, child, tables_read)?;
            } else if child.kind().to_lowercase() == "select" {
                process_select_clause(sql, child, tables_read)?;
            }
        }
        Ok(())
    }

    fn process_statements(
        sql: &str,
        tree: &Tree,
        statements: &Vec<Node<'_>>,
        tables_read: &mut BTreeSet<String>,
    ) -> Result<(), Error> {
        let mut cte_names = BTreeSet::new();

        // Determine the tables that will be modified:
        for statement in statements {
            validate_node(&statement, sql)?;
            for instruction in statement.children(&mut tree.walk()) {
                validate_node(&instruction, sql)?;
                match instruction.kind().to_lowercase().as_str() {
                    "set_operation" => {
                        let children = instruction
                            .children(&mut instruction.walk())
                            .collect::<Vec<_>>();
                        for child in children {
                            if child.kind().to_lowercase() == "select" {
                                process_select_clause(sql, child, tables_read)?;
                            }
                        }
                    }
                    "select" => process_select_clause(sql, instruction, tables_read)?,
                    "cte" => {
                        // Keep track of the names of any CTEs encountered, since we do not
                        // want to include these in the list of accessed tables (they are
                        // essentially temporary tables).
                        let cte_name = instruction
                            .children(&mut instruction.walk())
                            .filter(|child| child.kind().to_lowercase() == "identifier")
                            .collect::<Vec<_>>();
                        validate_list_len(&cte_name, 1)?;
                        let cte_name =
                            &sql.to_string()[cte_name[0].start_byte()..cte_name[0].end_byte()];
                        cte_names.insert(cte_name.to_string());

                        let cte_stmt = instruction
                            .children(&mut instruction.walk())
                            .filter(|child| child.kind().to_lowercase() == "statement")
                            .collect::<Vec<_>>();
                        validate_list_len(&cte_stmt, 1)?;
                        let cte_stmt = cte_stmt[0];
                        process_statements(sql, tree, &vec![cte_stmt], tables_read)?;
                    }
                    _ => (),
                }
            }
        }

        // Prune the CTE names from the list of tables read:
        for entry in cte_names {
            tables_read.remove(&entry);
        }
        Ok(())
    }

    // Instantiate the parser and read in the given sql string to generate a parsing tree:
    let mut parser = Parser::new();
    parser
        .set_language(&SQL_LANGUAGE.into())
        .map_err(|err| Error::ParseError(format!("Error setting language: {err}")))?;
    let tree = match parser.parse(sql, None) {
        Some(tree) => tree,
        None => return Err(Error::ParseError(format!("Could not parse '{sql}'"))),
    };

    // Collect the top-level statements:
    let statements = {
        let root_node = tree.root_node();
        validate_node(&root_node, sql)?;
        if root_node.kind().to_lowercase() != "program" {
            return Err(Error::ParseError(format!(
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

    let mut tables_read = BTreeSet::new();
    process_statements(sql, &tree, &statements, &mut tables_read)?;
    Ok(tables_read)
}

/// Parse the given string, representing a series of (semi-colon-separated) SQL commands,
/// into their constituents and determine the tables and views that will be affected when
/// the commands are executed, if any. Three sets are returned. The first contains tables
/// and views that are going to be edited (targets of commands like INSERT, UPDATE, DELETE,
/// ALTER, and TRUNCATE), the second contains tables and views that are going to be dropped
/// (targets of a DROP TABLE or DROP VIEW command). The third contains tables and views that
/// are only going to be read (targets of most other commands). Note table modifications that
/// occur _within_ a common table expression (CTE) are not recognized by this function. Such
/// table-modifying CTEs are supported in principle by PostgreSQL (see
/// <https://www.postgresql.org/docs/current/queries-with.html#QUERIES-WITH-MODIFYING>) but
/// seemingly not by SQLite (see <https://sqlite.org/lang_with.html>).
pub fn get_affected_tables(sql: &str) -> Result<(BTreeSet<String>, BTreeSet<String>), Error> {
    // Instantiate the parser and read in the given sql string:
    let mut parser = Parser::new();
    parser
        .set_language(&SQL_LANGUAGE.into())
        .map_err(|err| Error::ParseError(format!("Error setting language to SQL: {err}")))?;
    let tree = match parser.parse(sql, None) {
        Some(tree) => tree,
        None => return Err(Error::ParseError(format!("Could not parse '{sql}'"))),
    };

    // Collect the top-level statements:
    let statements = {
        let root_node = tree.root_node();
        validate_node(&root_node, sql)?;
        if root_node.kind().to_lowercase() != "program" {
            return Err(Error::ParseError(format!(
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
    let mut edited_tables = BTreeSet::new();
    let mut dropped_tables = BTreeSet::new();
    for statement in &statements {
        validate_node(&statement, sql)?;
        for instruction in statement.children(&mut tree.walk()) {
            validate_node(&instruction, sql)?;
            match instruction.kind().to_lowercase().as_str() {
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
                        let details = instruction.next_sibling().ok_or(Error::ParseError(
                            format!("No details found for '{}'", instruction.kind()),
                        ))?;
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
        .collect::<BTreeSet<_>>();

    Ok((edited_tables.clone(), dropped_tables.clone()))
}
