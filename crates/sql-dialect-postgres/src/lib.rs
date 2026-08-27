//! PostgreSQL dialect, backed by [`pg_query`], which wraps the actual
//! PostgreSQL server grammar (via `libpg_query`) rather than a
//! reimplementation. This is intentionally the "hard case" dialect in
//! this workspace: its native AST (a protobuf tree mirroring Postgres's
//! own parse nodes) looks nothing like `sqlparser-rs`'s AST used by
//! `sql-dialect-mysql`/`sql-dialect-generic`. That's exactly why the
//! `StatementInfo` boundary in `sql-ast` exists — the command layer never
//! sees either AST, only the small trait surface below.

use pg_query::{NodeEnum, NodeRef};
use sql_ast::{
    ParseError, ParsedStatement, SqlDialectParser, StatementInfo, StatementKind, TableRef,
};

pub struct PostgresSqlParser;

impl Default for PostgresSqlParser {
    fn default() -> Self {
        Self
    }
}

impl SqlDialectParser for PostgresSqlParser {
    fn name(&self) -> &'static str {
        "postgres"
    }

    fn parse(&self, sql: &str) -> Result<ParsedStatement, ParseError> {
        let mut statements = self.parse_batch(sql)?;
        if statements.len() != 1 {
            return Err(ParseError::Unsupported {
                dialect: self.name(),
                message: format!("expected exactly one statement, got {}", statements.len()),
            });
        }
        Ok(statements.remove(0))
    }

    fn parse_batch(&self, sql: &str) -> Result<Vec<ParsedStatement>, ParseError> {
        let result = pg_query::parse(sql).map_err(|e| ParseError::Syntax {
            dialect: self.name(),
            message: e.to_string(),
        })?;

        result
            .protobuf
            .stmts
            .into_iter()
            .map(|raw| {
                let node =
                    raw.stmt
                        .and_then(|boxed| boxed.node)
                        .ok_or_else(|| ParseError::Syntax {
                            dialect: self.name(),
                            message: "empty statement node".to_string(),
                        })?;
                Ok(ParsedStatement::new(
                    "postgres",
                    Box::new(PostgresStatement(node)),
                ))
            })
            .collect()
    }
}

struct PostgresStatement(NodeEnum);

impl StatementInfo for PostgresStatement {
    fn kind(&self) -> StatementKind {
        match self.0.to_ref() {
            NodeRef::SelectStmt(_) => StatementKind::Select,
            NodeRef::InsertStmt(_) => StatementKind::Insert,
            NodeRef::UpdateStmt(_) => StatementKind::Update,
            NodeRef::DeleteStmt(_) => StatementKind::Delete,
            NodeRef::CreateStmt(_)
            | NodeRef::CreateTableAsStmt(_)
            | NodeRef::AlterTableStmt(_)
            | NodeRef::DropStmt(_)
            | NodeRef::IndexStmt(_)
            | NodeRef::ViewStmt(_) => StatementKind::Ddl,
            NodeRef::TransactionStmt(_) => StatementKind::Transaction,
            _ => StatementKind::Other,
        }
    }

    fn tables(&self) -> Vec<TableRef> {
        // Best-effort: walk this statement's own node tree for RangeVar
        // references, same approach `pg_query::ParseResult::tables()`
        // uses at the whole-batch level. CTE-name filtering is skipped
        // here for brevity (see the TODO in pg_query's own
        // `parse_result.rs`, which has the same limitation).
        self.0
            .nodes()
            .into_iter()
            .filter_map(|(node, _depth, _ctx, _)| match node {
                NodeRef::RangeVar(v) => {
                    let schema = if v.schemaname.is_empty() {
                        None
                    } else {
                        Some(v.schemaname.clone())
                    };
                    Some(TableRef {
                        schema,
                        name: v.relname.clone(),
                    })
                }
                _ => None,
            })
            .collect()
    }

    fn to_sql(&self) -> String {
        self.0.deparse().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_postgres_specific_syntax() {
        let parser = PostgresSqlParser;
        // RETURNING has no MySQL equivalent; this is real-grammar
        // coverage, not a hand-rolled approximation.
        let stmt = parser
            .parse("INSERT INTO users (id, name) VALUES (1, 'a') RETURNING id")
            .expect("postgres RETURNING clause should parse");
        assert_eq!(stmt.kind(), StatementKind::Insert);
        assert_eq!(
            stmt.tables(),
            vec![TableRef {
                schema: None,
                name: "users".into()
            }]
        );
    }

    #[test]
    fn classifies_select_and_extracts_tables() {
        let parser = PostgresSqlParser;
        let stmt = parser
            .parse("SELECT u.id FROM users u JOIN orders o ON o.user_id = u.id")
            .unwrap();
        assert_eq!(stmt.kind(), StatementKind::Select);
        let tables: Vec<String> = stmt.tables().into_iter().map(|t| t.name).collect();
        assert_eq!(tables, vec!["users", "orders"]);
    }
}
