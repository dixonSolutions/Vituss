//! Generic/ANSI SQL dialect, backed by `sqlparser-rs`'s [`GenericDialect`].
//!
//! This is the fallback dialect for clients that don't negotiate a
//! specific database wire protocol, and a template for how a new dialect
//! crate plugs into `sql-ast`'s two traits.

use sql_ast::{
    ParseError, ParsedStatement, SqlDialectParser, StatementInfo, StatementKind, TableRef,
};
use sqlparser::ast::{FromTable, SetExpr, Statement, TableFactor};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub struct GenericSqlParser;

impl Default for GenericSqlParser {
    fn default() -> Self {
        Self
    }
}

impl SqlDialectParser for GenericSqlParser {
    fn name(&self) -> &'static str {
        "generic"
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
        let ast = Parser::parse_sql(&GenericDialect {}, sql).map_err(|e| ParseError::Syntax {
            dialect: self.name(),
            message: e.to_string(),
        })?;
        Ok(ast
            .into_iter()
            .map(|stmt| ParsedStatement::new("generic", Box::new(GenericStatement(stmt))))
            .collect())
    }
}

/// Wraps `sqlparser`'s native `Statement` so it can satisfy
/// [`StatementInfo`] without leaking the sqlparser AST type into the
/// command layer.
struct GenericStatement(Statement);

impl StatementInfo for GenericStatement {
    fn kind(&self) -> StatementKind {
        classify(&self.0)
    }

    fn tables(&self) -> Vec<TableRef> {
        extract_tables(&self.0)
    }

    fn to_sql(&self) -> String {
        self.0.to_string()
    }
}

/// Shared with other `sqlparser-rs`-backed dialect crates (e.g.
/// `sql-dialect-mysql`), since they parse into the same AST type and
/// only differ in grammar/dialect used at parse time.
pub fn classify(stmt: &Statement) -> StatementKind {
    match stmt {
        Statement::Query(_) => StatementKind::Select,
        Statement::Insert { .. } => StatementKind::Insert,
        Statement::Update { .. } => StatementKind::Update,
        Statement::Delete { .. } => StatementKind::Delete,
        Statement::CreateTable { .. }
        | Statement::AlterTable { .. }
        | Statement::Drop { .. }
        | Statement::CreateIndex { .. }
        | Statement::CreateView { .. } => StatementKind::Ddl,
        Statement::StartTransaction { .. }
        | Statement::Commit { .. }
        | Statement::Rollback { .. } => StatementKind::Transaction,
        _ => StatementKind::Other,
    }
}

pub fn extract_tables(stmt: &Statement) -> Vec<TableRef> {
    let mut out = Vec::new();
    match stmt {
        Statement::Query(query) => {
            if let SetExpr::Select(select) = query.body.as_ref() {
                for twj in &select.from {
                    collect_table_factor(&twj.relation, &mut out);
                    for join in &twj.joins {
                        collect_table_factor(&join.relation, &mut out);
                    }
                }
            }
        }
        Statement::Insert(insert) => out.push(TableRef {
            schema: None,
            name: insert.table_name.to_string(),
        }),
        Statement::Update { table, .. } => collect_table_factor(&table.relation, &mut out),
        Statement::Delete(delete) => {
            let twjs = match &delete.from {
                FromTable::WithFromKeyword(twjs) | FromTable::WithoutKeyword(twjs) => twjs,
            };
            for twj in twjs {
                collect_table_factor(&twj.relation, &mut out);
            }
        }
        _ => {}
    }
    out
}

fn collect_table_factor(factor: &TableFactor, out: &mut Vec<TableRef>) {
    if let TableFactor::Table { name, .. } = factor {
        let parts: Vec<String> = name.0.iter().map(|i| i.value.clone()).collect();
        let (schema, table) = match parts.len() {
            0 => return,
            1 => (None, parts[0].clone()),
            _ => (
                Some(parts[..parts.len() - 1].join(".")),
                parts[parts.len() - 1].clone(),
            ),
        };
        out.push(TableRef {
            schema,
            name: table,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_ddl() {
        let parser = GenericSqlParser;
        let stmt = parser.parse("CREATE TABLE users (id INT)").unwrap();
        assert_eq!(stmt.kind(), StatementKind::Ddl);
    }

    #[test]
    fn round_trips_to_sql() {
        let parser = GenericSqlParser;
        let stmt = parser.parse("SELECT id FROM users").unwrap();
        assert_eq!(stmt.to_sql(), "SELECT id FROM users");
    }
}
