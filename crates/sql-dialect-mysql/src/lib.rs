//! MySQL dialect, backed by `sqlparser-rs`'s [`MySqlDialect`].
//!
//! This is what Vitess's own `sqlparser` package plays today: the only
//! wire dialect it understands. Here it's one interchangeable
//! implementation of [`SqlDialectParser`] among several — nothing in
//! `vtgate-core` is MySQL-specific.
//!
//! Classification/table-extraction logic is shared with
//! `sql-dialect-generic` since both parse into `sqlparser-rs`'s AST and
//! only differ in grammar used at parse time. A dialect with a genuinely
//! different native AST (see `sql-dialect-postgres`) implements
//! [`StatementInfo`] from scratch instead.

use sql_ast::{
    ParseError, ParsedStatement, SqlDialectParser, StatementInfo, StatementKind, TableRef,
};
use sqlparser::ast::Statement;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

pub struct MySqlSqlParser;

impl Default for MySqlSqlParser {
    fn default() -> Self {
        Self
    }
}

impl SqlDialectParser for MySqlSqlParser {
    fn name(&self) -> &'static str {
        "mysql"
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
        let ast = Parser::parse_sql(&MySqlDialect {}, sql).map_err(|e| ParseError::Syntax {
            dialect: self.name(),
            message: e.to_string(),
        })?;
        Ok(ast
            .into_iter()
            .map(|stmt| ParsedStatement::new("mysql", Box::new(MySqlStatement(stmt))))
            .collect())
    }
}

struct MySqlStatement(Statement);

impl StatementInfo for MySqlStatement {
    fn kind(&self) -> StatementKind {
        sql_dialect_generic::classify(&self.0)
    }

    fn tables(&self) -> Vec<TableRef> {
        sql_dialect_generic::extract_tables(&self.0)
    }

    fn to_sql(&self) -> String {
        self.0.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sql_ast::StatementKind;

    #[test]
    fn parses_mysql_specific_syntax() {
        let parser = MySqlSqlParser;
        let stmt = parser
            .parse(
                "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = 'a'",
            )
            .expect("mysql upsert should parse");
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
        let parser = MySqlSqlParser;
        let stmt = parser
            .parse("SELECT u.id FROM users u JOIN orders o ON o.user_id = u.id")
            .unwrap();
        assert_eq!(stmt.kind(), StatementKind::Select);
        let tables: Vec<String> = stmt.tables().into_iter().map(|t| t.name).collect();
        assert_eq!(tables, vec!["users", "orders"]);
    }
}
