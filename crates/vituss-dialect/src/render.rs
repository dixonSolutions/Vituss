//! Cross-dialect rendering.
//!
//! Vituss plans against one AST and may have to execute the result on a shard
//! running a different engine than the client is speaking to. Rendering is
//! therefore a translation step, not a `to_string()`:
//!
//! 1. Named bind variables (`:id`, the internal form) are replaced with whatever
//!    placeholder the target engine's protocol wants, and the matching values are
//!    collected in positional order.
//! 2. Quoted identifiers are re-quoted with the target engine's quote character,
//!    and identifiers that would change meaning under the target's case-folding
//!    rules get quoted defensively.

use std::ops::ControlFlow;

use sqlparser::ast::{Statement, Value as AstValue, ValueWithSpan, VisitMut, VisitorMut};
use sqlparser::dialect::Dialect as ParserDialect;
use sqlparser::tokenizer::{Token, Tokenizer, Word};

use vituss_core::{BindVars, Error, Result, Value};

use crate::caps::{Capabilities, IdentifierCase, PlaceholderStyle, RowLock};
use crate::SqlDialect;

/// A statement rendered for one specific engine.
#[derive(Debug, Clone, PartialEq)]
pub struct RenderedQuery {
    pub sql: String,
    /// Bind values in the order the placeholders appear.
    pub params: Vec<Value>,
}

impl RenderedQuery {
    pub fn new(sql: impl Into<String>) -> Self {
        Self { sql: sql.into(), params: Vec::new() }
    }
}

/// The placeholder prefix Vituss uses inside its own ASTs.
///
/// It is deliberately `:name` — legal in none of the three target engines, so a
/// placeholder that escapes rendering fails loudly instead of silently meaning
/// something else.
pub const BIND_PREFIX: char = ':';

/// Format the `n`-th placeholder (0-based) for a placeholder style.
pub fn placeholder_for(style: PlaceholderStyle, ordinal: usize) -> String {
    match style {
        PlaceholderStyle::Question => "?".to_string(),
        PlaceholderStyle::DollarNumbered => format!("${}", ordinal + 1),
        PlaceholderStyle::AtNumbered => format!("@p{}", ordinal + 1),
    }
}

struct BindRewriter<'a> {
    style: PlaceholderStyle,
    bind_vars: &'a BindVars,
    params: Vec<Value>,
    error: Option<Error>,
}

impl VisitorMut for BindRewriter<'_> {
    type Break = ();

    fn pre_visit_value(&mut self, value: &mut ValueWithSpan) -> ControlFlow<()> {
        let AstValue::Placeholder(name) = &value.value else {
            return ControlFlow::Continue(());
        };
        if !name.starts_with(BIND_PREFIX) {
            // Already an engine-native placeholder (a pass-through prepared
            // statement). Leave it, but still count it so ordinals line up.
            self.params.push(Value::Null);
            return ControlFlow::Continue(());
        }
        let key = name.trim_start_matches(BIND_PREFIX).to_string();
        match self.bind_vars.get(&key) {
            Some(v) => {
                let rendered = placeholder_for(self.style, self.params.len());
                self.params.push(v.clone());
                value.value = AstValue::Placeholder(rendered);
                ControlFlow::Continue(())
            }
            None => {
                self.error = Some(Error::invalid(format!("missing bind variable {name:?}")));
                ControlFlow::Break(())
            }
        }
    }
}

/// Replace `:name` placeholders with the target engine's placeholder syntax,
/// returning the values in positional order.
pub fn bind(stmt: &Statement, bind_vars: &BindVars, style: PlaceholderStyle) -> Result<(Statement, Vec<Value>)> {
    let mut stmt = stmt.clone();
    let mut rw = BindRewriter { style, bind_vars, params: Vec::new(), error: None };
    let _ = stmt.visit(&mut rw);
    if let Some(e) = rw.error {
        return Err(e);
    }
    Ok((stmt, rw.params))
}

/// Re-quote identifiers in already-rendered SQL for a different engine.
///
/// Works on the token stream rather than the AST because identifiers appear in
/// far more AST positions than the visitor exposes (aliases, window names, index
/// hints, CTE names). Tokenising is exact where a regex would not be: it knows
/// that a backtick inside a string literal is not a quote character.
pub fn requote_identifiers(
    sql: &str,
    source: &dyn ParserDialect,
    target_quote: char,
    target_case: IdentifierCase,
) -> Result<String> {
    let tokens = Tokenizer::new(source, sql)
        .tokenize()
        .map_err(|e| Error::internal(format!("re-tokenising rendered SQL failed: {e}")))?;

    let mut out = String::with_capacity(sql.len() + 16);
    for tok in tokens {
        match tok {
            Token::Word(w) => out.push_str(&render_word(&w, target_quote, target_case)),
            other => out.push_str(&other.to_string()),
        }
    }
    Ok(out)
}

fn render_word(w: &Word, target_quote: char, target_case: IdentifierCase) -> String {
    let needs_quote = match w.quote_style {
        // Already quoted in the source: keep it quoted, just change the character.
        Some(_) => true,
        None => {
            // Unquoted. If the target folds case and the identifier is not already
            // in the folded case, quoting preserves the name the user wrote.
            // Keywords are left alone — quoting them would turn them into names.
            if w.keyword != sqlparser::keywords::Keyword::NoKeyword {
                false
            } else {
                match target_case {
                    IdentifierCase::FoldLower => w.value.chars().any(|c| c.is_ascii_uppercase()),
                    IdentifierCase::FoldUpper => w.value.chars().any(|c| c.is_ascii_lowercase()),
                    _ => false,
                }
            }
        }
    };
    if !needs_quote {
        return w.value.clone();
    }
    quote_with(&w.value, target_quote)
}

/// Quote an identifier with the engine's quote character, escaping any embedded
/// quote the way that engine expects.
pub fn quote_with(ident: &str, quote: char) -> String {
    match quote {
        '[' => format!("[{}]", ident.replace(']', "]]")),
        '`' => format!("`{}`", ident.replace('`', "``")),
        c => format!("{c}{}{c}", ident.replace(c, &format!("{c}{c}"))),
    }
}

/// Full render: bind, stringify, then translate identifier quoting.
///
/// Takes the target described by name + capabilities rather than as a trait
/// object so that [`SqlDialect::render`] can call it from a default method.
pub fn render_with(
    stmt: &Statement,
    bind_vars: &BindVars,
    source: &dyn SqlDialect,
    target_name: &str,
    target_caps: &Capabilities,
) -> Result<RenderedQuery> {
    let (mut bound, params) = bind(stmt, bind_vars, target_caps.placeholder_style)?;
    translate_row_locks(&mut bound, target_caps.row_lock);
    let sql = bound.to_string();
    let sql = if source.name() == target_name {
        sql
    } else {
        requote_identifiers(&sql, source.parser(), target_caps.identifier_quote, target_caps.identifier_case)?
    };
    Ok(RenderedQuery { sql, params })
}

/// Re-express a `FOR UPDATE` in the target engine's terms.
///
/// Vituss adds row locks itself — reserving sequence values, reading the rows a
/// DML is about to rewrite — and a lock clause written for one engine is a syntax
/// error on another. SQL Server wants a table hint; SQLite has no clause at all,
/// because a write transaction already holds the whole database.
///
/// Dropping the clause for SQLite is safe. Dropping it for an engine that *did*
/// need it would not be, which is why the three cases are distinguished rather
/// than lumped into "supported / not".
fn translate_row_locks(stmt: &mut Statement, target: RowLock) {
    let Statement::Query(query) = stmt else { return };
    if query.locks.is_empty() {
        return;
    }
    match target {
        RowLock::ForUpdate => {}
        RowLock::None => query.locks.clear(),
        RowLock::TableHint => {
            query.locks.clear();
            add_update_lock_hints(&mut query.body);
        }
    }
}

/// Attach `WITH (UPDLOCK, HOLDLOCK)` to every table in the FROM clause.
///
/// UPDLOCK takes the write lock now and HOLDLOCK keeps it until the transaction
/// ends — together, what `FOR UPDATE` means on the other engines.
fn add_update_lock_hints(body: &mut sqlparser::ast::SetExpr) {
    use sqlparser::ast::{Ident, SetExpr, TableFactor};

    let hint = |v: &mut Vec<sqlparser::ast::Expr>| {
        if v.is_empty() {
            v.push(sqlparser::ast::Expr::Identifier(Ident::new("UPDLOCK")));
            v.push(sqlparser::ast::Expr::Identifier(Ident::new("HOLDLOCK")));
        }
    };

    match body {
        SetExpr::Select(select) => {
            for twj in &mut select.from {
                if let TableFactor::Table { with_hints, .. } = &mut twj.relation {
                    hint(with_hints);
                }
                for join in &mut twj.joins {
                    if let TableFactor::Table { with_hints, .. } = &mut join.relation {
                        hint(with_hints);
                    }
                }
            }
        }
        SetExpr::Query(q) => add_update_lock_hints(&mut q.body),
        _ => {}
    }
}

/// Convenience wrapper over [`render_with`] when both sides are trait objects.
pub fn render_for(
    stmt: &Statement,
    bind_vars: &BindVars,
    source: &dyn SqlDialect,
    target: &dyn SqlDialect,
) -> Result<RenderedQuery> {
    render_with(stmt, bind_vars, source, target.name(), target.capabilities())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{mssql::MsSql, mysql::MySql, postgres::Postgres};

    fn parse(d: &dyn SqlDialect, sql: &str) -> Statement {
        d.parse_one(sql).expect("parse")
    }

    #[test]
    fn placeholders_follow_the_target_engine() {
        let my = MySql::new();
        let stmt = parse(&my, "SELECT id FROM user WHERE id = :id AND name = :name");
        let mut bv = BindVars::new();
        bv.insert("id".into(), Value::Int(7));
        bv.insert("name".into(), Value::Text("ada".into()));

        let q = render_for(&stmt, &bv, &my, &MySql::new()).unwrap();
        assert!(q.sql.contains("= ? AND"), "{}", q.sql);
        assert_eq!(q.params, vec![Value::Int(7), Value::Text("ada".into())]);

        let q = render_for(&stmt, &bv, &my, &Postgres::new()).unwrap();
        assert!(q.sql.contains("$1") && q.sql.contains("$2"), "{}", q.sql);

        let q = render_for(&stmt, &bv, &my, &MsSql::new()).unwrap();
        assert!(q.sql.contains("@p1") && q.sql.contains("@p2"), "{}", q.sql);
    }

    #[test]
    fn identifier_quoting_is_translated() {
        let my = MySql::new();
        let stmt = parse(&my, "SELECT `order`.`id` FROM `order`");
        let pg = render_for(&stmt, &BindVars::new(), &my, &Postgres::new()).unwrap();
        assert!(pg.sql.contains(r#""order""#), "{}", pg.sql);
        assert!(!pg.sql.contains('`'), "{}", pg.sql);

        let ms = render_for(&stmt, &BindVars::new(), &my, &MsSql::new()).unwrap();
        assert!(ms.sql.contains("[order]"), "{}", ms.sql);
    }

    #[test]
    fn row_locks_are_translated_to_each_engine() {
        let my = MySql::new();
        let stmt = parse(&my, "SELECT id FROM user WHERE id = :id FOR UPDATE");
        let mut bv = BindVars::new();
        bv.insert("id".into(), Value::Int(1));

        // MySQL and PostgreSQL keep the clause.
        assert!(render_for(&stmt, &bv, &my, &my).unwrap().sql.contains("FOR UPDATE"));
        assert!(render_for(&stmt, &bv, &my, &Postgres::new()).unwrap().sql.contains("FOR UPDATE"));

        // SQL Server has no such clause; the lock becomes a table hint.
        let ms = render_for(&stmt, &bv, &my, &MsSql::new()).unwrap();
        assert!(!ms.sql.contains("FOR UPDATE"), "{}", ms.sql);
        assert!(ms.sql.contains("UPDLOCK") && ms.sql.contains("HOLDLOCK"), "{}", ms.sql);

        // SQLite locks the whole database in a write transaction, so the clause is
        // dropped — and would be a syntax error if it were not.
        let lite = render_for(&stmt, &bv, &my, &crate::sqlite::Sqlite::new()).unwrap();
        assert!(!lite.sql.contains("FOR UPDATE"), "{}", lite.sql);
        assert!(!lite.sql.contains("UPDLOCK"), "{}", lite.sql);
    }

    #[test]
    fn missing_bind_variable_is_an_error_not_a_silent_null() {
        let my = MySql::new();
        let stmt = parse(&my, "SELECT 1 FROM user WHERE id = :id");
        let err = render_for(&stmt, &BindVars::new(), &my, &my).unwrap_err();
        assert!(err.message.contains("missing bind variable"), "{}", err.message);
    }
}
