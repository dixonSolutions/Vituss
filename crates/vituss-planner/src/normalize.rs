//! Literal → bind variable rewriting.
//!
//! Every literal in a *predicate* is lifted into a bind variable before the
//! statement is sent to a shard. Two reasons, both structural:
//!
//! * One plan then serves every execution of the same query shape, which is what
//!   makes a plan cache possible at all.
//! * The rendered SQL carries no user data, so it can be logged, and each shard's
//!   driver binds the values through its own protocol rather than Vituss building
//!   an escaped string.
//!
//! Rewriting is applied to specific expressions — `WHERE`, `HAVING`, join
//! conditions, `VALUES` — rather than to the whole statement. `ORDER BY 1`,
//! `GROUP BY 2` and `LIMIT 10` are positions and counts, not data: turning them
//! into parameters would change what they mean, or be rejected outright by some
//! engines.

use std::ops::ControlFlow;

use sqlparser::ast::{Expr, Statement, Value as AstValue, ValueWithSpan, VisitMut, VisitorMut};

use vituss_core::BindVars;

use crate::util::ast_value_to_value;

/// Accumulates bind variables across several expressions of one statement.
pub struct Normalizer {
    bind_vars: BindVars,
    counter: usize,
    prefix: String,
}

impl Normalizer {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self { bind_vars: BindVars::new(), counter: 0, prefix: prefix.into() }
    }

    /// Rewrite the literals in one expression.
    pub fn expr(&mut self, e: &mut Expr) {
        let _ = e.visit(self);
    }

    /// Rewrite the literals in an optional expression.
    pub fn opt_expr(&mut self, e: Option<&mut Expr>) {
        if let Some(e) = e {
            self.expr(e);
        }
    }

    /// Add a value under a fresh name and return that name.
    pub fn add(&mut self, v: vituss_core::Value) -> String {
        self.counter += 1;
        let name = format!("{}{}", self.prefix, self.counter);
        self.bind_vars.insert(name.clone(), v);
        name
    }

    pub fn finish(self) -> BindVars {
        self.bind_vars
    }

    pub fn bind_vars(&self) -> &BindVars {
        &self.bind_vars
    }
}

impl VisitorMut for Normalizer {
    type Break = ();

    fn pre_visit_value(&mut self, value: &mut ValueWithSpan) -> ControlFlow<()> {
        match &value.value {
            // Already a bind variable, from a client-side prepared statement.
            AstValue::Placeholder(_) => ControlFlow::Continue(()),
            // NULL carries no data, and `= NULL` behaves differently from
            // `= <parameter that happens to be null>`. It stays where the engine
            // can see it.
            AstValue::Null => ControlFlow::Continue(()),
            other => {
                let Ok(v) = ast_value_to_value(other) else {
                    return ControlFlow::Continue(());
                };
                let name = self.add(v);
                value.value = AstValue::Placeholder(format!(":{name}"));
                ControlFlow::Continue(())
            }
        }
    }
}

/// Give the client's own placeholders names.
///
/// A client that prepared `WHERE id = ?` sends its parameters positionally at
/// execute time. Vituss works in named bind variables, so each `?` (or `$1`, or
/// `@p1`) becomes `:p1`, `:p2` … in order. The count is returned so the wire
/// layer can tell the client how many parameters the statement takes.
pub fn number_client_placeholders(stmt: &mut Statement) -> usize {
    struct Numberer {
        count: usize,
    }
    impl VisitorMut for Numberer {
        type Break = ();
        fn pre_visit_value(&mut self, value: &mut ValueWithSpan) -> ControlFlow<()> {
            if let AstValue::Placeholder(p) = &value.value {
                // Leave Vituss's own placeholders alone: those are already named.
                if !p.starts_with(':') {
                    self.count += 1;
                    value.value = AstValue::Placeholder(format!(":p{}", self.count));
                }
            }
            ControlFlow::Continue(())
        }
    }
    let mut n = Numberer { count: 0 };
    let _ = stmt.visit(&mut n);
    n.count
}

/// The bind-variable name for the `i`-th client parameter (0-based).
pub fn client_placeholder_name(i: usize) -> String {
    format!("p{}", i + 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::{Query, SetExpr};
    use vituss_core::Value;
    use vituss_dialect::get;

    fn parse(sql: &str) -> Statement {
        get("mysql").unwrap().parse_one(sql).unwrap()
    }

    fn as_query(stmt: &mut Statement) -> &mut Query {
        match stmt {
            Statement::Query(q) => q,
            _ => panic!("not a query"),
        }
    }

    #[test]
    fn predicate_literals_become_bind_variables() {
        let mut stmt = parse("SELECT * FROM user WHERE id = 5 AND name = 'ada' ORDER BY 1 LIMIT 10");
        let mut n = Normalizer::new("v");
        {
            let q = as_query(&mut stmt);
            let SetExpr::Select(select) = &mut *q.body else { panic!() };
            n.opt_expr(select.selection.as_mut());
        }
        let bv = n.finish();
        assert_eq!(bv.get("v1"), Some(&Value::Int(5)));
        assert_eq!(bv.get("v2"), Some(&Value::Text("ada".into())));

        let rendered = stmt.to_string();
        assert!(rendered.contains(":v1") && rendered.contains(":v2"), "{rendered}");
        // No user data left in the statement text, so it is safe to log.
        assert!(!rendered.contains("ada"), "{rendered}");
        // Positions and counts are untouched: they are not data.
        assert!(rendered.contains("ORDER BY 1"), "{rendered}");
        assert!(rendered.contains("LIMIT 10"), "{rendered}");
    }

    #[test]
    fn nulls_and_client_placeholders_are_left_alone() {
        let mut stmt = parse("SELECT * FROM user WHERE deleted_at IS NULL AND id = ?");
        let mut n = Normalizer::new("v");
        {
            let q = as_query(&mut stmt);
            let SetExpr::Select(select) = &mut *q.body else { panic!() };
            n.opt_expr(select.selection.as_mut());
        }
        assert!(n.finish().is_empty());
        let rendered = stmt.to_string();
        assert!(rendered.contains("IS NULL") && rendered.contains('?'), "{rendered}");
    }
}
