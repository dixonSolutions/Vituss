//! Small conversions between the SQL AST and Vituss values.

use sqlparser::ast::{Expr, Value as AstValue, ValueWithSpan};

use vituss_core::{Error, Result, Value};

use crate::plan::RouteValue;

/// Convert a literal AST value to a Vituss value.
pub fn ast_value_to_value(v: &AstValue) -> Result<Value> {
    Ok(match v {
        AstValue::Null => Value::Null,
        AstValue::Boolean(b) => Value::Bool(*b),
        AstValue::Number(n, _) => match n.parse::<i64>() {
            Ok(i) => Value::Int(i),
            Err(_) => match n.parse::<u64>() {
                Ok(u) => Value::Uint(u),
                Err(_) => n
                    .parse::<f64>()
                    .map(Value::Float)
                    .map_err(|_| Error::invalid(format!("cannot interpret numeric literal {n:?}")))?,
            },
        },
        AstValue::SingleQuotedString(s)
        | AstValue::DoubleQuotedString(s)
        | AstValue::EscapedStringLiteral(s)
        | AstValue::UnicodeStringLiteral(s)
        | AstValue::NationalStringLiteral(s)
        | AstValue::TripleSingleQuotedString(s)
        | AstValue::TripleDoubleQuotedString(s) => Value::Text(s.clone()),
        AstValue::HexStringLiteral(h) => Value::Bytes(
            (0..h.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&h[i..(i + 2).min(h.len())], 16).unwrap_or(0))
                .collect(),
        ),
        other => return Err(Error::unsupported(format!("literal {other} cannot be used for routing"))),
    })
}

/// Interpret an expression as something a vindex can consume: a literal or a
/// bind variable. Anything else (a function call, a column reference, an
/// arithmetic expression) is not known at planning time, so the query cannot be
/// routed by it.
pub fn expr_to_route_value(e: &Expr) -> Option<RouteValue> {
    match e {
        Expr::Value(ValueWithSpan { value: AstValue::Placeholder(name), .. }) => {
            Some(RouteValue::BindVar(name.trim_start_matches(':').to_string()))
        }
        Expr::Value(ValueWithSpan { value, .. }) => ast_value_to_value(value).ok().map(RouteValue::Literal),
        // A cast around a literal is still a constant: `WHERE id = CAST(1 AS BIGINT)`.
        Expr::Cast { expr, .. } => expr_to_route_value(expr),
        Expr::Nested(inner) => expr_to_route_value(inner),
        Expr::UnaryOp { op: sqlparser::ast::UnaryOperator::Minus, expr } => match expr_to_route_value(expr) {
            Some(RouteValue::Literal(Value::Int(i))) => Some(RouteValue::Literal(Value::Int(-i))),
            Some(RouteValue::Literal(Value::Float(f))) => Some(RouteValue::Literal(Value::Float(-f))),
            _ => None,
        },
        Expr::Tuple(items) => items
            .iter()
            .map(expr_to_route_value)
            .collect::<Option<Vec<_>>>()
            .map(RouteValue::Tuple),
        _ => None,
    }
}

/// Split a boolean expression into its top-level AND terms.
///
/// Only these can be used for routing: a term under an OR does not have to hold
/// for every row the query returns.
pub fn and_terms(e: &Expr) -> Vec<&Expr> {
    fn walk<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
        match e {
            Expr::BinaryOp { left, op: sqlparser::ast::BinaryOperator::And, right } => {
                walk(left, out);
                walk(right, out);
            }
            Expr::Nested(inner) => walk(inner, out),
            other => out.push(other),
        }
    }
    let mut out = Vec::new();
    walk(e, &mut out);
    out
}

/// Split a boolean expression into its top-level OR terms.
pub fn or_terms(e: &Expr) -> Vec<&Expr> {
    fn walk<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
        match e {
            Expr::BinaryOp { left, op: sqlparser::ast::BinaryOperator::Or, right } => {
                walk(left, out);
                walk(right, out);
            }
            Expr::Nested(inner) => walk(inner, out),
            other => out.push(other),
        }
    }
    let mut out = Vec::new();
    walk(e, &mut out);
    out
}

/// The `(qualifier, column)` an expression refers to, if it is a plain column.
pub fn as_column_ref(e: &Expr) -> Option<(Option<String>, String)> {
    match e {
        Expr::Identifier(id) => Some((None, id.value.clone())),
        Expr::CompoundIdentifier(parts) => {
            let col = parts.last()?.value.clone();
            // `db.table.column` — the qualifier the query uses to refer to the
            // table is the part immediately before the column.
            let qual = if parts.len() >= 2 { Some(parts[parts.len() - 2].value.clone()) } else { None };
            Some((qual, col))
        }
        Expr::Nested(inner) => as_column_ref(inner),
        _ => None,
    }
}

/// Build an AST placeholder for a Vituss bind variable.
pub fn placeholder(name: &str) -> Expr {
    Expr::Value(ValueWithSpan {
        value: AstValue::Placeholder(format!(":{name}")),
        span: sqlparser::tokenizer::Span::empty(),
    })
}

/// Build an AST literal from a Vituss value.
pub fn literal(v: &Value) -> Expr {
    let value = match v {
        Value::Null => AstValue::Null,
        Value::Bool(b) => AstValue::Boolean(*b),
        Value::Int(i) => AstValue::Number(i.to_string(), false),
        Value::Uint(u) => AstValue::Number(u.to_string(), false),
        Value::Float(f) => AstValue::Number(f.to_string(), false),
        Value::Decimal(d) => AstValue::Number(d.to_string(), false),
        other => AstValue::SingleQuotedString(other.to_string()),
    };
    Expr::Value(ValueWithSpan { value, span: sqlparser::tokenizer::Span::empty() })
}
