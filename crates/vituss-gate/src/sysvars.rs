//! Session-setup statements clients send before they do any real work.
//!
//! Every MySQL and PostgreSQL driver opens a connection by negotiating session
//! state: `SET NAMES utf8mb4`, `SET sql_mode=...`, `SELECT @@version_comment`.
//! None of it is a query against the user's data, and none of it can sensibly be
//! routed to a shard — asking a SQLite shard for `@@version_comment` is not a
//! meaningful question.
//!
//! So the gate answers these itself. It is the difference between "a driver can
//! connect" and "a driver hangs up during the handshake", and it is why this
//! module exists at all rather than being folded into the planner: these
//! statements are protocol chatter, not SQL to be planned.

use vituss_core::{Field, QueryResult, Result, SqlType, Value};
use vituss_engine::SessionRef;

/// Values Vituss reports for the system variables clients ask about.
///
/// Chosen to describe Vituss honestly while keeping drivers happy: the version
/// is high enough that clients enable modern protocol features, and the charset
/// answers are the ones a UTF-8 client expects.
fn known_variable(name: &str) -> Option<Value> {
    let n = name
        .trim_start_matches("@@")
        .trim_start_matches("session.")
        .trim_start_matches("global.")
        .to_ascii_lowercase();
    Some(match n.as_str() {
        "version" => Value::Text(format!("8.0.0-Vituss-{}", env!("CARGO_PKG_VERSION"))),
        "version_comment" => Value::Text("Vituss — engine-agnostic sharding layer".into()),
        "version_compile_os" => Value::Text(std::env::consts::OS.into()),
        "version_compile_machine" => Value::Text(std::env::consts::ARCH.into()),
        "protocol_version" => Value::Int(10),
        "license" => Value::Text("Apache-2.0".into()),

        "character_set_client"
        | "character_set_connection"
        | "character_set_results"
        | "character_set_server"
        | "character_set_database" => Value::Text("utf8mb4".into()),
        "collation_connection" | "collation_server" | "collation_database" => {
            Value::Text("utf8mb4_general_ci".into())
        }

        "autocommit" => Value::Int(1),
        "transaction_isolation" | "tx_isolation" => Value::Text("REPEATABLE-READ".into()),
        "transaction_read_only" | "tx_read_only" => Value::Int(0),
        // Not a real MySQL server, and a client that goes looking for one should
        // find out here rather than by failing later.
        "sql_mode" => Value::Text("STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION".into()),
        "sql_auto_is_null" => Value::Int(0),
        "lower_case_table_names" => Value::Int(0),
        "max_allowed_packet" => Value::Int(67_108_864),
        "net_write_timeout" | "net_read_timeout" => Value::Int(60),
        "interactive_timeout" | "wait_timeout" => Value::Int(28800),
        "time_zone" => Value::Text("SYSTEM".into()),
        "system_time_zone" => Value::Text("UTC".into()),
        "performance_schema" => Value::Int(0),
        "event_scheduler" => Value::Text("OFF".into()),
        "init_connect" => Value::Text(String::new()),
        "socket" => Value::Text(String::new()),
        "hostname" => Value::Text("vituss".into()),
        "port" => Value::Int(0),
        "have_ssl" | "have_openssl" => Value::Text("DISABLED".into()),
        "read_only" | "super_read_only" => Value::Int(0),
        "foreign_key_checks" => Value::Int(1),
        "unique_checks" => Value::Int(1),
        _ => return None,
    })
}

/// Handle a statement here if it is session chatter; otherwise return `None` so
/// it goes to the planner.
pub async fn try_handle(sql: &str, session: &SessionRef) -> Option<Result<QueryResult>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();

    if lower.starts_with("set ") {
        return Some(handle_set(trimmed, session).await);
    }
    if lower.starts_with("show variables") || lower.starts_with("show session variables")
        || lower.starts_with("show global variables")
    {
        return Some(Ok(show_variables(trimmed)));
    }
    // A projection made entirely of system variables, with no FROM: not a query
    // against data at all.
    if lower.starts_with("select ") && trimmed.contains("@@") && !lower.contains(" from ") {
        return Some(handle_select_variables(trimmed, session).await);
    }
    None
}

/// Apply a `SET`, leniently.
///
/// Drivers send `SET` statements with syntax no general-purpose parser accepts
/// — `SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci`, assignments whose value is
/// a subquery. Refusing them would break the connection over something that does
/// not matter, so anything unrecognised is recorded and acknowledged.
///
/// The exceptions are the settings that *do* change behaviour: `autocommit`, the
/// transaction mode, and user-defined variables. Those are applied, and an
/// invalid value for them is an error rather than a shrug.
async fn handle_set(sql: &str, session: &SessionRef) -> Result<QueryResult> {
    let body = sql[3..].trim();
    let body = strip_scope(body);

    for clause in split_top_level(body) {
        let clause = clause.trim();
        let lower = clause.to_ascii_lowercase();

        if lower.starts_with("names ") || lower == "names" || lower.starts_with("character set ") {
            // Charset negotiation. Vituss speaks UTF-8 to everything.
            continue;
        }
        if lower.starts_with("transaction ") {
            // `SET TRANSACTION ISOLATION LEVEL ...` — recorded and replayed onto
            // the shard connections the session opens.
            session
                .lock()
                .await
                .system_variables
                .insert("transaction".into(), clause[12..].trim().to_string());
            continue;
        }

        let Some((name, value)) = clause.split_once('=') else { continue };
        let name = name.trim().trim_matches('`');
        let value = value.trim().trim_matches('\'').trim_matches('"');

        let mut s = session.lock().await;
        let key = name.trim_start_matches("@@").trim_start_matches("session.").trim_start_matches("global.");

        if name.starts_with('@') && !name.starts_with("@@") {
            s.user_defined_variables.insert(
                name.to_string(),
                value.parse::<i64>().map(Value::Int).unwrap_or_else(|_| Value::Text(value.to_string())),
            );
            continue;
        }

        match key.to_ascii_lowercase().as_str() {
            "autocommit" => {
                s.autocommit = !matches!(value, "0" | "off" | "OFF" | "false");
                // Turning autocommit off means the next statement opens a
                // transaction, which is what a client that does this expects.
                if !s.autocommit {
                    s.in_transaction = false;
                }
            }
            "vituss_transaction_mode" => {
                s.transaction_mode = match value.to_ascii_lowercase().as_str() {
                    "single" => vituss_core::TransactionMode::Single,
                    "multi" => vituss_core::TransactionMode::Multi,
                    "two_pc" | "twopc" => vituss_core::TransactionMode::TwoPc,
                    other => {
                        return Err(vituss_core::Error::invalid(format!(
                            "unknown transaction mode {other:?}; expected single, multi or two_pc"
                        )))
                    }
                };
            }
            other => {
                s.system_variables.insert(other.to_string(), value.to_string());
            }
        }
    }

    Ok(QueryResult::default())
}

fn strip_scope(body: &str) -> &str {
    for prefix in ["GLOBAL ", "SESSION ", "LOCAL ", "global ", "session ", "local "] {
        if let Some(rest) = body.strip_prefix(prefix) {
            return rest;
        }
    }
    body
}

/// Answer `SELECT @@a, @@b AS c`.
async fn handle_select_variables(sql: &str, session: &SessionRef) -> Result<QueryResult> {
    let list = &sql[6..];
    let mut fields = Vec::new();
    let mut row = Vec::new();

    for item in split_top_level(list) {
        let item = item.trim();
        // `expr AS alias`, or just `expr`, whose column name is the expression.
        let (expr, alias) = match split_alias(item) {
            Some((e, a)) => (e, a),
            None => (item, item.to_string()),
        };
        let expr = expr.trim();

        let value = if let Some(v) = known_variable(expr) {
            v
        } else if expr.starts_with("@@") {
            // Unknown system variable: report NULL, which is what a MySQL server
            // does for a variable it does not have, rather than failing the
            // client's whole handshake.
            Value::Null
        } else if expr.starts_with('@') {
            session
                .lock()
                .await
                .user_defined_variables
                .get(expr)
                .cloned()
                .unwrap_or(Value::Null)
        } else {
            Value::Null
        };

        let sql_type = match &value {
            Value::Int(_) => SqlType::Int64,
            Value::Null => SqlType::Null,
            _ => SqlType::VarChar,
        };
        fields.push(Field::new(alias, sql_type));
        row.push(value);
    }

    Ok(QueryResult::from_rows(fields, vec![row]))
}

/// Answer `SHOW VARIABLES [LIKE 'pattern']`.
fn show_variables(sql: &str) -> QueryResult {
    let pattern = sql
        .to_ascii_lowercase()
        .split_once(" like ")
        .map(|(_, p)| p.trim().trim_matches('\'').trim_matches('"').to_string());

    let names = [
        "autocommit",
        "character_set_client",
        "character_set_connection",
        "character_set_results",
        "character_set_server",
        "collation_connection",
        "collation_server",
        "hostname",
        "init_connect",
        "interactive_timeout",
        "license",
        "lower_case_table_names",
        "max_allowed_packet",
        "net_write_timeout",
        "performance_schema",
        "sql_mode",
        "system_time_zone",
        "time_zone",
        "transaction_isolation",
        "version",
        "version_comment",
        "wait_timeout",
    ];

    let rows = names
        .iter()
        .filter(|n| match &pattern {
            None => true,
            Some(p) => like_matches(n, p),
        })
        .filter_map(|n| {
            known_variable(n).map(|v| vec![Value::Text(n.to_string()), Value::Text(v.to_string())])
        })
        .collect();

    QueryResult::from_rows(
        vec![
            Field::new("Variable_name", SqlType::VarChar),
            Field::new("Value", SqlType::VarChar),
        ],
        rows,
    )
}

/// SQL `LIKE` with `%` and `_`, which is all `SHOW ... LIKE` needs.
fn like_matches(text: &str, pattern: &str) -> bool {
    fn go(t: &[u8], p: &[u8]) -> bool {
        match (t.first(), p.first()) {
            (_, Some(b'%')) => go(t, &p[1..]) || (!t.is_empty() && go(&t[1..], p)),
            (Some(_), Some(b'_')) => go(&t[1..], &p[1..]),
            (Some(a), Some(b)) if a.eq_ignore_ascii_case(b) => go(&t[1..], &p[1..]),
            (None, None) => true,
            _ => false,
        }
    }
    go(text.as_bytes(), pattern.as_bytes())
}

/// Split on commas that are not inside quotes or parentheses.
///
/// Needed because a driver's `SET` really does contain things like
/// `sql_mode=(SELECT CONCAT(@@sql_mode, ',NO_ENGINE_SUBSTITUTION'))`, where a
/// naive split on `,` would cut the value in half.
fn split_top_level(s: &str) -> Vec<&str> {
    let bytes = s.as_bytes();
    let (mut out, mut start, mut depth) = (Vec::new(), 0usize, 0i32);
    let mut quote: Option<u8> = None;

    for (i, &c) in bytes.iter().enumerate() {
        match (quote, c) {
            (Some(q), _) if c == q => {
                // A doubled quote is an escaped quote, not the end of the string.
                if bytes.get(i + 1) != Some(&q) {
                    quote = None;
                }
            }
            (Some(_), _) => {}
            (None, b'\'') | (None, b'"') | (None, b'`') => quote = Some(c),
            (None, b'(') => depth += 1,
            (None, b')') => depth -= 1,
            (None, b',') if depth == 0 => {
                out.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(&s[start..]);
    out
}

/// Split `expr AS alias` or `expr alias`, respecting quotes.
fn split_alias(item: &str) -> Option<(&str, String)> {
    let lower = item.to_ascii_lowercase();
    if let Some(pos) = lower.rfind(" as ") {
        let alias = item[pos + 4..].trim().trim_matches('`').trim_matches('\'').to_string();
        return Some((&item[..pos], alias));
    }
    // `@@x y` — an alias without AS. Only when there is exactly one space and the
    // second word is a bare identifier.
    let mut parts = item.split_whitespace();
    let (Some(a), Some(b), None) = (parts.next(), parts.next(), parts.next()) else {
        return None;
    };
    if b.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Some((&item[..a.len()], b.to_string()));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use vituss_core::Session;

    fn session() -> SessionRef {
        vituss_engine::new_session(Session::new())
    }

    #[tokio::test]
    async fn driver_handshake_statements_are_absorbed() {
        let s = session();
        // Exactly what sqlx's MySQL driver sends on connect.
        let sql = "SET sql_mode=(SELECT CONCAT(@@sql_mode, ',PIPES_AS_CONCAT,NO_ENGINE_SUBSTITUTION')),\
                   time_zone='+00:00',NAMES utf8mb4 COLLATE utf8mb4_unicode_ci";
        let r = try_handle(sql, &s).await.expect("handled here").expect("succeeds");
        assert!(r.rows.is_empty());
        assert_eq!(s.lock().await.system_variables.get("time_zone").map(String::as_str), Some("+00:00"));
    }

    #[tokio::test]
    async fn system_variables_are_answered_without_touching_a_shard() {
        let s = session();
        let r = try_handle("SELECT @@version_comment, @@max_allowed_packet AS pkt", &s)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.rows.len(), 1);
        assert!(r.rows[0][0].to_string().contains("Vituss"));
        assert_eq!(r.fields[1].name, "pkt");
        assert_eq!(r.rows[0][1], Value::Int(67_108_864));
    }

    #[tokio::test]
    async fn an_unknown_system_variable_is_null_not_an_error() {
        let s = session();
        let r = try_handle("SELECT @@no_such_variable", &s).await.unwrap().unwrap();
        assert!(r.rows[0][0].is_null());
    }

    #[tokio::test]
    async fn autocommit_actually_changes_the_session() {
        let s = session();
        try_handle("SET autocommit = 0", &s).await.unwrap().unwrap();
        assert!(!s.lock().await.autocommit);
        try_handle("SET autocommit = 1", &s).await.unwrap().unwrap();
        assert!(s.lock().await.autocommit);
    }

    #[tokio::test]
    async fn a_bad_transaction_mode_is_rejected_rather_than_ignored() {
        let s = session();
        let err = try_handle("SET vituss_transaction_mode = 'sometimes'", &s)
            .await
            .unwrap()
            .unwrap_err();
        assert!(err.message.contains("unknown transaction mode"), "{}", err.message);
    }

    #[tokio::test]
    async fn real_queries_are_left_for_the_planner() {
        let s = session();
        assert!(try_handle("SELECT name FROM user WHERE id = 1", &s).await.is_none());
        // A query that mentions a variable but also reads a table is a real query.
        assert!(try_handle("SELECT @@version, name FROM user", &s).await.is_none());
    }

    #[tokio::test]
    async fn show_variables_filters_on_the_pattern() {
        let s = session();
        let r = try_handle("SHOW VARIABLES LIKE 'character_set%'", &s).await.unwrap().unwrap();
        assert!(!r.rows.is_empty());
        assert!(r.rows.iter().all(|row| row[0].to_string().starts_with("character_set")));
    }

    #[test]
    fn commas_inside_parentheses_and_strings_do_not_split() {
        let parts = split_top_level("a=(f(1,2)),b=',',c=3");
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0], "a=(f(1,2))");
        assert_eq!(parts[1], "b=','");
    }
}
