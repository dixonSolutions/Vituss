//! Pieces every driver shares.

use vituss_core::{Code, Error, Result};
use vituss_dialect::{DialectRef, TwoPcStyle};

/// Does this statement produce a result set?
///
/// The [`Connection`](crate::Connection) API is text-based, so the driver has to
/// decide whether to ask its client library for rows or for an affected-row
/// count. Getting it wrong loses one or the other, so the check errs towards
/// "rows" — an unnecessary fetch of an empty set is cheap, a lost `RETURNING`
/// clause is not.
pub fn returns_rows(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    // Skip leading comments, which the planner does emit as routing hints.
    let trimmed = strip_leading_comments(trimmed);
    let first = trimmed
        .split(|c: char| c.is_whitespace() || c == '(')
        .find(|s| !s.is_empty())
        .unwrap_or("")
        .to_ascii_uppercase();

    if matches!(
        first.as_str(),
        "SELECT" | "WITH" | "SHOW" | "DESC" | "DESCRIBE" | "EXPLAIN" | "PRAGMA" | "VALUES" | "TABLE" | "CALL"
    ) {
        return true;
    }
    // A DML with RETURNING / OUTPUT produces rows as well.
    let upper = trimmed.to_ascii_uppercase();
    upper.contains(" RETURNING ") || upper.contains("OUTPUT INSERTED") || upper.contains("OUTPUT DELETED")
}

fn strip_leading_comments(mut s: &str) -> &str {
    loop {
        s = s.trim_start();
        if let Some(rest) = s.strip_prefix("/*") {
            match rest.find("*/") {
                Some(end) => s = &rest[end + 2..],
                None => return s,
            }
        } else if let Some(rest) = s.strip_prefix("--") {
            match rest.find('\n') {
                Some(end) => s = &rest[end + 1..],
                None => return "",
            }
        } else {
            return s;
        }
    }
}

/// Refuse two-phase commit on an engine that cannot do it from SQL.
///
/// Reported as `Unimplemented` rather than silently degraded to a chain of
/// single-shard commits: a caller who asked for atomicity must find out that they
/// are not getting it.
pub fn two_pc_unsupported(dialect: &DialectRef, op: &str) -> Error {
    let why = match dialect.capabilities().two_pc {
        TwoPcStyle::ExternalCoordinator => {
            "it requires an external transaction coordinator (MS DTC), which Vituss cannot drive from SQL"
        }
        TwoPcStyle::None => "the engine has no distributed transaction support",
        _ => "the engine reports 2PC support but produced no SQL for it",
    };
    Error::new(
        Code::Unimplemented,
        format!("cannot {op} on a {} backend: {why}", dialect.name()),
    )
}

/// Generate a driver's entire [`Connection`](crate::backend::Connection) impl.
///
/// The driver supplies one inherent method, `exec_impl`, and gets the rest for
/// free. Everything the macro generates is transaction control expressed as the
/// dialect's own SQL rather than as the client library's transaction object —
/// deliberately, because Vituss must be able to leave a transaction half-open on
/// a pooled connection and return to it on a later request, which a scoped
/// transaction guard cannot express.
///
/// The whole `impl` block is generated, `#[async_trait]` included: an attribute
/// macro cannot see through a macro call, so it has to be inside.
///
/// Requires the type to have `dialect: DialectRef`, `in_tx: bool` and
/// `healthy: bool` fields, and an inherent
/// `async fn exec_impl(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult>`.
#[macro_export]
macro_rules! impl_connection {
    ($ty:ty) => {
        #[::async_trait::async_trait]
        impl $crate::backend::Connection for $ty {
            async fn execute(
                &mut self,
                sql: &str,
                params: &[::vituss_core::Value],
            ) -> ::vituss_core::Result<::vituss_core::QueryResult> {
                self.exec_impl(sql, params).await
            }

            async fn begin(&mut self, isolation: Option<&str>) -> ::vituss_core::Result<()> {
                let sql = self.dialect.begin_sql(isolation);
                for stmt in sql.split(';').map(str::trim).filter(|s| !s.is_empty()) {
                    self.exec_impl(stmt, &[]).await?;
                }
                // MySQL's `SET TRANSACTION ISOLATION LEVEL` configures the *next*
                // transaction; the BEGIN itself still has to be sent.
                if !sql.to_ascii_uppercase().contains("BEGIN") {
                    self.exec_impl("BEGIN", &[]).await?;
                }
                self.in_tx = true;
                Ok(())
            }

            async fn commit(&mut self) -> ::vituss_core::Result<()> {
                self.exec_impl("COMMIT", &[]).await?;
                self.in_tx = false;
                Ok(())
            }

            async fn rollback(&mut self) -> ::vituss_core::Result<()> {
                let r = self.exec_impl("ROLLBACK", &[]).await;
                // The transaction is over either way: a rollback that itself failed
                // means the connection is unusable, not that the transaction lives on.
                self.in_tx = false;
                r.map(|_| ())
            }

            async fn savepoint(&mut self, name: &str) -> ::vituss_core::Result<()> {
                let sql = self.dialect.savepoint_sql(name);
                self.exec_impl(&sql, &[]).await.map(|_| ())
            }

            async fn rollback_to_savepoint(&mut self, name: &str) -> ::vituss_core::Result<()> {
                let sql = self.dialect.rollback_to_savepoint_sql(name);
                self.exec_impl(&sql, &[]).await.map(|_| ())
            }

            async fn release_savepoint(&mut self, name: &str) -> ::vituss_core::Result<()> {
                let sql = self.dialect.release_savepoint_sql(name);
                // Empty means the engine has no such statement (T-SQL); that is a
                // no-op, not an error.
                if sql.is_empty() {
                    return Ok(());
                }
                self.exec_impl(&sql, &[]).await.map(|_| ())
            }

            async fn prepare_two_pc(&mut self, xid: &str) -> ::vituss_core::Result<()> {
                let Some(sql) = self.dialect.two_pc_sql(xid) else {
                    return Err($crate::common::two_pc_unsupported(
                        &self.dialect,
                        "prepare a distributed transaction",
                    ));
                };
                if let Some(end) = sql.end {
                    self.exec_impl(&end, &[]).await?;
                }
                self.exec_impl(&sql.prepare, &[]).await?;
                self.in_tx = false;
                Ok(())
            }

            async fn commit_prepared(&mut self, xid: &str) -> ::vituss_core::Result<()> {
                let Some(sql) = self.dialect.two_pc_sql(xid) else {
                    return Err($crate::common::two_pc_unsupported(
                        &self.dialect,
                        "commit a prepared transaction",
                    ));
                };
                self.exec_impl(&sql.commit, &[]).await.map(|_| ())
            }

            async fn rollback_prepared(&mut self, xid: &str) -> ::vituss_core::Result<()> {
                let Some(sql) = self.dialect.two_pc_sql(xid) else {
                    return Err($crate::common::two_pc_unsupported(
                        &self.dialect,
                        "roll back a prepared transaction",
                    ));
                };
                self.exec_impl(&sql.rollback, &[]).await.map(|_| ())
            }

            async fn ping(&mut self) -> ::vituss_core::Result<()> {
                let sql = self.dialect.introspection().ping;
                self.exec_impl(sql, &[]).await.map(|_| ())
            }

            fn in_transaction(&self) -> bool {
                self.in_tx
            }

            fn is_healthy(&self) -> bool {
                self.healthy
            }
        }
    };
}

/// Start an `XA` transaction where the engine needs an explicit start statement.
pub fn two_pc_start(dialect: &DialectRef, xid: &str) -> Result<Option<String>> {
    match dialect.two_pc_sql(xid) {
        Some(sql) => Ok(Some(sql.start)),
        None => Err(two_pc_unsupported(dialect, "start a distributed transaction")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_kind_detection() {
        assert!(returns_rows("SELECT 1"));
        assert!(returns_rows("  with x as (select 1) select * from x"));
        assert!(returns_rows("/*vt+ route=-80 */ SELECT id FROM t"));
        assert!(returns_rows("INSERT INTO t(a) VALUES (1) RETURNING id"));
        assert!(returns_rows("INSERT INTO t OUTPUT INSERTED.id VALUES (1)"));
        assert!(!returns_rows("INSERT INTO t(a) VALUES (1)"));
        assert!(!returns_rows("UPDATE t SET a = 1"));
        assert!(!returns_rows("-- a comment\nDELETE FROM t"));
    }
}
