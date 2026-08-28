//! Error type shared across Vituss.
//!
//! Vitess models errors as gRPC codes plus a MySQL error number. Vituss keeps the
//! same idea but the numeric mapping is per-dialect: the same logical error must
//! surface as `1105` to a MySQL client, `XX000` to a PostgreSQL client and a
//! `50000`-class message to a SQL Server client. The mapping itself lives in
//! `vituss-dialect`; here we only carry the canonical [`Code`].

use std::fmt;

/// Canonical error class. Mirrors the subset of gRPC codes Vitess uses in
/// `vtrpcpb.Code`, which is what drives client retry behaviour.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Code {
    Ok,
    Canceled,
    Unknown,
    InvalidArgument,
    DeadlineExceeded,
    NotFound,
    AlreadyExists,
    PermissionDenied,
    ResourceExhausted,
    FailedPrecondition,
    Aborted,
    OutOfRange,
    Unimplemented,
    Internal,
    Unavailable,
    DataLoss,
    Unauthenticated,
    /// The query was rejected by the planner: it is valid SQL but Vituss cannot
    /// route it (e.g. a cross-shard construct with no supported plan).
    Unsupported,
}

impl fmt::Display for Code {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// The Vituss error type.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{code}: {message}")]
pub struct Error {
    pub code: Code,
    pub message: String,
    /// Native error number reported by an upstream engine, if this error was
    /// produced by forwarding a driver failure. Preserved so a client that
    /// switches on vendor codes keeps working through Vituss.
    pub native_code: Option<u32>,
    /// Native SQLSTATE reported by an upstream engine, if any.
    pub sql_state: Option<String>,
}

impl Error {
    pub fn new(code: Code, message: impl Into<String>) -> Self {
        Self { code, message: message.into(), native_code: None, sql_state: None }
    }

    /// Attach the upstream engine's native error number / SQLSTATE.
    pub fn with_native(mut self, native_code: Option<u32>, sql_state: Option<String>) -> Self {
        self.native_code = native_code;
        self.sql_state = sql_state;
        self
    }

    /// True when a client may safely retry: the query provably did not commit.
    pub fn is_retryable(&self) -> bool {
        matches!(self.code, Code::Unavailable | Code::Aborted | Code::ResourceExhausted)
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self::new(Code::Internal, msg)
    }
    pub fn invalid(msg: impl Into<String>) -> Self {
        Self::new(Code::InvalidArgument, msg)
    }
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::new(Code::NotFound, msg)
    }
    pub fn already_exists(msg: impl Into<String>) -> Self {
        Self::new(Code::AlreadyExists, msg)
    }
    pub fn unsupported(msg: impl Into<String>) -> Self {
        Self::new(Code::Unsupported, msg)
    }
    pub fn unavailable(msg: impl Into<String>) -> Self {
        Self::new(Code::Unavailable, msg)
    }
    pub fn failed_precondition(msg: impl Into<String>) -> Self {
        Self::new(Code::FailedPrecondition, msg)
    }
    pub fn unimplemented(msg: impl Into<String>) -> Self {
        Self::new(Code::Unimplemented, msg)
    }
    pub fn aborted(msg: impl Into<String>) -> Self {
        Self::new(Code::Aborted, msg)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Build an [`Error`] with `format!` syntax: `verr!(Unsupported, "no plan for {q}")`.
#[macro_export]
macro_rules! verr {
    ($code:ident, $($arg:tt)*) => {
        $crate::error::Error::new($crate::error::Code::$code, format!($($arg)*))
    };
}

/// `bail!` counterpart of [`verr!`].
#[macro_export]
macro_rules! vbail {
    ($code:ident, $($arg:tt)*) => {
        return Err($crate::verr!($code, $($arg)*))
    };
}
