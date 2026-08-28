//! Engine-neutral SQL values.
//!
//! Vitess carries values as `(type, raw bytes)` pairs because MySQL's wire format
//! is text-first. Vituss instead carries a decoded enum, because it must
//! interoperate with three engines whose wire encodings disagree. Each backend
//! driver converts to/from this enum; everything above `vituss-backend` — vindexes,
//! the planner, the engine primitives — only ever sees [`Value`].

use std::collections::BTreeMap;
use std::fmt;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;

/// Logical type of a column or value, normalised across engines.
///
/// This is intentionally coarser than any single engine's type system: it is the
/// intersection that Vituss needs in order to route, sort, aggregate and compare.
/// Engine-specific refinements (MySQL `TINYINT(1)`, PostgreSQL `citext`, SQL Server
/// `NVARCHAR(MAX)`) are mapped onto these by each dialect's type mapper.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqlType {
    Null,
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    Uint64,
    Float32,
    Float64,
    Decimal,
    Char,
    VarChar,
    Text,
    Binary,
    VarBinary,
    Blob,
    Date,
    Time,
    DateTime,
    Timestamp,
    Json,
    Uuid,
    /// Anything the dialect could not classify; carried through opaquely.
    Unknown,
}

impl SqlType {
    pub fn is_integral(self) -> bool {
        matches!(self, Self::Int8 | Self::Int16 | Self::Int32 | Self::Int64 | Self::Uint64)
    }
    pub fn is_numeric(self) -> bool {
        self.is_integral() || matches!(self, Self::Float32 | Self::Float64 | Self::Decimal)
    }
    pub fn is_text(self) -> bool {
        matches!(self, Self::Char | Self::VarChar | Self::Text | Self::Json | Self::Uuid)
    }
    pub fn is_binary(self) -> bool {
        matches!(self, Self::Binary | Self::VarBinary | Self::Blob)
    }
    pub fn is_temporal(self) -> bool {
        matches!(self, Self::Date | Self::Time | Self::DateTime | Self::Timestamp)
    }
}

/// A single SQL value.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "t", content = "v", rename_all = "snake_case")]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(f64),
    Decimal(Decimal),
    Text(String),
    Bytes(Vec<u8>),
    Date(NaiveDate),
    Time(NaiveTime),
    DateTime(NaiveDateTime),
    /// Timestamp with offset already normalised to UTC.
    Timestamp(chrono::DateTime<chrono::Utc>),
    Json(serde_json::Value),
    Uuid(uuid::Uuid),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn sql_type(&self) -> SqlType {
        match self {
            Value::Null => SqlType::Null,
            Value::Bool(_) => SqlType::Bool,
            Value::Int(_) => SqlType::Int64,
            Value::Uint(_) => SqlType::Uint64,
            Value::Float(_) => SqlType::Float64,
            Value::Decimal(_) => SqlType::Decimal,
            Value::Text(_) => SqlType::VarChar,
            Value::Bytes(_) => SqlType::VarBinary,
            Value::Date(_) => SqlType::Date,
            Value::Time(_) => SqlType::Time,
            Value::DateTime(_) => SqlType::DateTime,
            Value::Timestamp(_) => SqlType::Timestamp,
            Value::Json(_) => SqlType::Json,
            Value::Uuid(_) => SqlType::Uuid,
        }
    }

    /// Interpret as a signed integer where that is lossless.
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Bool(b) => Some(*b as i64),
            Value::Int(i) => Some(*i),
            Value::Uint(u) => i64::try_from(*u).ok(),
            Value::Float(f) if f.fract() == 0.0 => Some(*f as i64),
            Value::Decimal(d) => d.to_string().parse().ok(),
            Value::Text(s) => s.trim().parse().ok(),
            _ => None,
        }
    }

    /// Interpret as an unsigned integer where that is lossless.
    pub fn as_uint(&self) -> Option<u64> {
        match self {
            Value::Bool(b) => Some(*b as u64),
            Value::Int(i) => u64::try_from(*i).ok(),
            Value::Uint(u) => Some(*u),
            Value::Text(s) => s.trim().parse().ok(),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            Value::Int(i) => Some(*i as f64),
            Value::Uint(u) => Some(*u as f64),
            Value::Float(f) => Some(*f),
            Value::Decimal(d) => d.to_string().parse().ok(),
            Value::Text(s) => s.trim().parse().ok(),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Canonical byte representation used by vindexes.
    ///
    /// This *must* stay stable forever: it is the input to the hash functions
    /// that decide which shard a row lives on. Changing it re-shards the data.
    /// It is also engine-independent by construction, which is what lets the same
    /// keyspace be served by MySQL today and PostgreSQL tomorrow.
    pub fn to_vindex_bytes(&self) -> Vec<u8> {
        match self {
            Value::Null => Vec::new(),
            Value::Bool(b) => vec![*b as u8],
            Value::Int(i) => i.to_string().into_bytes(),
            Value::Uint(u) => u.to_string().into_bytes(),
            Value::Float(f) => f.to_string().into_bytes(),
            Value::Decimal(d) => d.to_string().into_bytes(),
            Value::Text(s) => s.as_bytes().to_vec(),
            Value::Bytes(b) => b.clone(),
            Value::Date(d) => d.to_string().into_bytes(),
            Value::Time(t) => t.to_string().into_bytes(),
            Value::DateTime(dt) => dt.to_string().into_bytes(),
            Value::Timestamp(ts) => ts.to_rfc3339().into_bytes(),
            Value::Json(j) => j.to_string().into_bytes(),
            // Bytes, not the hyphenated text form: a UUID means the same 128 bits
            // however the engine chose to spell it.
            Value::Uuid(u) => u.as_bytes().to_vec(),
        }
    }

    /// Total ordering used by ORDER BY / merge-sort in the engine.
    ///
    /// NULLs sort first (MySQL and PostgreSQL `ASC` default; SQL Server agrees for
    /// `ASC`). Cross-type comparison falls back to numeric coercion, then to the
    /// canonical byte form, so the ordering is total even for mixed columns.
    pub fn compare(&self, other: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
            _ => match (self.as_f64(), other.as_f64()) {
                (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
                _ => self.to_vindex_bytes().cmp(&other.to_vindex_bytes()),
            },
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Int(i) => write!(f, "{i}"),
            Value::Uint(u) => write!(f, "{u}"),
            Value::Float(x) => write!(f, "{x}"),
            Value::Decimal(d) => write!(f, "{d}"),
            Value::Text(s) => write!(f, "{s}"),
            Value::Bytes(b) => write!(f, "0x{}", hex::encode(b)),
            Value::Date(d) => write!(f, "{d}"),
            Value::Time(t) => write!(f, "{t}"),
            Value::DateTime(d) => write!(f, "{d}"),
            Value::Timestamp(t) => write!(f, "{}", t.to_rfc3339()),
            Value::Json(j) => write!(f, "{j}"),
            Value::Uuid(u) => write!(f, "{u}"),
        }
    }
}

macro_rules! from_int {
    ($($t:ty),*) => { $(impl From<$t> for Value { fn from(v: $t) -> Self { Value::Int(v as i64) } })* };
}
from_int!(i8, i16, i32, i64, isize);

impl From<u64> for Value {
    fn from(v: u64) -> Self {
        Value::Uint(v)
    }
}
impl From<u32> for Value {
    fn from(v: u32) -> Self {
        Value::Int(v as i64)
    }
}
impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}
impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}
impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Text(v.to_string())
    }
}
impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Text(v)
    }
}
impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Bytes(v)
    }
}
impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(v: Option<T>) -> Self {
        v.map(Into::into).unwrap_or(Value::Null)
    }
}

/// Named bind variables for a query.
///
/// Vituss always rewrites literals into bind variables during planning: it is the
/// only way to build one plan that serves many shards, and it is what lets the
/// same plan be rendered with `?`, `$1` or `@p1` placeholders depending on which
/// engine the shard actually runs.
pub type BindVars = BTreeMap<String, Value>;
