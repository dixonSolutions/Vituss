//! # vituss-dialect
//!
//! The plug-in layer that makes Vituss engine-agnostic.
//!
//! Vituss deliberately does **not** ship its own SQL grammar. Vitess had to write
//! one because it targeted exactly one engine and needed byte-level control of
//! MySQL syntax; Vituss targets several, so it borrows a maintained grammar per
//! engine ([`sqlparser`]) and confines everything else that differs — quoting,
//! placeholders, capabilities, introspection, error codes, transaction control —
//! behind the [`SqlDialect`] trait.
//!
//! Query *execution* is likewise delegated: shards run real MySQL, PostgreSQL or
//! SQL Server servers. Vituss adds routing, sharding and cluster management on
//! top; it never evaluates a join or an aggregate that a single shard could have
//! done itself.
//!
//! ## Adding an engine
//!
//! ```no_run
//! use std::sync::Arc;
//! use vituss_dialect::{register, SqlDialect};
//! # fn demo(my_engine: Arc<dyn SqlDialect>) {
//! register(my_engine);
//! # }
//! ```
//!
//! From that point the planner, the gate and the tablet can all target it; no
//! other crate needs to know it exists.

pub mod caps;
pub mod ddl;
pub mod dialect;
pub mod introspect;
pub mod mssql;
pub mod mysql;
pub mod postgres;
pub mod render;
pub mod sqlite;

pub use caps::{Capabilities, IdentifierCase, PlaceholderStyle, RowLock, TwoPcStyle};
pub use dialect::{get, register, registered, two_pc_compatible, DialectRef, NativeError, SqlDialect, TwoPcSql};
pub use ddl::ColumnType;
pub use introspect::Introspection;
pub use mssql::MsSql;
pub use mysql::MySql;
pub use postgres::Postgres;
pub use sqlite::Sqlite;
pub use render::{RenderedQuery, BIND_PREFIX};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn built_in_dialects_resolve_by_name_and_alias() {
        assert_eq!(get("mysql").unwrap().name(), "mysql");
        assert_eq!(get("mariadb").unwrap().name(), "mysql");
        assert_eq!(get("postgresql").unwrap().name(), "postgres");
        assert_eq!(get("sqlserver").unwrap().name(), "mssql");
        assert!(get("oracle").is_err());
    }

    #[test]
    fn each_engine_parses_its_own_syntax() {
        // Backtick quoting is MySQL-only.
        assert!(get("mysql").unwrap().parse_one("SELECT `a` FROM `t`").is_ok());
        // Bracket quoting is SQL Server-only.
        assert!(get("mssql").unwrap().parse_one("SELECT [a] FROM [t]").is_ok());
        // Dollar-quoted strings and casts are PostgreSQL.
        assert!(get("postgres").unwrap().parse_one("SELECT a::int FROM t").is_ok());
    }

    #[test]
    fn limit_rendering_differs_where_the_engines_differ() {
        assert_eq!(get("mysql").unwrap().limit_clause(Some(10), Some(5)), " LIMIT 10 OFFSET 5");
        assert_eq!(get("postgres").unwrap().limit_clause(Some(10), Some(5)), " LIMIT 10 OFFSET 5");
        assert_eq!(
            get("mssql").unwrap().limit_clause(Some(10), Some(5)),
            " OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY"
        );
    }

    #[test]
    fn two_pc_availability_is_reported_honestly() {
        let my = get("mysql").unwrap();
        let pg = get("postgres").unwrap();
        let ms = get("mssql").unwrap();
        assert!(my.two_pc_sql("v1").unwrap().prepare.starts_with("XA PREPARE"));
        assert!(pg.two_pc_sql("v1").unwrap().prepare.starts_with("PREPARE TRANSACTION"));
        assert!(ms.two_pc_sql("v1").is_none());

        assert!(two_pc_compatible(my.as_ref(), my.as_ref()));
        // Different protocols cannot be mixed in one distributed transaction.
        assert!(!two_pc_compatible(my.as_ref(), pg.as_ref()));
        assert!(!two_pc_compatible(ms.as_ref(), ms.as_ref()));
    }

    #[test]
    fn native_types_map_onto_the_neutral_type_system() {
        use vituss_core::SqlType;
        assert_eq!(get("mysql").unwrap().map_native_type("bigint(20) unsigned"), SqlType::Uint64);
        assert_eq!(get("mysql").unwrap().map_native_type("tinyint(1)"), SqlType::Bool);
        assert_eq!(get("postgres").unwrap().map_native_type("int8"), SqlType::Int64);
        assert_eq!(get("postgres").unwrap().map_native_type("timestamptz"), SqlType::Timestamp);
        assert_eq!(get("mssql").unwrap().map_native_type("nvarchar(50)"), SqlType::VarChar);
        assert_eq!(get("mssql").unwrap().map_native_type("uniqueidentifier"), SqlType::Uuid);
    }
}
