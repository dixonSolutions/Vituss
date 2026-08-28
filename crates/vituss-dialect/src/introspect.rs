//! Schema introspection SQL, per engine.
//!
//! The tablet needs the column list, primary keys and indexes of every table it
//! serves — that is what lets the planner know a `WHERE` predicate hits a unique
//! key. Every engine exposes this differently, so the queries are part of the
//! dialect plug-in rather than of the tablet.
//!
//! Each query takes exactly one parameter (the schema / database name) and must
//! return the documented column order, because the tablet reads them positionally.

/// SQL statements a backend uses to discover and monitor its schema.
#[derive(Debug, Clone)]
pub struct Introspection {
    /// `(table_name, table_type)` for one schema. `table_type` is `BASE TABLE`
    /// or `VIEW`.
    pub list_tables: &'static str,
    /// `(table_name, column_name, ordinal, native_type, is_nullable, default, is_auto_increment)`
    pub list_columns: &'static str,
    /// `(table_name, index_name, column_name, ordinal, is_unique, is_primary)`
    pub list_indexes: &'static str,
    /// `(table_name, constraint_name, column_name, referenced_table, referenced_column)`
    pub list_foreign_keys: &'static str,
    /// Returns the current schema/database name as a single value.
    pub current_schema: &'static str,
    /// Cheapest possible round trip, for health checks.
    pub ping: &'static str,
    /// Human-readable engine version string.
    pub server_version: &'static str,
    /// A monotonically increasing position in the engine's change stream, used to
    /// measure replication lag and to anchor VReplication. `None` when the engine
    /// exposes no such thing.
    pub replication_position: Option<&'static str>,
}
