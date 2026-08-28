//! Translating a schema between engines.
//!
//! Routing a `SELECT` across engines is a matter of syntax. Routing a
//! `CREATE TABLE` is a matter of type systems, which disagree about far more:
//! whether unsigned exists, how a generated key is declared, what a blob is
//! called, whether JSON is a type at all.

use vituss_core::{BindVars, SqlType};
use vituss_dialect::{get, render::render_for, ColumnType};

fn translate(sql: &str, from: &str, to: &str) -> (String, Vec<String>) {
    let source = get(from).unwrap();
    let target = get(to).unwrap();
    let stmt = source.parse_one(sql).expect("parse");
    let r = render_for(&stmt, &BindVars::new(), source.as_ref(), target.as_ref()).expect("render");
    (r.sql, r.warnings)
}

const MYSQL_TABLE: &str = "CREATE TABLE user (\
    user_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, \
    email VARCHAR(128), \
    balance DECIMAL(12,2), \
    payload JSON, \
    avatar BLOB, \
    created DATETIME, \
    PRIMARY KEY (user_id))";

#[test]
fn mysql_ddl_becomes_idiomatic_postgres() {
    let (sql, warnings) = translate(MYSQL_TABLE, "mysql", "postgres");

    // A generated key is part of the type in PostgreSQL, not a column option.
    assert!(sql.contains("BIGSERIAL"), "{sql}");
    assert!(!sql.contains("AUTO_INCREMENT"), "{sql}");
    assert!(!sql.contains("UNSIGNED"), "{sql}");

    assert!(sql.contains("NUMERIC(12, 2)"), "{sql}");
    assert!(sql.contains("JSONB"), "{sql}");
    assert!(sql.contains("BYTEA"), "{sql}");
    assert!(sql.contains("TIMESTAMP"), "{sql}");
    assert!(sql.contains("VARCHAR(128)"), "{sql}");

    // A generated key counts up from 1, so losing the unsignedness costs nothing
    // and is not worth warning about.
    assert!(warnings.is_empty(), "{warnings:?}");
}

#[test]
fn mysql_ddl_becomes_idiomatic_sql_server() {
    let (sql, _) = translate(MYSQL_TABLE, "mysql", "mssql");

    assert!(sql.contains("BIGINT") && sql.contains("IDENTITY(1,1)"), "{sql}");
    // N-prefixed types hold what a utf8mb4 column held.
    assert!(sql.contains("NVARCHAR(128)"), "{sql}");
    // No JSON type, and no unbounded VARCHAR either.
    assert!(sql.contains("NVARCHAR(MAX)"), "{sql}");
    assert!(sql.contains("VARBINARY(MAX)"), "{sql}");
    assert!(sql.contains("DATETIME2"), "{sql}");
}

#[test]
fn mysql_ddl_collapses_onto_sqlite() {
    let (sql, warnings) = translate(MYSQL_TABLE, "mysql", "sqlite");

    assert!(sql.contains("user_id INTEGER"), "{sql}");
    assert!(sql.contains("email TEXT"), "{sql}");
    assert!(sql.contains("avatar BLOB"), "{sql}");

    // SQLite only auto-assigns for an inline `INTEGER PRIMARY KEY`; this table
    // declares its key separately, and saying nothing would leave the operator to
    // discover it at the first insert.
    assert!(
        warnings.iter().any(|w| w.contains("will not be auto-assigned")),
        "{warnings:?}"
    );
}

#[test]
fn postgres_ddl_becomes_idiomatic_mysql() {
    let sql = "CREATE TABLE account (\
        id BIGSERIAL PRIMARY KEY, \
        ref UUID, \
        doc JSONB, \
        blob_data BYTEA, \
        seen TIMESTAMPTZ, \
        note TEXT)";
    let (out, _) = translate(sql, "postgres", "mysql");

    assert!(out.contains("BIGINT") && out.contains("AUTO_INCREMENT"), "{out}");
    // MySQL has no UUID type; the canonical text form is what applications index.
    assert!(out.contains("CHAR(36)"), "{out}");
    assert!(out.contains("JSON"), "{out}");
    assert!(out.contains("BLOB"), "{out}");
    assert!(out.contains("TIMESTAMP"), "{out}");
    assert!(out.contains("TEXT"), "{out}");
}

#[test]
fn an_unsigned_column_that_is_not_a_key_is_widened_and_reported() {
    let sql = "CREATE TABLE t (views INT UNSIGNED NOT NULL)";

    let (out, warnings) = translate(sql, "mysql", "postgres");
    // An unsigned 32-bit value does not fit in INTEGER.
    assert!(out.contains("BIGINT"), "{out}");
    assert!(
        warnings.iter().any(|w| w.contains("no unsigned integers")),
        "{warnings:?}"
    );

    // Staying on MySQL changes nothing and warns about nothing.
    let (same, none) = translate(sql, "mysql", "mysql");
    assert!(same.contains("UNSIGNED"), "{same}");
    assert!(none.is_empty());
}

#[test]
fn engine_specific_column_options_are_dropped_and_reported() {
    let sql = "CREATE TABLE t (\
        name VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, \
        touched TIMESTAMP ON UPDATE CURRENT_TIMESTAMP) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
    let (out, warnings) = translate(sql, "mysql", "postgres");

    assert!(!out.contains("CHARACTER SET"), "{out}");
    assert!(!out.to_uppercase().contains("ENGINE=INNODB"), "{out}");
    assert!(!out.contains("ON UPDATE"), "{out}");

    // Dropping `ON UPDATE CURRENT_TIMESTAMP` changes what the table does. The
    // person running the DDL is the one who can decide whether that matters.
    assert!(warnings.iter().any(|w| w.contains("ON UPDATE")), "{warnings:?}");
    assert!(warnings.iter().any(|w| w.contains("CHARACTER SET")), "{warnings:?}");
    assert!(warnings.iter().any(|w| w.contains("table options")), "{warnings:?}");
}

#[test]
fn a_type_no_one_else_has_is_passed_through_with_a_warning() {
    // PostgreSQL network types have no equivalent anywhere. Guessing would be
    // worse than passing them through and saying so.
    let (out, warnings) = translate("CREATE TABLE t (addr INET)", "postgres", "mysql");
    assert!(out.to_uppercase().contains("INET"), "{out}");
    assert!(
        warnings.iter().any(|w| w.contains("not a type Vituss can translate")),
        "{warnings:?}"
    );
}

#[test]
fn added_columns_are_translated_too() {
    let (out, _) = translate(
        "ALTER TABLE user ADD COLUMN score BIGINT UNSIGNED",
        "mysql",
        "postgres",
    );
    assert!(out.contains("NUMERIC(20)"), "{out}");
}

#[test]
fn declared_lengths_and_precision_survive_the_round_trip() {
    for engine in ["mysql", "postgres", "mssql"] {
        let d = get(engine).unwrap();
        let c = ColumnType {
            base: SqlType::Decimal,
            length: None,
            precision: Some(18),
            scale: Some(4),
            unsigned: false,
            auto_increment: false,
            source_text: "DECIMAL(18,4)".into(),
        };
        let rendered = d.render_column_type(&c);
        assert!(rendered.contains("18") && rendered.contains('4'), "{engine}: {rendered}");
    }
}

#[test]
fn dml_is_unaffected_by_the_ddl_translator() {
    // The translator only touches DDL; a SELECT going the same route must come
    // out with its identifiers intact and nothing else changed.
    let (out, warnings) = translate(
        "SELECT `id`, `name` FROM `user` WHERE `id` = 1",
        "mysql",
        "postgres",
    );
    assert!(out.contains(r#""id""#) && out.contains(r#""user""#), "{out}");
    assert!(warnings.is_empty(), "{warnings:?}");
}
