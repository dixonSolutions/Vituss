//! Cross-engine DDL translation.
//!
//! Routing a `SELECT` between engines is a matter of syntax. Routing a
//! `CREATE TABLE` is a matter of *type systems*, which is a harder problem: the
//! four engines do not agree on what an integer is, whether unsigned exists, how
//! a generated key is declared, or what to call a blob.
//!
//! So a column type is decomposed into the neutral [`ColumnType`] — a base
//! [`SqlType`] plus the length, precision and flags every engine can express —
//! and re-rendered by the target dialect. Where the target has no equivalent, the
//! translation says so in a warning rather than emitting something that will fail
//! or, worse, silently store the wrong thing.

use sqlparser::ast::{
    ColumnDef, ColumnOption, ColumnOptionDef, DataType, Ident, ObjectName, ObjectNamePart, Statement,
};

use vituss_core::SqlType;

use crate::dialect::SqlDialect;

/// A column's type, decomposed into what every engine can express.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnType {
    pub base: SqlType,
    /// `VARCHAR(n)`, `BINARY(n)`. `None` means unbounded or engine default.
    pub length: Option<u64>,
    /// `DECIMAL(p, s)`.
    pub precision: Option<u64>,
    pub scale: Option<u64>,
    /// The source declared it unsigned. Only MySQL can honour this.
    pub unsigned: bool,
    /// The engine generates the value: `AUTO_INCREMENT`, `SERIAL`, `IDENTITY`.
    pub auto_increment: bool,
    /// The type as the source wrote it, for diagnostics and for the case where
    /// nothing better can be said.
    pub source_text: String,
}

impl ColumnType {
    /// A length that is unbounded — `VARCHAR` with no size, `TEXT`, `MAX`.
    pub fn is_unbounded(&self) -> bool {
        self.length.is_none()
    }

    /// True when the source type carried no meaning this layer understood, so a
    /// pass-through is the least-wrong option.
    pub fn is_opaque(&self) -> bool {
        matches!(self.base, SqlType::Unknown)
    }
}

/// Multi-word and vendor type names normalised to something
/// [`SqlDialect::map_native_type`] recognises.
///
/// Kept here rather than in each dialect because these are *cross-engine*
/// spellings: a PostgreSQL dialect should not have to know what SQL Server calls
/// a GUID in order to read a statement that was written for SQL Server.
fn normalise(head: &str) -> (String, bool) {
    let upper = head.trim().to_ascii_uppercase();
    let squashed = upper.split_whitespace().collect::<Vec<_>>().join(" ");
    let (name, auto) = match squashed.as_str() {
        "CHARACTER VARYING" | "CHAR VARYING" | "NVARCHAR" | "NATIONAL VARCHAR" => ("varchar", false),
        "NCHAR" | "NATIONAL CHAR" | "CHARACTER" => ("char", false),
        "DOUBLE PRECISION" => ("double", false),
        "TIMESTAMP WITH TIME ZONE" | "DATETIMEOFFSET" => ("timestamptz", false),
        "TIMESTAMP WITHOUT TIME ZONE" | "DATETIME2" | "SMALLDATETIME" => ("datetime", false),
        "TIME WITH TIME ZONE" => ("timetz", false),
        "UNIQUEIDENTIFIER" => ("uuid", false),
        "BYTEA" => ("blob", false),
        // PostgreSQL's serial types are an integer plus a sequence, spelled as
        // one word. Splitting them here is what lets the target express the same
        // thing however it prefers to.
        "SERIAL" | "SERIAL4" => ("int", true),
        "BIGSERIAL" | "SERIAL8" => ("bigint", true),
        "SMALLSERIAL" | "SERIAL2" => ("smallint", true),
        other => (other, false),
    };
    (name.to_ascii_lowercase(), auto)
}

/// Type names understood regardless of which dialect is reading them.
///
/// The last resort, after the source dialect has been asked about its own
/// spelling. It exists so that a `CREATE TABLE` written for SQL Server is still
/// understood when the *client* connected as MySQL — the parse succeeded, but the
/// MySQL dialect has never heard of `UNIQUEIDENTIFIER`.
fn neutral_type(name: &str) -> SqlType {
    match name {
        "bool" | "boolean" | "bit" => SqlType::Bool,
        "tinyint" => SqlType::Int8,
        "smallint" | "int2" => SqlType::Int16,
        "int" | "integer" | "int4" | "mediumint" => SqlType::Int32,
        "bigint" | "int8" => SqlType::Int64,
        "float" | "float4" | "real" => SqlType::Float32,
        "double" | "float8" => SqlType::Float64,
        "decimal" | "numeric" | "money" | "smallmoney" => SqlType::Decimal,
        "char" | "bpchar" | "character" => SqlType::Char,
        "varchar" => SqlType::VarChar,
        "text" | "tinytext" | "mediumtext" | "longtext" | "ntext" | "clob" | "citext" | "string" => {
            SqlType::Text
        }
        "binary" => SqlType::Binary,
        "varbinary" | "bytes" => SqlType::VarBinary,
        "blob" | "bytea" | "tinyblob" | "mediumblob" | "longblob" | "image" => SqlType::Blob,
        "date" => SqlType::Date,
        "time" | "timetz" => SqlType::Time,
        "datetime" | "timestamp" => SqlType::DateTime,
        "timestamptz" => SqlType::Timestamp,
        "json" | "jsonb" => SqlType::Json,
        "uuid" => SqlType::Uuid,
        _ => SqlType::Unknown,
    }
}

/// Decompose a source column type into the neutral form.
pub fn decompose(dialect: &dyn SqlDialect, dt: &DataType, options: &[ColumnOptionDef]) -> ColumnType {
    let source_text = dt.to_string();
    let upper = source_text.to_ascii_uppercase();
    let unsigned = upper.split_whitespace().any(|w| w.trim_end_matches(',') == "UNSIGNED");

    let (head, arg_text) = match source_text.split_once('(') {
        Some((h, rest)) => (h, rest.trim_end_matches(')')),
        None => (source_text.as_str(), ""),
    };
    // `VARCHAR(MAX)` and `TEXT` both come out as "no declared length", which is
    // the same thing as far as any other engine is concerned.
    let nums: Vec<u64> = arg_text
        .split(',')
        .filter_map(|s| s.trim().parse::<u64>().ok())
        .collect();

    let (name, serial) = normalise(head);
    // The source's own spelling first — a dialect knows its own type names better
    // than any shared table does. Only fall back for names it does not recognise,
    // which is how a statement written for one engine is understood by another.
    let mut base = dialect.map_native_type(head.trim());
    if base == SqlType::Unknown {
        base = dialect.map_native_type(&name);
    }
    if base == SqlType::Unknown {
        base = neutral_type(&name);
    }

    let (length, precision, scale) = if base == SqlType::Decimal {
        (None, nums.first().copied(), nums.get(1).copied())
    } else if base.is_text() || base.is_binary() {
        (nums.first().copied(), None, None)
    } else {
        (None, None, None)
    };

    ColumnType {
        base,
        length,
        precision,
        scale,
        unsigned,
        auto_increment: serial || has_auto_increment(options),
        source_text,
    }
}

/// Does this column's option list declare an engine-generated value?
///
/// Each engine spells it differently and sqlparser models them differently, so
/// all four shapes are checked.
pub fn has_auto_increment(options: &[ColumnOptionDef]) -> bool {
    options.iter().any(|o| match &o.option {
        // SQL Server `IDENTITY(1,1)`, Snowflake `AUTOINCREMENT`.
        ColumnOption::Identity(_) => true,
        // PostgreSQL `GENERATED BY DEFAULT AS IDENTITY` — an identity column has
        // no generation expression; a computed column does.
        ColumnOption::Generated { generation_expr, .. } => generation_expr.is_none(),
        // MySQL and SQLite spell it as a bare keyword.
        ColumnOption::DialectSpecific(tokens) => tokens.iter().any(|t| {
            matches!(t, sqlparser::tokenizer::Token::Word(w)
                if w.value.eq_ignore_ascii_case("AUTO_INCREMENT")
                    || w.value.eq_ignore_ascii_case("AUTOINCREMENT"))
        }),
        _ => false,
    })
}

/// Column options that mean something on one engine and are a syntax error on
/// the others.
fn is_engine_specific(option: &ColumnOption) -> Option<&'static str> {
    match option {
        ColumnOption::CharacterSet(_) => Some("CHARACTER SET"),
        ColumnOption::Collation(_) => Some("COLLATE"),
        ColumnOption::Comment(_) => Some("COMMENT"),
        ColumnOption::OnUpdate(_) => Some("ON UPDATE"),
        ColumnOption::Identity(_) => Some("IDENTITY"),
        ColumnOption::DialectSpecific(_) => Some("a dialect-specific option"),
        _ => None,
    }
}

/// Rewrite a DDL statement's column types for a different engine.
///
/// Returns what could not be carried across. Silence would be the wrong
/// behaviour here: a dropped `ON UPDATE CURRENT_TIMESTAMP` changes what the table
/// does, and the operator running the DDL is the person who can decide whether
/// that matters.
pub fn translate(stmt: &mut Statement, source: &dyn SqlDialect, target: &dyn SqlDialect) -> Vec<String> {
    let mut warnings = Vec::new();
    match stmt {
        Statement::CreateTable(create) => {
            for column in &mut create.columns {
                translate_column(column, source, target, &mut warnings);
            }
            // MySQL's `ENGINE=InnoDB DEFAULT CHARSET=utf8mb4` and friends.
            if !matches!(create.table_options, sqlparser::ast::CreateTableOptions::None) {
                warnings.push(format!(
                    "dropped table options specific to {}; {} has no equivalent",
                    source.name(),
                    target.name()
                ));
                create.table_options = sqlparser::ast::CreateTableOptions::None;
            }
            if create.comment.take().is_some() {
                warnings.push(format!("dropped the table COMMENT: {} does not accept one inline", target.name()));
            }
        }
        Statement::AlterTable(alter) => {
            for op in &mut alter.operations {
                if let sqlparser::ast::AlterTableOperation::AddColumn { column_def, .. } = op {
                    translate_column(column_def, source, target, &mut warnings);
                }
            }
        }
        _ => {}
    }
    warnings
}

fn translate_column(
    column: &mut ColumnDef,
    source: &dyn SqlDialect,
    target: &dyn SqlDialect,
    warnings: &mut Vec<String>,
) {
    let mut decomposed = decompose(source, &column.data_type, &column.options);

    // A generated key starts at 1 and counts up, so the source's unsignedness
    // buys nothing and would push the column into a wider, slower type on every
    // engine that lacks it.
    if decomposed.auto_increment {
        decomposed.unsigned = false;
        // Some dialects fold the unsignedness into the base type itself.
        if decomposed.base == SqlType::Uint64 {
            decomposed.base = SqlType::Int64;
        }
    }

    if decomposed.is_opaque() {
        // An engine-specific type — a PostgreSQL `inet`, an enum, an array. It is
        // passed through untouched, because guessing would be worse, but the
        // operator is told it will only work if the target happens to have it.
        warnings.push(format!(
            "column {}: {} is not a type Vituss can translate; it was passed through unchanged \
             and will only work if {} understands it",
            column.name, decomposed.source_text, target.name()
        ));
        return;
    }

    if decomposed.unsigned && !target.capabilities().supports_unsigned {
        warnings.push(format!(
            "column {}: {} has no unsigned integers, so {} was widened to hold the same range",
            column.name,
            target.name(),
            decomposed.source_text
        ));
    }

    let rendered = target.render_column_type(&decomposed);
    column.data_type = DataType::Custom(
        ObjectName(vec![ObjectNamePart::Identifier(Ident::new(rendered))]),
        Vec::new(),
    );

    // Drop options that belong to the source engine, then add the target's own
    // way of saying "generate this value".
    column.options.retain(|o| {
        let drop = is_engine_specific(&o.option).is_some();
        if drop {
            if let Some(what) = is_engine_specific(&o.option) {
                // AUTO_INCREMENT is re-expressed below, so it is not a loss.
                if !(decomposed.auto_increment && matches!(what, "IDENTITY" | "a dialect-specific option")) {
                    warnings.push(format!(
                        "column {}: dropped {what}, which {} cannot express",
                        column.name,
                        target.name()
                    ));
                }
            }
        }
        !drop
    });

    if decomposed.auto_increment && target.auto_increment_option(&decomposed).is_none() {
        // The target expresses it in the type (PostgreSQL's SERIAL) or gets it
        // implicitly. SQLite only gets it implicitly on a *column-level*
        // `INTEGER PRIMARY KEY`; a table-level PRIMARY KEY constraint does not
        // count, and the difference is silent at DDL time and obvious at the
        // first insert.
        let column_level_pk = column
            .options
            .iter()
            .any(|o| matches!(o.option, ColumnOption::PrimaryKey(_)));
        if !column_level_pk && !target.capabilities().auto_increment_in_type {
            warnings.push(format!(
                "column {}: {} only generates values for a column declared \
                 `INTEGER PRIMARY KEY` inline; this table declares its primary key separately, \
                 so {} will not be auto-assigned",
                column.name,
                target.name(),
                column.name
            ));
        }
    }

    // `None` means the target expresses it in the type itself (PostgreSQL's
    // SERIAL) or gets it implicitly (SQLite's INTEGER PRIMARY KEY).
    if let Some(text) = decomposed
        .auto_increment
        .then(|| target.auto_increment_option(&decomposed))
        .flatten()
    {
        column.options.push(ColumnOptionDef {
            name: None,
            option: ColumnOption::DialectSpecific(vec![sqlparser::tokenizer::Token::Word(
                sqlparser::tokenizer::Word {
                    value: text,
                    quote_style: None,
                    keyword: sqlparser::keywords::Keyword::NoKeyword,
                },
            )]),
        });
    }
}

// ---------------------------------------------------------------------------
// Helpers for dialect implementations
// ---------------------------------------------------------------------------

/// `NAME(n)`, falling back to `default` when the source declared no length.
pub fn sized(c: &ColumnType, name: &str, default: u64) -> String {
    format!("{name}({})", c.length.unwrap_or(default))
}

/// `NAME(n)` where an absent length means the engine's unbounded form.
pub fn sized_or(c: &ColumnType, name: &str, unbounded: &str) -> String {
    match c.length {
        Some(n) => format!("{name}({n})"),
        None => unbounded.to_string(),
    }
}

/// `DECIMAL(p, s)`, carrying whatever precision the source declared.
pub fn decimal(c: &ColumnType, name: &str) -> String {
    match (c.precision, c.scale) {
        (Some(p), Some(s)) => format!("{name}({p}, {s})"),
        (Some(p), None) => format!("{name}({p})"),
        // An undeclared precision means "whatever the engine defaults to", which
        // is what leaving the parentheses off asks for.
        _ => name.to_string(),
    }
}
