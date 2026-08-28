//! VSchema loading, resolution and validation.

use vituss_vschema::*;

/// A two-keyspace layout: `commerce` sharded by `hash`, `lookupks` unsharded and
/// holding the sequence and the lookup table.
const EXAMPLE: &str = r#"
{
  "keyspaces": {
    "commerce": {
      "sharded": true,
      "dialect": "postgres",
      "vindexes": {
        "hash": { "type": "hash" },
        "email_idx": {
          "type": "lookup_unique",
          "params": { "table": "email_lookup", "from": "email", "to": "keyspace_id" },
          "owner": "user"
        }
      },
      "tables": {
        "user": {
          "column_vindexes": [
            { "column": "user_id", "name": "hash" },
            { "column": "email", "name": "email_idx" }
          ],
          "auto_increment": { "column": "user_id", "sequence": "lookupks.user_seq" }
        },
        "corder": {
          "column_vindexes": [ { "column": "user_id", "name": "hash" } ]
        },
        "country": { "type": "reference", "source": "lookupks.country" }
      }
    },
    "lookupks": {
      "sharded": false,
      "dialect": "mysql",
      "tables": {
        "user_seq": { "type": "sequence" },
        "email_lookup": {},
        "country": {}
      }
    }
  },
  "routing_rules": {
    "commerce.legacy_orders": ["commerce.corder"]
  }
}
"#;

fn example() -> VSchema {
    VSchema::from_json(EXAMPLE).expect("build vschema")
}

#[test]
fn builds_and_resolves_tables() {
    let vs = example();
    assert_eq!(vs.keyspace_names(), vec!["commerce", "lookupks"]);

    let user = vs.find_table(Some("commerce"), "user").unwrap();
    assert!(user.sharded);
    let primary = user.primary_vindex.as_ref().expect("primary vindex");
    assert_eq!(primary.vindex.kind(), "hash");
    assert_eq!(primary.columns, vec!["user_id"]);

    // Unqualified resolution works when the name is unique cluster-wide.
    assert_eq!(vs.find_table(None, "corder").unwrap().keyspace, "commerce");
}

#[test]
fn keyspaces_carry_their_own_engine() {
    let vs = example();
    assert_eq!(vs.keyspace("commerce").unwrap().dialect, "postgres");
    assert_eq!(vs.keyspace("lookupks").unwrap().dialect, "mysql");
}

#[test]
fn cheaper_vindexes_are_preferred() {
    let vs = example();
    let user = vs.find_table(Some("commerce"), "user").unwrap();
    // `hash` costs less than a lookup, so it must come first and be primary.
    assert_eq!(user.column_vindexes[0].vindex.kind(), "hash");
    assert_eq!(user.column_vindexes[1].vindex.kind(), "lookup_unique");

    // With only the email constrained, the lookup is the only option.
    let by_email = user.best_vindex(&["email".to_string()]).unwrap();
    assert_eq!(by_email.vindex.kind(), "lookup_unique");

    // With both, the cheap one wins.
    let both = user
        .best_vindex(&["email".to_string(), "user_id".to_string()])
        .unwrap();
    assert_eq!(both.vindex.kind(), "hash");
}

#[test]
fn vindex_ownership_comes_from_the_vschema() {
    let vs = example();
    let user = vs.find_table(Some("commerce"), "user").unwrap();
    let owned: Vec<&str> = user.owned_vindexes().map(|cv| cv.vindex.kind()).collect();
    // `user` owns the email lookup, so writes to `user` maintain it.
    assert_eq!(owned, vec!["lookup_unique"]);
    // `corder` owns nothing.
    let corder = vs.find_table(Some("commerce"), "corder").unwrap();
    assert_eq!(corder.owned_vindexes().count(), 0);
}

#[test]
fn ambiguous_unqualified_names_are_rejected() {
    let vs = example();
    // `country` exists in both keyspaces.
    let err = vs.find_table(None, "country").unwrap_err();
    assert!(err.message.contains("exists in 2 keyspaces"), "{}", err.message);
    // Qualified, it resolves.
    assert!(vs.find_table(Some("commerce"), "country").unwrap().is_reference());
}

#[test]
fn routing_rules_redirect_a_table() {
    let vs = example();
    let t = vs.find_table(Some("commerce"), "legacy_orders").unwrap();
    assert_eq!(t.name, "corder");
}

#[test]
fn a_sharded_table_without_a_unique_vindex_is_refused() {
    let spec = r#"{"keyspaces":{"ks":{"sharded":true,"tables":{"t":{}}}}}"#;
    let err = VSchema::from_json(spec).unwrap_err();
    assert!(err.message.contains("needs a unique column_vindex"), "{}", err.message);
}

#[test]
fn a_sequence_in_a_sharded_keyspace_is_refused() {
    let spec = r#"{"keyspaces":{"ks":{"sharded":true,"vindexes":{"h":{"type":"hash"}},
                    "tables":{"s":{"type":"sequence"}}}}}"#;
    let err = VSchema::from_json(spec).unwrap_err();
    assert!(err.message.contains("cannot live in a sharded keyspace"), "{}", err.message);
}

#[test]
fn an_auto_increment_pointing_at_a_missing_sequence_is_refused() {
    let spec = r#"{"keyspaces":{"ks":{"sharded":true,"vindexes":{"h":{"type":"hash"}},
                    "tables":{"t":{"column_vindexes":[{"column":"id","name":"h"}],
                                   "auto_increment":{"column":"id","sequence":"nope.seq"}}}}}}"#;
    let err = VSchema::from_json(spec).unwrap_err();
    assert!(err.message.contains("not in the VSchema"), "{}", err.message);
}

#[test]
fn an_unknown_vindex_kind_names_what_is_available() {
    let spec = r#"{"keyspaces":{"ks":{"sharded":true,"vindexes":{"h":{"type":"sha256"}},
                    "tables":{"t":{"column_vindexes":[{"column":"id","name":"h"}]}}}}}"#;
    let err = VSchema::from_json(spec).unwrap_err();
    assert!(err.message.contains("unknown vindex type"), "{}", err.message);
    assert!(err.message.contains("xxhash"), "{}", err.message);
}

#[test]
fn an_unsharded_keyspace_must_not_declare_vindexes() {
    let spec = r#"{"keyspaces":{"ks":{"sharded":false,"vindexes":{"h":{"type":"hash"}},
                    "tables":{"t":{"column_vindexes":[{"column":"id","name":"h"}]}}}}}"#;
    let err = VSchema::from_json(spec).unwrap_err();
    assert!(err.message.contains("unsharded"), "{}", err.message);
}

#[test]
fn a_vindex_owned_by_a_missing_table_is_refused() {
    let spec = r#"{"keyspaces":{"ks":{"sharded":true,
      "vindexes":{"h":{"type":"hash"},
                  "l":{"type":"lookup_unique","owner":"ghost",
                       "params":{"table":"lk","from":"a","to":"b"}}},
      "tables":{"t":{"column_vindexes":[{"column":"id","name":"h"}]}}}}}"#;
    let err = VSchema::from_json(spec).unwrap_err();
    assert!(err.message.contains("ghost"), "{}", err.message);
}

#[test]
fn multi_column_vindex_arity_is_checked() {
    let spec = r#"{"keyspaces":{"ks":{"sharded":true,
      "vindexes":{"mc":{"type":"multicol","params":{"column_count":"2"}}},
      "tables":{"t":{"column_vindexes":[{"columns":["a","b"],"name":"mc"}]}}}}}"#;
    let vs = VSchema::from_json(spec).unwrap();
    let t = vs.find_table(Some("ks"), "t").unwrap();
    assert_eq!(t.primary_vindex.as_ref().unwrap().columns, vec!["a", "b"]);
}

#[test]
fn sequences_are_discoverable_for_validation_tooling() {
    let vs = example();
    let seqs = vs.sequences();
    assert_eq!(seqs.len(), 1);
    assert_eq!(seqs[0].to_string(), "lookupks.user_seq");
}
