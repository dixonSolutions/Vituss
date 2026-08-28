//! Byte-for-byte compatibility with Vitess's vindex output.
//!
//! These vectors are lifted from Vitess's own unit tests. They are the contract
//! that lets an existing Vitess keyspace be served by Vituss without moving data:
//! if any of these change, every row's shard changes with it.

use vituss_core::{ShardDestination, Value};
use vituss_vindex::{create, VindexParams};

fn ksid(d: &ShardDestination) -> Vec<u8> {
    match d {
        ShardDestination::KeyspaceId(k) => k.0.clone(),
        other => panic!("expected a single keyspace id, got {other}"),
    }
}

async fn map_all(kind: &str, values: Vec<Value>) -> Vec<ShardDestination> {
    let v = create(kind, "t", &VindexParams::new()).expect("create vindex");
    let rows: Vec<Vec<Value>> = values.into_iter().map(|v| vec![v]).collect();
    v.map(None, &rows).await.expect("map")
}

#[tokio::test]
async fn hash_matches_vitess() {
    let got = map_all(
        "hash",
        vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
            Value::Int(6),
            Value::Int(0),
            Value::Int(-1),
            Value::Uint(18446744073709551615),
            Value::Int(9223372036854775807),
            Value::Int(-9223372036854775808),
        ],
    )
    .await;

    let want: [&[u8]; 11] = [
        b"\x16k@\xb4J\xbaK\xd6",
        b"\x06\xe7\xea\x22\xce\x92\x70\x8f",
        b"N\xb1\x90\xc9\xa2\xfa\x16\x9c",
        b"\xd2\xfd\x88g\xd5\r-\xfe",
        b"p\xbb\x02<\x81\x0c\xa8z",
        b"\xf0\x98H\x0a\xc4\xc4\xbeq",
        b"\x8c\xa6M\xe9\xc1\xb1#\xa7",
        b"5UP\xb2\x15\x0e$Q",
        b"5UP\xb2\x15\x0e$Q",
        b"\xf7}H\xaa\xdd\xa1\xf1\xbb",
        b"\x95\xf8\xa5\xe5\xdd1\xd9\x00",
    ];
    for (i, (g, w)) in got.iter().zip(want).enumerate() {
        assert_eq!(ksid(g), w, "hash vector {i} diverged from Vitess");
    }
}

#[tokio::test]
async fn hash_maps_null_and_junk_to_no_shard() {
    // A value that cannot be a numeric sharding key must match nothing rather
    // than scatter — otherwise a typo silently becomes a full scan.
    let got = map_all("hash", vec![Value::Null, Value::Text("not-a-number".into())]).await;
    assert!(matches!(got[0], ShardDestination::None));
    assert!(matches!(got[1], ShardDestination::None));
}

#[tokio::test]
async fn hash_is_reversible() {
    let v = create("hash", "t", &VindexParams::new()).unwrap();
    let mapped = v.map(None, &[vec![Value::Int(42)]]).await.unwrap();
    let k = match &mapped[0] {
        ShardDestination::KeyspaceId(k) => k.clone(),
        other => panic!("{other}"),
    };
    let back = v.reverse_map(&[k]).unwrap().unwrap();
    assert_eq!(back, vec![Value::Uint(42)]);
}

#[tokio::test]
async fn xxhash_matches_vitess() {
    let cases: Vec<(Value, [u8; 8])> = vec![
        (Value::Text("test1".into()), [0xd0, 0x1a, 0xb7, 0xe4, 0xd6, 0x97, 0x8f, 0x0b]),
        (Value::Text("test2".into()), [0x87, 0xeb, 0x11, 0x71, 0x4c, 0x0a, 0x0e, 0x89]),
        (
            Value::Text("testaverylongvaluetomakesurethisworks".into()),
            [0x81, 0xd8, 0xc3, 0x8e, 0x0d, 0x85, 0x0e, 0x6a],
        ),
        (Value::Int(1), [0xd4, 0x64, 0x05, 0x36, 0x76, 0x12, 0xb4, 0xb7]),
        // NULL hashes the empty byte string.
        (Value::Null, [0x99, 0xe9, 0xd8, 0x51, 0x37, 0xdb, 0x46, 0xef]),
        (Value::Int(-1), [0xd8, 0xe2, 0xa6, 0xa7, 0xc8, 0xc7, 0x62, 0x3d]),
        (Value::Uint(18446744073709551615), [0x47, 0x7c, 0xfa, 0x8d, 0x6d, 0x8f, 0x1f, 0x8d]),
        (Value::Int(9223372036854775807), [0xb3, 0x7e, 0xb0, 0x1f, 0x7b, 0xff, 0xaf, 0xd8]),
        (Value::Int(-9223372036854775808), [0x10, 0x2c, 0x27, 0xdd, 0xb2, 0x6a, 0x60, 0x9e]),
    ];
    for (input, want) in cases {
        let got = map_all("xxhash", vec![input.clone()]).await;
        assert_eq!(ksid(&got[0]), want, "xxhash({input}) diverged from Vitess");
    }
}

#[tokio::test]
async fn numeric_and_reverse_bits_round_trip() {
    let num = create("numeric", "n", &VindexParams::new()).unwrap();
    let got = num.map(None, &[vec![Value::Int(1)]]).await.unwrap();
    assert_eq!(ksid(&got[0]), vec![0, 0, 0, 0, 0, 0, 0, 1]);

    let rev = create("reverse_bits", "r", &VindexParams::new()).unwrap();
    let got = rev.map(None, &[vec![Value::Int(1)]]).await.unwrap();
    // Reversing the bits of 1 sets the top bit.
    assert_eq!(ksid(&got[0]), vec![0x80, 0, 0, 0, 0, 0, 0, 0]);
    let back = rev
        .reverse_map(&[vituss_core::KeyspaceId(vec![0x80, 0, 0, 0, 0, 0, 0, 0])])
        .unwrap()
        .unwrap();
    assert_eq!(back, vec![Value::Uint(1)]);
}

#[tokio::test]
async fn binary_is_the_identity_and_supports_ranges() {
    let b = create("binary", "b", &VindexParams::new()).unwrap();
    let got = b.map(None, &[vec![Value::Text("abc".into())]]).await.unwrap();
    assert_eq!(ksid(&got[0]), b"abc");

    // Being order-preserving, a range predicate becomes a key range.
    match b.range_map(&Value::Text("a".into()), &Value::Text("b".into())).unwrap().unwrap() {
        ShardDestination::KeyRange(kr) => {
            assert_eq!(kr.start, b"a");
            assert_eq!(kr.end, b"b");
        }
        other => panic!("expected a key range, got {other}"),
    }
}

#[tokio::test]
async fn null_vindex_pins_everything_to_the_zero_keyspace_id() {
    let got = map_all("null", vec![Value::Int(1), Value::Text("x".into()), Value::Null]).await;
    for d in &got {
        assert_eq!(ksid(d), vec![0u8; 8]);
    }
}

#[tokio::test]
async fn multicol_narrows_to_a_range_on_a_partial_key() {
    let mut p = VindexParams::new();
    p.insert("column_count".into(), "2".into());
    let v = create("multicol", "mc", &p).unwrap();
    assert_eq!(v.column_count(), 2);
    assert!(v.accepts_partial_columns());

    // Both columns present: one exact keyspace id.
    let full = v.map(None, &[vec![Value::Int(1), Value::Int(2)]]).await.unwrap();
    assert!(matches!(full[0], ShardDestination::KeyspaceId(_)));

    // Only the leading column: a key range, not a scatter.
    let partial = v.map(None, &[vec![Value::Int(1)]]).await.unwrap();
    match &partial[0] {
        ShardDestination::KeyRange(kr) => assert_eq!(kr.start.len(), 4),
        other => panic!("expected a key range, got {other}"),
    }
}

#[tokio::test]
async fn unknown_vindex_parameters_are_rejected() {
    let mut p = VindexParams::new();
    p.insert("hahs".into(), "true".into()); // typo for a param that does not exist
    let err = create("hash", "h", &p).unwrap_err();
    assert!(err.message.contains("unknown parameter"), "{}", err.message);
}

#[tokio::test]
async fn lookup_vindexes_require_a_cursor() {
    let mut p = VindexParams::new();
    p.insert("table".into(), "user_lookup".into());
    p.insert("from".into(), "email".into());
    p.insert("to".into(), "keyspace_id".into());
    let v = create("lookup_unique", "email_idx", &p).unwrap();
    assert!(v.needs_cursor());
    assert!(v.is_unique());
    // Cost must exceed every functional vindex so the planner never prefers a
    // round trip over a computation.
    assert!(v.cost() > create("hash", "h", &VindexParams::new()).unwrap().cost());

    let err = v.map(None, &[vec![Value::Text("a@b.c".into())]]).await.unwrap_err();
    assert!(err.message.contains("needs to run a query"), "{}", err.message);
}

#[tokio::test]
async fn write_only_lookup_scatters_instead_of_lying() {
    let mut p = VindexParams::new();
    p.insert("table".into(), "user_lookup".into());
    p.insert("from".into(), "email".into());
    p.insert("to".into(), "keyspace_id".into());
    p.insert("write_only".into(), "true".into());
    let v = create("lookup_unique", "email_idx", &p).unwrap();
    let got = v.map(None, &[vec![Value::Text("a@b.c".into())]]).await.unwrap();
    assert!(matches!(got[0], ShardDestination::AllShards));
}
