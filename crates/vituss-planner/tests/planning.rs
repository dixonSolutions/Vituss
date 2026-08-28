//! Routing decisions: what the planner does with each query shape.

use vituss_planner::{Plan, Planner, Primitive, RouteOpcode};
use vituss_vschema::VSchema;

const VSCHEMA: &str = r#"
{
  "keyspaces": {
    "commerce": {
      "sharded": true,
      "dialect": "mysql",
      "vindexes": {
        "hash": { "type": "hash" },
        "email_idx": {
          "type": "lookup_unique",
          "params": { "table": "email_lookup", "from": "email", "to": "keyspace_id" },
          "owner": "user"
        },
        "mc": { "type": "multicol", "params": { "column_count": "2" } }
      },
      "tables": {
        "user": {
          "column_vindexes": [
            { "column": "user_id", "name": "hash" },
            { "column": "email", "name": "email_idx" }
          ],
          "auto_increment": { "column": "user_id", "sequence": "main.user_seq" }
        },
        "corder": { "column_vindexes": [ { "column": "user_id", "name": "hash" } ] },
        "review":  { "column_vindexes": [ { "column": "review_id", "name": "hash" } ] },
        "basket":  { "column_vindexes": [ { "columns": ["region", "user_id"], "name": "mc" } ] },
        "country": { "type": "reference", "source": "main.country" }
      }
    },
    "main": {
      "sharded": false,
      "dialect": "mysql",
      "tables": { "user_seq": { "type": "sequence" }, "email_lookup": {}, "country": {}, "audit": {} }
    }
  }
}
"#;

fn vschema() -> VSchema {
    VSchema::from_json(VSCHEMA).expect("vschema")
}

fn plan(sql: &str) -> Plan {
    let vs = vschema();
    let dialect = vituss_dialect::get("mysql").unwrap();
    Planner::new(&vs, Some("commerce"), dialect).plan(sql).unwrap_or_else(|e| {
        panic!("planning {sql:?} failed: {}", e.message);
    })
}

/// Plan the same statement as a client speaking a different engine would send it.
fn plan_as(dialect: &str, sql: &str) -> Plan {
    let vs = vschema();
    let d = vituss_dialect::get(dialect).unwrap();
    Planner::new(&vs, Some("commerce"), d).plan(sql).unwrap_or_else(|e| {
        panic!("planning {sql:?} as {dialect} failed: {}", e.message);
    })
}

fn plan_err(sql: &str) -> String {
    let vs = vschema();
    let dialect = vituss_dialect::get("mysql").unwrap();
    match Planner::new(&vs, Some("commerce"), dialect).plan(sql) {
        Ok(p) => panic!("expected {sql:?} to be rejected, got:\n{}", p.explain()),
        Err(e) => e.message,
    }
}

fn opcode(p: &Plan) -> RouteOpcode {
    fn find(p: &Primitive) -> Option<RouteOpcode> {
        match p {
            Primitive::Route(r) => Some(r.opcode),
            Primitive::Dml(d) => Some(d.route.opcode),
            Primitive::Insert(i) => Some(i.opcode),
            Primitive::Limit(x) => find(&x.input),
            Primitive::Sort(x) => find(&x.input),
            Primitive::Distinct(x) => find(&x.input),
            Primitive::Aggregate(x) => find(&x.input),
            Primitive::Truncate { input, .. } => find(input),
            Primitive::Join(j) => find(&j.left),
            _ => None,
        }
    }
    find(&p.primitive).unwrap_or_else(|| panic!("no route in plan:\n{}", p.explain()))
}

fn route_sql(p: &Plan) -> String {
    fn find(p: &Primitive) -> Option<String> {
        match p {
            Primitive::Route(r) => Some(r.statement.to_string()),
            Primitive::Dml(d) => Some(d.route.statement.to_string()),
            Primitive::Insert(i) => Some(i.statement.to_string()),
            Primitive::Limit(x) => find(&x.input),
            Primitive::Sort(x) => find(&x.input),
            Primitive::Distinct(x) => find(&x.input),
            Primitive::Aggregate(x) => find(&x.input),
            Primitive::Truncate { input, .. } => find(input),
            _ => None,
        }
    }
    find(&p.primitive).expect("no route")
}

// ---------------------------------------------------------------------------
// SELECT routing
// ---------------------------------------------------------------------------

#[test]
fn equality_on_the_sharding_column_reaches_one_shard() {
    let p = plan("SELECT name FROM user WHERE user_id = 42");
    assert_eq!(opcode(&p), RouteOpcode::EqualUnique);
    // A single-shard query is sent untouched: the shard's own optimiser does the
    // rest, and Vituss adds nothing above the route.
    assert!(matches!(p.primitive, Primitive::Route(_)), "{}", p.explain());
}

#[test]
fn no_predicate_scatters() {
    assert_eq!(opcode(&plan("SELECT name FROM user")), RouteOpcode::Scatter);
}

#[test]
fn a_predicate_on_a_non_vindex_column_still_scatters() {
    assert_eq!(opcode(&plan("SELECT name FROM user WHERE name = 'ada'")), RouteOpcode::Scatter);
}

#[test]
fn in_lists_narrow_to_the_shards_that_hold_them() {
    assert_eq!(opcode(&plan("SELECT name FROM user WHERE user_id IN (1, 2, 3)")), RouteOpcode::In);
}

#[test]
fn an_or_of_equalities_is_still_routable() {
    assert_eq!(
        opcode(&plan("SELECT name FROM user WHERE user_id = 1 OR user_id = 2")),
        RouteOpcode::MultiEqual
    );
}

#[test]
fn an_or_with_one_unroutable_branch_scatters() {
    // Rows matching `name = 'x'` could be on any shard, so the whole query must
    // ask every shard — narrowing would silently lose rows.
    assert_eq!(
        opcode(&plan("SELECT name FROM user WHERE user_id = 1 OR name = 'x'")),
        RouteOpcode::Scatter
    );
}

#[test]
fn a_lookup_vindex_routes_a_query_on_a_secondary_column() {
    let p = plan("SELECT name FROM user WHERE email = 'ada@example.com'");
    assert_eq!(opcode(&p), RouteOpcode::EqualUnique);
    // The route carries the lookup vindex, so the executor knows it must consult
    // the lookup table before it can pick a shard.
    if let Primitive::Route(r) = &p.primitive {
        assert_eq!(r.vindex.as_ref().unwrap().vindex.kind(), "lookup_unique");
    } else {
        panic!("{}", p.explain());
    }
}

#[test]
fn the_cheapest_available_vindex_wins() {
    // Both columns are constrained; routing by `hash` avoids the lookup round trip.
    let p = plan("SELECT name FROM user WHERE user_id = 1 AND email = 'ada@example.com'");
    if let Primitive::Route(r) = &p.primitive {
        assert_eq!(r.vindex.as_ref().unwrap().vindex.kind(), "hash");
    } else {
        panic!("{}", p.explain());
    }
}

#[test]
fn an_unsharded_keyspace_needs_no_analysis() {
    let p = plan("SELECT * FROM main.audit WHERE anything = 1");
    assert_eq!(opcode(&p), RouteOpcode::Unsharded);
}

#[test]
fn a_reference_table_is_read_from_any_shard() {
    assert_eq!(opcode(&plan("SELECT * FROM country")), RouteOpcode::Reference);
}

#[test]
fn select_without_a_table_goes_to_any_shard() {
    assert_eq!(opcode(&plan("SELECT 1")), RouteOpcode::AnyShard);
}

// ---------------------------------------------------------------------------
// Cross-shard post-processing
// ---------------------------------------------------------------------------

#[test]
fn a_scattered_order_by_merges_shard_streams() {
    let p = plan("SELECT user_id, name FROM user ORDER BY name");
    // Each shard sorts its own rows...
    assert!(route_sql(&p).contains("ORDER BY name"), "{}", route_sql(&p));
    // ...and the gate merges the sorted streams rather than re-sorting everything.
    if let Primitive::Route(r) = &p.primitive {
        assert_eq!(r.order_by.len(), 1);
        assert_eq!(r.order_by[0].column, 1);
        assert!(!r.order_by[0].descending);
    } else {
        panic!("{}", p.explain());
    }
}

#[test]
fn a_sort_key_not_in_the_projection_is_added_then_trimmed() {
    let p = plan("SELECT name FROM user ORDER BY user_id");
    // The shards must return user_id for the merge to work...
    assert!(route_sql(&p).contains("user_id"), "{}", route_sql(&p));
    // ...but the client asked for one column, so it gets one column.
    match &p.primitive {
        Primitive::Truncate { columns, .. } => assert_eq!(*columns, 1),
        other => panic!("expected a truncate on top, got {other:?}"),
    }
}

#[test]
fn a_scattered_limit_is_widened_for_the_shards_and_applied_here() {
    let p = plan("SELECT name FROM user ORDER BY name LIMIT 10 OFFSET 20");
    // Any single shard could hold all 30 of the rows that matter.
    assert!(route_sql(&p).contains("LIMIT 30"), "{}", route_sql(&p));
    assert!(matches!(p.primitive, Primitive::Limit(_)), "{}", p.explain());
}

#[test]
fn a_single_shard_limit_is_left_entirely_to_the_shard() {
    let p = plan("SELECT name FROM user WHERE user_id = 1 ORDER BY name LIMIT 10 OFFSET 20");
    assert!(matches!(p.primitive, Primitive::Route(_)), "{}", p.explain());
    let sql = route_sql(&p);
    assert!(sql.contains("LIMIT 10") && sql.contains("OFFSET 20"), "{sql}");
}

#[test]
fn scattered_aggregates_are_combined_above_the_shards() {
    let p = plan("SELECT COUNT(*) FROM user");
    match &p.primitive {
        Primitive::Aggregate(a) => {
            assert_eq!(a.aggregates.len(), 1);
            assert_eq!(a.aggregates[0].func, vituss_planner::AggregateFunc::CountStar);
        }
        other => panic!("expected an aggregate, got {other:?}\n{}", p.explain()),
    }
}

#[test]
fn avg_is_split_into_sum_and_count_before_being_pushed_down() {
    let p = plan("SELECT AVG(score) FROM user");
    let sql = route_sql(&p);
    // The mean of per-shard means is not the mean, so the shards return the parts.
    assert!(sql.contains("SUM(score)"), "{sql}");
    assert!(sql.contains("COUNT(score)"), "{sql}");
    match &p.primitive {
        Primitive::Truncate { input, columns } => {
            assert_eq!(*columns, 1, "the extra COUNT column is not the client's");
            assert!(matches!(**input, Primitive::Aggregate(_)));
        }
        other => panic!("expected truncate over aggregate, got {other:?}\n{}", p.explain()),
    }
}

#[test]
fn group_by_makes_the_shards_sort_so_the_aggregate_can_stream() {
    let p = plan("SELECT country, COUNT(*) FROM user GROUP BY country");
    assert!(route_sql(&p).contains("ORDER BY country"), "{}", route_sql(&p));
    match &p.primitive {
        Primitive::Aggregate(a) => {
            assert!(a.ordered, "a sorted input should stream, not buffer");
            assert_eq!(a.group_by, vec![0]);
        }
        other => panic!("expected an aggregate, got {other:?}"),
    }
}

#[test]
fn count_distinct_across_shards_is_refused_with_a_reason() {
    let msg = plan_err("SELECT COUNT(DISTINCT country) FROM user");
    assert!(msg.contains("cannot be added"), "{msg}");
}

#[test]
fn a_single_shard_count_distinct_is_fine() {
    // Routed to one shard, the engine computes it exactly.
    let p = plan("SELECT COUNT(DISTINCT country) FROM user WHERE user_id = 1");
    assert_eq!(opcode(&p), RouteOpcode::EqualUnique);
    assert!(matches!(p.primitive, Primitive::Route(_)));
}

// ---------------------------------------------------------------------------
// Joins
// ---------------------------------------------------------------------------

#[test]
fn a_collocated_join_is_one_route() {
    // Both tables shard by `hash` on user_id, and the join equates those columns,
    // so matching rows are always on the same shard.
    let p = plan("SELECT u.name, o.price FROM user u JOIN corder o ON u.user_id = o.user_id");
    assert!(matches!(p.primitive, Primitive::Route(_)), "{}", p.explain());
    assert_eq!(opcode(&p), RouteOpcode::Scatter);
    // The whole join is pushed down; the shard's engine does it.
    assert!(route_sql(&p).contains("JOIN"), "{}", route_sql(&p));
}

#[test]
fn a_collocated_join_with_a_pinned_id_reaches_one_shard() {
    let p = plan(
        "SELECT u.name, o.price FROM user u JOIN corder o ON u.user_id = o.user_id \
         WHERE u.user_id = 42",
    );
    assert_eq!(opcode(&p), RouteOpcode::EqualUnique);
}

#[test]
fn a_non_collocated_join_becomes_a_nested_loop() {
    // `review` shards by review_id, so its rows are not next to the user's.
    let p = plan("SELECT u.name, r.body FROM user u JOIN review r ON u.user_id = r.user_id");
    match &p.primitive {
        Primitive::Join(j) => {
            assert_eq!(j.vars.len(), 1, "the right side is driven by a value from the left");
            assert_eq!(j.column_map, vec![0, -1]);
        }
        other => panic!("expected a join, got {other:?}\n{}", p.explain()),
    }
}

#[test]
fn a_cross_shard_join_with_nothing_to_join_on_is_refused() {
    let msg = plan_err("SELECT u.name, r.body FROM user u, review r");
    assert!(msg.contains("cartesian"), "{msg}");
}

// ---------------------------------------------------------------------------
// DML
// ---------------------------------------------------------------------------

#[test]
fn insert_rows_are_routed_individually() {
    let p = plan("INSERT INTO user (user_id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    match &p.primitive {
        Primitive::Insert(i) => {
            assert_eq!(i.rows.len(), 3);
            assert_eq!(i.vindex_column_positions, vec![0]);
            // Each row carries its own vindex input, because they may land on
            // different shards.
            assert_eq!(i.rows[0].vindex_values.len(), 1);
        }
        other => panic!("expected an insert, got {other:?}"),
    }
    assert!(p.is_dml);
}

#[test]
fn an_insert_that_owns_a_lookup_vindex_records_it() {
    let p = plan("INSERT INTO user (user_id, email) VALUES (1, 'a@b.c')");
    match &p.primitive {
        Primitive::Insert(i) => {
            assert_eq!(i.owned_vindexes.len(), 1);
            assert_eq!(i.owned_vindexes[0].vindex.kind(), "lookup_unique");
        }
        other => panic!("expected an insert, got {other:?}"),
    }
}

#[test]
fn an_insert_missing_the_sharding_column_is_refused_unless_a_sequence_supplies_it() {
    // `user` has a sequence for user_id, so omitting it is fine.
    let p = plan("INSERT INTO user (name) VALUES ('ada')");
    match &p.primitive {
        Primitive::Insert(i) => assert!(i.sequence.is_some()),
        other => panic!("expected an insert, got {other:?}"),
    }

    // `corder` has no sequence, so there is no way to know where the row goes.
    let msg = plan_err("INSERT INTO corder (price) VALUES (10)");
    assert!(msg.contains("sharding column"), "{msg}");
}

#[test]
fn an_insert_select_into_a_sharded_table_is_refused() {
    let msg = plan_err("INSERT INTO user (user_id, name) SELECT user_id, name FROM main.audit");
    assert!(msg.contains("routed one by one"), "{msg}");
}

#[test]
fn update_routes_on_the_sharding_column() {
    let p = plan("UPDATE user SET name = 'x' WHERE user_id = 7");
    assert_eq!(opcode(&p), RouteOpcode::EqualUnique);
    assert!(p.is_dml);
}

#[test]
fn changing_the_sharding_column_is_refused() {
    let msg = plan_err("UPDATE user SET user_id = 8 WHERE user_id = 7");
    assert!(msg.contains("would move the row to another shard"), "{msg}");
}

#[test]
fn an_update_touching_an_owned_lookup_reads_the_old_rows_first() {
    let p = plan("UPDATE user SET email = 'new@b.c' WHERE user_id = 7");
    match &p.primitive {
        Primitive::Dml(d) => {
            assert_eq!(d.owned_vindexes.len(), 1);
            let pre = d.pre_query.as_ref().expect("a pre-query is needed");
            // Locked, so the rows cannot move between reading and rewriting.
            assert!(pre.statement.to_string().contains("FOR UPDATE"));
        }
        other => panic!("expected a DML, got {other:?}"),
    }
}

#[test]
fn an_update_that_leaves_lookups_alone_needs_no_pre_query() {
    let p = plan("UPDATE user SET name = 'x' WHERE user_id = 7");
    match &p.primitive {
        Primitive::Dml(d) => assert!(d.pre_query.is_none()),
        other => panic!("expected a DML, got {other:?}"),
    }
}

#[test]
fn delete_scatters_when_it_has_to() {
    assert_eq!(opcode(&plan("DELETE FROM corder WHERE price > 100")), RouteOpcode::Scatter);
}

// ---------------------------------------------------------------------------
// Statement text sent to shards
// ---------------------------------------------------------------------------

#[test]
fn literals_are_replaced_by_bind_variables_before_the_query_leaves() {
    let p = plan("SELECT name FROM user WHERE user_id = 42 AND name = 'ada'");
    let sql = route_sql(&p);
    assert!(!sql.contains("ada"), "user data must not appear in the shard query: {sql}");
    assert!(sql.contains(":v"), "{sql}");
    if let Primitive::Route(r) = &p.primitive {
        assert_eq!(r.bind_vars.len(), 2);
    }
}

#[test]
fn keyspace_qualifiers_are_stripped_for_the_shard() {
    let p = plan("SELECT * FROM commerce.user WHERE user_id = 1");
    let sql = route_sql(&p);
    // The shard's database is the keyspace, so `commerce.user` would not resolve.
    assert!(!sql.contains("commerce."), "{sql}");
    assert!(sql.contains("user"), "{sql}");
}

// ---------------------------------------------------------------------------
// Session and DDL
// ---------------------------------------------------------------------------

#[test]
fn transaction_control_is_planned_as_session_state() {
    use vituss_planner::SessionOp;
    assert!(matches!(plan("BEGIN").primitive, Primitive::Session(SessionOp::Begin)));
    assert!(matches!(plan("COMMIT").primitive, Primitive::Session(SessionOp::Commit)));
    assert!(matches!(plan("ROLLBACK").primitive, Primitive::Session(SessionOp::Rollback)));
}

#[test]
fn ddl_is_broadcast_to_the_keyspace() {
    let p = plan("CREATE TABLE widget (id BIGINT PRIMARY KEY)");
    match &p.primitive {
        Primitive::Ddl(d) => assert_eq!(d.keyspace, "commerce"),
        other => panic!("expected DDL, got {other:?}"),
    }
}

#[test]
fn use_selects_a_keyspace_and_rejects_an_unknown_one() {
    use vituss_planner::SessionOp;
    match plan("USE main").primitive {
        Primitive::Session(SessionOp::Use { keyspace, .. }) => assert_eq!(keyspace, "main"),
        other => panic!("expected USE, got {other:?}"),
    }
    assert!(plan_err("USE nosuchkeyspace").contains("unknown keyspace"));
}

#[test]
fn an_unknown_table_names_itself() {
    let msg = plan_err("SELECT * FROM widgets");
    assert!(msg.contains("widgets"), "{msg}");
}

#[test]
fn plans_explain_themselves() {
    let text = plan("SELECT user_id, name FROM user ORDER BY name LIMIT 5").explain();
    assert!(text.contains("Route(Scatter)"), "{text}");
    assert!(text.contains("Limit"), "{text}");
}

// ---------------------------------------------------------------------------
// Predicate shapes that differ by engine but mean the same thing.
//
// The dialect layer exists so that a query does not route differently depending
// on which client wrote it. These are the spellings that used to slip through.

#[test]
fn a_postgres_any_array_routes_like_a_mysql_in_list() {
    // `= ANY(ARRAY[...])` is what a PostgreSQL driver sends for an IN list.
    let pg = plan_as("postgres", "SELECT * FROM \"user\" WHERE user_id = ANY(ARRAY[1, 2, 3])");
    let my = plan("SELECT * FROM user WHERE user_id IN (1, 2, 3)");
    assert_eq!(opcode(&pg), RouteOpcode::In);
    assert_eq!(
        opcode(&pg),
        opcode(&my),
        "the same logical query must not route differently per client engine"
    );
}

#[test]
fn some_is_accepted_wherever_any_is() {
    let p = plan_as("postgres", "SELECT * FROM \"user\" WHERE user_id = SOME(ARRAY[7, 8])");
    assert_eq!(opcode(&p), RouteOpcode::In);
}

#[test]
fn an_any_array_over_a_non_vindex_column_still_scatters() {
    let p = plan_as("postgres", "SELECT * FROM \"user\" WHERE nickname = ANY(ARRAY['a', 'b'])");
    assert_eq!(opcode(&p), RouteOpcode::Scatter);
}

#[test]
fn an_inequality_against_any_is_not_an_in_list() {
    // `> ANY(...)` means "greater than at least one of", which pins nothing.
    let p = plan_as("postgres", "SELECT * FROM \"user\" WHERE user_id > ANY(ARRAY[1, 2])");
    assert_eq!(opcode(&p), RouteOpcode::Scatter);
}

#[test]
fn a_row_value_pins_every_column_of_a_composite_vindex() {
    let p = plan("SELECT * FROM basket WHERE (region, user_id) IN ((1, 2))");
    assert_eq!(opcode(&p), RouteOpcode::EqualUnique);
}

#[test]
fn a_row_value_reads_the_same_as_the_equalities_written_out() {
    let rowvalue = plan("SELECT * FROM basket WHERE (region, user_id) IN ((1, 2))");
    let spelled = plan("SELECT * FROM basket WHERE region = 1 AND user_id = 2");
    assert_eq!(opcode(&rowvalue), opcode(&spelled));
}

#[test]
fn a_row_value_list_reaches_one_shard_per_tuple() {
    let p = plan("SELECT * FROM basket WHERE (region, user_id) IN ((1, 2), (3, 4))");
    assert_eq!(opcode(&p), RouteOpcode::MultiEqual);
}

#[test]
fn a_row_value_of_mismatched_arity_scatters() {
    // Nothing sensible to infer; better to ask everyone than to guess wrong.
    let p = plan("SELECT * FROM basket WHERE (region, user_id) IN ((1, 2), (3))");
    assert_eq!(opcode(&p), RouteOpcode::Scatter);
}

#[test]
fn a_row_value_naming_only_some_of_the_vindex_columns_does_not_pin_it() {
    let p = plan("SELECT * FROM basket WHERE (region) IN ((1), (2))");
    assert!(matches!(opcode(&p), RouteOpcode::Scatter | RouteOpcode::Range));
}
