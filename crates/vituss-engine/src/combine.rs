//! Putting shard results back together.
//!
//! Everything here is work the shards could not finish on their own. Each
//! function is deliberately narrow, because the planner has already decided
//! exactly which of them are needed — running an aggregate over rows that a
//! single shard already aggregated would be both slower and wrong.

use std::collections::BTreeMap;

use vituss_core::{Error, QueryResult, Result, Row, Value};
use vituss_planner::{AggregateExpr, AggregateFunc, OrderBy};

/// Merge already-sorted per-shard results into one sorted result.
///
/// Each shard sorted its own rows, so this is a k-way merge rather than a sort:
/// linear in the number of rows, and it never has to hold more than one row per
/// shard in memory beyond the output.
pub fn merge_sorted(results: Vec<QueryResult>, order_by: &[OrderBy]) -> QueryResult {
    if order_by.is_empty() {
        return concat(results);
    }

    let mut out = QueryResult::default();
    for r in &results {
        if !r.fields.is_empty() {
            out.fields = r.fields.clone();
            break;
        }
    }
    for r in &results {
        out.rows_affected += r.rows_affected;
        out.warnings.extend(r.warnings.clone());
    }

    let mut cursors: Vec<usize> = vec![0; results.len()];
    let total: usize = results.iter().map(|r| r.rows.len()).sum();
    out.rows.reserve(total);

    loop {
        let mut best: Option<usize> = None;
        for (i, r) in results.iter().enumerate() {
            if cursors[i] >= r.rows.len() {
                continue;
            }
            best = match best {
                None => Some(i),
                Some(b) => {
                    let cmp = compare_rows(&r.rows[cursors[i]], &results[b].rows[cursors[b]], order_by);
                    if cmp == std::cmp::Ordering::Less {
                        Some(i)
                    } else {
                        Some(b)
                    }
                }
            };
        }
        match best {
            None => break,
            Some(i) => {
                out.rows.push(results[i].rows[cursors[i]].clone());
                cursors[i] += 1;
            }
        }
    }
    out
}

/// Concatenate results without ordering.
pub fn concat(results: Vec<QueryResult>) -> QueryResult {
    let mut out = QueryResult::default();
    for r in results {
        out.append(r);
    }
    out
}

/// Sort rows in place. Used when the shards could not sort for us — after a
/// cross-shard join, where the rows are produced in join order.
pub fn sort(result: &mut QueryResult, order_by: &[OrderBy], limit: Option<usize>) {
    result.rows.sort_by(|a, b| compare_rows(a, b, order_by));
    if let Some(n) = limit {
        result.rows.truncate(n);
    }
}

fn compare_rows(a: &Row, b: &Row, order_by: &[OrderBy]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for ob in order_by {
        let (Some(x), Some(y)) = (a.get(ob.column), b.get(ob.column)) else { continue };
        // NULL ordering is a per-query choice, not a per-engine one: the planner
        // recorded what the query asked for, including the defaults each engine
        // would have applied.
        let ord = match (x.is_null(), y.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if ob.nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if ob.nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                let c = x.compare(y);
                if ob.descending {
                    c.reverse()
                } else {
                    c
                }
            }
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Drop duplicate rows, comparing only `columns` (all of them when empty).
pub fn distinct(result: &mut QueryResult, columns: &[usize]) {
    let mut seen: std::collections::HashSet<Vec<Vec<u8>>> = std::collections::HashSet::new();
    result.rows.retain(|row| {
        let key: Vec<Vec<u8>> = if columns.is_empty() {
            row.iter().map(Value::to_vindex_bytes).collect()
        } else {
            columns
                .iter()
                .filter_map(|i| row.get(*i))
                .map(Value::to_vindex_bytes)
                .collect()
        };
        seen.insert(key)
    });
}

/// Apply OFFSET then LIMIT.
pub fn limit(result: &mut QueryResult, count: Option<usize>, offset: Option<usize>) {
    if let Some(off) = offset {
        if off >= result.rows.len() {
            result.rows.clear();
        } else {
            result.rows.drain(..off);
        }
    }
    if let Some(n) = count {
        result.rows.truncate(n);
    }
}

/// Combine partial aggregates from several shards.
///
/// The shards each computed an aggregate over their own rows; this folds those
/// partials into the real answer. It works because the planner only pushed down
/// aggregates that *are* foldable — which is exactly why it rewrote `AVG` into
/// `SUM` and `COUNT` first, and why it refuses `COUNT(DISTINCT ...)`.
pub fn aggregate(
    input: QueryResult,
    aggregates: &[AggregateExpr],
    group_by: &[usize],
    ordered: bool,
) -> Result<QueryResult> {
    let mut out = QueryResult { fields: input.fields.clone(), ..Default::default() };

    if group_by.is_empty() {
        // One group covering everything. An aggregate over no rows still has an
        // answer — COUNT is 0, MIN is NULL — so a row is produced either way.
        let row = fold_group(&input.rows, aggregates, input.rows.first())?;
        out.rows.push(row);
        return Ok(out);
    }

    let key_of = |row: &Row| -> Vec<Vec<u8>> {
        group_by
            .iter()
            .map(|i| row.get(*i).map(Value::to_vindex_bytes).unwrap_or_default())
            .collect()
    };

    if ordered {
        // The shards returned rows sorted by the group key, so each group is a
        // contiguous run and nothing has to be buffered beyond it.
        let mut start = 0usize;
        while start < input.rows.len() {
            let key = key_of(&input.rows[start]);
            let mut end = start + 1;
            while end < input.rows.len() && key_of(&input.rows[end]) == key {
                end += 1;
            }
            let slice = &input.rows[start..end];
            out.rows.push(fold_group(slice, aggregates, slice.first())?);
            start = end;
        }
    } else {
        let mut groups: BTreeMap<Vec<Vec<u8>>, Vec<Row>> = BTreeMap::new();
        for row in &input.rows {
            groups.entry(key_of(row)).or_default().push(row.clone());
        }
        for (_, rows) in groups {
            out.rows.push(fold_group(&rows, aggregates, rows.first())?);
        }
    }

    Ok(out)
}

fn fold_group(rows: &[Row], aggregates: &[AggregateExpr], template: Option<&Row>) -> Result<Row> {
    let width = template.map(Vec::len).unwrap_or_else(|| {
        aggregates.iter().map(|a| a.column + 1).max().unwrap_or(0)
    });
    let mut out: Row = template.cloned().unwrap_or_else(|| vec![Value::Null; width]);

    for agg in aggregates {
        let column = agg.column;
        let partials = rows.iter().filter_map(|r| r.get(column)).filter(|v| !v.is_null());

        let value = match agg.func {
            AggregateFunc::Count | AggregateFunc::CountStar | AggregateFunc::Sum => {
                let mut acc: Option<Value> = None;
                for v in partials {
                    acc = Some(match acc {
                        None => v.clone(),
                        Some(a) => add(&a, v)?,
                    });
                }
                // SUM over no rows is NULL; COUNT over no rows is 0.
                acc.unwrap_or(match agg.func {
                    AggregateFunc::Sum => Value::Null,
                    _ => Value::Int(0),
                })
            }
            AggregateFunc::Min => partials
                .min_by(|a, b| a.compare(b))
                .cloned()
                .unwrap_or(Value::Null),
            AggregateFunc::Max => partials
                .max_by(|a, b| a.compare(b))
                .cloned()
                .unwrap_or(Value::Null),
            AggregateFunc::Avg => {
                let count_col = agg.count_column.ok_or_else(|| {
                    Error::internal("AVG was planned without the COUNT column it needs")
                })?;
                let mut sum: Option<Value> = None;
                let mut count: u64 = 0;
                for r in rows {
                    if let Some(v) = r.get(column).filter(|v| !v.is_null()) {
                        sum = Some(match sum {
                            None => v.clone(),
                            Some(a) => add(&a, v)?,
                        });
                    }
                    count += r.get(count_col).and_then(Value::as_uint).unwrap_or(0);
                }
                match (sum, count) {
                    // AVG over no rows is NULL, not a division by zero.
                    (_, 0) | (None, _) => Value::Null,
                    (Some(s), c) => divide(&s, c)?,
                }
            }
        };
        if column < out.len() {
            out[column] = value;
        }
    }
    Ok(out)
}

/// Add two partial aggregate values, widening as needed.
fn add(a: &Value, b: &Value) -> Result<Value> {
    use rust_decimal::Decimal;
    Ok(match (a, b) {
        (Value::Int(x), Value::Int(y)) => match x.checked_add(*y) {
            Some(v) => Value::Int(v),
            // A sum that overflows i64 becomes a decimal rather than wrapping:
            // a wrong total is worse than a slower one.
            None => Value::Decimal(Decimal::from(*x) + Decimal::from(*y)),
        },
        (Value::Uint(x), Value::Uint(y)) => match x.checked_add(*y) {
            Some(v) => Value::Uint(v),
            None => Value::Decimal(Decimal::from(*x) + Decimal::from(*y)),
        },
        (Value::Decimal(x), Value::Decimal(y)) => Value::Decimal(x + y),
        (Value::Decimal(x), other) | (other, Value::Decimal(x)) => {
            let y = decimal_of(other)?;
            Value::Decimal(x + y)
        }
        _ => {
            let (x, y) = (
                a.as_f64().ok_or_else(|| non_numeric(a))?,
                b.as_f64().ok_or_else(|| non_numeric(b))?,
            );
            Value::Float(x + y)
        }
    })
}

fn divide(sum: &Value, count: u64) -> Result<Value> {
    use rust_decimal::Decimal;
    if count == 0 {
        return Ok(Value::Null);
    }
    Ok(match sum {
        // Kept exact where the inputs were exact: an average of money must not
        // acquire a floating-point tail.
        Value::Decimal(d) => Value::Decimal(d / Decimal::from(count)),
        Value::Int(i) => Value::Decimal(Decimal::from(*i) / Decimal::from(count)),
        Value::Uint(u) => Value::Decimal(Decimal::from(*u) / Decimal::from(count)),
        other => Value::Float(other.as_f64().ok_or_else(|| non_numeric(other))? / count as f64),
    })
}

fn decimal_of(v: &Value) -> Result<rust_decimal::Decimal> {
    use std::str::FromStr;
    rust_decimal::Decimal::from_str(&v.to_string()).map_err(|_| non_numeric(v))
}

fn non_numeric(v: &Value) -> Error {
    Error::invalid(format!("cannot combine aggregate partials: {v} is not numeric"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use vituss_core::{Field, SqlType};

    fn result(rows: Vec<Vec<Value>>) -> QueryResult {
        QueryResult::from_rows(vec![Field::new("a", SqlType::Int64), Field::new("b", SqlType::Int64)], rows)
    }

    fn asc(col: usize) -> OrderBy {
        OrderBy { column: col, descending: false, nulls_first: true }
    }

    #[test]
    fn merging_sorted_shards_keeps_the_order() {
        let a = result(vec![vec![Value::Int(1), Value::Int(0)], vec![Value::Int(4), Value::Int(0)]]);
        let b = result(vec![vec![Value::Int(2), Value::Int(0)], vec![Value::Int(3), Value::Int(0)]]);
        let merged = merge_sorted(vec![a, b], &[asc(0)]);
        let got: Vec<i64> = merged.rows.iter().map(|r| r[0].as_int().unwrap()).collect();
        assert_eq!(got, vec![1, 2, 3, 4]);
    }

    #[test]
    fn nulls_sort_where_the_query_asked() {
        let rows = result(vec![
            vec![Value::Int(2), Value::Null],
            vec![Value::Null, Value::Null],
            vec![Value::Int(1), Value::Null],
        ]);
        let mut first = rows.clone();
        sort(&mut first, &[OrderBy { column: 0, descending: false, nulls_first: true }], None);
        assert!(first.rows[0][0].is_null());

        let mut last = rows;
        sort(&mut last, &[OrderBy { column: 0, descending: false, nulls_first: false }], None);
        assert!(last.rows[2][0].is_null());
    }

    #[test]
    fn counts_from_several_shards_are_added() {
        let input = result(vec![vec![Value::Int(3), Value::Null], vec![Value::Int(4), Value::Null]]);
        let aggs = vec![AggregateExpr {
            func: AggregateFunc::CountStar,
            column: 0,
            count_column: None,
            distinct: false,
            alias: "c".into(),
        }];
        let out = aggregate(input, &aggs, &[], false).unwrap();
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0][0], Value::Int(7));
    }

    #[test]
    fn average_is_recomputed_from_sums_and_counts_not_averaged_again() {
        // Shard A: sum 10 over 2 rows. Shard B: sum 30 over 6 rows.
        // The true mean is 40/8 = 5, not (5 + 5)/2 by coincidence — use uneven
        // values so a naive mean-of-means would be wrong.
        let input = result(vec![
            vec![Value::Int(10), Value::Int(2)],
            vec![Value::Int(90), Value::Int(6)],
        ]);
        let aggs = vec![AggregateExpr {
            func: AggregateFunc::Avg,
            column: 0,
            count_column: Some(1),
            distinct: false,
            alias: "avg".into(),
        }];
        let out = aggregate(input, &aggs, &[], false).unwrap();
        // (10 + 90) / (2 + 6) = 12.5. Mean-of-means would have given (5 + 15)/2 = 10.
        assert_eq!(out.rows[0][0].as_f64().unwrap(), 12.5);
    }

    #[test]
    fn grouping_folds_each_key_separately() {
        let input = result(vec![
            vec![Value::Text("gb".into()), Value::Int(2)],
            vec![Value::Text("gb".into()), Value::Int(3)],
            vec![Value::Text("fr".into()), Value::Int(5)],
        ]);
        let aggs = vec![AggregateExpr {
            func: AggregateFunc::Sum,
            column: 1,
            count_column: None,
            distinct: false,
            alias: "s".into(),
        }];
        let out = aggregate(input, &aggs, &[0], false).unwrap();
        assert_eq!(out.rows.len(), 2);
        let totals: std::collections::BTreeMap<String, i64> = out
            .rows
            .iter()
            .map(|r| (r[0].to_string(), r[1].as_int().unwrap()))
            .collect();
        assert_eq!(totals["gb"], 5);
        assert_eq!(totals["fr"], 5);
    }

    #[test]
    fn a_sum_that_overflows_widens_instead_of_wrapping() {
        let input = result(vec![
            vec![Value::Int(i64::MAX), Value::Null],
            vec![Value::Int(1), Value::Null],
        ]);
        let aggs = vec![AggregateExpr {
            func: AggregateFunc::Sum,
            column: 0,
            count_column: None,
            distinct: false,
            alias: "s".into(),
        }];
        let out = aggregate(input, &aggs, &[], false).unwrap();
        assert!(matches!(out.rows[0][0], Value::Decimal(_)), "{:?}", out.rows[0][0]);
    }

    #[test]
    fn limit_and_offset_apply_in_that_order() {
        let mut r = result((0..10).map(|i| vec![Value::Int(i), Value::Null]).collect());
        limit(&mut r, Some(3), Some(5));
        let got: Vec<i64> = r.rows.iter().map(|x| x[0].as_int().unwrap()).collect();
        assert_eq!(got, vec![5, 6, 7]);
    }

    #[test]
    fn distinct_keeps_the_first_of_each_duplicate() {
        let mut r = result(vec![
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(1), Value::Int(2)],
            vec![Value::Int(2), Value::Int(3)],
        ]);
        distinct(&mut r, &[0]);
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][1], Value::Int(1));
    }
}
