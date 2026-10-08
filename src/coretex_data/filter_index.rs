//! B5 — metadata inverted index: turn a filter into a candidate id set
//! *before* any distance is computed.
//!
//! Design rules:
//!
//! 1. **Superset only.** [`FilterIndex::scan`] may return ids that do not
//!    match the filter, but never omits one that does. Callers still run
//!    [`super::DataManager::matches_filter`] on every candidate — the index
//!    buys scale, the linear predicate keeps exactness. Shapes whose
//!    superset reasoning runs backwards (`$not`) fall back to
//!    [`IndexScan::All`], i.e. the full scan the code always had.
//! 2. **Exact when it is cheap:** equality, `$in`, single-key `$ne` and
//!    `$exists` resolve to set lookups or set differences — never a scan.
//! 3. **Validated against `data_version`:** the caller holds the data read
//!    lock while asking (see [`super::DataManager::index_scan`]), and
//!    writers bump the version under the write lock, so a cache hit always
//!    describes the snapshot being queried.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use super::VectorRecord;

/// What the index can say about one filter. See the module docs for the
/// superset rule.
#[derive(Debug)]
pub(crate) enum IndexScan {
    /// The filter cannot be narrowed (`$not`, an object with no field keys,
    /// a non-array `$and`/`$or` value). The caller does what it always did:
    /// a full pass with `matches_filter`.
    All,
    /// Only these ids *can* match: a superset of the true matches. The
    /// caller must still evaluate `matches_filter` on each candidate.
    Candidates(HashSet<String>),
}

/// Inverted index over one snapshot of a collection's record metadata:
/// `(field, canonical value) -> ids` equality postings plus a per-field
/// existence set.
pub(crate) struct FilterIndex {
    /// Every id in the snapshot — the universe for `$exists: false`.
    all: HashSet<String>,
    /// field -> canonical JSON value -> ids whose metadata field equals it.
    eq: HashMap<String, HashMap<String, HashSet<String>>>,
    /// field -> ids whose metadata contains the field at all.
    exists: HashMap<String, HashSet<String>>,
}

/// Canonical key for one metadata value. `Value`'s `Display` is its compact
/// JSON encoding, which is injective: two values share a key exactly when
/// they are the same JSON value — the relation `matches_filter` uses for
/// equality. The `1` vs `1.0` number boundary is covered by the superset
/// tests below, which check the scan against `matches_filter` itself.
fn value_key(value: &Value) -> String {
    value.to_string()
}

impl FilterIndex {
    pub(crate) fn build(records: &HashMap<String, VectorRecord>) -> Self {
        let mut index = FilterIndex {
            all: HashSet::with_capacity(records.len()),
            eq: HashMap::new(),
            exists: HashMap::new(),
        };
        for (id, record) in records {
            index.all.insert(id.clone());
            // Non-object metadata has no fields; matches_filter sees every
            // field as missing there, and `all - exists` reproduces that.
            if let Some(fields) = record.metadata.as_object() {
                for (field, value) in fields {
                    index
                        .exists
                        .entry(field.clone())
                        .or_default()
                        .insert(id.clone());
                    index
                        .eq
                        .entry(field.clone())
                        .or_default()
                        .entry(value_key(value))
                        .or_default()
                        .insert(id.clone());
                }
            }
        }
        index
    }

    /// The id set that *might* match `filter` — a superset, never a subset.
    pub(crate) fn scan(&self, filter: &Value) -> IndexScan {
        match filter {
            Value::Object(obj) => {
                if obj.is_empty() {
                    return IndexScan::All;
                }
                // Short-circuit order mirrors matches_filter: `$and`, then
                // `$or`, then `$not` — each wins over sibling keys.
                if let Some(v) = obj.get("$and") {
                    return match v.as_array() {
                        Some(conds) => self.plan_and(conds),
                        // matches_filter treats a non-array `$and` as
                        // "matches everything".
                        None => IndexScan::All,
                    };
                }
                if let Some(v) = obj.get("$or") {
                    return match v.as_array() {
                        Some(conds) => self.plan_or(conds),
                        None => IndexScan::All,
                    };
                }
                if obj.contains_key("$not") {
                    // A complement of a superset would be a *subset* — the
                    // one direction that can lose matches. Fall back.
                    return IndexScan::All;
                }
                // Implicit AND over field keys; other top-level `$...` keys
                // are ignored, exactly as matches_filter ignores them.
                let mut acc: Option<HashSet<String>> = None;
                for (key, value) in obj {
                    if key.starts_with('$') {
                        continue;
                    }
                    let step = self.scan_field(key, value);
                    acc = Some(match acc {
                        None => step,
                        Some(prev) => prev.intersection(&step).cloned().collect(),
                    });
                }
                match acc {
                    Some(ids) => IndexScan::Candidates(ids),
                    None => IndexScan::All,
                }
            }
            // An array filter is "any child matches" — the same shape as $or.
            Value::Array(conds) => self.plan_or(conds),
            // Scalar filters match everything in matches_filter.
            _ => IndexScan::All,
        }
    }

    fn plan_and(&self, conds: &[Value]) -> IndexScan {
        let mut acc: Option<HashSet<String>> = None;
        for cond in conds {
            match self.scan(cond) {
                // `All` = "cannot narrow" = at most everything; the
                // intersection keeps whatever the other conjuncts narrowed.
                IndexScan::All => {}
                IndexScan::Candidates(ids) => {
                    acc = Some(match acc {
                        None => ids,
                        Some(prev) => prev.intersection(&ids).cloned().collect(),
                    });
                }
            }
        }
        match acc {
            Some(ids) => IndexScan::Candidates(ids),
            None => IndexScan::All,
        }
    }

    fn plan_or(&self, conds: &[Value]) -> IndexScan {
        let mut acc: HashSet<String> = HashSet::new();
        for cond in conds {
            match self.scan(cond) {
                // One un-narrowable branch can contain anything — the union
                // would too, so fall back wholesale.
                IndexScan::All => return IndexScan::All,
                IndexScan::Candidates(ids) => acc.extend(ids),
            }
        }
        IndexScan::Candidates(acc)
    }

    /// Candidate ids for `field: cond`. Always a superset of the true
    /// matches; exact whenever the shape allows it.
    fn scan_field(&self, field: &str, cond: &Value) -> HashSet<String> {
        let operator_group = cond.as_object().is_some_and(|obj| {
            const OPS: [&str; 8] = [
                "$gt", "$gte", "$lt", "$lte", "$ne", "$in", "$exists", "$regex",
            ];
            obj.keys().any(|k| OPS.contains(&k.as_str()))
        });

        // Plain equality: matches_filter compares `meta_val != value` for
        // anything that is not an operator group (unknown `$...` keys fall
        // into this bucket there too).
        if !operator_group {
            return self
                .eq
                .get(field)
                .and_then(|by_value| by_value.get(&value_key(cond)))
                .cloned()
                .unwrap_or_default();
        }
        let obj = cond.as_object().expect("operator_group implies object");

        // `$exists: false` anywhere in the group rejects every record that
        // *has* the field, so the whole group reduces to the missing set.
        if obj.get("$exists") == Some(&Value::Bool(false)) {
            return self.missing(field);
        }

        // Single-operator groups where the operator pins the answer exactly.
        if obj.len() == 1 {
            if let Some(in_list) = obj.get("$in") {
                return match in_list.as_array() {
                    Some(values) => {
                        let mut ids = HashSet::new();
                        for value in values {
                            if let Some(hit) = self
                                .eq
                                .get(field)
                                .and_then(|by_value| by_value.get(&value_key(value)))
                            {
                                ids.extend(hit.iter().cloned());
                            }
                        }
                        ids
                    }
                    // A non-array `$in` never matches in matches_filter.
                    None => HashSet::new(),
                };
            }
            if let Some(ne_value) = obj.get("$ne") {
                // `$ne` only judges records that *have* the field.
                let mut ids = self.exists_set(field);
                if let Some(hit) = self
                    .eq
                    .get(field)
                    .and_then(|by_value| by_value.get(&value_key(ne_value)))
                {
                    ids.retain(|id| !hit.contains(id));
                }
                return ids;
            }
            if let Some(exists) = obj.get("$exists") {
                if let Some(want) = exists.as_bool() {
                    return if want {
                        self.exists_set(field)
                    } else {
                        self.missing(field)
                    };
                }
                // Non-boolean `$exists` judges nothing in matches_filter:
                // records with the field pass, records without do not.
            }
        }

        // Everything else — `$gt`/`$lt` ranges, `$regex`, mixed groups —
        // requires the field to exist; the existence set is the tightest
        // superset the index keeps for it.
        self.exists_set(field)
    }

    fn exists_set(&self, field: &str) -> HashSet<String> {
        self.exists.get(field).cloned().unwrap_or_default()
    }

    fn missing(&self, field: &str) -> HashSet<String> {
        match self.exists.get(field) {
            Some(have) => self.all.difference(have).cloned().collect(),
            None => self.all.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coretex_data::DataManager;
    use serde_json::json;

    fn records() -> HashMap<String, VectorRecord> {
        let mut records = HashMap::new();
        let mut add = |id: &str, meta: Value| {
            records.insert(
                id.to_string(),
                VectorRecord {
                    vector: vec![0.0; 4],
                    metadata: meta,
                },
            );
        };
        add("r1", json!({"group": "alpha", "score": 10, "tag": "x"}));
        add("r2", json!({"group": "beta", "score": 1.0, "tag": "y"}));
        add("r3", json!({"group": "alpha", "score": 50}));
        add(
            "r4",
            json!({"group": "beta", "score": 100, "deep": {"obj": 1}, "arr": [1, 2]}),
        );
        add("r5", json!({"other": true}));
        add("r6", json!(["array", "metadata"]));
        add("r7", json!(42));
        add("r8", json!({"group": "alpha", "score": 10, "extra": {"$gt": 9}}));
        records
    }

    /// The ground truth: run matches_filter itself over the snapshot.
    fn truth(records: &HashMap<String, VectorRecord>, filter: &Value) -> HashSet<String> {
        records
            .iter()
            .filter(|(_, r)| DataManager::matches_filter(&r.metadata, filter))
            .map(|(id, _)| id.clone())
            .collect()
    }

    fn sorted(set: &HashSet<String>) -> Vec<String> {
        let mut v: Vec<String> = set.iter().cloned().collect();
        v.sort();
        v
    }

    /// The invariant the whole design rests on: for every filter shape
    /// matches_filter understands, the scan never omits a real match.
    #[test]
    fn scan_is_always_a_superset_of_the_true_matches() {
        let records = records();
        let index = FilterIndex::build(&records);
        let filters = vec![
            json!({"group": "alpha"}),
            json!({"score": 1}),
            json!({"score": 1.0}),
            json!({"unknown_field": "x"}),
            json!({"group": {"$in": ["alpha", "beta"]}}),
            json!({"group": {"$in": "not-an-array"}}),
            json!({"score": {"$in": [10, 50]}}),
            json!({"score": {"$gt": 5}}),
            json!({"score": {"$gte": 10}}),
            json!({"score": {"$lt": 10}}),
            json!({"score": {"$lte": 1.0}}),
            json!({"score": {"$gt": "abc"}}),
            json!({"group": {"$ne": "alpha"}}),
            json!({"tag": {"$exists": true}}),
            json!({"tag": {"$exists": false}}),
            json!({"tag": {"$exists": false, "$gt": 1}}),
            json!({"tag": {"$exists": "yes"}}),
            json!({"tag": {"$regex": "^x"}}),
            json!({"extra": {"$gt": 9}}),
            json!({"extra": {"$foo": 1}}),
            json!({"deep": {"obj": 1}}),
            json!({"arr": [1, 2]}),
            json!({"group": "alpha", "score": {"$gte": 0}}),
            json!({"$and": [{"group": "alpha"}, {"score": {"$gt": 5}}]}),
            json!({"$and": [{"group": "alpha"}], "group": "beta"}),
            json!({"$and": "not-an-array"}),
            json!({"$or": [{"group": "alpha"}, {"score": {"$lt": 5}}]}),
            json!({"$or": [{"group": "alpha"}, {"tag": {"$regex": "^y"}}]}),
            json!({"$or": [{"group": "alpha"}, {"$not": {"score": 1}}]}),
            json!({"$not": {"group": "alpha"}}),
            json!([{"group": "alpha"}, {"other": true}]),
            json!(["never"]),
            json!({"$comment": "top-level dollar keys are ignored"}),
            json!({}),
            json!("scalar"),
            json!(42),
            json!(null),
            json!(true),
        ];
        for filter in &filters {
            let expected = truth(&records, filter);
            match index.scan(filter) {
                IndexScan::All => {}
                IndexScan::Candidates(candidates) => {
                    for id in &expected {
                        assert!(
                            candidates.contains(id),
                        "filter {} must not lose {} (candidates {:?}, truth {:?})",
                            filter,
                            id,
                            sorted(&candidates),
                            sorted(&expected)
                        );
                    }
                }
            }
        }
    }

    /// Shapes the index can answer exactly must return exactly the truth —
    /// that is what turns the pre-filter from "cheaper" into sublinear.
    #[test]
    fn tight_shapes_are_exact() {
        let records = records();
        let index = FilterIndex::build(&records);
        let cases: Vec<(Value, Vec<&str>)> = vec![
            (json!({"group": "alpha"}), vec!["r1", "r3", "r8"]),
            (json!({"score": {"$in": [10, 50]}}), vec!["r1", "r3", "r8"]),
            (json!({"group": {"$ne": "alpha"}}), vec!["r2", "r4"]),
            (json!({"tag": {"$exists": true}}), vec!["r1", "r2"]),
            (
                json!({"tag": {"$exists": false}}),
                vec!["r3", "r4", "r5", "r6", "r7", "r8"],
            ),
            (json!({"unknown_field": "x"}), vec![]),
            (json!({"group": {"$in": "not-an-array"}}), vec![]),
            (
                json!({"$and": [{"group": "alpha"}], "group": "beta"}),
                vec!["r1", "r3", "r8"],
            ),
            (
                json!([{"group": "alpha"}, {"other": true}]),
                vec!["r1", "r3", "r5", "r8"],
            ),
        ];
        for (filter, expected) in cases {
            match index.scan(&filter) {
                IndexScan::Candidates(ids) => {
                    let sorted_ids = sorted(&ids);
                    let got: Vec<&str> = sorted_ids.iter().map(String::as_str).collect();
                    assert_eq!(got, expected, "filter {}", filter);
                }
                IndexScan::All => panic!("filter {} should be narrowed", filter),
            }
        }
    }

    /// `$not` must fall back rather than return a subset.
    #[test]
    fn not_filter_falls_back_to_full_scan() {
        let index = FilterIndex::build(&records());
        match index.scan(&json!({"$not": {"group": "alpha"}})) {
            IndexScan::All => {}
            IndexScan::Candidates(_) => panic!("$not must not be narrowed from a superset"),
        }
    }

    /// The point of B5: a narrow equality filter yields a candidate set as
    /// small as the match set, not as large as the collection.
    #[test]
    fn narrow_filter_candidates_are_as_small_as_the_match_set() {
        let mut records = HashMap::new();
        for i in 0..1000 {
            let group = if i % 10 == 0 { "a" } else { "b" };
            records.insert(
                format!("v{i}"),
                VectorRecord {
                    vector: vec![i as f32; 4],
                    metadata: json!({ "group": group }),
                },
            );
        }
        let index = FilterIndex::build(&records);
        match index.scan(&json!({ "group": "a" })) {
            IndexScan::Candidates(ids) => {
                assert_eq!(ids.len(), 100, "one tenth of the collection");
            }
            IndexScan::All => panic!("equality filter must be narrowed"),
        }
    }
}
