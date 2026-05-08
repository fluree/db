//! Person + Company graph generator.
//!
//! Lifted from `insert_formats.rs`. Produces a deterministic linked-data
//! graph of `~10%` `Company` nodes and `~90%` `Person` nodes per
//! transaction, with `Company.ex:employees` and `Company.ex:customers`
//! refs to the persons in the same transaction.
//!
//! ## Determinism
//!
//! Output is byte-identical across runs for the same `(txn_idx, nodes_per_txn)`.
//! No RNG: ages cycle through `18..=65`; founding dates derive from
//! `2000-01-01 + (gid * 17 % 9000)` days.
//!
//! ## Re-use
//!
//! Used today by `insert_formats.rs`. Reusable by:
//! - any future `import_*` bench (Turtle / N-Quads ingest)
//! - any future `transact_*` bench (single-flake or batch commit throughput)
//! - any future `query_*` bench that wants a small but realistic linked graph

use serde_json::{json, Value as JsonValue};

#[derive(Debug, Clone)]
pub struct PersonData {
    pub id: String,
    pub name: String,
    pub email: String,
    pub age: u32,
}

#[derive(Debug, Clone)]
pub struct CompanyData {
    pub id: String,
    pub name: String,
    pub founded: String,
    pub employee_ids: Vec<String>,
    pub customer_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TxnData {
    pub persons: Vec<PersonData>,
    pub companies: Vec<CompanyData>,
}

/// Generate one transaction's worth of Person + Company data. Deterministic
/// in `(txn_idx, nodes_per_txn)`.
///
/// `nodes_per_txn` must be at least 1. The split is `max(1, nodes/10)`
/// companies and the remainder persons.
pub fn generate_txn_data(txn_idx: usize, nodes_per_txn: usize) -> TxnData {
    assert!(nodes_per_txn >= 1, "nodes_per_txn must be >= 1");

    let n_companies = std::cmp::max(1, nodes_per_txn / 10);
    let n_persons = nodes_per_txn - n_companies;
    let global_base = txn_idx * nodes_per_txn;

    let persons: Vec<PersonData> = (0..n_persons)
        .map(|i| {
            let gid = global_base + n_companies + i;
            PersonData {
                id: format!("ex:person-{gid:06}"),
                name: format!("Person {gid:06}"),
                email: format!("person{gid}@example.org"),
                age: 18 + (gid % 48) as u32,
            }
        })
        .collect();

    let companies: Vec<CompanyData> = (0..n_companies)
        .map(|i| {
            let gid = global_base + i;

            // Distribute persons across companies for refs.
            let chunk = if n_persons == 0 {
                0
            } else {
                n_persons / n_companies
            };
            let start = i * chunk;
            let end = if i == n_companies - 1 {
                n_persons
            } else {
                start + chunk
            };
            let mid = start + (end - start) / 2;

            let employee_ids: Vec<String> = (start..mid).map(|p| persons[p].id.clone()).collect();
            let customer_ids: Vec<String> = (mid..end).map(|p| persons[p].id.clone()).collect();

            // Deterministic date: 2000-01-01 + (gid * 17 % 9000) days.
            let base_date = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
            let days_offset = (gid * 17 % 9000) as i64;
            let founded = base_date + chrono::Duration::days(days_offset);

            CompanyData {
                id: format!("ex:company-{gid:06}"),
                name: format!("Company {gid:06}"),
                founded: founded.format("%Y-%m-%d").to_string(),
                employee_ids,
                customer_ids,
            }
        })
        .collect();

    TxnData { persons, companies }
}

/// Render a `TxnData` as a JSON-LD `@graph` document with the standard
/// `ex:` and `xsd:` prefixes. Lifted verbatim from `insert_formats.rs`.
pub fn txn_data_to_jsonld(data: &TxnData) -> JsonValue {
    let mut graph = Vec::with_capacity(data.persons.len() + data.companies.len());

    for p in &data.persons {
        graph.push(json!({
            "@id": p.id,
            "@type": "ex:Person",
            "ex:name": p.name,
            "ex:email": p.email,
            "ex:age": {"@value": p.age, "@type": "xsd:integer"}
        }));
    }

    for c in &data.companies {
        let employees: Vec<JsonValue> =
            c.employee_ids.iter().map(|id| json!({"@id": id})).collect();
        let customers: Vec<JsonValue> =
            c.customer_ids.iter().map(|id| json!({"@id": id})).collect();
        graph.push(json!({
            "@id": c.id,
            "@type": "ex:Company",
            "ex:name": c.name,
            "ex:founded": {"@value": c.founded, "@type": "xsd:date"},
            "ex:employees": employees,
            "ex:customers": customers
        }));
    }

    json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": graph
    })
}

/// Render a `TxnData` as a Turtle document with `@prefix ex:` and
/// `@prefix xsd:` headers. Lifted verbatim from `insert_formats.rs`.
pub fn txn_data_to_turtle(data: &TxnData) -> String {
    let mut buf = String::with_capacity(data.persons.len() * 200 + data.companies.len() * 400);
    buf.push_str("@prefix ex: <http://example.org/ns/> .\n");
    buf.push_str("@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\n");

    for p in &data.persons {
        buf.push_str(&p.id);
        buf.push_str(" a ex:Person ;\n");
        buf.push_str(&format!("    ex:name \"{}\" ;\n", p.name));
        buf.push_str(&format!("    ex:email \"{}\" ;\n", p.email));
        buf.push_str(&format!("    ex:age \"{}\"^^xsd:integer .\n\n", p.age));
    }

    for c in &data.companies {
        buf.push_str(&c.id);
        buf.push_str(" a ex:Company ;\n");
        buf.push_str(&format!("    ex:name \"{}\" ;\n", c.name));
        buf.push_str(&format!("    ex:founded \"{}\"^^xsd:date", c.founded));

        if !c.employee_ids.is_empty() {
            buf.push_str(" ;\n    ex:employees ");
            for (j, eid) in c.employee_ids.iter().enumerate() {
                if j > 0 {
                    buf.push_str(", ");
                }
                buf.push_str(eid);
            }
        }

        if !c.customer_ids.is_empty() {
            buf.push_str(" ;\n    ex:customers ");
            for (j, cid) in c.customer_ids.iter().enumerate() {
                if j > 0 {
                    buf.push_str(", ");
                }
                buf.push_str(cid);
            }
        }

        buf.push_str(" .\n\n");
    }

    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_across_runs() {
        let a = generate_txn_data(0, 10);
        let b = generate_txn_data(0, 10);
        assert_eq!(a.persons.len(), b.persons.len());
        assert_eq!(a.companies.len(), b.companies.len());
        assert_eq!(a.persons[0].id, b.persons[0].id);
        assert_eq!(a.companies[0].founded, b.companies[0].founded);
    }

    #[test]
    fn ten_percent_companies_split() {
        let data = generate_txn_data(0, 100);
        assert_eq!(data.companies.len(), 10);
        assert_eq!(data.persons.len(), 90);
    }

    #[test]
    fn small_txn_has_at_least_one_company() {
        let data = generate_txn_data(0, 1);
        assert_eq!(data.companies.len(), 1);
        assert_eq!(data.persons.len(), 0);
    }

    #[test]
    fn jsonld_roundtrip_compiles() {
        let data = generate_txn_data(0, 10);
        let v = txn_data_to_jsonld(&data);
        assert!(v["@graph"].is_array());
    }

    #[test]
    fn turtle_has_prefixes() {
        let data = generate_txn_data(0, 10);
        let s = txn_data_to_turtle(&data);
        assert!(s.contains("@prefix ex:"));
        assert!(s.contains("@prefix xsd:"));
    }

    #[test]
    fn txns_are_independent() {
        // Different txn_idx → no overlap in IDs (the global_base shift guarantees it).
        let a = generate_txn_data(0, 10);
        let b = generate_txn_data(1, 10);
        let a_ids: Vec<_> = a
            .persons
            .iter()
            .map(|p| p.id.clone())
            .chain(a.companies.iter().map(|c| c.id.clone()))
            .collect();
        let b_ids: Vec<_> = b
            .persons
            .iter()
            .map(|p| p.id.clone())
            .chain(b.companies.iter().map(|c| c.id.clone()))
            .collect();
        for id in &a_ids {
            assert!(
                !b_ids.contains(id),
                "id {id} appears in both txn 0 and txn 1"
            );
        }
    }
}
