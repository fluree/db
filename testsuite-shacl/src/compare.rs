//! Compare an actual [`ValidateReport`] against the expected results.

use std::fmt;

use fluree_db_api::validate::{ReportResult, ValidateReport};
use serde_json::json;

use crate::manifest::{ExpectedResult, TermPat};

/// Why a report did not match.
pub struct Mismatch(String);

impl fmt::Display for Mismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Compare the actual report to the expected conformance + result multiset.
/// Returns `None` on a match.
pub fn compare_report(
    expected_conforms: bool,
    expected: &[ExpectedResult],
    actual: &ValidateReport,
) -> Option<Mismatch> {
    if actual.conforms != expected_conforms {
        return Some(Mismatch(format!(
            "conforms: expected {expected_conforms}, got {} ({} results: {})",
            actual.conforms,
            actual.results.len(),
            summarize(&actual.results),
        )));
    }
    if actual.results.len() != expected.len() {
        return Some(Mismatch(format!(
            "result count: expected {}, got {} ({})",
            expected.len(),
            actual.results.len(),
            summarize(&actual.results),
        )));
    }

    // Multiset match with backtracking (result sets are small).
    let mut used = vec![false; actual.results.len()];
    if match_all(expected, &actual.results, &mut used) {
        None
    } else {
        Some(Mismatch(format!(
            "results do not match expected set (actual: {})",
            summarize(&actual.results),
        )))
    }
}

fn match_all(expected: &[ExpectedResult], actual: &[ReportResult], used: &mut [bool]) -> bool {
    let Some(exp) = expected.first() else {
        return true;
    };
    for (i, act) in actual.iter().enumerate() {
        if !used[i] && matches(exp, act) {
            used[i] = true;
            if match_all(&expected[1..], actual, used) {
                return true;
            }
            used[i] = false;
        }
    }
    false
}

fn matches(exp: &ExpectedResult, act: &ReportResult) -> bool {
    // Focus node: actual is an IRI string, or a skolemized blank-node label.
    let focus_ok = match &exp.focus {
        TermPat::Absent | TermPat::Any => true,
        TermPat::Json(j) => {
            !act.focus_node.starts_with("_:") && *j == json!({"@id": act.focus_node})
        }
    };
    if !focus_ok {
        return false;
    }

    // Result path: compared only when the expected path is a plain IRI;
    // blank-node (complex) paths match leniently, absent accepts anything.
    let path_ok = match &exp.path {
        TermPat::Absent | TermPat::Any => true,
        TermPat::Json(j) => match &act.result_path {
            Some(p) => *j == json!({"@id": p}),
            None => false,
        },
    };
    if !path_ok {
        return false;
    }

    if let Some(sev) = &exp.severity {
        if *sev != act.severity {
            return false;
        }
    }
    if let Some(component) = &exp.component {
        if *component != act.constraint_component {
            return false;
        }
    }

    match &exp.value {
        TermPat::Absent | TermPat::Any => true,
        TermPat::Json(j) => act.value.as_ref() == Some(j),
    }
}

fn summarize(results: &[ReportResult]) -> String {
    results
        .iter()
        .map(|r| {
            format!(
                "[{} {} @{}{}]",
                short(&r.constraint_component),
                r.focus_node,
                r.result_path.as_deref().unwrap_or("-"),
                r.value
                    .as_ref()
                    .map(|v| format!(" = {v}"))
                    .unwrap_or_default(),
            )
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn short(iri: &str) -> &str {
    iri.rsplit_once('#').map_or(iri, |(_, l)| l)
}
