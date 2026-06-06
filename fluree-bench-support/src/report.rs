//! End-of-run summary tables.
//!
//! Generic version of `insert_formats.rs`'s `print_summary`. Benches that
//! want a human-readable table at the end of a group can collect rows and
//! call [`print_summary`] to emit a markdown-style table to stderr.
//!
//! Criterion already produces its own statistics; this helper is for the
//! domain-specific cross-scenario comparisons benches sometimes want
//! (e.g., "JSON-LD vs Turtle at each scale", with a ratio column).

use std::fmt::Write;

/// One row of a bench summary. `metrics` is an ordered map of (name, value)
/// pairs; benches choose the units (ms, ns/op, flakes/sec, etc.) and the
/// summary helper formats them as numbers without imposing units.
#[derive(Debug, Clone)]
pub struct SummaryRow {
    pub label: String,
    pub metrics: Vec<(String, f64)>,
}

impl SummaryRow {
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            metrics: Vec::new(),
        }
    }

    pub fn add(mut self, name: impl Into<String>, value: f64) -> Self {
        self.metrics.push((name.into(), value));
        self
    }
}

/// Print a markdown-style table of summary rows to stderr.
///
/// Column order is taken from the first row. Subsequent rows must declare
/// the same metric names in the same order; mismatched rows are silently
/// padded with empty cells (no panic — bench output should not crash on a
/// shape mismatch).
pub fn print_summary(title: &str, rows: &[SummaryRow]) {
    if rows.is_empty() {
        eprintln!("(no rows for {title})");
        return;
    }
    let column_names: Vec<&str> = rows[0].metrics.iter().map(|(k, _)| k.as_str()).collect();

    let label_w = rows.iter().map(|r| r.label.len()).max().unwrap_or(0).max(8);
    let value_w = 12;

    // Header.
    let mut buf = String::new();
    let _ = write!(buf, "  ┌─{:─<label_w$}─", "");
    for _ in &column_names {
        let _ = write!(buf, "┬─{:─<value_w$}─", "");
    }
    buf.push_str("┐\n");

    let _ = write!(buf, "  │ {:<label_w$} ", title);
    for name in &column_names {
        let _ = write!(buf, "│ {:>value_w$} ", name);
    }
    buf.push_str("│\n");

    let _ = write!(buf, "  ├─{:─<label_w$}─", "");
    for _ in &column_names {
        let _ = write!(buf, "┼─{:─<value_w$}─", "");
    }
    buf.push_str("┤\n");

    for row in rows {
        let _ = write!(buf, "  │ {:<label_w$} ", row.label);
        for (i, _name) in column_names.iter().enumerate() {
            let v = row.metrics.get(i).map(|(_, v)| *v);
            match v {
                Some(x) => {
                    let _ = write!(buf, "│ {x:>value_w$.2} ");
                }
                None => {
                    let _ = write!(buf, "│ {:>value_w$} ", "");
                }
            }
        }
        buf.push_str("│\n");
    }

    let _ = write!(buf, "  └─{:─<label_w$}─", "");
    for _ in &column_names {
        let _ = write!(buf, "┴─{:─<value_w$}─", "");
    }
    buf.push_str("┘\n");

    eprintln!();
    eprint!("{buf}");
    eprintln!();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn print_summary_doesnt_panic_on_empty() {
        print_summary("empty", &[]);
    }

    #[test]
    fn print_summary_renders() {
        let rows = vec![
            SummaryRow::new("a").add("ms", 1.5).add("flakes/s", 1000.0),
            SummaryRow::new("b").add("ms", 2.5).add("flakes/s", 2000.0),
        ];
        print_summary("test", &rows);
    }

    #[test]
    fn rows_with_mismatched_metric_count_dont_panic() {
        let rows = vec![
            SummaryRow::new("a").add("ms", 1.0).add("flakes/s", 100.0),
            SummaryRow::new("b").add("ms", 2.0), // missing the second metric
        ];
        print_summary("test", &rows);
    }
}
