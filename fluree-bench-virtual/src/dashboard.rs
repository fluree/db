//! Self-contained HTML dashboard for a vbench run (or a merged set of runs).
//!
//! Information-design treatment for a dev-facing telemetry tool: a summary strip
//! of stat tiles above one dense per-query comparison table (native vs virtual),
//! state encoded as pills and a left severity stripe, a telemetry-teal accent
//! kept separate from the semantic pass/warn/fail colours, tabular numerics, and
//! a monospace face for the data columns. Theme-aware (light/dark) and
//! responsive — the wide table scrolls inside its own container so the page body
//! never scrolls sideways. Output is body content only (no `<html>/<head>/<body>`)
//! so it can be published directly as an Artifact.

use std::collections::BTreeMap;

use crate::corpus::{Corpus, HashGate};
use crate::schema::{Counters, RunMeta, RunRecord, Status};
use crate::spans;

const CSS: &str = r#"
:root{
  --bg:#f6f8fa; --surface:#ffffff; --surface-2:#f0f3f7; --border:#e2e6ec;
  --text:#1a2130; --muted:#5b6576; --accent:#0d9488;
  --good:#15803d; --warn:#b45309; --bad:#b91c1c; --dnf:#6d5f95;
  --good-bg:#e7f5ec; --warn-bg:#fdf1e0; --bad-bg:#fbe9e9; --dnf-bg:#efecf7;
  --mono:ui-monospace,"SF Mono","JetBrains Mono",Menlo,Consolas,monospace;
  --sans:ui-sans-serif,-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
}
@media (prefers-color-scheme:dark){:root{
  --bg:#0e1117; --surface:#161b24; --surface-2:#1c2230; --border:#28303d;
  --text:#e6e9ef; --muted:#8b95a6; --accent:#2dd4bf;
  --good:#4ade80; --warn:#fbbf24; --bad:#f87171; --dnf:#b3a8d6;
  --good-bg:#12271a; --warn-bg:#2a2012; --bad-bg:#2a1516; --dnf-bg:#1e1a2b;
}}
:root[data-theme="light"]{
  --bg:#f6f8fa; --surface:#ffffff; --surface-2:#f0f3f7; --border:#e2e6ec;
  --text:#1a2130; --muted:#5b6576; --accent:#0d9488;
  --good:#15803d; --warn:#b45309; --bad:#b91c1c; --dnf:#6d5f95;
  --good-bg:#e7f5ec; --warn-bg:#fdf1e0; --bad-bg:#fbe9e9; --dnf-bg:#efecf7;
}
:root[data-theme="dark"]{
  --bg:#0e1117; --surface:#161b24; --surface-2:#1c2230; --border:#28303d;
  --text:#e6e9ef; --muted:#8b95a6; --accent:#2dd4bf;
  --good:#4ade80; --warn:#fbbf24; --bad:#f87171; --dnf:#b3a8d6;
  --good-bg:#12271a; --warn-bg:#2a2012; --bad-bg:#2a1516; --dnf-bg:#1e1a2b;
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font-family:var(--sans);
  font-size:14px;line-height:1.5;-webkit-font-smoothing:antialiased}
.wrap{max-width:1280px;margin:0 auto;padding:28px 20px 64px}
header.top{display:flex;flex-wrap:wrap;align-items:baseline;gap:10px 16px;
  padding-bottom:16px;border-bottom:1px solid var(--border);margin-bottom:22px}
.brand{font-weight:700;font-size:20px;letter-spacing:-.01em}
.brand .mark{color:var(--accent)}
.tagline{color:var(--muted);font-size:13px}
.prov{margin-left:auto;color:var(--muted);font-size:12px;font-family:var(--mono);
  display:flex;flex-wrap:wrap;gap:4px 14px;justify-content:flex-end}
.prov b{color:var(--text);font-weight:600}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));
  gap:12px;margin-bottom:26px}
.tile{background:var(--surface);border:1px solid var(--border);border-radius:10px;
  padding:14px 16px}
.tile .k{color:var(--muted);font-size:11px;text-transform:uppercase;
  letter-spacing:.06em;margin-bottom:6px}
.tile .v{font-size:23px;font-weight:700;font-variant-numeric:tabular-nums;
  font-family:var(--mono)}
.tile .v small{font-size:13px;color:var(--muted);font-weight:500}
.tile.good .v{color:var(--good)} .tile.bad .v{color:var(--bad)}
h2{font-size:13px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);
  margin:0 0 10px;font-weight:600}
.tablewrap{overflow-x:auto;border:1px solid var(--border);border-radius:10px;
  background:var(--surface)}
table{border-collapse:collapse;width:100%;font-size:13px}
thead th{position:sticky;top:0;background:var(--surface-2);text-align:right;
  padding:9px 10px;font-weight:600;color:var(--muted);white-space:nowrap;
  border-bottom:1px solid var(--border);cursor:pointer;user-select:none}
thead th.l{text-align:left}
thead th:hover{color:var(--text)}
tbody td{padding:8px 10px;border-bottom:1px solid var(--border);text-align:right;
  white-space:nowrap;font-variant-numeric:tabular-nums;font-family:var(--mono)}
tbody tr:last-child td{border-bottom:none}
tbody tr:hover{background:var(--surface-2)}
td.l,th.l{text-align:left;font-family:var(--sans)}
td.qid{font-family:var(--mono);font-weight:600}
td.bi{max-width:320px;white-space:normal;color:var(--text)}
td.bi .tags{margin-top:3px;display:flex;flex-wrap:wrap;gap:3px}
.tag{font-family:var(--mono);font-size:10.5px;color:var(--muted);
  background:var(--surface-2);border:1px solid var(--border);border-radius:4px;
  padding:0 5px}
.sev{width:3px;padding:0!important}
tr.fail .sev{background:var(--bad)} tr.regress .sev{background:var(--warn)}
.pill{display:inline-block;font-family:var(--mono);font-size:11px;font-weight:600;
  padding:1px 7px;border-radius:99px;line-height:1.6}
.pill.ok{color:var(--good);background:var(--good-bg)}
.pill.err{color:var(--bad);background:var(--bad-bg)}
.pill.xerr{color:var(--dnf);background:var(--dnf-bg)}
.pill.dnf{color:var(--warn);background:var(--warn-bg)}
.ratio{display:inline-block;min-width:52px;text-align:center;border-radius:5px;
  padding:1px 7px;font-weight:700}
.r0{color:var(--good);background:var(--good-bg)}
.r1{color:var(--accent);background:var(--surface-2)}
.r2{color:var(--warn);background:var(--warn-bg)}
.r3{color:var(--bad);background:var(--bad-bg)}
.hash.match{color:var(--good)} .hash.miss{color:var(--bad);font-weight:700}
.hash.na{color:var(--muted)}
.muted{color:var(--muted)}
.miss{color:var(--warn);font-family:var(--mono);font-size:11px}
.legend{margin-top:18px;color:var(--muted);font-size:12px;line-height:1.7}
.legend code{font-family:var(--mono);color:var(--text)}
.note{margin-top:8px;padding:10px 12px;border-left:3px solid var(--accent);
  background:var(--surface);border-radius:0 8px 8px 0;color:var(--muted);font-size:12.5px}
"#;

const SORT_JS: &str = r"
<script>
(function(){
  var t=document.querySelector('table'); if(!t)return;
  var tb=t.tBodies[0];
  t.tHead.querySelectorAll('th').forEach(function(th,ci){
    var dir=1;
    th.addEventListener('click',function(){
      dir=-dir;
      var rows=[].slice.call(tb.rows);
      rows.sort(function(a,b){
        var x=a.cells[ci].dataset.sort, y=b.cells[ci].dataset.sort;
        var nx=parseFloat(x), ny=parseFloat(y);
        if(!isNaN(nx)&&!isNaN(ny))return (nx-ny)*dir;
        return String(x).localeCompare(String(y))*dir;
      });
      rows.forEach(function(r){tb.appendChild(r);});
    });
  });
})();
</script>
";

/// A run file feeding the dashboard: its meta and records.
pub struct RunData {
    pub meta: RunMeta,
    pub records: Vec<RunRecord>,
}

/// Render the dashboard HTML body (style + markup + script) for one or more runs.
pub fn render(runs: &[RunData], corpus: &Corpus, title: &str) -> String {
    // target id -> kind ("native"/"virtual"), first-seen order preserved.
    let mut kinds: BTreeMap<String, String> = BTreeMap::new();
    let mut native_ids: Vec<String> = Vec::new();
    let mut virtual_ids: Vec<String> = Vec::new();
    for run in runs {
        for t in &run.meta.targets {
            if kinds.insert(t.id.clone(), t.kind.clone()).is_none() {
                if t.kind == "virtual" {
                    virtual_ids.push(t.id.clone());
                } else {
                    native_ids.push(t.id.clone());
                }
            }
        }
    }
    // (query_id, target) -> record (last run wins).
    let mut recs: BTreeMap<(String, String), &RunRecord> = BTreeMap::new();
    for run in runs {
        for r in &run.records {
            recs.insert((r.query_id.clone(), r.target.clone()), r);
        }
    }
    let native = native_ids.first().cloned();
    let virt = virtual_ids.first().cloned();

    let mut body = String::new();
    body.push_str("<style>");
    body.push_str(CSS);
    body.push_str("</style>\n<div class=\"wrap\">\n");

    // Header + provenance.
    let meta = &runs.last().map(|r| &r.meta).expect("at least one run");
    body.push_str("<header class=\"top\">");
    body.push_str("<span class=\"brand\"><span class=\"mark\">v</span>bench</span>");
    body.push_str(&format!("<span class=\"tagline\">{}</span>", esc(title)));
    body.push_str("<span class=\"prov\">");
    body.push_str(&format!(
        "<span><b>git</b> {}{}</span>",
        esc(&meta.git_commit),
        if meta.git_dirty { "-dirty" } else { "" }
    ));
    body.push_str(&format!(
        "<span><b>profile</b> {}</span>",
        esc(&meta.build_profile)
    ));
    body.push_str(&format!("<span><b>host</b> {}</span>", esc(&meta.host)));
    body.push_str(&format!("<span>{}</span>", esc(&meta.timestamp)));
    body.push_str("</span></header>\n");

    // Build per-query joined rows (corpus order).
    let mut rows_html = String::new();
    let (mut n_native_ms, mut n_virt_ms) = (0u64, 0u64);
    let (mut correctness_pass, mut correctness_total) = (0usize, 0usize);
    let (mut hash_match, mut hash_total) = (0usize, 0usize);
    let mut worst_ratio = 0f64;
    let mut shown = 0usize;

    for q in &corpus.queries {
        let nrec = native
            .as_ref()
            .and_then(|t| recs.get(&(q.id.clone(), t.clone())).copied());
        let vrec = virt
            .as_ref()
            .and_then(|t| recs.get(&(q.id.clone(), t.clone())).copied());
        if nrec.is_none() && vrec.is_none() {
            continue;
        }
        shown += 1;
        if let Some(n) = nrec {
            n_native_ms += n.wall_ms;
        }
        if let Some(v) = vrec {
            n_virt_ms += v.wall_ms;
        }

        // Ratio (virtual / native), where both present and native > 0.
        let ratio = match (nrec, vrec) {
            (Some(n), Some(v)) if n.wall_ms > 0 => Some(v.wall_ms as f64 / n.wall_ms as f64),
            _ => None,
        };
        if let Some(r) = ratio {
            worst_ratio = worst_ratio.max(r);
        }

        // Correctness where both are Ok. A `rows_only` query (nondeterministic
        // LIMIT) is gated on row count, not hash — comparing its hash would raise
        // a false mismatch.
        let rows_only = q.hash_gate == HashGate::RowsOnly;
        let hash_state = match (nrec, vrec) {
            (Some(n), Some(v)) if n.status == Status::Ok && v.status == Status::Ok => {
                hash_total += 1;
                correctness_total += 1;
                let parity = if rows_only {
                    n.rows == v.rows
                } else {
                    n.result_hash == v.result_hash
                };
                if parity {
                    hash_match += 1;
                    correctness_pass += 1;
                    if rows_only {
                        HashState::RowsMatch
                    } else {
                        HashState::Match
                    }
                } else {
                    HashState::Miss
                }
            }
            _ => HashState::Na,
        };

        let fail = matches!(hash_state, HashState::Miss);
        let regress = ratio.is_some_and(|r| r >= 20.0);
        let sev_class = if fail {
            " class=\"fail\""
        } else if regress {
            " class=\"regress\""
        } else {
            ""
        };

        rows_html.push_str(&format!("<tr{sev_class}><td class=\"sev\"></td>"));
        rows_html.push_str(&format!(
            "<td class=\"l qid\" data-sort=\"{}\">{}</td>",
            esc(&q.id),
            esc(&q.id)
        ));
        // BI question + tags + class.
        let tags: String = q
            .tags
            .iter()
            .map(|t| {
                format!(
                    "<span class=\"tag\">{}</span>",
                    esc(&format!("{t:?}").to_lowercase())
                )
            })
            .collect();
        rows_html.push_str(&format!(
            "<td class=\"l bi\" data-sort=\"{}\">{}<div class=\"tags\"><span class=\"tag\">{}</span>{}</div></td>",
            esc(&q.id),
            esc(&q.bi_question),
            esc(&q.class),
            tags
        ));
        rows_html.push_str(&cell_ms(nrec));
        rows_html.push_str(&cell_ms(vrec));
        rows_html.push_str(&cell_ratio(ratio));
        rows_html.push_str(&cell_status(nrec));
        rows_html.push_str(&cell_status(vrec));
        rows_html.push_str(&cell_hash(hash_state));
        rows_html.push_str(&cell_counter(vrec, |c| {
            spans::span_count(c, "r2rml.scan_table")
        }));
        rows_html.push_str(&cell_pruned(vrec));
        rows_html.push_str(&cell_missing(vrec));
        rows_html.push_str("</tr>\n");
    }

    // Summary tiles.
    body.push_str("<div class=\"tiles\">");
    body.push_str(&tile("queries", &shown.to_string(), None));
    body.push_str(&tile(
        "native median Σ",
        &format!("{n_native_ms}<small> ms</small>"),
        None,
    ));
    if virt.is_some() {
        body.push_str(&tile(
            "virtual median Σ",
            &format!("{n_virt_ms}<small> ms</small>"),
            None,
        ));
        let hclass = if hash_total > 0 && hash_match == hash_total {
            Some("good")
        } else if hash_match < hash_total {
            Some("bad")
        } else {
            None
        };
        body.push_str(&tile(
            "hash match",
            &format!("{hash_match}<small> / {hash_total}</small>"),
            hclass,
        ));
        body.push_str(&tile(
            "worst ratio",
            &format!("{worst_ratio:.0}<small>×</small>"),
            None,
        ));
    }
    let _ = (correctness_pass, correctness_total);
    body.push_str("</div>\n");

    // Table.
    body.push_str("<h2>corpus — native vs virtual</h2>\n<div class=\"tablewrap\"><table>\n");
    body.push_str("<thead><tr>");
    for (label, left) in [
        ("", true),
        ("query", true),
        ("BI question / tags", true),
        ("native ms", false),
        ("virtual ms", false),
        ("ratio", false),
        ("nat", false),
        ("virt", false),
        ("hash", false),
        ("scans", false),
        ("pruned/sel", false),
        ("missing", false),
    ] {
        let cls = if left { " class=\"l\"" } else { "" };
        body.push_str(&format!("<th{cls}>{}</th>", esc(label)));
    }
    body.push_str("</tr></thead>\n<tbody>\n");
    body.push_str(&rows_html);
    body.push_str("</tbody></table></div>\n");

    // Legend + caveats.
    body.push_str("<div class=\"legend\">");
    body.push_str(
        "<b>ratio</b> = virtual median wall ÷ native median wall (higher = virtual slower). ",
    );
    body.push_str("<b>scans</b> = <code>r2rml.scan_table</code> spans. ");
    body.push_str(
        "<b>pruned/sel</b> = Iceberg files pruned vs selected by <code>iceberg.scan_plan</code>. ",
    );
    body.push_str("<b>missing</b> = expected-for-virtual pathway spans that did not fire. ");
    body.push_str("A red stripe marks a native↔virtual result-hash mismatch; amber marks a ≥20× slowdown. Click a header to sort.");
    body.push_str("</div>");
    if virt.is_none() {
        body.push_str("<div class=\"note\">Native-only run: no virtual target present, so ratio, hash-match, and pathway-span columns are empty. Add a virtual target to populate the comparison.</div>");
    }

    body.push_str("</div>\n");
    body.push_str(SORT_JS);
    body
}

#[derive(Clone, Copy)]
enum HashState {
    Match,
    /// Parity confirmed by row count (a `rows_only`-gated query).
    RowsMatch,
    Miss,
    Na,
}

fn tile(k: &str, v: &str, class: Option<&str>) -> String {
    let c = class.map_or(String::new(), |c| format!(" {c}"));
    format!(
        "<div class=\"tile{c}\"><div class=\"k\">{}</div><div class=\"v\">{v}</div></div>",
        esc(k)
    )
}

fn cell_ms(rec: Option<&RunRecord>) -> String {
    match rec {
        Some(r) => format!("<td data-sort=\"{}\">{}</td>", r.wall_ms, fmt_ms(r.wall_ms)),
        None => "<td class=\"muted\" data-sort=\"-1\">—</td>".to_string(),
    }
}

fn cell_ratio(ratio: Option<f64>) -> String {
    match ratio {
        Some(r) => {
            let bucket = if r < 2.0 {
                "r0"
            } else if r < 10.0 {
                "r1"
            } else if r < 50.0 {
                "r2"
            } else {
                "r3"
            };
            let label = if r >= 100.0 {
                format!("{r:.0}×")
            } else {
                format!("{r:.1}×")
            };
            format!("<td data-sort=\"{r}\"><span class=\"ratio {bucket}\">{label}</span></td>")
        }
        None => "<td class=\"muted\" data-sort=\"-1\">—</td>".to_string(),
    }
}

fn cell_status(rec: Option<&RunRecord>) -> String {
    match rec {
        Some(r) => {
            let (cls, label) = match r.status {
                Status::Ok => ("ok", "ok"),
                Status::Error => ("err", "err"),
                Status::ExpectedError => ("xerr", "xerr"),
                Status::Dnf => ("dnf", "dnf"),
            };
            format!("<td data-sort=\"{label}\"><span class=\"pill {cls}\">{label}</span></td>")
        }
        None => "<td class=\"muted\" data-sort=\"~\">—</td>".to_string(),
    }
}

fn cell_hash(state: HashState) -> String {
    match state {
        HashState::Match => "<td class=\"hash match\" data-sort=\"1\">✓</td>".to_string(),
        HashState::RowsMatch => "<td class=\"hash match\" data-sort=\"1\">✓ rows</td>".to_string(),
        HashState::Miss => "<td class=\"hash miss\" data-sort=\"0\">✗ mismatch</td>".to_string(),
        HashState::Na => "<td class=\"hash na\" data-sort=\"2\">—</td>".to_string(),
    }
}

fn cell_counter(rec: Option<&RunRecord>, f: impl Fn(&Counters) -> u64) -> String {
    match rec {
        Some(r) => {
            let n = f(&r.counters);
            format!("<td data-sort=\"{n}\">{n}</td>")
        }
        None => "<td class=\"muted\" data-sort=\"-1\">—</td>".to_string(),
    }
}

fn cell_pruned(rec: Option<&RunRecord>) -> String {
    match rec {
        Some(r) => {
            let p = r.counters.files_pruned;
            let s = r.counters.files_selected;
            format!("<td data-sort=\"{s}\">{p}/{s}</td>")
        }
        None => "<td class=\"muted\" data-sort=\"-1\">—</td>".to_string(),
    }
}

fn cell_missing(rec: Option<&RunRecord>) -> String {
    match rec {
        Some(r) if !r.spans_missing.is_empty() => {
            let m = r
                .spans_missing
                .iter()
                .map(|s| s.replace("r2rml.", "").replace("iceberg.", ""))
                .collect::<Vec<_>>()
                .join(",");
            format!("<td class=\"miss\" data-sort=\"1\">{}</td>", esc(&m))
        }
        Some(_) => "<td class=\"muted\" data-sort=\"0\">—</td>".to_string(),
        None => "<td class=\"muted\" data-sort=\"-1\">—</td>".to_string(),
    }
}

fn fmt_ms(ms: u64) -> String {
    if ms >= 1000 {
        format!("{:.2}s", ms as f64 / 1000.0)
    } else {
        format!("{ms}")
    }
}

/// Minimal HTML-escape for injected text.
fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
