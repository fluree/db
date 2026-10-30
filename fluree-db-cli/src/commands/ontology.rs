//! `fluree ontology` — ontology tools for document extraction.
//!
//! `check` reports what an HTTP reader will and won't read of an
//! ontology (and catalog), applying the model's input policy locally;
//! `from` writes an ontology in the shape the model reads well. Both read
//! files only: no project, no ledger, nothing sent anywhere. The logic is in
//! `fluree-db-ontology`; this module reads the files and prints.

use crate::cli::{
    OntologyAction, OntologyCheckArgs, OntologyFromArgs, OntologyInputFormat, OntologyOutputFormat,
};
use crate::error::{CliError, CliResult};
use colored::Colorize;
use fluree_db_ontology::check::thousands;
use fluree_db_ontology::read::Parsed;
use fluree_db_ontology::vocab::{WELL_KNOWN_PREFIXES, XT_INSTANCES};
use fluree_db_ontology::write::{self, OutputFormat};
use fluree_db_ontology::{check, CheckOptions, Finding, RdfFormat, Report};
use std::collections::BTreeMap;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

pub fn run(action: OntologyAction) -> CliResult<()> {
    match action {
        OntologyAction::Check(args) => run_check(&args),
        OntologyAction::From(args) => run_from(&args),
    }
}

/// A file's text (`-` is stdin) and its size in bytes.
fn read_text(path: &Path) -> CliResult<String> {
    if path == Path::new("-") {
        let mut text = String::new();
        std::io::stdin()
            .read_to_string(&mut text)
            .map_err(|e| CliError::Input(format!("failed to read stdin: {e}")))?;
        return Ok(text);
    }
    std::fs::read_to_string(path)
        .map_err(|e| CliError::Input(format!("failed to read {}: {e}", path.display())))
}

/// The RDF format to read `path` as: the flag, else its extension, else the
/// text's first character.
fn input_format(
    path: &Path,
    flag: Option<OntologyInputFormat>,
    text: &str,
) -> CliResult<RdfFormat> {
    if let Some(flag) = flag {
        return Ok(match flag {
            OntologyInputFormat::Turtle => RdfFormat::Turtle,
            OntologyInputFormat::Jsonld => RdfFormat::JsonLd,
        });
    }
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase);
    match ext.as_deref() {
        Some("trig") => Err(CliError::Input(format!(
            "{}: TriG isn't read yet; pass the ontology as Turtle or JSON-LD (a saved request as JSON-LD works)",
            path.display()
        ))),
        Some(_) if RdfFormat::from_path(path).is_none() && path != Path::new("-") => {
            Err(CliError::Input(format!(
                "{}: not an RDF file this command reads (Turtle .ttl or JSON-LD .jsonld); use --input-format to say which it is",
                path.display()
            )))
        }
        _ => Ok(RdfFormat::from_path(path).unwrap_or_else(|| RdfFormat::sniff(text))),
    }
}

fn parse_file(path: &Path, flag: Option<OntologyInputFormat>) -> CliResult<(Parsed, usize)> {
    let text = read_text(path)?;
    let format = input_format(path, flag, &text)?;
    let parsed = fluree_db_ontology::parse(&text, format, None)
        .map_err(|e| CliError::Input(format!("{}: {e}", path.display())))?;
    Ok((parsed, text.len()))
}

fn run_check(args: &OntologyCheckArgs) -> CliResult<()> {
    let (mut parsed, bytes) = parse_file(&args.file, args.input_format)?;
    let mut bytes = bytes;
    if let Some(catalog) = &args.catalog {
        let (extra, extra_bytes) = parse_file(catalog, None)?;
        bytes += extra_bytes;
        // into the request's catalog graph when the ontology file uses graph
        // roles; otherwise the flat split finds the records
        let by_graph = parsed.split().by_graph;
        parsed.quads.extend(extra.quads.into_iter().map(|mut q| {
            if by_graph {
                q.graph = Some(Arc::from(XT_INSTANCES));
            }
            q
        }));
        parsed.prefixes.extend(extra.prefixes);
    }
    let options = CheckOptions {
        seeds: args.seed.clone(),
        depth: args.depth,
        source_bytes: Some(bytes),
    };
    let report = check(&parsed, &options).map_err(|e| CliError::Usage(e.to_string()))?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_report(&args.file, &report, &Names::new(&parsed));
    }
    if report.passes() {
        Ok(())
    } else {
        Err(CliError::ExitCode(1))
    }
}

/// Prefixed names for display: the file's prefixes, then the well-known ones.
struct Names(Vec<(String, String)>);

impl Names {
    fn new(parsed: &Parsed) -> Self {
        let mut prefixes: Vec<(String, String)> = parsed
            .prefixes
            .iter()
            .filter(|(p, _)| !p.is_empty())
            .cloned()
            .collect();
        prefixes.extend(
            WELL_KNOWN_PREFIXES
                .iter()
                .map(|(p, ns)| ((*p).to_string(), (*ns).to_string())),
        );
        // the longest namespace wins
        prefixes.sort_by_key(|(_, ns)| std::cmp::Reverse(ns.len()));
        Names(prefixes)
    }

    fn show(&self, iri: &str) -> String {
        self.0
            .iter()
            .find_map(|(p, ns)| {
                iri.strip_prefix(ns.as_str())
                    .filter(|local| !local.is_empty() && !local.contains(['/', '#']))
                    .map(|local| format!("{p}:{local}"))
            })
            .unwrap_or_else(|| format!("<{iri}>"))
    }

    /// A report key for display: an IRI in it as a prefixed name. (The JSON
    /// report keeps keys exactly as an extraction response writes them.)
    fn key(&self, key: &str) -> String {
        match key.strip_prefix("rdf:type ") {
            Some(rest) if rest.contains("://") => format!("rdf:type {}", self.show(rest)),
            _ if key.contains("://") => self.show(key),
            _ => key.to_string(),
        }
    }

    fn list(&self, iris: &[String], count: usize) -> String {
        let mut out = iris
            .iter()
            .map(|i| self.show(i))
            .collect::<Vec<_>>()
            .join(", ");
        if count > iris.len() && !iris.is_empty() {
            out.push_str(&format!(" and {} more", thousands(count - iris.len())));
        }
        out
    }
}

fn print_report(file: &Path, report: &Report, names: &Names) {
    let mut head = format!(
        "{}: {}, {}",
        file.display(),
        count(report.classes, "class", "classes"),
        count(report.properties, "property", "properties")
    );
    if report.records > 0 {
        head.push_str(&format!(
            ", {}",
            count(report.records, "catalog record", "catalog records")
        ));
    }
    println!("{}", head.bold());
    if let Some(slice) = &report.slice {
        println!(
            "Sliced from {} classes and {} properties: {} step(s) from {}.",
            thousands(slice.classes_total),
            thousands(slice.properties_total),
            slice.depth,
            names.list(&slice.seeds, slice.seeds.len())
        );
    }
    let e = &report.estimate;
    println!(
        "About {} serialized tokens per window before document text (a window holds {}).",
        thousands(e.tokens),
        thousands(e.limit)
    );

    if !report.will_fail.is_empty() {
        println!(
            "\n{}",
            "Will fail: a request with this would be refused"
                .red()
                .bold()
        );
        for f in &report.will_fail {
            print_finding(f, names, "✗".red().to_string());
        }
    }
    print_counts(
        "Read another way by the reader",
        &report.adapted,
        &report.examples.adapted,
        names,
    );
    print_counts(
        "Not read by the reader",
        &report.not_shown,
        &report.examples.not_shown,
        names,
    );
    if !report.advice.is_empty() {
        println!("\n{}", "Advice".bold());
        for f in &report.advice {
            print_finding(f, names, "•".to_string());
        }
    }
    println!();
    if report.passes() {
        println!("{}", "Nothing here would make a request fail.".green());
    } else {
        let n = report.will_fail.len();
        println!(
            "{}",
            format!(
                "{n} problem{} would make a request fail.",
                if n == 1 { "" } else { "s" }
            )
            .red()
        );
    }
}

/// `1 class`, `2 classes`.
fn count(n: usize, one: &str, many: &str) -> String {
    format!("{} {}", thousands(n), if n == 1 { one } else { many })
}

fn print_counts(
    title: &str,
    counts: &BTreeMap<String, usize>,
    examples: &BTreeMap<String, Vec<String>>,
    names: &Names,
) {
    if counts.is_empty() {
        return;
    }
    println!("\n{}", title.bold());
    let width = counts
        .keys()
        .map(|k| names.key(k).chars().count())
        .max()
        .unwrap_or(0);
    for (key, n) in counts {
        let ex = examples.get(key).map(Vec::as_slice).unwrap_or_default();
        // the count is of triples, the examples are the terms they are about
        let ex = if ex.is_empty() {
            String::new()
        } else {
            format!(
                "  {}",
                format!("e.g. {}", names.list(ex, ex.len())).dimmed()
            )
        };
        let key = names.key(key);
        println!("  {key:<width$}  {:>6}{ex}", thousands(*n));
    }
}

fn print_finding(f: &Finding, names: &Names, mark: String) {
    println!("  {mark} {}", f.message);
    if !f.examples.is_empty() {
        println!("      {}", names.list(&f.examples, f.count).dimmed());
    }
}

fn run_from(args: &OntologyFromArgs) -> CliResult<()> {
    let (parsed, _) = parse_file(&args.file, args.input_format)?;
    let out = write::rewrite(&parsed, &args.seed, args.depth)
        .map_err(|e| CliError::Usage(e.to_string()))?;
    let format = match args.format {
        OntologyOutputFormat::Turtle => OutputFormat::Turtle,
        OntologyOutputFormat::Jsonld => OutputFormat::JsonLd,
    };
    let prefixes = write::used_prefixes(&out.quads, &parsed.prefixes);
    let text = write::serialize(&out.quads, &prefixes, format)
        .map_err(|e| CliError::Input(format!("failed to write the ontology: {e}")))?;
    match &args.output {
        Some(path) => std::fs::write(path, &text)
            .map_err(|e| CliError::Input(format!("failed to write {}: {e}", path.display())))?,
        None => print!("{text}"),
    }

    let mut summary = format!(
        "{}, {}",
        count(out.classes, "class", "classes"),
        count(out.properties, "property", "properties")
    );
    if out.records > 0 {
        summary.push_str(&format!(
            ", {}",
            count(out.records, "catalog record", "catalog records")
        ));
    }
    if let Some(slice) = &out.slice {
        summary.push_str(&format!(
            " (sliced from {} classes, {} properties)",
            thousands(slice.classes_total),
            thousands(slice.properties_total)
        ));
    }
    eprintln!("{summary}");
    if !out.adapted.is_empty() {
        let changes: Vec<String> = out
            .adapted
            .iter()
            .map(|(k, v)| format!("{k}: {}", thousands(*v)))
            .collect();
        eprintln!("rewritten: {}", changes.join("; "));
    }
    eprintln!("run `fluree ontology check` on the result to see what an HTTP reader will read");
    Ok(())
}
