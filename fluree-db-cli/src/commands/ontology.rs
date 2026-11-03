//! `fluree ontology` and `fluree catalog` — ontology and catalog tools for
//! document extraction.
//!
//! `ontology check` reports what an HTTP reader will and won't read of an
//! ontology (and catalog), applying its input policy locally;
//! `ontology from` writes an ontology in the shape the model reads well, from
//! RDF or from tables (CSV, a workbook, SQL DDL); `catalog from` writes SKOS
//! records from rows. All read files only: no project, no ledger, nothing
//! sent anywhere. The logic is in `fluree-db-ontology`; this module reads the
//! files and prints.

use crate::cli::{
    CatalogAction, CatalogFromArgs, OntologyAction, OntologyCheckArgs, OntologyFromArgs,
    OntologyFromFormat, OntologyInputFormat, OntologyOutputFormat,
};
use crate::error::{CliError, CliResult};
use colored::Colorize;
use fluree_db_ontology::catalog::{self, CatalogOptions};
use fluree_db_ontology::check::thousands;
use fluree_db_ontology::convert::{self, tabular, EmitOptions};
use fluree_db_ontology::estimate::{MAX_TEXT_CHARS, TOKENS_PER_WINDOW};
use fluree_db_ontology::read::Parsed;
use fluree_db_ontology::read::Quad;
use fluree_db_ontology::vocab::{WELL_KNOWN_PREFIXES, XT_INSTANCES};
use fluree_db_ontology::write::{self, OutputFormat};
use fluree_db_ontology::{check, CheckOptions, Finding, RdfFormat, Report};
use std::collections::{BTreeMap, HashMap};
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

/// What `ontology from` reads a file as.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Source {
    Rdf,
    Csv,
    Workbook,
    Sql,
    /// YAML, read by what it holds: an Ossie document, dbt, or a Snowflake
    /// semantic view
    Yaml,
    Snowflake,
    Genie,
    Dbt,
    Ossie,
}

fn source(path: &Path, flag: Option<OntologyFromFormat>) -> Source {
    match flag {
        Some(OntologyFromFormat::Turtle | OntologyFromFormat::Jsonld) => return Source::Rdf,
        Some(OntologyFromFormat::Csv) => return Source::Csv,
        Some(OntologyFromFormat::Xlsx) => return Source::Workbook,
        Some(OntologyFromFormat::Sql) => return Source::Sql,
        Some(OntologyFromFormat::Snowflake) => return Source::Snowflake,
        Some(OntologyFromFormat::Genie) => return Source::Genie,
        Some(OntologyFromFormat::Dbt) => return Source::Dbt,
        Some(OntologyFromFormat::Ossie) => return Source::Ossie,
        None => {}
    }
    if path.is_dir() {
        return Source::Dbt;
    }
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase);
    match ext.as_deref() {
        Some("csv" | "tsv") => Source::Csv,
        Some("xlsx" | "xlsm") => Source::Workbook,
        Some("sql" | "ddl") => Source::Sql,
        Some("yaml" | "yml") => Source::Yaml,
        // Genie exports and Ossie documents are JSON too; anything else JSON is JSON-LD
        Some("json") => match std::fs::read_to_string(path)
            .ok()
            .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
        {
            Some(v) if convert::semantic::is_genie(&v) => Source::Genie,
            Some(v) if convert::ossie::is_semantic_model(&v) || convert::ossie::is_ontology(&v) => {
                Source::Ossie
            }
            _ => Source::Rdf,
        },
        _ => Source::Rdf,
    }
}

/// A YAML or JSON document.
fn read_document(path: &Path) -> CliResult<serde_json::Value> {
    convert::parse_document(&read_text(path)?)
        .map_err(|e| CliError::Input(format!("{}: {e}", path.display())))
}

/// A dbt project read from its directory: the YAML files that hold semantic
/// models or model properties (`target/`, `dbt_packages/` and the like passed
/// over), the `{% docs %}` blocks of its Markdown, and the files that couldn't
/// be read (noted, never skipped silently).
struct DbtProject {
    files: Vec<(String, String)>,
    blocks: HashMap<String, String>,
    notes: Vec<String>,
}

fn dbt_project(dir: &Path) -> CliResult<DbtProject> {
    let mut project = DbtProject {
        files: Vec::new(),
        blocks: HashMap::new(),
        notes: Vec::new(),
    };
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let entries = std::fs::read_dir(&d)
            .map_err(|e| CliError::Input(format!("failed to read {}: {e}", d.display())))?;
        let mut paths: Vec<_> = entries.filter_map(Result::ok).map(|e| e.path()).collect();
        paths.sort();
        for path in paths {
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default();
            if path.is_dir() {
                if !name.starts_with('.')
                    && !matches!(name, "target" | "dbt_packages" | "node_modules" | "logs")
                {
                    stack.push(path);
                }
            } else if name.ends_with(".md") {
                if let Ok(text) = std::fs::read_to_string(&path) {
                    project.blocks.extend(convert::dbt::doc_blocks(&text));
                }
            } else if name.ends_with(".yml") || name.ends_with(".yaml") {
                let shown = path.display().to_string();
                let Ok(text) = std::fs::read_to_string(&path) else {
                    project
                        .notes
                        .push(format!("{shown}: skipped, not readable as text"));
                    continue;
                };
                // a .yml is YAML, even when it opens with `{`
                match convert::from_yaml::<serde_json::Value>(&text) {
                    Ok(doc) if convert::dbt::is_dbt(&doc) => project.files.push((shown, text)),
                    Ok(_) => {}
                    Err(e) => project
                        .notes
                        .push(format!("{shown}: skipped, not valid YAML ({e})")),
                }
            }
        }
    }
    if project.files.is_empty() {
        return Err(CliError::Input(format!(
            "{}: no dbt semantic models or model properties found",
            dir.display()
        )));
    }
    Ok(project)
}

fn file_stem(path: &Path) -> String {
    path.file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("ontology")
        .to_string()
}

/// `https://example.com/<file name>/`, for IRIs minted without `--base`.
fn default_base(path: &Path) -> String {
    format!("https://example.com/{}/", convert::slug(&file_stem(path)))
}

/// A prefix for the minted namespace: its last path segment's letters
/// (`shop` for `https://example.com/shop/`), else `ex`.
fn base_prefix(base: &str) -> String {
    let segment = base
        .trim_end_matches(['/', '#', ':'])
        .rsplit(['/', '#', ':'])
        .next()
        .unwrap_or_default();
    let letters: String = segment
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .collect::<String>()
        .to_ascii_lowercase();
    if letters.starts_with(|c: char| c.is_ascii_alphabetic()) && letters.len() <= 12 {
        letters
    } else {
        "ex".to_string()
    }
}

fn output_format(format: OntologyOutputFormat) -> OutputFormat {
    match format {
        OntologyOutputFormat::Turtle => OutputFormat::Turtle,
        OntologyOutputFormat::Jsonld => OutputFormat::JsonLd,
    }
}

/// Serialize and write to `path`, or to stdout.
fn emit(
    quads: &[Quad],
    prefixes: &BTreeMap<String, String>,
    format: OutputFormat,
    path: Option<&Path>,
) -> CliResult<()> {
    let text = write::serialize(quads, prefixes, format)
        .map_err(|e| CliError::Input(format!("failed to write RDF: {e}")))?;
    match path {
        Some(path) => std::fs::write(path, &text)
            .map_err(|e| CliError::Input(format!("failed to write {}: {e}", path.display()))),
        None => {
            print!("{text}");
            Ok(())
        }
    }
}

fn read_bytes(path: &Path) -> CliResult<Vec<u8>> {
    std::fs::read(path)
        .map_err(|e| CliError::Input(format!("failed to read {}: {e}", path.display())))
}

fn read_sheets(path: &Path) -> CliResult<Vec<tabular::Sheet>> {
    let failed = |e: tabular::TabularError| CliError::Input(format!("{}: {e}", path.display()));
    match source(path, None) {
        Source::Workbook => tabular::read_xlsx(&read_bytes(path)?).map_err(failed),
        _ => Ok(vec![
            tabular::read_csv(&read_text(path)?, &file_stem(path)).map_err(failed)?
        ]),
    }
}

fn run_from_tables(args: &OntologyFromArgs, kind: Source) -> CliResult<()> {
    if !args.seed.is_empty() {
        return Err(CliError::Usage(
            "--seed slices an RDF vocabulary; it doesn't apply to tables".into(),
        ));
    }
    let base = args
        .base
        .clone()
        .unwrap_or_else(|| default_base(&args.file));
    let mut notes = Vec::new();
    let failed =
        |e: &dyn std::fmt::Display| CliError::Input(format!("{}: {e}", args.file.display()));
    let tables = |model: convert::Model| convert::emit(&model, &EmitOptions { base: base.clone() });
    let kind = match kind {
        Source::Yaml => {
            let doc = read_document(&args.file)?;
            if convert::ossie::is_semantic_model(&doc) || convert::ossie::is_ontology(&doc) {
                Source::Ossie
            } else if convert::dbt::is_dbt(&doc) {
                Source::Dbt
            } else {
                Source::Snowflake
            }
        }
        other => other,
    };
    let out = match kind {
        Source::Sql => {
            let (model, skipped) =
                convert::sql::read(&read_text(&args.file)?).map_err(|e| failed(&e))?;
            if skipped > 0 {
                notes.push(format!(
                    "{skipped} statement(s) no SQL dialect could parse were skipped"
                ));
            }
            tables(model)
        }
        Source::Snowflake => tables(
            convert::semantic::from_snowflake(&read_text(&args.file)?).map_err(|e| failed(&e))?,
        ),
        Source::Genie => {
            let export = read_document(&args.file)?;
            let comments = args.comments.as_deref().map(read_document).transpose()?;
            if comments.is_none() {
                notes.push(
                    "a Genie export has no descriptions; pass Unity Catalog's comments with --comments"
                        .into(),
                );
            }
            tables(
                convert::semantic::from_genie(&export, comments.as_ref())
                    .map_err(|e| failed(&e))?,
            )
        }
        Source::Dbt => {
            let project = if args.file.is_dir() {
                dbt_project(&args.file)?
            } else {
                DbtProject {
                    files: vec![(args.file.display().to_string(), read_text(&args.file)?)],
                    blocks: HashMap::new(),
                    notes: Vec::new(),
                }
            };
            notes.extend(project.notes);
            let (model, left_out) = convert::dbt::read(&project.files, &project.blocks);
            if model.tables.is_empty() {
                return Err(failed(&"no dbt semantic models or model properties"));
            }
            if left_out > 0 {
                notes.push(format!(
                    "{left_out} column(s) of models that semantic models stand for were left out (no relation or accepted values, and no semantic model uses them)"
                ));
            }
            tables(model)
        }
        Source::Ossie => {
            let doc = convert::ossie::document(&read_text(&args.file)?).map_err(|e| failed(&e))?;
            if convert::ossie::is_ontology(&doc) {
                convert::ossie::ontology(&doc, &base)
            } else if convert::ossie::is_semantic_model(&doc) {
                tables(convert::ossie::semantic_model(&doc))
            } else {
                return Err(failed(
                    &"not an Ossie semantic model (`datasets:`) or ontology (`ontology:`)",
                ));
            }
        }
        _ => tables(tabular::model(&read_sheets(&args.file)?, args.enum_max)),
    };
    // an empty ontology is never a result: say what was found instead
    if out.classes == 0 {
        return Err(CliError::Input(format!(
            "{}: no classes found to make an ontology from",
            args.file.display()
        )));
    }
    notes.extend(out.notes.iter().cloned());
    let declared = vec![(base_prefix(&base), base.clone())];
    let format = output_format(args.format);
    match &args.catalog_out {
        Some(path) => {
            emit(
                &out.ontology,
                &write::used_prefixes(&out.ontology, &declared),
                format,
                args.output.as_deref(),
            )?;
            emit(
                &out.catalog,
                &write::used_prefixes(&out.catalog, &declared),
                format,
                Some(path),
            )?;
        }
        None => {
            let all: Vec<Quad> = out.ontology.iter().chain(&out.catalog).cloned().collect();
            emit(
                &all,
                &write::used_prefixes(&all, &declared),
                format,
                args.output.as_deref(),
            )?;
        }
    }

    let mut summary = format!(
        "{}, {}",
        count(out.classes, "class", "classes"),
        count(out.properties, "property", "properties")
    );
    if out.enum_classes > 0 {
        summary.push_str(&format!(
            "; {} made {}, with {}",
            count(out.enum_classes, "enumeration", "enumerations"),
            if out.enum_classes == 1 {
                "a class"
            } else {
                "classes"
            },
            count(out.records, "catalog record", "catalog records")
        ));
    }
    if out.keys_left_out > 0 {
        summary.push_str(&format!(
            "; {} left out",
            count(
                out.keys_left_out,
                "primary key column",
                "primary key columns"
            )
        ));
    }
    eprintln!("{summary}");
    for note in notes {
        eprintln!("{note}");
    }
    if args.base.is_none() {
        eprintln!("IRIs minted under {base}; set --base to your own namespace");
    }
    eprintln!("run `fluree ontology check` on the result to see what an HTTP reader will read");
    Ok(())
}

fn run_from(args: &OntologyFromArgs) -> CliResult<()> {
    let kind = source(&args.file, args.input_format);
    if kind != Source::Rdf {
        return run_from_tables(args, kind);
    }
    let rdf_format = match args.input_format {
        Some(OntologyFromFormat::Turtle) => Some(OntologyInputFormat::Turtle),
        Some(OntologyFromFormat::Jsonld) => Some(OntologyInputFormat::Jsonld),
        _ => None,
    };
    let (parsed, _) = parse_file(&args.file, rdf_format)?;
    let out = write::rewrite(&parsed, &args.seed, args.depth)
        .map_err(|e| CliError::Usage(e.to_string()))?;
    let prefixes = write::used_prefixes(&out.quads, &parsed.prefixes);
    emit(
        &out.quads,
        &prefixes,
        output_format(args.format),
        args.output.as_deref(),
    )?;

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

pub fn run_catalog(action: CatalogAction) -> CliResult<()> {
    match action {
        CatalogAction::From(args) => run_catalog_from(&args),
    }
}

fn run_catalog_from(args: &CatalogFromArgs) -> CliResult<()> {
    let sheets = read_sheets(&args.file)?;
    let sheet = match &args.sheet {
        Some(name) => sheets
            .iter()
            .find(|s| s.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| {
                CliError::Usage(format!(
                    "{} has no sheet {name:?} (its sheets: {})",
                    args.file.display(),
                    sheets
                        .iter()
                        .map(|s| s.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            })?,
        None => sheets
            .first()
            .ok_or_else(|| CliError::Input(format!("{} has no table", args.file.display())))?,
    };
    let base = args
        .base
        .clone()
        .unwrap_or_else(|| default_base(&args.file));
    // the ontology's prefixes, so its class reads as it does there
    let mut declared = Vec::new();
    let class = match &args.ontology {
        Some(path) => {
            let (parsed, _) = parse_file(path, None)?;
            let ontology = parsed.split().ontology;
            declared.extend(parsed.prefixes.iter().cloned());
            fluree_db_ontology::slice::resolve_seed(&args.class, &ontology, &parsed.prefixes)
                .map_err(|e| CliError::Usage(e.to_string().replace("--seed", "--class")))?
        }
        None if args.class.contains("://") || args.class.starts_with("urn:") => args.class.clone(),
        None => format!(
            "{base}{}",
            convert::pascal(&convert::class_label(&args.class))
        ),
    };
    let options = CatalogOptions {
        class: class.clone(),
        label: args.label.clone(),
        alt: args.alt.clone(),
        code: args.code.clone(),
        definition: args.definition.clone(),
        describe: args.describe.clone(),
        id: args.id.clone(),
        base: base.clone(),
    };
    let out = catalog::from_sheet(sheet, &options).map_err(|e| CliError::Usage(e.to_string()))?;
    declared.push((base_prefix(&base), base.clone()));
    emit(
        &out.quads,
        &write::used_prefixes(&out.quads, &declared),
        output_format(args.format),
        args.output.as_deref(),
    )?;

    let mut summary = format!("{} of <{class}>", count(out.records, "record", "records"));
    if out.merged > 0 {
        summary.push_str(&format!(
            "; {} folded into an earlier record with the same id",
            count(out.merged, "row", "rows")
        ));
    }
    if out.kept_apart > 0 {
        summary.push_str(&format!(
            "; {} kept apart from another whose id reads alike in an IRI (suffixed)",
            count(out.kept_apart, "record", "records")
        ));
    }
    if out.without_label > 0 {
        summary.push_str(&format!(
            "; {} without a name left out",
            count(out.without_label, "row", "rows")
        ));
    }
    eprintln!("{summary}");
    let e = &out.estimate;
    eprintln!(
        "About {} serialized tokens, repeated in every extraction window (a window holds {} with the ontology and the text).",
        thousands(e.catalog_tokens),
        thousands(TOKENS_PER_WINDOW)
    );
    if e.tokens > TOKENS_PER_WINDOW {
        eprintln!(
            "{}",
            "warning: this catalog alone is more than one window holds; send the records the documents need, or split it by class".yellow()
        );
    } else if e.document_chars < MAX_TEXT_CHARS {
        eprintln!(
            "{}",
            format!(
                "warning: with this catalog a window has room for about {} characters of text",
                thousands(e.document_chars)
            )
            .yellow()
        );
    }
    if args.base.is_none() {
        eprintln!("IRIs minted under {base}; set --base to your own namespace");
    }
    if args.ontology.is_none() && !(args.class.contains("://") || args.class.starts_with("urn:")) {
        eprintln!("class minted as <{class}>; pass --ontology to use your ontology's class");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_prefix_is_named_for_the_namespace() {
        assert_eq!(base_prefix("https://example.com/shop/"), "shop");
        assert_eq!(base_prefix("https://example.com/crm#"), "crm");
        assert_eq!(base_prefix("urn:acme:crm:"), "crm");
        // a segment that isn't a usable prefix falls back to ex
        assert_eq!(base_prefix("https://example.com/2026/"), "ex");
        assert_eq!(
            base_prefix("https://example.com/a-very-long-namespace-name/"),
            "ex"
        );
    }
}
