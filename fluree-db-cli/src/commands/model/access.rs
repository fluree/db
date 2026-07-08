//! `fluree model access` — the access-profile policy compiler.
//!
//! Users declare intent ("apps may write Leads"); this module compiles it to
//! the cheapest enforcement shape that expresses it and transacts the result
//! as ordinary data:
//!
//! * a **policy class** (`rdfs:Class`) — the assignment unit grants and
//!   tokens carry;
//! * a **view policy** (`f:onClass` — exact for reads);
//! * a **modify policy** as a **property allow-list** (`f:onProperty` +
//!   `f:allow`) — class-targeted modify cannot cover new-subject inserts and
//!   `f:query` evaluates against pre-state, so the allow-list is the correct
//!   (and cheapest) write shape today;
//! * a declarative **profile node** recording the intent (type, properties,
//!   compiler version) so `enable` is idempotent and future `sync`/`verify`
//!   re-derive instead of reverse-engineering.
//!
//! Exactness honesty: a property allow-list is only class-exact when the
//! properties are unique to the class. The compiler partitions derived
//! properties by observed usage, discloses shared-property blast radius, and
//! requires `--allow-shared` to include them. `rdf:type` is always included
//! (required for creation) and always flagged: until the engine supports
//! object-value constraints on type flakes, a allow_list holder can assert
//! other types using only allowed properties.

use serde_json::{json, Value};

use super::{query, resolve_mode, upsert};
use crate::cli::ModelAccessAction;
use crate::context::LedgerMode;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;

const F: &str = "https://ns.flur.ee/db#";
const FM: &str = "https://ns.flur.ee/model#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDFS_CLASS: &str = "http://www.w3.org/2000/01/rdf-schema#Class";
const RDFS_LABEL: &str = "http://www.w3.org/2000/01/rdf-schema#label";
const SH: &str = "http://www.w3.org/ns/shacl#";
const COMPILER_VERSION: &str = "1";

/// Access profiles: what the compiled policy set permits.
#[derive(Clone, Copy, PartialEq)]
enum Profile {
    /// View entities of the class (class-exact today).
    Read,
    /// Create + edit entities of the class (property-compiled).
    Write,
    /// Submit without reading back (write-only; the anonymous-form shape).
    Intake,
}

impl Profile {
    fn parse(s: &str) -> CliResult<Self> {
        match s {
            "read" => Ok(Self::Read),
            "write" => Ok(Self::Write),
            "intake" => Ok(Self::Intake),
            other => Err(CliError::Usage(format!(
                "profile must be read | write | intake (got '{other}')"
            ))),
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Intake => "intake",
        }
    }

    fn wants_view(&self) -> bool {
        matches!(self, Self::Read | Self::Write)
    }

    fn wants_modify(&self) -> bool {
        matches!(self, Self::Write | Self::Intake)
    }
}

pub async fn run(action: &ModelAccessAction, dirs: &FlureeDir, direct: bool) -> CliResult<()> {
    match action {
        ModelAccessAction::Enable {
            dataset,
            profile,
            entity,
            properties,
            allow_shared,
            class_iri,
            space,
            connected,
            dry_run,
            remote,
        } => {
            run_enable(
                dataset,
                profile,
                entity,
                properties,
                *allow_shared,
                class_iri.as_deref(),
                space.as_deref(),
                connected.as_deref(),
                *dry_run,
                remote.as_deref(),
                dirs,
                direct,
            )
            .await
        }
        ModelAccessAction::Show { dataset, remote } => {
            run_show(dataset, remote.as_deref(), dirs, direct).await
        }
        ModelAccessAction::Verify { dataset, remote } => {
            run_verify(dataset, remote.as_deref(), dirs, direct).await
        }
        ModelAccessAction::Sync {
            dataset,
            dry_run,
            remote,
        } => run_sync(dataset, *dry_run, remote.as_deref(), dirs, direct).await,
    }
}

// ── enable ──────────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn run_enable(
    dataset: &str,
    profile_str: &str,
    entity: &str,
    explicit_properties: &[String],
    allow_shared: bool,
    class_iri_override: Option<&str>,
    space: Option<&str>,
    connected: Option<&str>,
    dry_run: bool,
    remote: Option<&str>,
    dirs: &FlureeDir,
    direct: bool,
) -> CliResult<()> {
    let profile = Profile::parse(profile_str)?;
    require_absolute_iri("--entity", entity)?;
    for p in explicit_properties {
        require_absolute_iri("--property", p)?;
    }
    if space.is_some() && remote.is_none() {
        return Err(CliError::Usage(
            "--space attaches the policy class to a hosted stack's grant; pass --remote <r>".into(),
        ));
    }
    let connected_steps = match connected {
        Some(path) => {
            if profile != Profile::Read {
                return Err(CliError::Usage(
                    "--connected is supported with --profile read only: relationship gates \
                     evaluate against pre-transaction state, so gating writes would deny \
                     every create (the connection triples are not visible yet). Engine \
                     support for staged-state gates lifts this later."
                        .into(),
                ));
            }
            Some(parse_property_path(path)?)
        }
        None => None,
    };

    let mode = resolve_mode(dataset, remote, dirs, direct).await?;

    // 1. Derive the property surface: explicit → SHACL → observed → fail.
    let (properties, derivation) = if !explicit_properties.is_empty() {
        (explicit_properties.to_vec(), "explicit")
    } else {
        let from_shape = derive_from_shacl(&mode, entity).await?;
        if !from_shape.is_empty() {
            (from_shape, "shacl-shape")
        } else {
            let observed = derive_from_observed(&mode, entity).await?;
            if observed.is_empty() {
                return Err(CliError::Usage(format!(
                    "cannot derive properties for {entity}: no SHACL shape targets it and \
                     no instances exist. Pass --property <iri> explicitly (fail-closed)."
                )));
            }
            (observed, "observed-data")
        }
    };

    // 2. Uniqueness partition: which of these properties do OTHER classes use?
    let mut included: Vec<String> = Vec::new();
    let mut shared: Vec<(String, Vec<String>)> = Vec::new();
    for prop in &properties {
        if prop == RDF_TYPE {
            continue; // handled below, always included + flagged
        }
        let others = classes_sharing_property(&mode, prop, entity).await?;
        if others.is_empty() {
            included.push(prop.clone());
        } else {
            shared.push((prop.clone(), others));
        }
    }
    let mut allowed: Vec<String> = vec![RDF_TYPE.to_string()];
    allowed.extend(included.iter().cloned());
    if allow_shared {
        allowed.extend(shared.iter().map(|(p, _)| p.clone()));
    }

    // 3. Compile.
    let class_iri = class_iri_override.map(String::from).unwrap_or_else(|| {
        format!(
            "{}/access/{}",
            entity.trim_end_matches('/'),
            profile.as_str()
        )
    });
    let graph = compile(
        &class_iri,
        entity,
        profile,
        &allowed,
        connected_steps.as_deref(),
        connected,
        derivation,
        allow_shared,
    );

    // 4. Report.
    let exactness = if shared.is_empty() || !allow_shared {
        "class-exact (all allowed properties are unique to this class)"
    } else {
        "property-approximate (shared properties included — see below)"
    };
    println!("Profile:    {} {}", profile.as_str(), entity);
    println!("Class:      {class_iri}");
    println!("Derivation: {derivation}");
    println!("Exactness:  {exactness}");
    if let Some(path) = connected {
        println!("Connected:  {path} (view gated by relationship to the requesting identity)");
    }
    if profile.wants_modify() {
        println!("Allowed:    {} properties (+ rdf:type)", allowed.len() - 1);
        println!(
            "  note: rdf:type is required for creation; until the engine constrains type\n\
             \x20 object values, allow-list holders can assert other types using only\n\
             \x20 allowed properties."
        );
    }
    for (prop, others) in &shared {
        let status = if allow_shared {
            "INCLUDED (--allow-shared)"
        } else {
            "EXCLUDED (pass --allow-shared to include)"
        };
        println!(
            "  shared: {prop} — also used by {} — {status}",
            others.join(", ")
        );
    }

    if dry_run {
        println!("\n-- dry run; compiled JSON-LD --");
        println!("{}", serde_json::to_string_pretty(&graph)?);
        return Ok(());
    }

    // 5. Transact (data plane — policies live in the ledger). Policies land
    //    BEFORE the grant so a partial failure leaves unused policies
    //    (harmless) rather than a grant naming classes that don't exist.
    upsert(&mode, &graph).await?;
    println!("\nEnabled. Policies written to '{dataset}'.");

    // 6. Grant attachment (hosted stacks): merge the class into the space's
    //    grant so minted tokens carry it.
    if let (Some(space_id), Some(remote_name)) = (space, remote) {
        attach_grant(
            dirs,
            remote_name,
            dataset,
            space_id,
            &class_iri,
            profile.wants_modify(),
        )
        .await?;
    } else {
        println!(
            "Attach to a grant so tokens carry it (or re-run with --space <id>):\n\
             \x20 POST /v1/datasets/{dataset}/grants\n\
             \x20 {{\"scopeType\": \"space\", \"scopeRef\": \"<spaceId>\", \"access\": \"{}\", \"policyClasses\": [\"{class_iri}\"]}}",
            if profile.wants_modify() { "write" } else { "read" },
        );
    }
    Ok(())
}

// ── grant attachment (hosted stacks) ────────────────────────────────────

/// Merge `class_iri` into the space's grant on the dataset via the stack's
/// grants API. System-plane state (grants) is the one place the compiler
/// talks to an API instead of the data plane — grants are router-owned
/// invariants (scope validation, membership checks).
async fn attach_grant(
    dirs: &FlureeDir,
    remote_name: &str,
    dataset: &str,
    space_id: &str,
    class_iri: &str,
    wants_write: bool,
) -> CliResult<()> {
    use crate::config::TomlSyncConfigStore;
    use fluree_db_nameservice::RemoteName;
    use fluree_db_nameservice_sync::{RemoteEndpoint, SyncConfigStore};

    let store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    let remote = store
        .get_remote(&RemoteName::new(remote_name))
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{remote_name}' not found")))?;
    let base_url = match &remote.endpoint {
        RemoteEndpoint::Http { base_url } => base_url.clone(),
        _ => {
            return Err(CliError::Config(format!(
                "remote '{remote_name}' is not an HTTP remote"
            )));
        }
    };

    // The remote points at the data plane (…/v1/fluree); the grants API
    // lives at the stack root.
    let trimmed = base_url.trim_end_matches('/');
    let root = trimmed.strip_suffix("/v1/fluree").ok_or_else(|| {
        CliError::Config(format!(
            "remote '{remote_name}' ({base_url}) does not look like a hosted stack \
             (expected a …/v1/fluree data-plane URL); attach the grant manually"
        ))
    })?;
    let token = remote.auth.token.clone().ok_or_else(|| {
        CliError::Config(format!(
            "remote '{remote_name}' has no auth token; run `fluree auth login` first"
        ))
    })?;

    // Base ledger name (no branch) is the dataset id on the stack.
    let dataset_id = dataset.split(':').next().unwrap_or(dataset);
    let grants_url = format!("{root}/v1/datasets/{dataset_id}/grants");
    let http = reqwest::Client::new();

    // Read existing grants: merge classes, never clobber; keep (or upgrade)
    // the access level.
    let list: Value = http
        .get(&grants_url)
        .bearer_auth(&token)
        .send()
        .await
        .map_err(|e| CliError::Config(format!("grants read failed: {e}")))?
        .error_for_status()
        .map_err(|e| CliError::Config(format!("grants read failed: {e}")))?
        .json()
        .await
        .map_err(|e| CliError::Config(format!("grants read returned non-JSON: {e}")))?;
    let existing = list["grants"]
        .as_array()
        .map(|grants| {
            grants
                .iter()
                .find(|g| {
                    g["scopeType"].as_str() == Some("space")
                        && g["scopeRef"].as_str() == Some(space_id)
                })
                .cloned()
        })
        .unwrap_or(None);

    let mut classes: Vec<String> = existing
        .as_ref()
        .and_then(|g| g["policyClasses"].as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    if !classes.iter().any(|c| c == class_iri) {
        classes.push(class_iri.to_string());
    }

    let existing_access = existing
        .as_ref()
        .and_then(|g| g["access"].as_str())
        .map(String::from);
    let access = match (&existing_access, wants_write) {
        (Some(a), true) if a == "read" => {
            println!("  grant access upgraded read → write (profile requires writes)");
            "write".to_string()
        }
        (Some(a), _) => a.clone(),
        (None, true) => "write".to_string(),
        (None, false) => "read".to_string(),
    };

    let body = json!({
        "scopeType": "space",
        "scopeRef": space_id,
        "access": access,
        "policyClasses": classes,
    });
    http.post(&grants_url)
        .bearer_auth(&token)
        .json(&body)
        .send()
        .await
        .map_err(|e| CliError::Config(format!("grant upsert failed: {e}")))?
        .error_for_status()
        .map_err(|e| CliError::Config(format!("grant upsert rejected: {e}")))?;

    println!(
        "Grant attached: space {space_id} → {dataset_id} ({access}, {} class{})",
        classes.len(),
        if classes.len() == 1 { "" } else { "es" }
    );
    Ok(())
}

fn require_absolute_iri(flag: &str, v: &str) -> CliResult<()> {
    if v.starts_with("http://") || v.starts_with("https://") || v.starts_with("urn:") {
        Ok(())
    } else {
        Err(CliError::Usage(format!(
            "{flag} must be an absolute IRI (got '{v}') — e.g. https://example.org/Lead"
        )))
    }
}

/// Properties declared by a SHACL shape targeting the entity class.
async fn derive_from_shacl(mode: &LedgerMode, entity: &str) -> CliResult<Vec<String>> {
    let q = json!({
        "@context": {"sh": SH},
        "select": ["?path"],
        "where": [
            {"@id": "?shape", "sh:targetClass": {"@id": entity}},
            {"@id": "?shape", "sh:property": {"@id": "?p"}},
            {"@id": "?p", "sh:path": {"@id": "?path"}}
        ]
    });
    Ok(iri_rows(&query(mode, &q).await?))
}

/// Distinct predicates observed on instances of the entity class.
async fn derive_from_observed(mode: &LedgerMode, entity: &str) -> CliResult<Vec<String>> {
    let q = json!({
        "select": ["?p"],
        "where": [
            {"@id": "?s", "@type": entity},
            {"@id": "?s", "?p": "?o"}
        ]
    });
    let mut props = iri_rows(&query(mode, &q).await?);
    props.retain(|p| p != RDF_TYPE);
    props.sort();
    props.dedup();
    Ok(props)
}

/// Other classes whose instances also use this property.
async fn classes_sharing_property(
    mode: &LedgerMode,
    prop: &str,
    entity: &str,
) -> CliResult<Vec<String>> {
    let q = json!({
        "select": ["?c"],
        "where": [
            {"@id": "?s", prop: "?o"},
            {"@id": "?s", "@type": "?c"}
        ]
    });
    let mut classes = iri_rows(&query(mode, &q).await?);
    classes.sort();
    classes.dedup();
    classes.retain(|c| c != entity && !c.starts_with(F));
    Ok(classes)
}

/// Flatten a select result of single-binding rows into IRI strings.
fn iri_rows(result: &Value) -> Vec<String> {
    let rows = match result {
        Value::Array(rows) => rows.as_slice(),
        _ => return vec![],
    };
    rows.iter()
        .filter_map(|row| match row {
            Value::String(s) => Some(s.clone()),
            Value::Array(inner) => inner.first().and_then(|v| v.as_str()).map(String::from),
            Value::Object(o) => o.get("@id").and_then(|v| v.as_str()).map(String::from),
            _ => None,
        })
        .collect()
}

/// One step of a `--connected` property path.
#[derive(Debug, PartialEq)]
struct PathStep {
    iri: String,
    inverse: bool,
}

/// Parse a SPARQL property-path subset: sequence (`/`) of optionally
/// inverse (`^`) angle-bracketed absolute IRIs.
///
/// `"<https://x/memberOf>/^<https://x/team>"` →
/// identity —memberOf→ ?v0 ←team— subject.
fn parse_property_path(path: &str) -> CliResult<Vec<PathStep>> {
    // Split on '/' outside angle brackets (IRIs contain slashes).
    let mut raw_steps: Vec<String> = vec![String::new()];
    let mut in_iri = false;
    for c in path.chars() {
        match c {
            '<' => {
                in_iri = true;
                raw_steps.last_mut().unwrap().push(c);
            }
            '>' => {
                in_iri = false;
                raw_steps.last_mut().unwrap().push(c);
            }
            '/' if !in_iri => raw_steps.push(String::new()),
            _ => raw_steps.last_mut().unwrap().push(c),
        }
    }

    raw_steps
        .iter()
        .map(|raw| {
            let raw = raw.trim();
            let (inverse, rest) = match raw.strip_prefix('^') {
                Some(r) => (true, r.trim()),
                None => (false, raw),
            };
            let iri = rest
                .strip_prefix('<')
                .and_then(|r| r.strip_suffix('>'))
                .ok_or_else(|| {
                    CliError::Usage(format!(
                        "--connected step '{raw}' must be an angle-bracketed IRI, optionally \
                         inverse: ^<iri>. Supported subset: sequence (/) and inverse (^) — \
                         alternatives (|) and transitive (+/*) are not supported yet."
                    ))
                })?;
            if !(iri.starts_with("http://")
                || iri.starts_with("https://")
                || iri.starts_with("urn:"))
            {
                return Err(CliError::Usage(format!(
                    "--connected step IRI must be absolute (got '{iri}')"
                )));
            }
            Ok(PathStep {
                iri: iri.to_string(),
                inverse,
            })
        })
        .collect()
}

/// Expand path steps into `f:query` where-patterns anchored
/// `?$identity` —path→ `?$this`.
fn compile_path_where(steps: &[PathStep]) -> Vec<Value> {
    let n = steps.len();
    let node = |i: usize| -> String {
        if i == 0 {
            "?$identity".to_string()
        } else if i == n {
            "?$this".to_string()
        } else {
            format!("?v{}", i - 1)
        }
    };
    steps
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let (from, to) = (node(i), node(i + 1));
            if s.inverse {
                json!({"@id": to, s.iri.clone(): {"@id": from}})
            } else {
                json!({"@id": from, s.iri.clone(): {"@id": to}})
            }
        })
        .collect()
}

/// Compile the profile into its JSON-LD artifacts.
///
/// `derivation` and `allow_shared` are recorded on the intent node so
/// `sync` knows whether (and how) the property surface may be re-derived:
/// explicit lists are authored and never touched; shape/observed lists
/// re-derive with the same shared-property choice the author made.
#[allow(clippy::too_many_arguments)]
fn compile(
    class_iri: &str,
    entity: &str,
    profile: Profile,
    allowed: &[String],
    connected_steps: Option<&[PathStep]>,
    connected_raw: Option<&str>,
    derivation: &str,
    allow_shared: bool,
) -> Value {
    let mut nodes: Vec<Value> = Vec::new();

    // The policy class — the assignment unit grants and tokens carry.
    nodes.push(json!({
        "@id": class_iri,
        "@type": RDFS_CLASS,
        RDFS_LABEL: format!("{} access: {}", profile.as_str(), entity),
    }));

    // The declarative profile node — intent, not artifact. `sync`/`verify`
    // re-derive from this; the compiler version makes upgrades a recompile.
    let mut profile_node = json!({
        "@id": format!("{class_iri}/profile"),
        "@type": format!("{FM}AccessProfile"),
        format!("{FM}profile"): profile.as_str(),
        format!("{FM}onType"): {"@id": entity},
        format!("{FM}property"): allowed.iter().map(|p| json!({"@id": p})).collect::<Vec<_>>(),
        format!("{FM}policyClass"): {"@id": class_iri},
        format!("{FM}compilerVersion"): COMPILER_VERSION,
        format!("{FM}derivation"): derivation,
        format!("{FM}allowShared"): allow_shared,
    });
    if let Some(path) = connected_raw {
        profile_node[format!("{FM}connected")] = json!(path);
    }
    nodes.push(profile_node);

    if profile.wants_view() {
        let mut view = json!({
            "@id": format!("{class_iri}/view"),
            "@type": [format!("{F}AccessPolicy"), class_iri],
            format!("{F}action"): {"@id": format!("{F}view")},
            format!("{F}onClass"): {"@id": entity},
        });
        match connected_steps {
            Some(steps) => {
                // Relationship gate: the flake is visible when the query
                // returns rows — `f:query` REPLACES `f:allow` (they are
                // alternative decision modes in the policy model).
                let where_patterns = compile_path_where(steps);
                view[format!("{F}query")] =
                    json!(serde_json::to_string(&json!({"where": where_patterns}))
                        .expect("static JSON serializes"));
            }
            None => {
                view[format!("{F}allow")] = json!(true);
            }
        }
        nodes.push(view);
    }
    if profile.wants_modify() {
        nodes.push(json!({
            "@id": format!("{class_iri}/modify"),
            "@type": [format!("{F}AccessPolicy"), class_iri],
            format!("{F}action"): {"@id": format!("{F}modify")},
            format!("{F}allow"): true,
            format!("{F}onProperty"): allowed.iter().map(|p| json!({"@id": p})).collect::<Vec<_>>(),
        }));
    }

    json!({"@graph": nodes})
}

// ── show ────────────────────────────────────────────────────────────────

async fn run_show(
    dataset: &str,
    remote: Option<&str>,
    dirs: &FlureeDir,
    direct: bool,
) -> CliResult<()> {
    let mode = resolve_mode(dataset, remote, dirs, direct).await?;
    let q = json!({
        "@context": {"fm": FM},
        "select": {"?profile": ["*"]},
        "where": [{"@id": "?profile", "@type": "fm:AccessProfile"}]
    });
    let result = query(&mode, &q).await?;
    let rows = result.as_array().cloned().unwrap_or_default();
    if rows.is_empty() {
        println!("No access profiles on '{dataset}'.");
        println!("Enable one: fluree model access enable {dataset} --profile write --entity <iri>");
        return Ok(());
    }
    println!("Access profiles on '{dataset}':\n");
    for row in &rows {
        let get = |k: &str| -> String {
            row.get(k)
                .map(render_value)
                .unwrap_or_else(|| "-".to_string())
        };
        println!("• {}", get("@id"));
        println!("    profile:  {}", get("fm:profile"));
        println!("    type:     {}", get("fm:onType"));
        println!("    class:    {}", get("fm:policyClass"));
        println!("    props:    {}", get("fm:property"));
        println!("    compiler: v{}", get("fm:compilerVersion"));
    }
    Ok(())
}

// ── verify / sync ───────────────────────────────────────────────────────

/// A stored `fm:AccessProfile` intent node, decoded.
struct StoredProfile {
    class_iri: String,
    profile: Profile,
    entity: String,
    /// Full allow-list as stored (rdf:type included).
    allowed: Vec<String>,
    connected: Option<String>,
    /// How the property surface was derived at enable time. Absent on
    /// nodes written by older compilers or other front ends — treated as
    /// `explicit` (the safest reading: never rewrite an authored list).
    derivation: Option<String>,
    allow_shared: bool,
}

async fn fetch_profiles(mode: &LedgerMode) -> CliResult<Vec<StoredProfile>> {
    let q = json!({
        "@context": {"fm": FM},
        "select": {"?profile": ["*"]},
        "where": [{"@id": "?profile", "@type": "fm:AccessProfile"}]
    });
    let result = query(mode, &q).await?;
    let rows = result.as_array().cloned().unwrap_or_default();
    let mut out = Vec::new();
    for row in &rows {
        let Some(class_iri) = value_id(row.get("fm:policyClass")) else {
            continue;
        };
        let Some(profile_str) = value_str(row.get("fm:profile")) else {
            continue;
        };
        let Ok(profile) = Profile::parse(&profile_str) else {
            continue;
        };
        let Some(entity) = value_id(row.get("fm:onType")) else {
            continue;
        };
        out.push(StoredProfile {
            class_iri,
            profile,
            entity,
            allowed: value_ids(row.get("fm:property")),
            connected: value_str(row.get("fm:connected")),
            derivation: value_str(row.get("fm:derivation")),
            allow_shared: value_bool(row.get("fm:allowShared")).unwrap_or(false),
        });
    }
    Ok(out)
}

/// Read a subject's properties; `None` when the subject has no triples.
async fn fetch_node(mode: &LedgerMode, id: &str) -> CliResult<Option<Value>> {
    let q = json!({
        "@context": {"f": F},
        "select": {id: ["*"]},
    });
    let result = query(mode, &q).await?;
    let node = match result {
        Value::Array(mut a) => {
            if a.is_empty() {
                return Ok(None);
            }
            a.remove(0)
        }
        other => other,
    };
    let Some(obj) = node.as_object() else {
        return Ok(None);
    };
    if obj.keys().all(|k| k == "@id") {
        return Ok(None);
    }
    Ok(Some(node))
}

async fn run_verify(
    dataset: &str,
    remote: Option<&str>,
    dirs: &FlureeDir,
    direct: bool,
) -> CliResult<()> {
    let mode = resolve_mode(dataset, remote, dirs, direct).await?;
    let profiles = fetch_profiles(&mode).await?;
    if profiles.is_empty() {
        println!("No access profiles on '{dataset}' — nothing to verify.");
        return Ok(());
    }

    let mut drifted = 0usize;
    for sp in &profiles {
        println!("• {} ({} {})", sp.class_iri, sp.profile.as_str(), sp.entity);
        let steps = match &sp.connected {
            Some(path) => Some(parse_property_path(path)?),
            None => None,
        };
        let expected = compile(
            &sp.class_iri,
            &sp.entity,
            sp.profile,
            &sp.allowed,
            steps.as_deref(),
            sp.connected.as_deref(),
            sp.derivation.as_deref().unwrap_or("explicit"),
            sp.allow_shared,
        );
        let nodes = expected["@graph"].as_array().expect("compile emits @graph");

        let mut profile_drifted = false;
        for kind in ["view", "modify"] {
            let id = format!("{}/{kind}", sp.class_iri);
            let want = nodes
                .iter()
                .find(|n| n["@id"].as_str() == Some(id.as_str()));
            let have = fetch_node(&mode, &id).await?;
            match (want, have) {
                (Some(w), Some(h)) => {
                    let drift = diff_policy(w, &h);
                    if drift.is_empty() {
                        println!("    {kind}:   OK");
                    } else {
                        profile_drifted = true;
                        for d in drift {
                            println!("    {kind}:   DRIFT — {d}");
                        }
                    }
                }
                (Some(_), None) => {
                    profile_drifted = true;
                    println!("    {kind}:   MISSING — re-run `enable` (or `sync`) to recompile");
                }
                (None, Some(_)) => {
                    profile_drifted = true;
                    println!(
                        "    {kind}:   UNEXPECTED — the {} profile compiles no {kind} policy, \
                         but one exists in the ledger",
                        sp.profile.as_str()
                    );
                }
                (None, None) => {}
            }
        }
        if profile_drifted {
            drifted += 1;
        }
    }

    println!(
        "\n{} profile{} checked, {drifted} drifted",
        profiles.len(),
        if profiles.len() == 1 { "" } else { "s" }
    );
    if drifted > 0 {
        Err(CliError::Config(format!(
            "{drifted} profile(s) drifted from their declared intent — \
             `fluree model access sync` recompiles derivable ones; re-run `enable` for the rest"
        )))
    } else {
        Ok(())
    }
}

async fn run_sync(
    dataset: &str,
    dry_run: bool,
    remote: Option<&str>,
    dirs: &FlureeDir,
    direct: bool,
) -> CliResult<()> {
    let mode = resolve_mode(dataset, remote, dirs, direct).await?;
    let profiles = fetch_profiles(&mode).await?;
    if profiles.is_empty() {
        println!("No access profiles on '{dataset}' — nothing to sync.");
        return Ok(());
    }

    for sp in &profiles {
        let derivation = sp.derivation.as_deref().unwrap_or("explicit");
        println!("• {} ({derivation})", sp.class_iri);

        let derived = match derivation {
            "shacl-shape" => derive_from_shacl(&mode, &sp.entity).await?,
            "observed-data" => derive_from_observed(&mode, &sp.entity).await?,
            _ => {
                println!("    explicit property list — skipped (re-run `enable` to change)");
                continue;
            }
        };
        if derived.is_empty() {
            println!(
                "    derivation source vanished (no shape / no instances) — left \
                 unchanged; re-run `enable` to redeclare"
            );
            continue;
        }

        // Same uniqueness partition as `enable`, honoring the stored choice.
        let mut new_allowed: Vec<String> = vec![RDF_TYPE.to_string()];
        for prop in &derived {
            if prop == RDF_TYPE {
                continue;
            }
            let others = classes_sharing_property(&mode, prop, &sp.entity).await?;
            if others.is_empty() || sp.allow_shared {
                new_allowed.push(prop.clone());
            } else {
                println!(
                    "    shared: {prop} — also used by {} — EXCLUDED",
                    others.join(", ")
                );
            }
        }

        let mut want = new_allowed.clone();
        want.sort();
        let mut have = sp.allowed.clone();
        have.sort();
        if want == have {
            println!("    unchanged");
            continue;
        }
        let added: Vec<&String> = want.iter().filter(|p| !have.contains(p)).collect();
        let removed: Vec<&String> = have.iter().filter(|p| !want.contains(p)).collect();
        if !added.is_empty() {
            println!(
                "    + {}",
                added
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        if !removed.is_empty() {
            println!(
                "    - {}",
                removed
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        let steps = match &sp.connected {
            Some(path) => Some(parse_property_path(path)?),
            None => None,
        };
        let graph = compile(
            &sp.class_iri,
            &sp.entity,
            sp.profile,
            &new_allowed,
            steps.as_deref(),
            sp.connected.as_deref(),
            derivation,
            sp.allow_shared,
        );
        if dry_run {
            println!("    (dry run — not transacted)");
            continue;
        }
        upsert(&mode, &graph).await?;
        println!("    recompiled");
    }
    Ok(())
}

/// Semantic comparison of a compiled policy node against the node actually
/// in the ledger. Tolerates compacted keys/values in query results
/// (`f:action` vs the full IRI) — drift means a REAL difference.
fn diff_policy(expected: &Value, actual: &Value) -> Vec<String> {
    let mut drift = Vec::new();

    let want_types: Vec<String> = value_ids(expected.get("@type"));
    let have_types: Vec<String> = value_ids(actual.get("@type"))
        .iter()
        .map(|t| expand(t))
        .collect();
    for t in &want_types {
        if !have_types.contains(t) {
            drift.push(format!("missing @type {t}"));
        }
    }

    for key in [format!("{F}action"), format!("{F}onClass")] {
        if let Some(want) = expected.get(&key) {
            let want_id = value_id(Some(want)).map(|s| expand(&s));
            let have_id = get_prop(actual, &key)
                .and_then(|v| value_id(Some(v)))
                .map(|s| expand(&s));
            if want_id != have_id {
                drift.push(format!(
                    "{key}: expected {}, found {}",
                    want_id.as_deref().unwrap_or("(none)"),
                    have_id.as_deref().unwrap_or("(none)")
                ));
            }
        }
    }

    let on_property = format!("{F}onProperty");
    if let Some(want) = expected.get(&on_property) {
        let mut want_props: Vec<String> = value_ids(Some(want)).iter().map(|s| expand(s)).collect();
        let mut have_props: Vec<String> = get_prop(actual, &on_property)
            .map(|v| value_ids(Some(v)))
            .unwrap_or_default()
            .iter()
            .map(|s| expand(s))
            .collect();
        want_props.sort();
        have_props.sort();
        if want_props != have_props {
            let extra: Vec<&String> = have_props
                .iter()
                .filter(|p| !want_props.contains(p))
                .collect();
            let missing: Vec<&String> = want_props
                .iter()
                .filter(|p| !have_props.contains(p))
                .collect();
            let mut parts = Vec::new();
            if !extra.is_empty() {
                parts.push(format!(
                    "extra in ledger: {}",
                    extra
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            if !missing.is_empty() {
                parts.push(format!(
                    "missing: {}",
                    missing
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            drift.push(format!("allow-list differs ({})", parts.join("; ")));
        }
    }

    let query_key = format!("{F}query");
    let allow_key = format!("{F}allow");
    if let Some(want_q) = expected.get(&query_key) {
        let want_parsed: Option<Value> =
            value_str(Some(want_q)).and_then(|s| serde_json::from_str(&s).ok());
        let have_parsed: Option<Value> = get_prop(actual, &query_key)
            .and_then(|v| value_str(Some(v)))
            .and_then(|s| serde_json::from_str(&s).ok());
        if want_parsed != have_parsed {
            drift.push("relationship gate (f:query) differs".into());
        }
    } else if expected.get(&allow_key).is_some()
        && get_prop(actual, &allow_key).and_then(|v| value_bool(Some(v))) != Some(true)
    {
        drift.push("f:allow is not true".into());
    }

    drift
}

// ── JSON-LD value helpers (query results come back compacted) ───────────

/// Expand a compacted IRI back to its full form for comparisons.
fn expand(s: &str) -> String {
    if let Some(rest) = s.strip_prefix("f:") {
        format!("{F}{rest}")
    } else if let Some(rest) = s.strip_prefix("fm:") {
        format!("{FM}{rest}")
    } else {
        s.to_string()
    }
}

/// Look up a property on a result node by full IRI, tolerating the
/// compacted key form the query's @context produces.
fn get_prop<'a>(node: &'a Value, full: &str) -> Option<&'a Value> {
    if let Some(v) = node.get(full) {
        return Some(v);
    }
    let local = full.rsplit(['#', '/']).next().unwrap_or(full);
    for prefix in ["f:", "fm:"] {
        if let Some(v) = node.get(format!("{prefix}{local}")) {
            return Some(v);
        }
    }
    None
}

fn value_id(v: Option<&Value>) -> Option<String> {
    let ids = value_ids(v);
    ids.into_iter().next()
}

fn value_ids(v: Option<&Value>) -> Vec<String> {
    let mut out = Vec::new();
    let Some(v) = v else { return out };
    let items: Vec<&Value> = match v {
        Value::Array(a) => a.iter().collect(),
        other => vec![other],
    };
    for item in items {
        match item {
            Value::String(s) => out.push(s.clone()),
            Value::Object(o) => {
                if let Some(id) = o.get("@id").and_then(|x| x.as_str()) {
                    out.push(id.to_string());
                } else if let Some(val) = o.get("@value").and_then(|x| x.as_str()) {
                    out.push(val.to_string());
                }
            }
            _ => {}
        }
    }
    out
}

fn value_str(v: Option<&Value>) -> Option<String> {
    let v = v?;
    let item = match v {
        Value::Array(a) => a.first()?,
        other => other,
    };
    match item {
        Value::String(s) => Some(s.clone()),
        Value::Object(o) => o.get("@value").and_then(|x| x.as_str()).map(String::from),
        _ => None,
    }
}

fn value_bool(v: Option<&Value>) -> Option<bool> {
    let v = v?;
    let item = match v {
        Value::Array(a) => a.first()?,
        other => other,
    };
    match item {
        Value::Bool(b) => Some(*b),
        Value::Object(o) => o.get("@value").and_then(serde_json::Value::as_bool),
        _ => None,
    }
}

fn render_value(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Object(o) => o
            .get("@id")
            .and_then(|x| x.as_str())
            .unwrap_or("-")
            .to_string(),
        Value::Array(items) => items
            .iter()
            .map(render_value)
            .collect::<Vec<_>>()
            .join(", "),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compile_write_profile_emits_class_profile_view_and_allow_list() {
        let allowed = vec![RDF_TYPE.to_string(), "https://example.org/name".to_string()];
        let graph = compile(
            "https://example.org/Lead/access/write",
            "https://example.org/Lead",
            Profile::Write,
            &allowed,
            None,
            None,
            "explicit",
            false,
        );
        let nodes = graph["@graph"].as_array().unwrap();
        assert_eq!(nodes.len(), 4, "class + profile + view + modify");

        let modify = nodes
            .iter()
            .find(|n| n["@id"].as_str().unwrap().ends_with("/modify"))
            .expect("modify policy");
        let types: Vec<&str> = modify["@type"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t.as_str().unwrap())
            .collect();
        assert!(types.contains(&"https://ns.flur.ee/db#AccessPolicy"));
        assert!(types.contains(&"https://example.org/Lead/access/write"));
        let props = modify["https://ns.flur.ee/db#onProperty"]
            .as_array()
            .unwrap();
        assert_eq!(props.len(), 2, "rdf:type + name");
    }

    #[test]
    fn compile_read_profile_has_no_modify_policy() {
        let graph = compile(
            "https://example.org/Lead/access/read",
            "https://example.org/Lead",
            Profile::Read,
            &[RDF_TYPE.to_string()],
            None,
            None,
            "explicit",
            false,
        );
        let nodes = graph["@graph"].as_array().unwrap();
        assert_eq!(nodes.len(), 3, "class + profile + view (no modify)");
        assert!(nodes
            .iter()
            .all(|n| !n["@id"].as_str().unwrap().ends_with("/modify")));
    }

    #[test]
    fn compile_intake_profile_has_no_view_policy() {
        let graph = compile(
            "https://example.org/Lead/access/intake",
            "https://example.org/Lead",
            Profile::Intake,
            &[RDF_TYPE.to_string()],
            None,
            None,
            "explicit",
            false,
        );
        let nodes = graph["@graph"].as_array().unwrap();
        assert_eq!(nodes.len(), 3, "class + profile + modify (no view)");
        assert!(nodes
            .iter()
            .all(|n| !n["@id"].as_str().unwrap().ends_with("/view")));
    }

    #[test]
    fn profile_parse_rejects_unknown() {
        assert!(Profile::parse("admin").is_err());
    }

    #[test]
    fn absolute_iri_gate() {
        assert!(require_absolute_iri("--entity", "https://example.org/Lead").is_ok());
        assert!(require_absolute_iri("--entity", "urn:example:Lead").is_ok());
        assert!(require_absolute_iri("--entity", "ex:Lead").is_err());
    }
}

#[cfg(test)]
mod connected_tests {
    use super::*;

    #[test]
    fn parses_single_inverse_step() {
        let steps = parse_property_path("^<https://example.org/owner>").unwrap();
        assert_eq!(steps.len(), 1);
        assert!(steps[0].inverse);
        assert_eq!(steps[0].iri, "https://example.org/owner");
    }

    #[test]
    fn parses_sequence_with_inverse_tail() {
        let steps =
            parse_property_path("<https://example.org/memberOf>/^<https://example.org/team>")
                .unwrap();
        assert_eq!(steps.len(), 2);
        assert!(!steps[0].inverse);
        assert!(steps[1].inverse);
    }

    #[test]
    fn rejects_unbracketed_and_transitive() {
        assert!(parse_property_path("https://example.org/owner").is_err());
        assert!(parse_property_path("<https://example.org/parent>+").is_err());
        assert!(parse_property_path("ex:owner").is_err());
    }

    #[test]
    fn path_expands_to_anchored_patterns() {
        let steps =
            parse_property_path("<https://example.org/memberOf>/^<https://example.org/team>")
                .unwrap();
        let patterns = compile_path_where(&steps);
        assert_eq!(patterns.len(), 2);
        // identity —memberOf→ ?v0
        assert_eq!(patterns[0]["@id"], "?$identity");
        assert_eq!(patterns[0]["https://example.org/memberOf"]["@id"], "?v0");
        // ?$this —team→ ?v0 (inverse step targets ?$this as subject)
        assert_eq!(patterns[1]["@id"], "?$this");
        assert_eq!(patterns[1]["https://example.org/team"]["@id"], "?v0");
    }

    #[test]
    fn connected_view_policy_uses_query_not_allow() {
        let steps = parse_property_path("^<https://example.org/owner>").unwrap();
        let g = compile(
            "https://example.org/Lead/access/read",
            "https://example.org/Lead",
            Profile::Read,
            &[RDF_TYPE.to_string()],
            Some(&steps),
            Some("^<https://example.org/owner>"),
            "explicit",
            false,
        );
        let nodes = g["@graph"].as_array().unwrap();
        let view = nodes
            .iter()
            .find(|n| n["@id"].as_str().unwrap().ends_with("/view"))
            .expect("view policy");
        assert!(view.get("https://ns.flur.ee/db#allow").is_none());
        let q = view["https://ns.flur.ee/db#query"].as_str().unwrap();
        let parsed: Value = serde_json::from_str(q).unwrap();
        assert_eq!(parsed["where"][0]["@id"], "?$this");
        // profile node records the raw path
        let profile = nodes
            .iter()
            .find(|n| n["@id"].as_str().unwrap().ends_with("/profile"))
            .unwrap();
        assert_eq!(
            profile["https://ns.flur.ee/model#connected"],
            "^<https://example.org/owner>"
        );
    }
}

#[cfg(test)]
mod verify_tests {
    use super::*;

    fn compiled_write_nodes() -> Vec<Value> {
        let allowed = vec![RDF_TYPE.to_string(), "https://example.org/name".to_string()];
        let g = compile(
            "https://example.org/Lead/access/write",
            "https://example.org/Lead",
            Profile::Write,
            &allowed,
            None,
            None,
            "shacl-shape",
            false,
        );
        g["@graph"].as_array().unwrap().clone()
    }

    fn node(nodes: &[Value], suffix: &str) -> Value {
        nodes
            .iter()
            .find(|n| n["@id"].as_str().unwrap().ends_with(suffix))
            .cloned()
            .unwrap()
    }

    #[test]
    fn identical_nodes_have_no_drift() {
        let nodes = compiled_write_nodes();
        let modify = node(&nodes, "/modify");
        assert!(diff_policy(&modify, &modify).is_empty());
    }

    #[test]
    fn compacted_actual_matches_full_expected() {
        let nodes = compiled_write_nodes();
        let view = node(&nodes, "/view");
        // The same node as a query result would render it: compact keys/values.
        let actual = serde_json::json!({
            "@id": "https://example.org/Lead/access/write/view",
            "@type": ["f:AccessPolicy", "https://example.org/Lead/access/write"],
            "f:action": {"@id": "f:view"},
            "f:onClass": {"@id": "https://example.org/Lead"},
            "f:allow": true,
        });
        assert!(diff_policy(&view, &actual).is_empty());
    }

    #[test]
    fn hand_widened_allow_list_is_drift() {
        let nodes = compiled_write_nodes();
        let modify = node(&nodes, "/modify");
        let mut actual = modify.clone();
        actual[format!("{F}onProperty")] = serde_json::json!([
            {"@id": RDF_TYPE},
            {"@id": "https://example.org/name"},
            {"@id": "https://example.org/salary"}
        ]);
        let drift = diff_policy(&modify, &actual);
        assert_eq!(drift.len(), 1);
        assert!(drift[0].contains("salary"), "{drift:?}");
        assert!(drift[0].contains("extra in ledger"), "{drift:?}");
    }

    #[test]
    fn flipped_allow_is_drift() {
        let nodes = compiled_write_nodes();
        let view = node(&nodes, "/view");
        let mut actual = view.clone();
        actual[format!("{F}allow")] = serde_json::json!(false);
        let drift = diff_policy(&view, &actual);
        assert!(drift.iter().any(|d| d.contains("f:allow")), "{drift:?}");
    }

    #[test]
    fn retargeted_class_is_drift() {
        let nodes = compiled_write_nodes();
        let view = node(&nodes, "/view");
        let mut actual = view.clone();
        actual[format!("{F}onClass")] = serde_json::json!({"@id": "https://example.org/Invoice"});
        let drift = diff_policy(&view, &actual);
        assert!(drift.iter().any(|d| d.contains("onClass")), "{drift:?}");
    }

    #[test]
    fn profile_node_records_derivation_and_shared_choice() {
        let nodes = compiled_write_nodes();
        let profile = node(&nodes, "/profile");
        assert_eq!(profile[format!("{FM}derivation")], "shacl-shape");
        assert_eq!(profile[format!("{FM}allowShared")], false);
    }

    #[test]
    fn value_helpers_tolerate_all_shapes() {
        assert_eq!(
            value_id(Some(&serde_json::json!({"@id": "https://x/A"}))),
            Some("https://x/A".to_string())
        );
        assert_eq!(
            value_ids(Some(
                &serde_json::json!([{"@id": "https://x/A"}, "https://x/B"])
            )),
            vec!["https://x/A", "https://x/B"]
        );
        assert_eq!(
            value_str(Some(&serde_json::json!(["write"]))),
            Some("write".to_string())
        );
        assert_eq!(
            value_bool(Some(&serde_json::json!({"@value": true}))),
            Some(true)
        );
        assert_eq!(expand("f:allow"), format!("{F}allow"));
        assert_eq!(expand("https://x/A"), "https://x/A");
    }
}
