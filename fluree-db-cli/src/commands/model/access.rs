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
fn compile(
    class_iri: &str,
    entity: &str,
    profile: Profile,
    allowed: &[String],
    connected_steps: Option<&[PathStep]>,
    connected_raw: Option<&str>,
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
