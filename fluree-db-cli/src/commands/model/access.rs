//! `fluree model access` — a stateless policy scaffolder.
//!
//! Users declare intent ("apps may write Leads"); this module emits the
//! engine's own vocabulary — nothing else. With the policy engine's write
//! verbs (`f:create` / `f:update` / `f:delete`), intent maps directly to
//! thin class-targeted policies:
//!
//! * **read**   → a view policy (`f:onClass` + `f:allow`, or an `f:query`
//!   relationship gate);
//! * **write**  → the view policy plus a write policy carrying
//!   `f:action [create, update, delete]` on the class — full ownership of
//!   instances. Verb semantics make this exact: class targeting matches
//!   pre ∪ post state, and `rdf:type` writes match by the class they mint,
//!   so a Lead grant can never create a Contract.
//! * **intake** → a create-only policy (submit without read-back — the
//!   anonymous-form shape).
//!
//! `--property` optionally narrows the write policy to a column set
//! (`f:onProperty` conjunction). The property *surface* of a class is
//! otherwise SHACL's job (`model entity define`, closed shapes) —
//! validation owns "what a valid X looks like", policy owns "who may
//! cause which state transitions".
//!
//! There is NO stored intent language: the compiled policies are small and
//! self-describing, the `--connected` path survives verbatim inside the
//! stored `f:query`'s `@path` context term, and hand-editing a policy is
//! legitimate authorship — not drift.
//!
//! Re-running `enable` REPLACES the two policy nodes it owns
//! (`{class}/view`, `{class}/write`) atomically: one transaction
//! wildcard-deletes both ids and inserts the fresh compilation. Anything
//! less is unsafe — the policy loader gives `f:allow` precedence over
//! `f:query`, so a property-merge that left a stale `f:allow: true` behind
//! would silently disable a newly added `--connected` gate. Owning both
//! ids on every run also makes profile switches on the same class exact
//! (write → read revokes `{class}/write`; intake drops `{class}/view`).

use serde_json::{json, Value};

use super::{query, replace_nodes, resolve_mode};
use crate::cli::ModelAccessAction;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;

const F: &str = "https://ns.flur.ee/db#";
const RDFS_CLASS: &str = "http://www.w3.org/2000/01/rdf-schema#Class";
const RDFS_LABEL: &str = "http://www.w3.org/2000/01/rdf-schema#label";

/// Access profiles: what the compiled policy set permits.
#[derive(Clone, Copy, PartialEq)]
enum Profile {
    /// View instances of the class.
    Read,
    /// Full instance ownership: view + create/update/delete.
    Write,
    /// Submit without reading back (create-only; the anonymous-form shape).
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

    /// Write verbs granted by this profile (empty = no write policy).
    fn write_verbs(&self) -> &'static [&'static str] {
        match self {
            Self::Read => &[],
            Self::Write => &["create", "update", "delete"],
            Self::Intake => &["create"],
        }
    }
}

pub async fn run(action: &ModelAccessAction, dirs: &FlureeDir, direct: bool) -> CliResult<()> {
    match action {
        ModelAccessAction::Enable {
            dataset,
            profile,
            class,
            properties,
            policy_class,
            space,
            connected,
            dry_run,
            remote,
        } => {
            run_enable(
                dataset,
                profile,
                class,
                properties,
                policy_class.as_deref(),
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
    class: &str,
    properties: &[String],
    policy_class_override: Option<&str>,
    space: Option<&str>,
    connected: Option<&str>,
    dry_run: bool,
    remote: Option<&str>,
    dirs: &FlureeDir,
    direct: bool,
) -> CliResult<()> {
    let profile = Profile::parse(profile_str)?;
    require_absolute_iri("--class", class)?;
    for p in properties {
        require_absolute_iri("--property", p)?;
    }
    if space.is_some() && remote.is_none() {
        return Err(CliError::Usage(
            "--space attaches the policy class to a hosted stack's grant; pass --remote <r>".into(),
        ));
    }
    if let Some(path) = connected {
        if profile != Profile::Read {
            return Err(CliError::Usage(
                "--connected is supported with --profile read only: relationship \
                 gates evaluate the connection triples, which don't exist yet for \
                 a subject being created. Verb-scoped write gates are future work."
                    .into(),
            ));
        }
        validate_connected_path(path)?;
    }
    if !properties.is_empty() && profile == Profile::Read {
        return Err(CliError::Usage(
            "--property narrows WRITE policies to a column set; the read profile \
             has no write policy. Property-level read filtering is a policy the \
             engine supports (f:onProperty view) but this scaffolder keeps read \
             class-scoped — author it directly if you need it."
                .into(),
        ));
    }

    let policy_class = policy_class_override.map(String::from).unwrap_or_else(|| {
        format!(
            "{}/access/{}",
            class.trim_end_matches('/'),
            profile.as_str()
        )
    });
    let graph = compile(&policy_class, class, profile, properties, connected);

    println!("Profile:      {} {}", profile.as_str(), class);
    println!("Policy class: {policy_class}");
    match profile {
        Profile::Read => println!("Grants:       view"),
        Profile::Write => println!("Grants:       view + create/update/delete"),
        Profile::Intake => println!("Grants:       create only (no read-back)"),
    }
    if !properties.is_empty() {
        println!(
            "Columns:      write narrowed to {} propert{} (f:onProperty)",
            properties.len(),
            if properties.len() == 1 { "y" } else { "ies" }
        );
    }
    if let Some(path) = connected {
        println!("Connected:    {path} (view gated by relationship to the requesting identity)");
    }
    println!(
        "note: the property SURFACE of {class} is governed by its SHACL shape\n\
         \x20 (model entity define, ideally --closed) — validation owns what a valid\n\
         \x20 instance looks like; this policy owns who may create/change them."
    );

    if dry_run {
        println!("\n-- dry run; compiled JSON-LD --");
        println!("{}", serde_json::to_string_pretty(&graph)?);
        return Ok(());
    }

    // Transact (data plane — policies live in the ledger). Policies land
    // BEFORE the grant so a partial failure leaves unused policies
    // (harmless) rather than a grant naming classes that don't exist.
    // Both policy ids are replaced atomically whether or not this profile
    // emits them — see the module doc for why merge semantics are unsafe.
    let mode = resolve_mode(dataset, remote, dirs, direct).await?;
    let view_id = format!("{policy_class}/view");
    let write_id = format!("{policy_class}/write");
    replace_nodes(&mode, &graph, &[&view_id, &write_id]).await?;
    println!("\nPolicies transacted to '{dataset}'.");

    if let (Some(space_id), Some(remote_name)) = (space, remote) {
        attach_grant(
            dirs,
            remote_name,
            dataset,
            space_id,
            &policy_class,
            !profile.write_verbs().is_empty(),
        )
        .await?;
    } else {
        println!(
            "Attach to a space grant so app tokens carry it:\n  \
             fluree model access enable {dataset} --profile {} --class {class} \
             --space <spaceId> --remote <r>",
            profile.as_str()
        );
    }
    Ok(())
}

/// Validate a `--connected` SPARQL property path (light: the engine's
/// `@path` parser is authoritative; we only catch obvious mangling).
fn validate_connected_path(path: &str) -> CliResult<()> {
    if path.trim().is_empty() {
        return Err(CliError::Usage("--connected path is empty".into()));
    }
    let opens = path.matches('<').count();
    let closes = path.matches('>').count();
    if opens != closes {
        return Err(CliError::Usage(format!(
            "--connected path has unbalanced angle brackets: {path}"
        )));
    }
    Ok(())
}

// ── compile ─────────────────────────────────────────────────────────────

/// Compile the profile into its JSON-LD artifacts: the policy class (the
/// assignment unit grants and tokens carry) and one or two thin policies
/// targeting the governed class (`f:onClass`). Deterministic ids
/// ({policy_class}/view, {policy_class}/write) make re-running idempotent
/// — `enable` atomically replaces both owned nodes with the fresh
/// compilation.
fn compile(
    policy_class: &str,
    class: &str,
    profile: Profile,
    properties: &[String],
    connected: Option<&str>,
) -> Value {
    let mut nodes: Vec<Value> = Vec::new();

    nodes.push(json!({
        "@id": policy_class,
        "@type": RDFS_CLASS,
        RDFS_LABEL: format!("{} access: {}", profile.as_str(), class),
    }));

    if profile.wants_view() {
        let mut view = json!({
            "@id": format!("{policy_class}/view"),
            "@type": [format!("{F}AccessPolicy"), policy_class],
            format!("{F}action"): {"@id": format!("{F}view")},
            format!("{F}onClass"): {"@id": class},
        });
        match connected {
            Some(path) => {
                // Relationship gate: the flake is visible when the query
                // returns rows. The path rides the engine's `@path`
                // context-term feature VERBATIM — readable in the stored
                // policy and reversible, no client-side expansion.
                let gate = json!({
                    "@context": {"connected": {"@path": path}},
                    "where": [
                        {"@id": "?$identity", "connected": {"@id": "?$this"}}
                    ]
                });
                view[format!("{F}query")] =
                    json!(serde_json::to_string(&gate).expect("static JSON serializes"));
            }
            None => {
                view[format!("{F}allow")] = json!(true);
            }
        }
        nodes.push(view);
    }

    let verbs = profile.write_verbs();
    if !verbs.is_empty() {
        let action: Vec<Value> = verbs
            .iter()
            .map(|v| json!({"@id": format!("{F}{v}")}))
            .collect();
        let mut write = json!({
            "@id": format!("{policy_class}/write"),
            "@type": [format!("{F}AccessPolicy"), policy_class],
            format!("{F}action"): action,
            format!("{F}onClass"): {"@id": class},
            format!("{F}allow"): true,
        });
        if !properties.is_empty() {
            // Column narrowing: policy applies to flakes matching class AND
            // property — "may edit status of Leads, nothing else".
            write[format!("{F}onProperty")] = json!(properties
                .iter()
                .map(|p| json!({"@id": p}))
                .collect::<Vec<_>>());
        }
        nodes.push(write);
    }

    json!({"@graph": nodes})
}

// ── show ────────────────────────────────────────────────────────────────

/// List compiled access policies grouped by policy class — read straight
/// from the `f:` artifacts (there is no separate intent store to consult).
async fn run_show(
    dataset: &str,
    remote: Option<&str>,
    dirs: &FlureeDir,
    direct: bool,
) -> CliResult<()> {
    let mode = resolve_mode(dataset, remote, dirs, direct).await?;
    let q = json!({
        "@context": {"f": F},
        "select": {"?policy": ["*"]},
        "where": [{"@id": "?policy", "@type": "f:AccessPolicy"}]
    });
    let result = query(&mode, &q).await?;
    let rows = result.as_array().cloned().unwrap_or_default();
    if rows.is_empty() {
        println!("No access policies on '{dataset}'.");
        println!(
            "Enable a profile: fluree model access enable {dataset} \
             --profile write --class <iri>"
        );
        return Ok(());
    }

    println!("Access policies on '{dataset}':\n");
    for row in &rows {
        let id = row.get("@id").and_then(Value::as_str).unwrap_or("-");
        println!("• {id}");
        let classes: Vec<String> = list_values(row.get("@type"))
            .into_iter()
            .filter(|t| t != "f:AccessPolicy" && t != &format!("{F}AccessPolicy"))
            .collect();
        if !classes.is_empty() {
            println!("    class:    {}", classes.join(", "));
        }
        let actions = list_values(row.get("f:action"));
        if !actions.is_empty() {
            println!("    action:   {}", actions.join(", "));
        }
        let on_class = list_values(row.get("f:onClass"));
        if !on_class.is_empty() {
            println!("    onClass:  {}", on_class.join(", "));
        }
        let on_prop = list_values(row.get("f:onProperty"));
        if !on_prop.is_empty() {
            println!("    columns:  {}", on_prop.join(", "));
        }
        if let Some(gate) = row.get("f:query") {
            if let Some(path) = extract_path(gate) {
                println!("    gate:     connected via {path}");
            } else {
                println!("    gate:     f:query condition");
            }
        } else if row.get("f:allow").is_some() {
            println!("    decision: allow");
        }
    }
    Ok(())
}

/// Pull the `@path` expression back out of a stored relationship gate —
/// the storage form is reversible by construction.
fn extract_path(gate: &Value) -> Option<String> {
    let raw = match gate {
        Value::String(s) => s.clone(),
        Value::Array(a) => a.first()?.as_str()?.to_string(),
        Value::Object(o) => o.get("@value")?.as_str()?.to_string(),
        _ => return None,
    };
    let parsed: Value = serde_json::from_str(&raw).ok()?;
    let ctx = parsed.get("@context")?.as_object()?;
    for term in ctx.values() {
        if let Some(path) = term.get("@path").and_then(Value::as_str) {
            return Some(path.to_string());
        }
    }
    None
}

fn list_values(v: Option<&Value>) -> Vec<String> {
    let mut out = Vec::new();
    let Some(v) = v else { return out };
    let items: Vec<&Value> = match v {
        Value::Array(a) => a.iter().collect(),
        other => vec![other],
    };
    for item in items {
        match item {
            Value::String(s) => out.push(s.clone()),
            Value::Bool(b) => out.push(b.to_string()),
            Value::Object(o) => {
                if let Some(id) = o.get("@id").and_then(Value::as_str) {
                    out.push(id.to_string());
                } else if let Some(val) = o.get("@value") {
                    out.push(val.to_string());
                }
            }
            other => out.push(other.to_string()),
        }
    }
    out
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

/// Merge `policy_class` into the space's grant on the dataset via the stack's
/// grants API. System-plane state (grants) is the one place the compiler
/// talks to an API instead of the data plane — grants are router-owned
/// invariants (scope validation, membership checks).
async fn attach_grant(
    dirs: &FlureeDir,
    remote_name: &str,
    dataset: &str,
    space_id: &str,
    policy_class: &str,
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
    if !classes.iter().any(|c| c == policy_class) {
        classes.push(policy_class.to_string());
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
#[cfg(test)]
mod tests {
    use super::*;

    const LEAD: &str = "https://example.org/Lead";
    const POLICY_CLASS: &str = "https://example.org/Lead/access/write";

    /// Exact-id lookup — the default class IRI itself ends in the profile
    /// name (…/access/write), so suffix matching would be ambiguous.
    fn node<'a>(g: &'a Value, id: &str) -> Option<&'a Value> {
        g["@graph"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["@id"].as_str().unwrap() == id)
    }

    #[test]
    fn write_profile_emits_class_view_and_verb_policy() {
        let g = compile(POLICY_CLASS, LEAD, Profile::Write, &[], None);
        assert_eq!(g["@graph"].as_array().unwrap().len(), 3);

        let view = node(&g, &format!("{POLICY_CLASS}/view")).expect("view policy");
        assert_eq!(view[format!("{F}onClass")]["@id"], LEAD);
        assert_eq!(view[format!("{F}allow")], true);

        let write = node(&g, &format!("{POLICY_CLASS}/write")).expect("write policy");
        let verbs: Vec<&str> = write[format!("{F}action")]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["@id"].as_str().unwrap())
            .collect();
        assert_eq!(
            verbs,
            vec![
                "https://ns.flur.ee/db#create",
                "https://ns.flur.ee/db#update",
                "https://ns.flur.ee/db#delete"
            ]
        );
        assert_eq!(write[format!("{F}onClass")]["@id"], LEAD);
        // No property allow-list, no rdf:type entry — verb semantics make
        // the class grant exact on their own.
        assert!(write.get(format!("{F}onProperty")).is_none());
    }

    #[test]
    fn read_profile_has_no_write_policy() {
        let g = compile(
            "https://example.org/Lead/access/read",
            LEAD,
            Profile::Read,
            &[],
            None,
        );
        assert!(node(&g, "https://example.org/Lead/access/read/write").is_none());
        assert!(node(&g, "https://example.org/Lead/access/read/view").is_some());
    }

    #[test]
    fn intake_profile_is_create_only_without_view() {
        let g = compile(
            "https://example.org/Lead/access/intake",
            LEAD,
            Profile::Intake,
            &[],
            None,
        );
        assert!(node(&g, "https://example.org/Lead/access/intake/view").is_none());
        let write =
            node(&g, "https://example.org/Lead/access/intake/write").expect("create policy");
        let verbs = write[format!("{F}action")].as_array().unwrap();
        assert_eq!(verbs.len(), 1);
        assert_eq!(verbs[0]["@id"], "https://ns.flur.ee/db#create");
    }

    #[test]
    fn property_narrowing_adds_on_property_conjunction() {
        let props = vec!["https://example.org/status".to_string()];
        let g = compile(POLICY_CLASS, LEAD, Profile::Write, &props, None);
        let write = node(&g, &format!("{POLICY_CLASS}/write")).unwrap();
        assert_eq!(
            write[format!("{F}onProperty")][0]["@id"],
            "https://example.org/status"
        );
    }

    #[test]
    fn connected_gate_stores_path_verbatim_and_reversibly() {
        let path = "<https://example.org/memberOf>/^<https://example.org/team>";
        let g = compile(
            "https://example.org/Lead/access/read",
            LEAD,
            Profile::Read,
            &[],
            Some(path),
        );
        let view = node(&g, "https://example.org/Lead/access/read/view").unwrap();
        assert!(
            view.get(format!("{F}allow")).is_none(),
            "f:query replaces f:allow"
        );
        let stored = view[format!("{F}query")].as_str().unwrap();
        let parsed: Value = serde_json::from_str(stored).unwrap();
        assert_eq!(parsed["@context"]["connected"]["@path"], path);
        assert_eq!(parsed["where"][0]["@id"], "?$identity");
        // And show can pull it back out.
        assert_eq!(
            extract_path(&view[format!("{F}query")]).as_deref(),
            Some(path)
        );
    }

    #[test]
    fn connected_path_validation() {
        assert!(validate_connected_path("<https://x/a>/^<https://x/b>").is_ok());
        assert!(validate_connected_path("").is_err());
        assert!(validate_connected_path("<https://x/a").is_err());
    }

    #[test]
    fn absolute_iri_gate() {
        assert!(require_absolute_iri("--class", "https://example.org/Lead").is_ok());
        assert!(require_absolute_iri("--class", "ex:Lead").is_err());
    }

    #[test]
    fn profile_parse_rejects_unknown() {
        assert!(Profile::parse("admin").is_err());
    }
}
