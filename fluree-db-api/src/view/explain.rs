//! Explanations of derived facts.

use crate::query::helpers::parse_jsonld_query;
use crate::view::GraphDb;
use crate::{ApiError, Fluree, QueryExecutionOptions, Result, Tracker};
use fluree_db_core::value::FlakeValue;
use fluree_db_core::{GraphDbRef, LedgerSnapshot, OverlayProvider};
use fluree_db_query::datalog_rules::{ExplainTarget, Explanation};
use fluree_db_query::execute::{explain_derived_facts, QueryRequest};
use fluree_db_query::parse::IriEncoder;
use fluree_db_query::schema_bundle::SchemaBundleOverlay;
use fluree_graph_json_ld::{details_with_vocab_policy, ParsedContext};
use serde_json::{json, Map, Value as JsonValue};
use std::sync::Arc;

/// Request keys an explanation's materialization reads, as a query's would.
const REASONING_KEYS: &[&str] = &[
    "reasoning",
    "rules",
    "ontology",
    "reasoningBudget",
    "reasoningTime",
    "opts",
];

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

impl Fluree {
    /// Explain derived facts. For each rule that derives them: the body row
    /// that matched, the facts it read (each marked asserted or derived), the
    /// negated patterns it found empty and the graph searched, and the
    /// solutions an aggregate counted in the row's group.
    ///
    /// `request` carries a query's `@context` and reasoning keys
    /// (`reasoning`, `rules`, `reasoningTime`, `reasoningBudget`), merged with
    /// the ledger's reasoning settings as a query's are, and an `explain`
    /// node map naming the facts by subject and one predicate, with an
    /// optional object and `@graph`:
    ///
    /// ```json
    /// {
    ///   "@context": {"ex": "http://example.org/"},
    ///   "reasoning": "datalog",
    ///   "explain": {"@id": "ex:acct2", "@type": "ex:Candidate"}
    /// }
    /// ```
    ///
    /// An explanation shows premises whatever a policy would hide, so a view
    /// under a non-root policy is refused, including the ledger's configured
    /// default policy.
    pub async fn explain_derived(&self, db: &GraphDb, request: &JsonValue) -> Result<Explanation> {
        self.explain_derived_with_options(db, request, &QueryExecutionOptions::default(), None)
            .await
    }

    /// [`Self::explain_derived`] for a host's request: `options.cancellation`
    /// stops the materialization and the replays, and `tracker` charges
    /// their fuel.
    pub async fn explain_derived_with_options(
        &self,
        db: &GraphDb,
        request: &JsonValue,
        options: &QueryExecutionOptions,
        tracker: Option<&Tracker>,
    ) -> Result<Explanation> {
        let db = &self.wrap_policy_defaults(db.clone()).await?;
        if !db.is_root() {
            return Err(ApiError::invalid_query(
                "an explanation shows its premises whatever a policy would hide; ask with an \
                 unrestricted view",
            ));
        }
        let obj = request
            .as_object()
            .ok_or_else(|| ApiError::invalid_query("an explain request is a JSON object"))?;
        let node = obj
            .get("explain")
            .and_then(JsonValue::as_object)
            .ok_or_else(|| {
                ApiError::invalid_query(
                    "an explain request names its facts with an \"explain\" node map",
                )
            })?;

        let mut query = Map::new();
        if let Some(context) = obj.get("@context").or_else(|| obj.get("context")) {
            query.insert("@context".to_string(), context.clone());
        }
        for &key in REASONING_KEYS {
            if let Some(value) = obj.get(key) {
                query.insert(key.to_string(), value.clone());
            }
        }
        query.insert("select".to_string(), json!(["?s"]));
        query.insert("where".to_string(), json!({"@id": "?s", "?p": "?o"}));
        let (_, parsed) = parse_jsonld_query(
            &JsonValue::Object(query),
            &db.snapshot,
            db.default_context.as_ref(),
            None,
        )?;
        let executable = self.build_executable_for_view(db, &parsed, None).await?;
        let target = explain_target(node, &parsed.context, &db.snapshot)?;

        let db_ref = db.as_graph_db_ref();
        let schema = executable
            .reasoning
            .schema_bundle
            .as_ref()
            .filter(|bundle| !bundle.is_empty())
            .map(|bundle| SchemaBundleOverlay::new(db_ref.overlay, bundle.clone()));
        let overlay: &dyn OverlayProvider = schema
            .as_ref()
            .map_or(db_ref.overlay, |o| o as &dyn OverlayProvider);
        let base = GraphDbRef::new(db_ref.snapshot, db_ref.g_id, overlay, db_ref.t);
        Ok(explain_derived_facts(
            base,
            &executable.reasoning.modes,
            executable.reasoning.rules_source_g_id,
            db.binary_store.as_ref(),
            QueryRequest {
                tracker,
                cancellation: options.cancellation.as_ref(),
            },
            &target,
        )
        .await?)
    }
}

/// The facts an `explain` node map names.
fn explain_target(
    node: &Map<String, JsonValue>,
    context: &ParsedContext,
    snapshot: &LedgerSnapshot,
) -> Result<ExplainTarget> {
    let expand = |s: &str, vocab: bool| {
        details_with_vocab_policy(s, context, vocab, false)
            .map(|(iri, _)| iri)
            .map_err(|e| ApiError::invalid_query(format!("invalid IRI `{s}`: {e}")))
    };
    let encode = |iri: &str| {
        IriEncoder::encode_iri(snapshot, iri).ok_or_else(|| {
            ApiError::invalid_query(format!("<{iri}> does not occur in this ledger"))
        })
    };
    let node_ref = |value: &JsonValue| -> Result<Option<FlakeValue>> {
        match value {
            JsonValue::Null => Ok(None),
            JsonValue::String(s) if s.starts_with('?') => Ok(None),
            JsonValue::String(s) => Ok(Some(FlakeValue::Ref(encode(&expand(s, true)?)?))),
            other => Err(ApiError::invalid_query(format!(
                "an explained @type is a class IRI or a variable, got {other}"
            ))),
        }
    };

    let subject = node
        .get("@id")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| ApiError::invalid_query("the explained node needs an \"@id\""))?;
    let subject = encode(&expand(subject, false)?)?;
    let graph = node
        .get("@graph")
        .map(|g| {
            g.as_str()
                .ok_or_else(|| ApiError::invalid_query("an explained \"@graph\" is an IRI"))
                .and_then(|g| expand(g, false))
        })
        .transpose()?
        .map(Arc::from);

    let mut properties = node
        .iter()
        .filter(|(key, _)| !matches!(key.as_str(), "@id" | "@graph"));
    let (key, value) = properties.next().ok_or_else(|| {
        ApiError::invalid_query("the explained node names one predicate, such as \"@type\"")
    })?;
    if properties.next().is_some() {
        return Err(ApiError::invalid_query(
            "the explained node names exactly one predicate",
        ));
    }
    let (predicate, object) = if key == "@type" {
        (encode(RDF_TYPE)?, node_ref(value)?)
    } else {
        let object =
            match value {
                JsonValue::Object(o) => match o.get("@id").and_then(JsonValue::as_str) {
                    Some(id) => Some(FlakeValue::Ref(encode(&expand(id, false)?)?)),
                    None => return Err(ApiError::invalid_query(
                        "an explained object is a variable, {\"@id\": …}, a string, a number or a \
                         boolean",
                    )),
                },
                JsonValue::Null => None,
                JsonValue::String(s) if s.starts_with('?') => None,
                JsonValue::String(s) => Some(FlakeValue::String(s.clone())),
                JsonValue::Number(n) => n
                    .as_i64()
                    .map(FlakeValue::Long)
                    .or_else(|| n.as_f64().map(FlakeValue::Double)),
                JsonValue::Bool(b) => Some(FlakeValue::Boolean(*b)),
                JsonValue::Array(_) => {
                    return Err(ApiError::invalid_query(
                        "explain one object at a time, or a variable for all of them",
                    ))
                }
            };
        (encode(&expand(key, true)?)?, object)
    };
    Ok(ExplainTarget {
        subject,
        predicate,
        object,
        graph,
    })
}
