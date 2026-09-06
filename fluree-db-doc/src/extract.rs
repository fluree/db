//! Entities and relations: the language model's pass, and what becomes of
//! its answer.
//!
//! Per chunk, the model is shown the ontology, the entities the gazetteer
//! already found in that chunk, and the text, and asked for JSON. The
//! answer is cached on everything that shaped the ask, so a re-run over an
//! unchanged corpus and model costs nothing.
//!
//! Resolution is where judgement is applied to what came back:
//!
//! - an entity the gazetteer knows keeps the gazetteer's IRI, whatever the
//!   model called it; one it does not know is minted under a name-derived
//!   IRI, so the same name in two documents lands on one node;
//! - an entity whose excerpt cannot be found in the chunk is a
//!   hallucination and is dropped, relations and all; a new entity whose
//!   class is not in the ontology is kept but flagged, since "March" typed
//!   `schema:Thing` is what a model returns when asked for more than the
//!   text holds, and a project without a review step can ask to drop them;
//! - every relation is written reified, with its excerpt and the gate's
//!   verdict; a direct edge is written only for a predicate the model
//!   admits, as spelled or after an unambiguous repair.

use crate::cache::DocCache;
use crate::chunk::Chunk;
use crate::gazetteer::{normalize_name, Gazetteer, Mention};
use crate::llm::{LlmClient, Part, Request};
use crate::model::{Model, Verdict};
use crate::prompt;
use crate::{vocab, DocError, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

const MAX_TOKENS: u32 = 8000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RelationMode {
    /// Reified relation nodes, plus a direct edge for every admitted one.
    #[default]
    Direct,
    /// Reified relation nodes only: review before anything becomes an edge.
    Reified,
    /// Entities only; relations the model returns are discarded.
    Off,
}

#[derive(Debug, Clone, Default)]
pub struct ExtractOptions {
    pub relations: RelationMode,
    /// The project's own priorities, rendered into the system prompt.
    pub guidance: Option<String>,
    /// Replaces the system prompt template; must keep `{model}`.
    pub system_template: Option<String>,
    /// Replaces the user prompt template; must keep `{document}`.
    pub user_template: Option<String>,
    pub drop_off_model: bool,
}

/// What resolution keeps, beyond what the gate decides.
#[derive(Debug, Clone, Copy, Default)]
pub struct ResolvePolicy {
    pub relations: RelationMode,
    pub drop_off_model: bool,
}

/// One entity as the model returned it.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct LlmEntity {
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub class: Option<String>,
    #[serde(rename = "nerLabel")]
    pub ner_label: Option<String>,
    pub context: Option<String>,
    #[serde(rename = "alternateNames")]
    pub alternate_names: Option<Vec<String>>,
    pub attributes: Option<Map<String, Value>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct LlmRelation {
    #[serde(rename = "subjectName")]
    pub subject_name: Option<String>,
    pub predicate: Option<String>,
    #[serde(rename = "predicateLabel")]
    pub predicate_label: Option<String>,
    #[serde(rename = "objectName")]
    pub object_name: Option<String>,
    #[serde(rename = "objectIsLiteral")]
    pub object_is_literal: bool,
    pub context: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ChunkExtraction {
    pub entities: Vec<LlmEntity>,
    pub relations: Vec<LlmRelation>,
    #[serde(skip)]
    pub from_cache: bool,
}

/// Strip a ```json fence, which models add however firmly they are asked
/// not to.
pub fn clean_json(content: &str) -> String {
    let mut s = content.trim().to_string();
    if let Some(rest) = s.strip_prefix("```") {
        let after = rest.split_once('\n').map(|(_, b)| b).unwrap_or("");
        s = after.to_string();
    }
    if let Some(idx) = s.rfind("```") {
        s.truncate(idx);
    }
    s.trim().to_string()
}

/// Tolerates missing keys and malformed items; fails only on JSON that
/// does not parse, which the caller retries once.
pub fn parse_extraction(text: &str) -> std::result::Result<ChunkExtraction, serde_json::Error> {
    let value: Value = serde_json::from_str(&clean_json(text))?;
    let mut out = ChunkExtraction::default();
    for item in value
        .get("entities")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Ok(e) = serde_json::from_value::<LlmEntity>(item.clone()) {
            out.entities.push(e);
        }
    }
    for item in value
        .get("relations")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Ok(r) = serde_json::from_value::<LlmRelation>(item.clone()) {
            out.relations.push(r);
        }
    }
    Ok(out)
}

pub struct Extractor {
    client: LlmClient,
    system_prompt: String,
    user_template: String,
    cache: Option<DocCache>,
}

impl std::fmt::Debug for Extractor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Extractor")
            .field("model", &self.client.model())
            .finish()
    }
}

impl Extractor {
    pub fn new(
        client: LlmClient,
        model: &Model,
        cache: Option<DocCache>,
        opts: &ExtractOptions,
    ) -> Result<Self> {
        let system_template = opts
            .system_template
            .as_deref()
            .unwrap_or(prompt::SYSTEM_PROMPT_TEMPLATE);
        if !system_template.contains("{model}") {
            return Err(DocError::Config(
                "system prompt template has no {model} slot".into(),
            ));
        }
        let user_template = opts
            .user_template
            .clone()
            .unwrap_or_else(|| prompt::USER_PROMPT_TEMPLATE.to_string());
        if !user_template.contains("{document}") {
            return Err(DocError::Config(
                "user prompt template has no {document} slot".into(),
            ));
        }
        let system_prompt = prompt::system_prompt_from(
            system_template,
            &model.render_text(),
            opts.guidance.as_deref(),
        );
        Ok(Self {
            client,
            system_prompt,
            user_template,
            cache,
        })
    }

    pub fn model_name(&self) -> &str {
        self.client.model()
    }

    /// Changes when the endpoint model, the ontology, the guidance or
    /// either prompt template does.
    pub fn fingerprint(&self) -> String {
        let mut h = Sha256::new();
        h.update(self.client.model().as_bytes());
        h.update([0]);
        h.update(self.system_prompt.as_bytes());
        h.update([0]);
        h.update(self.user_template.as_bytes());
        hex::encode(h.finalize())
    }

    /// The model's answer for one chunk, from the cache when the same ask
    /// was made before.
    pub fn extract_chunk(&self, text: &str, existing: &str) -> Result<ChunkExtraction> {
        let user = prompt::user_prompt_from(&self.user_template, existing, text);
        let key = DocCache::extraction_key(self.client.model(), &self.system_prompt, &user);
        if let Some(hit) = self.cache.as_ref().and_then(|c| c.load_extraction(&key)) {
            let mut hit = hit;
            hit.from_cache = true;
            return Ok(hit);
        }
        let answer = self.ask(&[user.as_str()])?;
        let extraction = match parse_extraction(&answer) {
            Ok(x) => x,
            Err(e) => {
                let retry = prompt::retry_prompt(&e.to_string());
                let again = self.ask(&[user.as_str(), answer.as_str(), retry.as_str()])?;
                parse_extraction(&again).map_err(|e| {
                    DocError::Model(format!("extraction answer is not JSON after retry: {e}"))
                })?
            }
        };
        if let Some(cache) = &self.cache {
            cache.store_extraction(&key, &extraction)?;
        }
        Ok(extraction)
    }

    /// One call; `turns` alternate user, assistant, user so a retry
    /// carries the failed answer.
    fn ask(&self, turns: &[&str]) -> Result<String> {
        // A retry folds the earlier exchange into one user turn: both wire
        // shapes accept it, and the model sees what it said and why it
        // failed.
        let joined;
        let user: &str = if turns.len() == 1 {
            turns[0]
        } else {
            joined = format!(
                "{}\n\n## YOUR PREVIOUS ANSWER\n{}\n\n{}",
                turns[0], turns[1], turns[2]
            );
            &joined
        };
        let answer = self.client.complete(&Request {
            system: Some(&self.system_prompt),
            parts: vec![Part::Text(user)],
            intent: "extraction",
            json: true,
            max_tokens: MAX_TOKENS,
        })?;
        answer.ok_or_else(|| DocError::Model("extraction answer was empty".into()))
    }
}

// ---------------------------------------------------------------------------
// resolution
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ExtractionStats {
    /// Occurrences found by the gazetteer scan.
    pub gazetteer_mentions: usize,
    /// Occurrences the model reported and the text confirmed.
    pub llm_mentions: usize,
    pub entities_known: usize,
    pub entities_new: usize,
    /// Entities the model named that the text does not contain.
    pub hallucinated: usize,
    /// New entities whose class is not in the ontology, kept and flagged.
    pub off_model: usize,
    /// The same, dropped on request.
    pub off_model_dropped: usize,
    /// Relations whose object named no entity, written with a literal
    /// object and never as an edge.
    pub relations_literal_object: usize,
    pub relations_valid: usize,
    pub relations_repaired: usize,
    pub relations_rejected: usize,
    pub attributes: usize,
}

/// Everything extraction adds to a document's transaction.
#[derive(Debug, Default)]
pub struct ExtractionGraph {
    /// Mention, entity and relation nodes, all stamped with the document
    /// except the entity nodes, which other documents may share.
    pub nodes: Vec<Value>,
    /// `{ "@id": subject, predicate: { "@id": object } }`, one per admitted
    /// relation with an entity object.
    pub direct: Vec<Value>,
    pub stats: ExtractionStats,
}

/// What resolution needs per chunk: the chunk, its IRI, what the gazetteer
/// found in it, and what the model said about it.
pub struct ChunkInput<'a> {
    pub chunk: &'a Chunk,
    pub chunk_iri: String,
    pub mentions: &'a [Mention],
    pub extraction: Option<&'a ChunkExtraction>,
}

struct Resolved {
    iri: String,
    known: bool,
    name: String,
    class: Option<String>,
    off_model: bool,
    ner_label: Option<String>,
    alternates: Vec<String>,
    attributes: Map<String, Value>,
    occurrences: usize,
}

pub fn resolve(
    doc_iri: &str,
    entity_prefix: &str,
    inputs: &[ChunkInput<'_>],
    gazetteer: Option<&Gazetteer>,
    model: Option<&Model>,
    policy: ResolvePolicy,
) -> ExtractionGraph {
    let mode = policy.relations;
    let mut out = ExtractionGraph::default();
    let mut entities: Vec<Resolved> = Vec::new();
    let mut by_name: HashMap<String, usize> = HashMap::new();
    let mut by_iri: HashMap<String, usize> = HashMap::new();
    let mut mention_nodes: Vec<Value> = Vec::new();
    let mut relation_nodes: Vec<Value> = Vec::new();
    let mut mention_seq = 0usize;
    let mut relation_seq = 0usize;
    // Direct edges already written, so two chunks stating one fact write
    // one edge.
    let mut direct_seen: HashSet<(String, String, String)> = HashSet::new();

    let intern = |entities: &mut Vec<Resolved>,
                  by_name: &mut HashMap<String, usize>,
                  by_iri: &mut HashMap<String, usize>,
                  iri: String,
                  known: bool,
                  name: &str,
                  class: Option<String>|
     -> usize {
        if let Some(i) = by_iri.get(&iri) {
            by_name.entry(normalize_name(name)).or_insert(*i);
            return *i;
        }
        entities.push(Resolved {
            iri: iri.clone(),
            known,
            name: name.to_string(),
            class,
            off_model: false,
            ner_label: None,
            alternates: Vec::new(),
            attributes: Map::new(),
            occurrences: 0,
        });
        let i = entities.len() - 1;
        by_iri.insert(iri, i);
        by_name.insert(normalize_name(name), i);
        i
    };

    for input in inputs {
        let chunk_iri = &input.chunk_iri;
        // Gazetteer mentions first: exact, and the anchor the model's
        // answer is reconciled against.
        let mut covered: Vec<(usize, usize, usize)> = Vec::new();
        for m in input.mentions {
            let Some(g) = gazetteer else { break };
            let entry = &g.entries()[m.entry];
            let idx = intern(
                &mut entities,
                &mut by_name,
                &mut by_iri,
                entry.iri.clone(),
                true,
                &entry.name,
                entry.types.first().cloned(),
            );
            for label in &entry.labels {
                by_name.entry(normalize_name(label)).or_insert(idx);
            }
            entities[idx].occurrences += 1;
            out.stats.gazetteer_mentions += 1;
            mention_nodes.push(mention_node(
                doc_iri,
                chunk_iri,
                input.chunk,
                mention_seq,
                m.begin,
                m.end,
                &m.surface,
                &entry.iri,
                "gazetteer",
            ));
            mention_seq += 1;
            covered.push((m.begin, m.end, idx));
        }

        let Some(extraction) = input.extraction else {
            continue;
        };
        for e in &extraction.entities {
            let Some(name) = e.name.as_deref().map(str::trim).filter(|n| !n.is_empty()) else {
                continue;
            };
            let names =
                std::iter::once(name).chain(e.alternate_names.iter().flatten().map(String::as_str));
            // The gazetteer's IRI wins over anything minted, and over the
            // model's spelling of the name.
            let known = gazetteer.and_then(|g| names.clone().find_map(|n| g.lookup(n)));
            let idx = match known {
                Some(entry) => intern(
                    &mut entities,
                    &mut by_name,
                    &mut by_iri,
                    entry.iri.clone(),
                    true,
                    &entry.name,
                    entry.types.first().cloned(),
                ),
                None => {
                    let class = e
                        .class
                        .as_deref()
                        .and_then(|c| model.and_then(|m| m.class_iri(c)));
                    let already = names.clone().find_map(|n| by_name.get(&normalize_name(n)));
                    let off_model = class.is_none() && already.is_none() && model.is_some();
                    if off_model && policy.drop_off_model {
                        out.stats.off_model_dropped += 1;
                        continue;
                    }
                    let iri = match already {
                        Some(i) => entities[*i].iri.clone(),
                        None => minted_iri(entity_prefix, name),
                    };
                    let idx = intern(
                        &mut entities,
                        &mut by_name,
                        &mut by_iri,
                        iri,
                        false,
                        name,
                        class,
                    );
                    if off_model && !entities[idx].off_model {
                        entities[idx].off_model = true;
                        out.stats.off_model += 1;
                    }
                    idx
                }
            };
            for n in names.clone() {
                by_name.entry(normalize_name(n)).or_insert(idx);
            }
            let r = &mut entities[idx];
            if r.ner_label.is_none() {
                r.ner_label = e.ner_label.clone();
            }
            for alt in e.alternate_names.iter().flatten() {
                if alt != &r.name && !r.alternates.iter().any(|a| a == alt) {
                    r.alternates.push(alt.clone());
                }
            }
            if let (Some(attrs), Some(m)) = (&e.attributes, model) {
                for (prop, value) in attrs {
                    let property = match m.judge_attribute(prop) {
                        Verdict::Valid => prop.clone(),
                        Verdict::Repaired { property, .. } => property,
                        Verdict::Rejected { .. } => continue,
                    };
                    if !r.attributes.contains_key(&property) {
                        r.attributes.insert(property, value.clone());
                        out.stats.attributes += 1;
                    }
                }
            }

            // Where in the chunk: the excerpt, narrowed to the name.
            let spans = locate(&input.chunk.text, e.context.as_deref(), names.clone());
            if spans.is_empty() && r.known {
                // A known entity the gazetteer already anchored needs no
                // second mention; one it did not find is unconfirmed.
                if !covered.iter().any(|c| c.2 == idx) {
                    out.stats.hallucinated += 1;
                }
                continue;
            }
            if spans.is_empty() {
                out.stats.hallucinated += 1;
                continue;
            }
            for (begin, end) in spans {
                if covered.iter().any(|c| begin < c.1 && c.0 < end) {
                    continue;
                }
                let surface: String = input
                    .chunk
                    .text
                    .chars()
                    .skip(begin)
                    .take(end - begin)
                    .collect();
                mention_nodes.push(mention_node(
                    doc_iri,
                    chunk_iri,
                    input.chunk,
                    mention_seq,
                    begin,
                    end,
                    &surface,
                    &entities[idx].iri,
                    "llm",
                ));
                mention_seq += 1;
                covered.push((begin, end, idx));
                entities[idx].occurrences += 1;
                out.stats.llm_mentions += 1;
            }
        }

        if mode == RelationMode::Off {
            continue;
        }
        for rel in &extraction.relations {
            let (Some(subject_name), Some(predicate), Some(object_name)) = (
                rel.subject_name.as_deref().map(str::trim),
                rel.predicate.as_deref().map(str::trim),
                rel.object_name.as_deref().map(str::trim),
            ) else {
                continue;
            };
            let Some(&subject) = by_name.get(&normalize_name(subject_name)) else {
                continue;
            };
            if entities[subject].occurrences == 0 {
                continue;
            }
            // An object naming no entity is written as the literal it
            // was: the statement is evidence either way, an edge only
            // when both ends are nodes.
            let object = if rel.object_is_literal {
                None
            } else {
                by_name
                    .get(&normalize_name(object_name))
                    .copied()
                    .filter(|o| entities[*o].occurrences > 0)
            };
            if !rel.object_is_literal && object.is_none() {
                out.stats.relations_literal_object += 1;
            }

            let subject_types: Vec<String> = entities[subject].class.iter().cloned().collect();
            let verdict = match model {
                Some(m) => m.judge(predicate, &subject_types),
                None => Verdict::Rejected {
                    reason: "no model to judge against".into(),
                },
            };
            let effective = match &verdict {
                Verdict::Valid => predicate.to_string(),
                Verdict::Repaired { property, .. } => property.clone(),
                Verdict::Rejected { .. } => predicate.to_string(),
            };
            match &verdict {
                Verdict::Valid => out.stats.relations_valid += 1,
                Verdict::Repaired { .. } => out.stats.relations_repaired += 1,
                Verdict::Rejected { .. } => out.stats.relations_rejected += 1,
            }

            let admitted = !matches!(verdict, Verdict::Rejected { .. });
            let asserted = admitted && mode == RelationMode::Direct && object.is_some();
            let mut node = json!({
                "@id": format!("{doc_iri}/relation/{relation_seq}"),
                "@type": vocab::RELATION,
                "rdf:subject": { "@id": entities[subject].iri },
                "rdf:predicate": { "@id": effective },
                "rdfs:label": format!(
                    "{subject_name} | {} | {object_name}",
                    rel.predicate_label.as_deref().unwrap_or(predicate)
                ),
                vocab::VERDICT: verdict.note(),
                vocab::ASSERTED: asserted,
                vocab::EXTRACTED_BY: "llm",
                vocab::SOURCE_CHUNK: { "@id": chunk_iri },
                vocab::SOURCE_DOCUMENT: doc_iri,
            });
            relation_seq += 1;
            match object {
                Some(o) => node["rdf:object"] = json!({ "@id": entities[o].iri }),
                None => node["rdf:object"] = json!(object_name),
            }
            if let Some(excerpt) = rel
                .context
                .as_deref()
                .map(str::trim)
                .filter(|c| !c.is_empty())
            {
                node[vocab::EXCERPT] = json!(excerpt);
            }
            match &verdict {
                Verdict::Repaired { note, .. } => {
                    node[vocab::ORIGINAL_PREDICATE] = json!(predicate);
                    node[vocab::REPAIR_NOTE] = json!(note);
                }
                Verdict::Rejected { reason } => {
                    node[vocab::REJECTION_REASON] = json!(reason);
                }
                Verdict::Valid => {}
            }
            relation_nodes.push(node);

            if asserted {
                let o = object.expect("asserted implies an object entity");
                let key = (
                    entities[subject].iri.clone(),
                    effective.clone(),
                    entities[o].iri.clone(),
                );
                if direct_seen.insert(key) {
                    out.direct.push(json!({
                        "@id": entities[subject].iri,
                        effective: { "@id": entities[o].iri }
                    }));
                }
            }
        }
    }

    for r in &entities {
        if r.occurrences == 0 {
            continue;
        }
        if r.known {
            out.stats.entities_known += 1;
            // The node lives in the ledger the entity came from; only what
            // this run learned about it is added here.
            if !r.attributes.is_empty() {
                let mut node = json!({ "@id": r.iri });
                for (k, v) in &r.attributes {
                    node[k] = v.clone();
                }
                out.direct.push(node);
            }
            continue;
        }
        out.stats.entities_new += 1;
        let mut types = vec![json!(vocab::ENTITY)];
        if let Some(c) = &r.class {
            types.push(json!(c));
        }
        let mut node = json!({
            "@id": r.iri,
            "@type": types,
            "schema:name": r.name,
        });
        if !r.alternates.is_empty() {
            node["skos:altLabel"] = json!(r.alternates);
        }
        if r.off_model {
            node[vocab::OFF_MODEL] = json!(true);
        }
        if let Some(label) = &r.ner_label {
            node[vocab::NER_LABEL] = json!(label);
        }
        for (k, v) in &r.attributes {
            node[k] = v.clone();
        }
        out.nodes.push(node);
    }
    out.nodes.extend(mention_nodes);
    out.nodes.extend(relation_nodes);
    out
}

#[allow(clippy::too_many_arguments)]
fn mention_node(
    doc_iri: &str,
    chunk_iri: &str,
    chunk: &Chunk,
    seq: usize,
    begin: usize,
    end: usize,
    surface: &str,
    entity_iri: &str,
    by: &str,
) -> Value {
    let mut node = json!({
        "@id": format!("{chunk_iri}/mention/{seq}"),
        "@type": [vocab::MENTION, "nif:RFC5147String"],
        "nif:beginIndex": begin,
        "nif:endIndex": end,
        "nif:anchorOf": surface,
        "nif:referenceContext": { "@id": chunk_iri },
        "nif:entity": { "@id": entity_iri },
        vocab::EXTRACTED_BY: by,
        vocab::SOURCE_DOCUMENT: doc_iri,
    });
    if let Some(element) = chunk.element_at(begin) {
        node[vocab::SOURCE_ELEMENT] = json!(element);
    }
    node
}

/// A name-derived IRI, so one name is one node across documents and runs.
pub fn minted_iri(prefix: &str, name: &str) -> String {
    let digest = Sha256::digest(normalize_name(name).as_bytes());
    format!("{prefix}entity/{}", hex::encode(&digest[..12]))
}

/// Every span of the entity's name inside the excerpt's occurrences in the
/// chunk, as character offsets. The excerpt is matched with whitespace
/// collapsed on both sides, since the model retypes it. Falls back to the
/// name alone when the excerpt is missing or not in the chunk, and to the
/// excerpt span when the name is not inside it.
fn locate<'a>(
    text: &str,
    excerpt: Option<&str>,
    names: impl Iterator<Item = &'a str> + Clone,
) -> Vec<(usize, usize)> {
    let chars: Vec<char> = text.chars().collect();
    let folded: Vec<char> = chars
        .iter()
        .map(|c| c.to_lowercase().next().unwrap_or(*c))
        .collect();
    let mut spans = Vec::new();

    let excerpt_spans: Vec<(usize, usize)> = excerpt
        .map(|e| find_collapsed(&folded, &fold(e)))
        .unwrap_or_default();
    for (b, e) in &excerpt_spans {
        let inner = &folded[*b..*e];
        let mut found_name = false;
        for name in names.clone() {
            let needle = fold(name);
            for (nb, ne) in find_collapsed(inner, &needle) {
                spans.push((b + nb, b + ne));
                found_name = true;
            }
        }
        if !found_name {
            spans.push((*b, *e));
        }
    }
    if spans.is_empty() {
        for name in names {
            spans.extend(find_collapsed(&folded, &fold(name)));
        }
    }
    spans.sort_unstable();
    spans.dedup();
    spans
}

fn fold(s: &str) -> Vec<char> {
    s.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .map(|c| c.to_lowercase().next().unwrap_or(c))
        .collect()
}

/// Whole-word occurrences of `needle` (already whitespace-collapsed) in
/// `hay`, where any run of whitespace in `hay` matches one space.
fn find_collapsed(hay: &[char], needle: &[char]) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    if needle.is_empty() {
        return out;
    }
    let is_word = |c: char| c.is_alphanumeric() || c == '_';
    let mut start = 0;
    while start < hay.len() {
        let mut h = start;
        let mut n = 0;
        while n < needle.len() && h < hay.len() {
            if needle[n] == ' ' && hay[h].is_whitespace() {
                while h < hay.len() && hay[h].is_whitespace() {
                    h += 1;
                }
                n += 1;
            } else if hay[h] == needle[n] {
                h += 1;
                n += 1;
            } else {
                break;
            }
        }
        if n == needle.len() {
            let before_ok = start == 0 || !is_word(hay[start - 1]);
            let after_ok = h >= hay.len() || !is_word(hay[h]);
            if before_ok && after_ok {
                out.push((start, h));
                start = h;
                continue;
            }
        }
        start += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gazetteer::GazetteerBuilder;
    use crate::model::{ModelBuilder, PropertyKind};

    fn model() -> Model {
        let mut b = ModelBuilder::default();
        b.add_class_rows(&[
            json!(["schema:Person", "Person", null, null]),
            json!(["schema:Organization", "Organization", null, null]),
        ]);
        b.add_property_rows(
            &[
                json!([
                    "schema:worksFor",
                    "works for",
                    null,
                    "schema:Person",
                    "schema:Organization"
                ]),
                json!([
                    "schema:jobTitle",
                    "job title",
                    null,
                    "schema:Person",
                    "schema:Text"
                ]),
            ],
            PropertyKind::Unknown,
        );
        b.build()
    }

    fn gazetteer() -> Gazetteer {
        let mut b = GazetteerBuilder::default();
        b.add_rows(
            &[json!([
                "https://ex.org/org/acme",
                "Acme Corporation",
                "schema:Organization"
            ])],
            "skos:prefLabel",
        );
        b.add_rows(
            &[json!(["https://ex.org/org/acme", "Acme", null])],
            "skos:altLabel",
        );
        b.build("en")
    }

    fn chunk(text: &str) -> Chunk {
        Chunk {
            header_path: vec![],
            text: text.into(),
            source_ids: vec!["urn:d/element/1".into()],
            spans: vec![crate::chunk::ChunkSpan {
                source_id: "urn:d/element/1".into(),
                begin: 0,
                end: text.chars().count(),
            }],
        }
    }

    #[test]
    fn parse_tolerates_fences_and_missing_keys() {
        let x = parse_extraction("```json\n{\"entities\":[{\"name\":\"A\"}]}\n```").unwrap();
        assert_eq!(x.entities[0].name.as_deref(), Some("A"));
        assert!(x.relations.is_empty());
        assert!(parse_extraction("nope").is_err());
    }

    #[test]
    fn known_entity_keeps_its_iri_and_new_one_is_minted_once() {
        let text = "Jane Doe joined Acme as CTO. Jane Doe leads engineering.";
        let c = chunk(text);
        let g = gazetteer();
        let mentions = g.scan(text);
        let extraction = ChunkExtraction {
            entities: vec![
                LlmEntity {
                    name: Some("Jane Doe".into()),
                    class: Some("schema:Person".into()),
                    ner_label: Some("PERSON".into()),
                    context: Some("Jane  Doe joined Acme".into()),
                    attributes: Some(
                        [("schema:jobTitle".to_string(), json!("CTO"))]
                            .into_iter()
                            .collect(),
                    ),
                    ..Default::default()
                },
                LlmEntity {
                    name: Some("ACME".into()),
                    class: Some("schema:Organization".into()),
                    context: Some("joined Acme as CTO".into()),
                    ..Default::default()
                },
                LlmEntity {
                    name: Some("Ghost Inc".into()),
                    class: Some("schema:Organization".into()),
                    context: Some("Ghost Inc was never here".into()),
                    ..Default::default()
                },
                // In the text, but typed outside the ontology: noise.
                LlmEntity {
                    name: Some("CTO".into()),
                    class: Some("schema:Thing".into()),
                    context: Some("Acme as CTO".into()),
                    ..Default::default()
                },
            ],
            relations: vec![LlmRelation {
                subject_name: Some("Jane Doe".into()),
                predicate: Some("works for".into()),
                predicate_label: Some("works for".into()),
                object_name: Some("Acme".into()),
                object_is_literal: false,
                context: Some("Jane Doe joined Acme as CTO.".into()),
            }],
            from_cache: false,
        };
        let inputs = [ChunkInput {
            chunk: &c,
            chunk_iri: "urn:d/chunk/0".into(),
            mentions: &mentions,
            extraction: Some(&extraction),
        }];
        let g2 = gazetteer();
        let m = model();
        let out = resolve(
            "urn:d",
            "urn:t:",
            &inputs,
            Some(&g2),
            Some(&m),
            ResolvePolicy::default(),
        );

        assert_eq!(out.stats.gazetteer_mentions, 1);
        assert_eq!(
            out.stats.llm_mentions, 2,
            "Jane once from the excerpt (the second Jane is outside it), and CTO"
        );
        assert_eq!(out.stats.entities_known, 1);
        assert_eq!(out.stats.entities_new, 2, "Jane, and the flagged CTO");
        assert_eq!(out.stats.hallucinated, 1);
        assert_eq!(out.stats.off_model, 1);
        let cto = out
            .nodes
            .iter()
            .find(|n| n["schema:name"] == "CTO")
            .expect("off-model entity kept and flagged");
        assert_eq!(cto[vocab::OFF_MODEL], true);
        assert_eq!(cto["@type"], json!([vocab::ENTITY]));
        assert_eq!(out.stats.relations_repaired, 1);
        assert_eq!(out.stats.attributes, 1);

        let jane = out
            .nodes
            .iter()
            .find(|n| n["schema:name"] == "Jane Doe")
            .expect("minted entity node");
        let jane_iri = jane["@id"].as_str().unwrap();
        assert_eq!(jane_iri, minted_iri("urn:t:", "jane doe"));
        assert_eq!(jane["@type"][1], "schema:Person");
        assert_eq!(jane["schema:jobTitle"], "CTO");
        assert!(
            out.nodes
                .iter()
                .all(|n| n["schema:name"] != "Acme Corporation"),
            "a known entity's node is not rewritten"
        );

        let acme_mention = out
            .nodes
            .iter()
            .find(|n| n[vocab::EXTRACTED_BY] == "gazetteer")
            .unwrap();
        assert_eq!(acme_mention["nif:entity"]["@id"], "https://ex.org/org/acme");
        assert_eq!(acme_mention["nif:anchorOf"], "Acme");
        assert_eq!(acme_mention[vocab::SOURCE_ELEMENT], "urn:d/element/1");

        let rel = out
            .nodes
            .iter()
            .find(|n| n["@type"] == vocab::RELATION)
            .unwrap();
        assert_eq!(rel["rdf:predicate"]["@id"], "schema:worksFor");
        assert_eq!(rel[vocab::VERDICT], "repaired");
        assert_eq!(rel[vocab::ASSERTED], true);
        assert_eq!(rel["rdf:object"]["@id"], "https://ex.org/org/acme");
        assert_eq!(out.direct.len(), 1);
        assert_eq!(out.direct[0]["@id"], jane_iri);
        assert_eq!(
            out.direct[0]["schema:worksFor"]["@id"],
            "https://ex.org/org/acme"
        );

        let dropped = resolve(
            "urn:d",
            "urn:t:",
            &inputs,
            Some(&g2),
            Some(&m),
            ResolvePolicy {
                relations: RelationMode::Direct,
                drop_off_model: true,
            },
        );
        assert_eq!(dropped.stats.off_model_dropped, 1);
        assert!(dropped.nodes.iter().all(|n| n["schema:name"] != "CTO"));
    }

    #[test]
    fn rejected_relation_is_evidence_only_and_reified_mode_writes_no_edge() {
        let text = "Jane Doe met Bob Roe.";
        let c = chunk(text);
        let extraction = ChunkExtraction {
            entities: vec![
                LlmEntity {
                    name: Some("Jane Doe".into()),
                    class: Some("schema:Person".into()),
                    context: Some("Jane Doe met".into()),
                    ..Default::default()
                },
                LlmEntity {
                    name: Some("Bob Roe".into()),
                    class: Some("schema:Person".into()),
                    context: Some("met Bob Roe".into()),
                    ..Default::default()
                },
            ],
            relations: vec![
                LlmRelation {
                    subject_name: Some("Jane Doe".into()),
                    predicate: Some("schema:knows".into()),
                    object_name: Some("Bob Roe".into()),
                    ..Default::default()
                },
                LlmRelation {
                    subject_name: Some("Jane Doe".into()),
                    predicate: Some("schema:worksFor".into()),
                    object_name: Some("Bob Roe".into()),
                    ..Default::default()
                },
                // The object names nobody in the text: kept as a literal
                // statement, never an edge.
                LlmRelation {
                    subject_name: Some("Jane Doe".into()),
                    predicate: Some("schema:worksFor".into()),
                    object_name: Some("Initech".into()),
                    ..Default::default()
                },
            ],
            from_cache: false,
        };
        let inputs = [ChunkInput {
            chunk: &c,
            chunk_iri: "urn:d/chunk/0".into(),
            mentions: &[],
            extraction: Some(&extraction),
        }];
        let m = model();
        let out = resolve(
            "urn:d",
            "urn:t:",
            &inputs,
            None,
            Some(&m),
            ResolvePolicy {
                relations: RelationMode::Reified,
                drop_off_model: false,
            },
        );
        assert_eq!(out.stats.relations_rejected, 1);
        assert_eq!(out.stats.relations_valid, 2);
        assert_eq!(out.stats.relations_literal_object, 1);
        assert!(out.direct.is_empty());
        let literal = out
            .nodes
            .iter()
            .find(|n| n["rdf:object"] == "Initech")
            .expect("literal-object relation");
        assert_eq!(literal[vocab::ASSERTED], false);
        let direct = resolve(
            "urn:d",
            "urn:t:",
            &inputs,
            None,
            Some(&m),
            ResolvePolicy::default(),
        );
        assert_eq!(
            direct.direct.len(),
            1,
            "the literal-object relation makes no edge even in direct mode"
        );
        let rejected = out
            .nodes
            .iter()
            .find(|n| n[vocab::VERDICT] == "rejected")
            .unwrap();
        assert!(rejected[vocab::REJECTION_REASON]
            .as_str()
            .unwrap()
            .contains("schema:knows"));
        assert_eq!(rejected[vocab::ASSERTED], false);

        let off = resolve(
            "urn:d",
            "urn:t:",
            &inputs,
            None,
            Some(&m),
            ResolvePolicy {
                relations: RelationMode::Off,
                drop_off_model: false,
            },
        );
        assert!(off.nodes.iter().all(|n| n["@type"] != vocab::RELATION));
    }

    #[test]
    fn locate_collapses_whitespace_and_narrows_to_name() {
        let text = "The   quick\nfox, Reynard, ran. Reynard again.";
        let spans = locate(
            text,
            Some("quick fox, Reynard, ran"),
            ["Reynard"].into_iter(),
        );
        assert_eq!(spans, vec![(17, 24)]);
        let fallback = locate(text, Some("not in text"), ["reynard"].into_iter());
        assert_eq!(fallback.len(), 2);
        assert!(locate(text, None, ["nobody"].into_iter()).is_empty());
    }
}
