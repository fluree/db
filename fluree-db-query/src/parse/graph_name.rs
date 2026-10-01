//! How a graph name written in a JSON-LD document resolves.
//!
//! One order serves every position a name can be written in: insert and
//! delete templates (a node-level `@graph` selector, a `["graph", g, …]`
//! item, the update `graph` key) and a node-level `@graph` in `where`. Each
//! position then decides what the classified name means there (a template
//! cannot write `txn-meta`, say), but none of them reads a name differently.

use fluree_db_core::dataset_ref::GraphSel;
use fluree_graph_json_ld::{details_with_vocab_policy, ParsedContext};
use std::collections::HashMap;

/// The name an update's WHERE dataset gives the ledger's default graph when
/// the WHERE's own default graph is another (a top-level `graph`, or `from`),
/// so that a where node's `"@graph": "default"` reads the graph a template's
/// `"default"` writes. It contains a space, which no IRI can, so no graph
/// written under an IRI has it; an update refuses it as a graph name or VALUES
/// value it writes ([`reserved_graph_name_refusal`]).
pub const LEDGER_DEFAULT_GRAPH: &str = "@ledger default";

/// The refusal for [`LEDGER_DEFAULT_GRAPH`] written as a graph name or a
/// VALUES value in an update: there it names only the ledger's default graph,
/// which a `where` reads by `"@graph": "default"`.
pub fn reserved_graph_name_refusal() -> String {
    format!(
        "\"{LEDGER_DEFAULT_GRAPH}\" is reserved in an update for the ledger's default \
         graph; write \"@graph\": \"default\" in the where"
    )
}

/// In an update (`env.ledger_id` set), the refusal for `name` written as a
/// graph name or VALUES value in its `where` ([`reserved_graph_name_refusal`]).
pub fn reserved_graph_name(env: &GraphNameEnv, name: &str) -> Option<String> {
    (env.ledger_id.is_some() && name == LEDGER_DEFAULT_GRAPH).then(reserved_graph_name_refusal)
}

/// What resolves a graph name beyond the `@context`: the ledger the document
/// belongs to (for the `config` and `txn-meta` keywords) and the `fromNamed`
/// aliases in scope. Empty for a document with neither, such as a query.
#[derive(Debug, Clone, Default)]
pub struct GraphNameEnv {
    /// The ledger (`name:branch`) the document writes or reads.
    pub ledger_id: Option<String>,
    /// `fromNamed` aliases, alias -> graph IRI.
    pub aliases: HashMap<String, String>,
    /// Where `"default"` in a `where` is the ledger's default graph but not
    /// the WHERE's own default graph, the dataset name that reads it
    /// ([`LEDGER_DEFAULT_GRAPH`]). `None`: `"default"` is the WHERE's default
    /// graph (an update without `graph` or `from`, or a query).
    pub ledger_default_graph: Option<String>,
}

/// A graph name as written, classified by the one resolution order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WrittenGraphName<'a> {
    /// `?name`: a variable bound by a `where`.
    Var(&'a str),
    /// A `fromNamed` alias and the IRI it names. Checked before the
    /// keywords, so an alias named `config` keeps meaning its IRI.
    Alias { alias: &'a str, iri: &'a str },
    /// `default`: the default graph.
    Default,
    /// `config`: the ledger's own config graph.
    Config,
    /// `txn-meta`: the ledger's transaction-metadata graph.
    TxnMeta,
    /// Anything else, expanded as a node identifier (compact IRIs and
    /// `@base` apply, `@vocab` does not). Not necessarily absolute.
    Expanded(String),
}

/// Classify `raw`: a variable, else a `fromNamed` alias, else a keyword
/// (from the one keyword table, [`GraphSel::keyword`]), else a node
/// identifier expanded against `context`.
pub fn classify_written_graph_name<'a>(
    raw: &'a str,
    aliases: &'a HashMap<String, String>,
    context: &ParsedContext,
    strict_compact_iri: bool,
) -> fluree_graph_json_ld::Result<WrittenGraphName<'a>> {
    if raw.starts_with('?') {
        return Ok(WrittenGraphName::Var(raw));
    }
    if let Some((alias, iri)) = aliases.get_key_value(raw) {
        return Ok(WrittenGraphName::Alias { alias, iri });
    }
    match GraphSel::keyword(raw) {
        Some(GraphSel::Default) => Ok(WrittenGraphName::Default),
        Some(GraphSel::Config) => Ok(WrittenGraphName::Config),
        Some(GraphSel::TxnMeta) => Ok(WrittenGraphName::TxnMeta),
        Some(GraphSel::Named(_)) | None => {
            let (expanded, _) = details_with_vocab_policy(raw, context, false, strict_compact_iri)?;
            Ok(WrittenGraphName::Expanded(expanded))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn one_order_variable_alias_keyword_expansion() {
        let context = fluree_graph_json_ld::parse_context(&json!({
            "ex": "http://example.org/",
            "@base": "http://base.example/"
        }))
        .unwrap();
        let aliases: HashMap<String, String> = [
            (
                "config".to_string(),
                "http://example.org/aliased".to_string(),
            ),
            ("g1".to_string(), "http://example.org/one".to_string()),
        ]
        .into();
        let classify =
            |raw: &'static str| classify_written_graph_name(raw, &aliases, &context, true).unwrap();

        assert_eq!(classify("?g"), WrittenGraphName::Var("?g"));
        assert_eq!(
            classify("g1"),
            WrittenGraphName::Alias {
                alias: "g1",
                iri: "http://example.org/one"
            }
        );
        // An alias wins over the keyword it spells.
        assert!(matches!(classify("config"), WrittenGraphName::Alias { .. }));
        assert_eq!(classify("default"), WrittenGraphName::Default);
        assert_eq!(classify("txn-meta"), WrittenGraphName::TxnMeta);
        assert_eq!(
            classify("ex:g"),
            WrittenGraphName::Expanded("http://example.org/g".to_string())
        );
        assert_eq!(
            classify("http://example.org/full"),
            WrittenGraphName::Expanded("http://example.org/full".to_string())
        );
        // `@base` applies (an identifier, not a vocabulary term).
        assert_eq!(
            classify("rel"),
            WrittenGraphName::Expanded("http://base.example/rel".to_string())
        );

        let no_aliases = HashMap::new();
        assert_eq!(
            classify_written_graph_name("config", &no_aliases, &context, true).unwrap(),
            WrittenGraphName::Config
        );
    }
}
