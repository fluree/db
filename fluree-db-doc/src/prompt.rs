//! The extraction prompts, shared verbatim with Fluree AI's hosted
//! extraction so an account and a local run make the same asks of the
//! model. Do not casually reword: the provider-side prompt cache is keyed
//! on the exact text, and the parser and gate downstream consume exactly
//! the JSON these ask for.

/// `{guidance}` is the project's own priorities or nothing; `{model}` is
/// the rendered ontology.
pub const SYSTEM_PROMPT_TEMPLATE: &str = "You are an entity and relation extraction engine. You extract structured information from documents using a provided ontology model. You always respond with valid JSON.

## INSTRUCTIONS

### Entities
- Identify real-world entities mentioned in the DOCUMENT that match classes in the MODEL.
- For each entity, provide:
  - \"name\": the canonical name of the entity
  - \"type\": a class URI from the MODEL (e.g. \"schema:Organization\"). Use ONLY types listed in the MODEL. If no specific class fits, use \"schema:Thing\". Prefer the most specific type available (e.g. \"schema:City\" over \"schema:Place\").
  - \"nerLabel\": one of PERSON, ORG, GPE, LOC, FAC, EVENT, PRODUCT, WORK_OF_ART, LANGUAGE, MISC, CONCEPT
  - \"context\": an exact text excerpt from the DOCUMENT where this entity appears
  - \"alternateNames\": other surface forms in the DOCUMENT for the same entity (optional)
  - \"attributes\": an object mapping MODEL property URIs to literal values stated in the DOCUMENT for this entity (optional). Use ONLY properties from the MODEL whose range is a datatype (not a class), applicable to the entity's type. Copy each value exactly as written, including units when printed. Omit the key when the DOCUMENT states no attribute values.

### Relations
- Extract only SIGNIFICANT relations that reveal an entity's role, connections, or importance in the document. Skip trivial, redundant, or encyclopedic relations not supported by the text.
- Limit to ~15 relations per document chunk. Focus on the most informative ones.
- Use ONLY properties declared in the MODEL.
- For each relation, provide:
  - \"subjectName\": name of the subject entity (must match an entity or EXISTING ENTITY name)
  - \"predicate\": the property URI from the MODEL
  - \"predicateLabel\": human-readable label
  - \"objectName\": name of the object entity or a literal value
  - \"objectIsLiteral\": true if literal, false if entity
  - \"context\": exact text excerpt supporting this relation

### Entity Reuse Rules
- EXISTING ENTITIES were already identified by upstream NER workers.
- If an EXISTING ENTITY matches one you found, use the EXACT SAME \"name\".
- You can create relations involving EXISTING ENTITIES.
- Only create NEW entities for things not already in EXISTING ENTITIES.

### Verification
1. Every entity \"type\" must exist in the MODEL. If not, use \"schema:Thing\".
2. Every relation \"predicate\" must exist in the MODEL PROPERTIES. If not, omit the relation.
3. Every entity must actually be mentioned in the DOCUMENT text.
4. Every \"attributes\" key must be a MODEL property with a datatype range, and its value must appear in the DOCUMENT. If not, omit that attribute.

{guidance}## MODEL (ontology classes and properties)
{model}";

/// Frames the project's guidance between the rules and the model: it may
/// outrank the relation-significance heuristics, never the verification
/// rules.
pub const GUIDANCE_HEADER: &str = "## PROJECT GUIDANCE
What this project is looking for. These priorities take precedence over the generic relation-significance heuristics above: a relation or entity the guidance names is significant by definition, and is never dropped to stay within the relation budget. The Verification rules still apply — types and predicates must exist in the MODEL.

";

/// `{existing}` is the EXISTING ENTITIES block, `{document}` the chunk.
pub const USER_PROMPT_TEMPLATE: &str = "Extract entities and relations from the DOCUMENT below. Return ONLY valid JSON: {\"entities\": [...], \"relations\": [...]}

## EXISTING ENTITIES (from upstream NER workers)
{existing}

## DOCUMENT
{document}";

pub fn render_guidance(guidance: Option<&str>) -> String {
    match guidance.map(str::trim).filter(|g| !g.is_empty()) {
        Some(g) => format!("{GUIDANCE_HEADER}{g}\n\n"),
        None => String::new(),
    }
}

pub fn system_prompt(model_text: &str, guidance: Option<&str>) -> String {
    system_prompt_from(SYSTEM_PROMPT_TEMPLATE, model_text, guidance)
}

/// The system prompt from any template carrying the `{model}` and
/// `{guidance}` slots. A template without the guidance slot carries no
/// guidance.
pub fn system_prompt_from(template: &str, model_text: &str, guidance: Option<&str>) -> String {
    template
        .replace("{guidance}", &render_guidance(guidance))
        .replace("{model}", model_text)
}

pub fn user_prompt(existing: &str, document: &str) -> String {
    user_prompt_from(USER_PROMPT_TEMPLATE, existing, document)
}

/// The user prompt from any template carrying the `{existing}` and
/// `{document}` slots.
pub fn user_prompt_from(template: &str, existing: &str, document: &str) -> String {
    template
        .replace("{existing}", existing.trim_end())
        .replace("{document}", document)
}

/// Sent as a second user turn after an answer that did not parse.
pub fn retry_prompt(error: &str) -> String {
    format!(
        "Your previous response was not valid JSON. Error: {error}\n\
         Please return ONLY valid JSON with {{\"entities\": [...], \"relations\": [...]}}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_prompt_substitutes_both_slots() {
        let p = system_prompt("schema:Person", None);
        assert!(p.contains("schema:Person"));
        assert!(!p.contains("{model}"));
        assert!(!p.contains("{guidance}"));
        assert!(!p.contains("PROJECT GUIDANCE"));
        let g = system_prompt("m", Some("  Find part numbers.  "));
        let guidance = g.find("## PROJECT GUIDANCE").unwrap();
        let model = g.find("## MODEL").unwrap();
        let rules = g.find("### Verification").unwrap();
        assert!(rules < guidance && guidance < model);
        assert_eq!(system_prompt("m", Some(" \n")), system_prompt("m", None));
    }

    #[test]
    fn override_templates_keep_their_slots() {
        assert_eq!(
            system_prompt_from("X {model} Y", "m", Some("focus")),
            "X m Y"
        );
        assert_eq!(user_prompt_from("D: {document}", "- x", "text"), "D: text");
    }

    #[test]
    fn user_prompt_substitutes_both_slots() {
        let p = user_prompt("- \"Acme\"\n", "Hello.");
        assert!(p.ends_with("## DOCUMENT\nHello."));
        assert!(p.contains("- \"Acme\"\n\n## DOCUMENT"));
    }
}
