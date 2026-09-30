//! `# PRAGMA name: value` directives — see [`Pragmas`].

use crate::ast::{MetaPragma, Pragmas, Prologue, QueryBody, SparqlAst};
use crate::diag::{DiagCode, Diagnostic};
use crate::lex::Comment;

/// The request forms a pragma applies to.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Form {
    Query,
    Update,
    Both,
}

/// Every pragma name, in the order the unknown-name error lists them.
const NAMES: &[(&str, Form)] = &[
    ("reasoning", Form::Query),
    ("reasoning-max-facts", Form::Query),
    ("reasoning-max-seconds", Form::Query),
    ("reasoning-max-memory-mb", Form::Query),
    ("include-system-facts", Form::Query),
    ("union-default-graph", Form::Query),
    ("min-t", Form::Query),
    ("meta", Form::Both),
    ("max-fuel", Form::Both),
    ("identity", Form::Both),
    ("policy-class", Form::Both),
    ("policy-values", Form::Both),
    ("default-allow", Form::Both),
    ("event-time", Form::Update),
    ("validation-mode", Form::Update),
    ("unique-properties", Form::Update),
];

/// Collect the `# PRAGMA` directives among `comments`, with an error
/// diagnostic for each one that cannot be applied to `ast`'s request form.
pub(super) fn extract_pragmas(comments: &[Comment], ast: &SparqlAst) -> (Pragmas, Vec<Diagnostic>) {
    // An update's prologue accumulates across its operations; the last
    // operation's holds every prefix the request declares.
    let (is_update, prologue) = match &ast.body {
        QueryBody::Update(request) => (
            true,
            request
                .operations
                .last()
                .map_or(&ast.prologue, |op| &op.prologue),
        ),
        _ => (false, &ast.prologue),
    };
    let request = Request {
        is_update,
        prologue,
    };
    let mut pragmas = Pragmas::default();
    let mut diagnostics = Vec::new();

    for comment in comments {
        let Some(rest) = strip_keyword_ci(&comment.text, "PRAGMA") else {
            continue;
        };
        let rest = rest.trim_start();
        let name_end = rest
            .find(|c: char| c.is_whitespace() || c == ':')
            .unwrap_or(rest.len());
        let (name, value) = rest.split_at(name_end);
        let value = value.trim_start();
        let value = value.strip_prefix(':').unwrap_or(value).trim();

        if let Err(message) = request.apply(&mut pragmas, name, value) {
            diagnostics.push(
                Diagnostic::error(DiagCode::InvalidPragma, message, comment.span).with_help(
                    "a comment that starts with PRAGMA is a Fluree directive; \
                     reword the comment if it is not meant as one",
                ),
            );
        }
    }

    (pragmas, diagnostics)
}

struct Request<'a> {
    is_update: bool,
    prologue: &'a Prologue,
}

impl Request<'_> {
    fn apply(&self, pragmas: &mut Pragmas, name: &str, value: &str) -> Result<(), String> {
        let name = name.to_ascii_lowercase();
        let Some((name, form)) = NAMES.iter().copied().find(|(known, _)| *known == name) else {
            let known: Vec<&str> = NAMES.iter().map(|(n, _)| *n).collect();
            return Err(if name.is_empty() {
                format!("expected a pragma name after PRAGMA: {}", known.join(", "))
            } else {
                format!(
                    "unknown pragma `{name}`; expected one of: {}",
                    known.join(", ")
                )
            });
        };
        match (form, self.is_update) {
            (Form::Query, true) => {
                return Err(format!("pragma `{name}` applies to queries, not updates"))
            }
            (Form::Update, false) => {
                return Err(format!("pragma `{name}` applies to updates, not queries"))
            }
            _ => {}
        }

        match name {
            // Last pragma wins if repeated; an empty mode list is preserved so
            // lowering can reject `# PRAGMA reasoning:` with no value.
            "reasoning" => pragmas.reasoning = Some(list(value)),
            // The raw value is preserved (even if empty) so lowering can reject
            // an invalid number with a proper error.
            "reasoning-max-facts" => pragmas.reasoning_max_facts = Some(value.to_string()),
            "reasoning-max-seconds" => pragmas.reasoning_max_seconds = Some(value.to_string()),
            "reasoning-max-memory-mb" => pragmas.reasoning_max_memory_mb = Some(value.to_string()),
            "include-system-facts" => pragmas.include_system_facts = Some(boolean(name, value)?),
            "union-default-graph" => pragmas.union_default_graph = Some(boolean(name, value)?),
            "min-t" => {
                pragmas.min_t = Some(
                    value
                        .parse::<i64>()
                        .ok()
                        .filter(|t| *t >= 0)
                        .ok_or_else(|| format!("pragma `{name}` expects a non-negative integer"))?,
                );
            }
            "meta" => pragmas.meta = Some(meta(value)?),
            "max-fuel" => {
                pragmas.max_fuel = Some(
                    value
                        .parse::<f64>()
                        .ok()
                        .filter(|f| f.is_finite() && *f >= 0.0)
                        .ok_or_else(|| format!("pragma `{name}` expects a non-negative number"))?,
                );
            }
            "identity" => match self.iris(value).as_slice() {
                [iri] => pragmas.identity = Some(iri.clone()),
                _ => return Err(format!("pragma `{name}` expects one IRI")),
            },
            "policy-class" => {
                let classes = self.iris(value);
                if classes.is_empty() {
                    return Err(format!("pragma `{name}` expects at least one IRI"));
                }
                pragmas.policy_class = Some(classes);
            }
            "policy-values" => match serde_json::from_str(value) {
                Ok(serde_json::Value::Object(values)) => pragmas.policy_values = Some(values),
                _ => return Err(format!("pragma `{name}` expects a one-line JSON object")),
            },
            "default-allow" => pragmas.default_allow = Some(boolean(name, value)?),
            "event-time" => {
                if value.is_empty() {
                    return Err(format!("pragma `{name}` expects an RFC 3339 timestamp"));
                }
                pragmas.event_time = Some(value.to_string());
            }
            "validation-mode" => {
                let mode = value.to_ascii_lowercase();
                if mode != "warn" && mode != "reject" {
                    return Err(format!("pragma `{name}` expects warn or reject"));
                }
                pragmas.validation_mode = Some(mode);
            }
            "unique-properties" => {
                let properties = self.iris(value);
                if properties.is_empty() {
                    return Err(format!("pragma `{name}` expects at least one IRI"));
                }
                pragmas.unique_properties = Some(properties);
            }
            _ => unreachable!("every name in NAMES has an arm"),
        }
        Ok(())
    }

    /// IRIs separated by commas or whitespace. `<…>` is taken as written; a
    /// `prefix:local` whose prefix the request declares is expanded, and anything
    /// else (a DID, a URN, an unprefixed full IRI) is kept as is.
    fn iris(&self, value: &str) -> Vec<String> {
        list(value)
            .into_iter()
            .map(|iri| {
                if let Some(inner) = iri.strip_prefix('<').and_then(|i| i.strip_suffix('>')) {
                    return inner.to_string();
                }
                iri.split_once(':')
                    .and_then(|(prefix, local)| {
                        self.prologue
                            .prefixes
                            .iter()
                            .rev()
                            .find(|decl| decl.prefix.as_ref() == prefix)
                            .map(|decl| format!("{}{local}", decl.iri))
                    })
                    .unwrap_or(iri)
            })
            .filter(|iri| !iri.is_empty())
            .collect()
    }
}

/// Items separated by commas or whitespace.
fn list(value: &str) -> Vec<String> {
    value
        .split([',', ' ', '\t'])
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

fn boolean(name: &str, value: &str) -> Result<bool, String> {
    if value.eq_ignore_ascii_case("true") {
        Ok(true)
    } else if value.eq_ignore_ascii_case("false") {
        Ok(false)
    } else {
        Err(format!("pragma `{name}` expects true or false"))
    }
}

/// `true`, `false`, or a list drawn from `time`, `fuel`, `policy`.
fn meta(value: &str) -> Result<MetaPragma, String> {
    if value.eq_ignore_ascii_case("true") {
        return Ok(MetaPragma {
            time: true,
            fuel: true,
            policy: true,
        });
    }
    if value.eq_ignore_ascii_case("false") {
        return Ok(MetaPragma::default());
    }
    let items = list(value);
    if items.is_empty() {
        return Err("pragma `meta` expects true, false, or a list of time, fuel, policy".into());
    }
    let mut meta = MetaPragma::default();
    for item in items {
        match item.to_ascii_lowercase().as_str() {
            "time" => meta.time = true,
            "fuel" => meta.fuel = true,
            "policy" => meta.policy = true,
            other => {
                return Err(format!(
                    "pragma `meta` does not know `{other}`; expected time, fuel, or policy"
                ))
            }
        }
    }
    Ok(meta)
}

/// Strip a case-insensitive keyword prefix followed by a word boundary
/// (whitespace, `:`, or end of input). Returns the remainder.
fn strip_keyword_ci<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    let trimmed = input.trim_start();
    if trimmed.len() < keyword.len() || !trimmed.is_char_boundary(keyword.len()) {
        return None;
    }
    let (head, rest) = trimmed.split_at(keyword.len());
    if !head.eq_ignore_ascii_case(keyword) {
        return None;
    }
    match rest.chars().next() {
        None => Some(rest),
        Some(c) if c.is_whitespace() || c == ':' => Some(rest),
        Some(_) => None,
    }
}
