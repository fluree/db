//! `where:` argument → JSON-LD WHERE patterns.
//!
//! A filter entry binds the field's values to a fresh variable and constrains it.
//! Binding is what gives multi-valued fields their ANY semantics for free: a
//! triple pattern has one solution per value, so a subject survives if *some*
//! value satisfies the constraint. It also means a filtered field is implicitly
//! required — which is the intent of `where`, and why `EXISTS: false` has to be
//! spelled as a negation instead.
//!
//! IRI-valued constraints (`id`, and reference fields) cannot go through the
//! S-expression language, which has no IRI atom; they lower to `values` patterns.

use async_graphql::indexmap::IndexMap;
use async_graphql::Value as GqlValue;
use serde_json::json;
use serde_json::Value as Json;

use crate::error::{Error, Result};
use crate::naming::Namer;
use crate::schema::model::{Direction, Field, FieldType, SchemaModel};

/// Filter operators, in GraphDB's naming.
const OPS: &[&str] = &[
    "EQ", "NEQ", "IN", "NIN", "LT", "LTE", "GT", "GTE", "RE", "IRE", "NRE", "NIRE", "EXISTS",
];

/// Accumulates WHERE patterns and hands out fresh variable names.
pub struct PatternBuilder<'a> {
    patterns: Vec<Json>,
    next_var: usize,
    /// Expands the `ex:alice` form a client sends back to us; the lowered query
    /// carries no context, so operands have to arrive as full IRIs.
    namer: &'a Namer,
}

impl<'a> PatternBuilder<'a> {
    pub fn new(namer: &'a Namer) -> Self {
        PatternBuilder {
            patterns: Vec::new(),
            next_var: 0,
            namer,
        }
    }

    /// A variable name that cannot collide with anything a user wrote — GraphQL
    /// documents never name variables in the query IR.
    pub fn fresh_var(&mut self) -> String {
        let v = format!("?_gql{}", self.next_var);
        self.next_var += 1;
        v
    }

    pub fn push(&mut self, pattern: Json) {
        self.patterns.push(pattern);
    }

    pub fn into_patterns(self) -> Vec<Json> {
        self.patterns
    }

    /// Lower one filter object against `subject_var`, whose static type is `type_name`.
    pub fn apply(
        &mut self,
        model: &SchemaModel,
        type_name: &str,
        subject_var: &str,
        filter: &GqlValue,
    ) -> Result<()> {
        let mut nested = Vec::new();
        self.collect(model, type_name, subject_var, filter, &mut nested)?;
        self.patterns.extend(nested);
        Ok(())
    }

    /// Lower into `out` rather than straight onto `self`, so combinators can wrap
    /// the patterns their branch produced.
    fn collect(
        &mut self,
        model: &SchemaModel,
        type_name: &str,
        subject_var: &str,
        filter: &GqlValue,
        out: &mut Vec<Json>,
    ) -> Result<()> {
        let GqlValue::Object(entries) = filter else {
            return Err(Error::Lower(format!(
                "`where` on `{type_name}` must be an object"
            )));
        };

        for (key, value) in entries {
            if matches!(value, GqlValue::Null) {
                continue;
            }
            match key.as_str() {
                "AND" => {
                    for branch in as_list(value, "AND")? {
                        self.collect(model, type_name, subject_var, branch, out)?;
                    }
                }
                "OR" => {
                    let branches = as_list(value, "OR")?;
                    if branches.is_empty() {
                        continue;
                    }
                    let mut union = vec![json!("union")];
                    for branch in branches {
                        let mut branch_patterns = Vec::new();
                        self.collect(model, type_name, subject_var, branch, &mut branch_patterns)?;
                        union.push(Json::Array(branch_patterns));
                    }
                    out.push(Json::Array(union));
                }
                "EXISTS" => {
                    return Err(Error::Lower(format!(
                        "`EXISTS` is only meaningful on a field; at the top of a `where` on \
                         `{type_name}` there is nothing for it to test"
                    )))
                }
                "NOT" => {
                    let mut inner = Vec::new();
                    self.collect(model, type_name, subject_var, value, &mut inner)?;
                    out.push(not_exists(inner));
                }
                field_name => {
                    let field = model
                        .fields_of(type_name)
                        .and_then(|fs| fs.iter().find(|f| f.name == field_name))
                        .ok_or_else(|| {
                            Error::Lower(format!("`{type_name}` has no field `{field_name}`"))
                        })?;
                    self.apply_field(model, type_name, subject_var, field, value, out)?;
                }
            }
        }
        Ok(())
    }

    fn apply_field(
        &mut self,
        model: &SchemaModel,
        type_name: &str,
        subject_var: &str,
        field: &Field,
        constraint: &GqlValue,
        out: &mut Vec<Json>,
    ) -> Result<()> {
        let label = format!("{type_name}.{}", field.name);

        let GqlValue::Object(entries) = constraint else {
            return Err(Error::Lower(format!(
                "the filter on `{label}` must be an object of operators"
            )));
        };

        // A reference field's filter mixes operators on the reference itself
        // (`EXISTS`, `EQ`, `IN`) with constraints on the referenced subject, so
        // the two have to be separated before either can be lowered.
        let mut ops = IndexMap::new();
        let mut nested = IndexMap::new();
        for (key, value) in entries {
            if OPS.contains(&key.as_str()) {
                ops.insert(key.clone(), value.clone());
            } else if field.ty.is_composite() {
                nested.insert(key.clone(), value.clone());
            } else {
                return Err(Error::Lower(format!(
                    "unknown filter operator `{key}` on `{label}`"
                )));
            }
        }
        let nested = (!nested.is_empty()).then(|| GqlValue::Object(nested));

        // `EXISTS: false` is the one entry that must not bind the field.
        if let Some(GqlValue::Boolean(false)) = ops.get("EXISTS") {
            if ops.len() > 1 || nested.is_some() {
                return Err(Error::Lower(format!(
                    "`EXISTS: false` on `{label}` cannot be combined with other constraints: \
                     they would require the field to exist"
                )));
            }
            let probe = self.fresh_var();
            out.push(not_exists(vec![match field.direction {
                Direction::Forward => triple(subject_var, &field.iri, &probe),
                Direction::Reverse => triple(&probe, &field.iri, subject_var),
            }]));
            return Ok(());
        }

        // Everything else binds the field's values and constrains the binding.
        // `id` is the exception: it *is* the subject, so it needs no triple.
        let value_var = if field.is_id() {
            subject_var.to_string()
        } else {
            let v = self.fresh_var();
            // A reverse field's predicate runs the other way, so the pattern
            // has the value as its subject.
            out.push(match field.direction {
                Direction::Forward => triple(subject_var, &field.iri, &v),
                Direction::Reverse => triple(&v, &field.iri, subject_var),
            });
            v
        };

        // An enum backed by IRIs is compared the same way a reference is: the
        // S-expression language has no IRI atom, so it goes through `values`.
        let enum_is_iri_valued = match &field.ty {
            FieldType::Enum(name) => model.enum_type(name).is_some_and(|e| e.iri_valued),
            _ => false,
        };
        let iri_valued = field.is_id() || field.ty.is_composite() || enum_is_iri_valued;
        let mut exprs: Vec<String> = Vec::new();

        for (op, operand) in &ops {
            if matches!(operand, GqlValue::Null) {
                continue;
            }
            match op.as_str() {
                "EXISTS" => {} // `true`: the binding above is the whole constraint.
                "EQ" | "IN" if iri_valued => {
                    let operands: Vec<&GqlValue> = if op == "EQ" {
                        vec![operand]
                    } else {
                        as_list(operand, "IN")?.iter().collect()
                    };
                    let iris = operands
                        .into_iter()
                        .map(|v| iri_operand(model, field, v, &label))
                        .collect::<Result<Vec<_>>>()?;
                    out.push(json!([
                        "values",
                        [
                            value_var,
                            iris.into_iter()
                                .map(|iri| json!({ "@id": self.namer.expand(&iri) }))
                                .collect::<Vec<_>>()
                        ]
                    ]));
                }
                _ if iri_valued => {
                    return Err(Error::Lower(format!(
                        "`{op}` is not supported on the IRI-valued field `{label}`; \
                         use `EQ`, `IN` or `EXISTS`"
                    )));
                }
                "EQ" => exprs.push(format!(
                    "(= {value_var} {})",
                    literal(model, field, operand, &label)?
                )),
                "NEQ" => exprs.push(format!(
                    "(!= {value_var} {})",
                    literal(model, field, operand, &label)?
                )),
                "LT" => exprs.push(format!(
                    "(< {value_var} {})",
                    literal(model, field, operand, &label)?
                )),
                "LTE" => exprs.push(format!(
                    "(<= {value_var} {})",
                    literal(model, field, operand, &label)?
                )),
                "GT" => exprs.push(format!(
                    "(> {value_var} {})",
                    literal(model, field, operand, &label)?
                )),
                "GTE" => exprs.push(format!(
                    "(>= {value_var} {})",
                    literal(model, field, operand, &label)?
                )),
                "IN" | "NIN" => {
                    let items = as_list(operand, op)?
                        .iter()
                        .map(|v| literal(model, field, v, &label))
                        .collect::<Result<Vec<_>>>()?;
                    let op_name = if op == "IN" { "in" } else { "not-in" };
                    exprs.push(format!("({op_name} {value_var} [{}])", items.join(" ")));
                }
                "RE" | "IRE" | "NRE" | "NIRE" => {
                    let pattern = match operand {
                        GqlValue::String(s) => s.clone(),
                        other => {
                            return Err(Error::Lower(format!(
                                "`{op}` on `{label}` needs a string pattern, got {other}"
                            )))
                        }
                    };
                    // The engine has no regex-flags argument, so case-insensitivity
                    // goes in the pattern itself.
                    let pattern = if op.starts_with('I') || op.starts_with("NI") {
                        format!("(?i){pattern}")
                    } else {
                        pattern
                    };
                    let call = format!("(regex {value_var} {})", quote(&pattern));
                    exprs.push(if op.starts_with('N') {
                        format!("(not {call})")
                    } else {
                        call
                    });
                }
                other => {
                    return Err(Error::Lower(format!(
                        "unknown filter operator `{other}` on `{label}`"
                    )))
                }
            }
        }

        for expr in exprs {
            out.push(json!(["filter", expr]));
        }
        if let Some(nested) = nested {
            let target = field.ty.type_name().to_string();
            self.collect(model, &target, &value_var, &nested, out)?;
        }
        Ok(())
    }
}

/// `["not-exists", p1, p2, …]` — the patterns are spread, not nested in an array.
fn not_exists(patterns: Vec<Json>) -> Json {
    let mut out = vec![json!("not-exists")];
    out.extend(patterns);
    Json::Array(out)
}

/// `{"@id": subject, predicate: object}` as a WHERE pattern.
pub fn triple(subject: &str, predicate: &str, object: &str) -> Json {
    json!({ "@id": subject, predicate: object })
}

fn as_list<'a>(value: &'a GqlValue, op: &str) -> Result<&'a Vec<GqlValue>> {
    match value {
        GqlValue::List(items) => Ok(items),
        other => Err(Error::Lower(format!("`{op}` needs a list, got {other}"))),
    }
}

/// The IRI a filter operand names.
///
/// An enum operand arrives as the GraphQL name; the underlying IRI is what the
/// query has to carry.
fn iri_operand(
    model: &SchemaModel,
    field: &Field,
    value: &GqlValue,
    label: &str,
) -> Result<String> {
    if let FieldType::Enum(name) = &field.ty {
        let GqlValue::Enum(member) = value else {
            return Err(Error::Lower(format!(
                "`{label}` needs a `{name}` value, got {value}"
            )));
        };
        return model
            .enum_type(name)
            .and_then(|e| e.value_for(member.as_str()))
            .map(std::string::ToString::to_string)
            .ok_or_else(|| {
                Error::Lower(format!("`{member}` is not a member of `{name}` ({label})"))
            });
    }
    match value {
        GqlValue::String(s) => Ok(s.clone()),
        other => Err(Error::Lower(format!(
            "`{label}` needs an IRI string, got {other}"
        ))),
    }
}

/// Render a GraphQL scalar as an S-expression atom.
fn literal(model: &SchemaModel, field: &Field, value: &GqlValue, label: &str) -> Result<String> {
    // An enum member is a name for a value, not the value itself.
    if let (FieldType::Enum(name), GqlValue::Enum(member)) = (&field.ty, value) {
        let underlying = model
            .enum_type(name)
            .and_then(|e| e.value_for(member.as_str()))
            .ok_or_else(|| {
                Error::Lower(format!("`{member}` is not a member of `{name}` ({label})"))
            })?;
        return Ok(quote(underlying));
    }
    Ok(match value {
        GqlValue::String(s) => quote(s),
        GqlValue::Number(n) => n.to_string(),
        GqlValue::Boolean(b) => b.to_string(),
        GqlValue::Enum(e) => quote(e.as_str()),
        other => {
            return Err(Error::Lower(format!(
                "`{label}` cannot be compared against {other}"
            )))
        }
    })
}

/// Quote a string for the S-expression tokenizer, which understands `\"` and `\\`.
fn quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        if ch == '"' || ch == '\\' {
            out.push('\\');
        }
        out.push(ch);
    }
    out.push('"');
    out
}
