//! An owned selection tree read straight from the parsed document.
//!
//! Lowering cannot use async-graphql's resolver-facing selection API
//! (`SelectionField::selection_set`, `Lookahead`): both flatten fragment spreads
//! and inline fragments into the parent selection and discard the type condition,
//! which makes `... on Person { name }` indistinguishable from a plain field on an
//! interface- or union-typed position. We therefore walk the parsed
//! `ExecutableDocument` ourselves and keep the condition on each node.
//!
//! Variables are substituted here, so everything downstream sees const values.

use std::collections::HashMap;

use async_graphql::parser::types::{
    DocumentOperations, ExecutableDocument, FragmentDefinition, OperationDefinition,
    Selection as AstSelection, SelectionSet,
};
use async_graphql::{Name, Positioned, Value, Variables};

use crate::error::{Error, Result};
use crate::limits::Limits;

/// How far into the selection tree the walk currently is.
///
/// Only a field descends: an inline fragment or a spread is flattened into the
/// level that holds it, so counting them would refuse a document that selects
/// nothing deeper than a permitted one.
#[derive(Debug, Clone, Copy)]
struct Depth {
    current: usize,
    max: usize,
}

impl Depth {
    fn descend(self) -> Result<Self> {
        let current = self.current + 1;
        if current > self.max {
            return Err(Error::Parse(format!(
                "selection nests deeper than the {} levels this endpoint allows",
                self.max
            )));
        }
        Ok(Self { current, ..self })
    }
}

/// One selected field, with its sub-selection.
#[derive(Debug, Clone, PartialEq)]
pub struct Selection {
    /// The key this field's value appears under in the response (alias or name).
    pub response_key: String,
    /// The schema field name.
    pub name: String,
    /// Arguments, with variables already resolved.
    pub arguments: Vec<(String, Value)>,
    /// The type condition of the nearest enclosing fragment, if the field came
    /// from one. `None` means the field was selected directly on the parent type.
    pub type_condition: Option<String>,
    pub children: Vec<Selection>,
}

impl Selection {
    /// Argument lookup by name.
    pub fn argument(&self, name: &str) -> Option<&Value> {
        self.arguments
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v)
    }
}

/// The root selections of one operation, plus the operation's own metadata.
#[derive(Debug, Clone)]
pub struct Operation {
    pub name: Option<String>,
    pub ty: async_graphql::parser::types::OperationType,
    pub selections: Vec<Selection>,
}

/// Extract the selection tree for `operation_name` (or the only operation, if
/// unnamed), substituting `variables`.
pub fn extract(
    doc: &ExecutableDocument,
    operation_name: Option<&str>,
    variables: &Variables,
    limits: &Limits,
) -> Result<Operation> {
    let (name, op) = pick_operation(doc, operation_name)?;
    let ctx = Ctx {
        fragments: &doc.fragments,
        variables,
        defaults: variable_defaults(&op.node),
    };
    let mut out = Vec::new();
    walk(
        &op.node.selection_set.node,
        None,
        &ctx,
        &mut Vec::new(),
        Depth {
            current: 0,
            max: limits.max_depth,
        },
        &mut out,
    )?;
    Ok(Operation {
        name: name.map(std::string::ToString::to_string),
        ty: op.node.ty,
        selections: out,
    })
}

fn pick_operation<'a>(
    doc: &'a ExecutableDocument,
    operation_name: Option<&str>,
) -> Result<(Option<&'a Name>, &'a Positioned<OperationDefinition>)> {
    match (&doc.operations, operation_name) {
        (DocumentOperations::Single(op), None) => Ok((None, op)),
        (DocumentOperations::Single(_), Some(n)) => Err(Error::Parse(format!(
            "operation `{n}` not found: the document has a single anonymous operation"
        ))),
        (DocumentOperations::Multiple(ops), Some(n)) => ops
            .get_key_value(n)
            .map(|(k, v)| (Some(k), v))
            .ok_or_else(|| Error::Parse(format!("operation `{n}` not found"))),
        (DocumentOperations::Multiple(ops), None) => {
            if ops.len() == 1 {
                let (k, v) = ops.iter().next().expect("len checked");
                Ok((Some(k), v))
            } else {
                Err(Error::Parse(
                    "the document defines several operations; specify `operationName`".to_string(),
                ))
            }
        }
    }
}

fn variable_defaults(op: &OperationDefinition) -> HashMap<Name, Value> {
    op.variable_definitions
        .iter()
        .filter_map(|d| {
            d.node
                .default_value()
                .map(|v| (d.node.name.node.clone(), v.clone()))
        })
        .collect()
}

/// What the walk carries unchanged from the document it started on.
struct Ctx<'a> {
    fragments: &'a HashMap<Name, Positioned<FragmentDefinition>>,
    variables: &'a Variables,
    defaults: HashMap<Name, Value>,
}

/// Flatten fragments into `out` while carrying the innermost type condition down.
///
/// `seen_fragments` guards against a cyclic fragment spread. Cyclic spreads are a
/// validation error async-graphql also reports, but we walk the document before
/// validation runs, so the guard has to be here.
fn walk(
    set: &SelectionSet,
    type_condition: Option<&str>,
    ctx: &Ctx<'_>,
    seen_fragments: &mut Vec<Name>,
    depth: Depth,
    out: &mut Vec<Selection>,
) -> Result<()> {
    for item in &set.items {
        match &item.node {
            AstSelection::Field(f) => {
                let f = &f.node;
                let mut arguments = Vec::with_capacity(f.arguments.len());
                for (n, v) in &f.arguments {
                    let resolved = v.node.clone().into_const_with(|var| {
                        ctx.variables
                            .get(&var)
                            .or_else(|| ctx.defaults.get(&var))
                            .cloned()
                            .ok_or_else(|| {
                                Error::Parse(format!("variable `${var}` is not defined"))
                            })
                    })?;
                    arguments.push((n.node.to_string(), resolved));
                }
                let mut children = Vec::new();
                walk(
                    &f.selection_set.node,
                    None,
                    ctx,
                    seen_fragments,
                    depth.descend()?,
                    &mut children,
                )?;
                out.push(Selection {
                    response_key: f.response_key().node.to_string(),
                    name: f.name.node.to_string(),
                    arguments,
                    type_condition: type_condition.map(std::string::ToString::to_string),
                    children,
                });
            }
            AstSelection::InlineFragment(frag) => {
                let cond = frag
                    .node
                    .type_condition
                    .as_ref()
                    .map(|c| c.node.on.node.as_str())
                    .or(type_condition);
                walk(
                    &frag.node.selection_set.node,
                    cond,
                    ctx,
                    seen_fragments,
                    depth,
                    out,
                )?;
            }
            AstSelection::FragmentSpread(spread) => {
                let fname = &spread.node.fragment_name.node;
                let Some(frag) = ctx.fragments.get(fname) else {
                    return Err(Error::Parse(format!("unknown fragment `{fname}`")));
                };
                if seen_fragments.contains(fname) {
                    return Err(Error::Parse(format!("fragment `{fname}` is cyclic")));
                }
                seen_fragments.push(fname.clone());
                let result = walk(
                    &frag.node.selection_set.node,
                    Some(frag.node.type_condition.node.on.node.as_str()),
                    ctx,
                    seen_fragments,
                    depth,
                    out,
                );
                seen_fragments.pop();
                result?;
            }
        }
    }
    Ok(())
}
