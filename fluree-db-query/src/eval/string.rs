//! String function implementations
//!
//! Implements SPARQL string functions: STR, LANG, LCASE, UCASE, STRLEN,
//! CONTAINS, STRSTARTS, STRENDS, REGEX, CONCAT, STRBEFORE, STRAFTER,
//! REPLACE, SUBSTR, ENCODE_FOR_URI, STRDT, STRLANG

use crate::binding::{Binding, RowAccess};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::Expression;
use fluree_db_core::FlakeValue;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use std::sync::Arc;

use super::helpers::{build_regex_with_flags, check_arity};
use super::value::ComparableValue;
use crate::parse::UnresolvedDatatypeConstraint;

fn anchored_literal_regex_prefix<'a>(pattern: &'a str, flags: &str) -> Option<&'a str> {
    if !flags.is_empty() {
        return None;
    }
    let prefix = pattern.strip_prefix('^')?;
    if prefix.is_empty() {
        return None;
    }
    if prefix.bytes().any(|b| {
        matches!(
            b,
            b'.' | b'+'
                | b'*'
                | b'?'
                | b'('
                | b')'
                | b'['
                | b']'
                | b'{'
                | b'}'
                | b'|'
                | b'\\'
                | b'^'
                | b'$'
        )
    }) {
        return None;
    }
    Some(prefix)
}

/// Extract the language tag from a binding, if present.
/// Returns Some(lang) for language-tagged literals, None otherwise.
/// Handles both materialized (`Lit`) and binary-store (`EncodedLit`) bindings.
fn extract_lang_tag<R: RowAccess>(
    expr: &Expression,
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Option<Arc<str>> {
    if let Expression::Var(var_id) = expr {
        match row.get(*var_id) {
            Some(Binding::Lit { dtc, .. }) => {
                return dtc.lang_tag().map(Arc::from);
            }
            Some(Binding::EncodedLit { lang_id, .. }) => {
                if let Some(store) = ctx.and_then(|c| c.binary_store.as_deref()) {
                    if let Some(meta) = store.decode_meta(*lang_id, i32::MIN) {
                        if let Some(lang_str) = meta.lang {
                            return Some(Arc::from(lang_str));
                        }
                    }
                }
            }
            _ => {}
        }
    }
    None
}

/// Wrap a string result with an optional language tag.
/// If lang is Some, returns TypedLiteral with the language tag.
/// Otherwise returns a plain String.
fn string_with_lang(s: &str, lang: Option<Arc<str>>) -> ComparableValue {
    match lang {
        Some(tag) => ComparableValue::TypedLiteral {
            val: FlakeValue::String(s.to_string()),
            dtc: Some(UnresolvedDatatypeConstraint::LangTag(tag)),
        },
        None => ComparableValue::String(Arc::from(s)),
    }
}

pub fn eval_str<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "STR")?;
    let val = args[0].eval_to_comparable(row, ctx)?;
    Ok(val.and_then(|v| match &v {
        ComparableValue::Sid(..) => {
            // Expand SID to full IRI using namespace codes from execution context.
            // Per W3C SPARQL spec, STR() on an IRI must return the full IRI string.
            let namespaces = ctx.map(|c| c.active_snapshot.namespaces());
            v.into_string_value_with_namespaces(namespaces)
        }
        _ => v.into_string_value(),
    }))
}

pub fn eval_lang<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "LANG")?;
    let tag = match &args[0] {
        Expression::Var(var_id) => match row.get(*var_id) {
            Some(Binding::Lit { dtc, .. }) => dtc
                .lang_tag()
                .map(std::string::ToString::to_string)
                .unwrap_or_default(),
            Some(Binding::EncodedLit { lang_id, .. }) => {
                if let Some(store) = ctx.and_then(|c| c.binary_store.as_deref()) {
                    store
                        .decode_meta(*lang_id, i32::MIN)
                        .and_then(|m| m.lang)
                        .unwrap_or_default()
                } else {
                    String::new()
                }
            }
            _ => String::new(),
        },
        _ => String::new(),
    };
    Ok(Some(ComparableValue::String(Arc::from(tag))))
}

pub fn eval_lcase<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "LCASE")?;
    let lang = extract_lang_tag(&args[0], row, ctx);
    match args[0].eval_to_comparable(row, ctx)? {
        Some(v) => match v.as_str() {
            Some(s) => Ok(Some(string_with_lang(&s.to_lowercase(), lang))),
            None => Err(QueryError::InvalidFilter(
                "LCASE requires a string argument".to_string(),
            )),
        },
        None => Ok(None),
    }
}

pub fn eval_ucase<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "UCASE")?;
    let lang = extract_lang_tag(&args[0], row, ctx);
    match args[0].eval_to_comparable(row, ctx)? {
        Some(v) => match v.as_str() {
            Some(s) => Ok(Some(string_with_lang(&s.to_uppercase(), lang))),
            None => Err(QueryError::InvalidFilter(
                "UCASE requires a string argument".to_string(),
            )),
        },
        None => Ok(None),
    }
}

pub fn eval_strlen<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "STRLEN")?;
    match args[0].eval_to_comparable(row, ctx)? {
        Some(v) => match v.as_str() {
            Some(s) => {
                // ASCII fast-path: len() is O(1) and equals char count for ASCII
                let len = if s.is_ascii() {
                    s.len()
                } else {
                    s.chars().count()
                };
                Ok(Some(ComparableValue::Long(len as i64)))
            }
            None => Err(QueryError::InvalidFilter(
                "STRLEN requires a string argument".to_string(),
            )),
        },
        None => Ok(None),
    }
}

pub fn eval_contains<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 2, "CONTAINS")?;
    let haystack = args[0].eval_to_comparable(row, ctx)?;
    let needle = args[1].eval_to_comparable(row, ctx)?;
    match (haystack, needle) {
        (Some(ComparableValue::String(h)), Some(ComparableValue::String(n))) => {
            Ok(Some(ComparableValue::Bool(h.contains(n.as_ref()))))
        }
        (None, _) | (_, None) => Ok(None),
        _ => Err(QueryError::InvalidFilter(
            "CONTAINS requires string arguments".to_string(),
        )),
    }
}

pub fn eval_str_starts<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 2, "STRSTARTS")?;
    let haystack = args[0].eval_to_comparable(row, ctx)?;
    let prefix = args[1].eval_to_comparable(row, ctx)?;
    match (haystack, prefix) {
        (Some(ComparableValue::String(h)), Some(ComparableValue::String(p))) => {
            Ok(Some(ComparableValue::Bool(h.starts_with(p.as_ref()))))
        }
        (None, _) | (_, None) => Ok(None),
        _ => Err(QueryError::InvalidFilter(
            "STRSTARTS requires string arguments".to_string(),
        )),
    }
}

pub fn eval_str_ends<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 2, "STRENDS")?;
    let haystack = args[0].eval_to_comparable(row, ctx)?;
    let suffix = args[1].eval_to_comparable(row, ctx)?;
    match (haystack, suffix) {
        (Some(ComparableValue::String(h)), Some(ComparableValue::String(s))) => {
            Ok(Some(ComparableValue::Bool(h.ends_with(s.as_ref()))))
        }
        (None, _) | (_, None) => Ok(None),
        _ => Err(QueryError::InvalidFilter(
            "STRENDS requires string arguments".to_string(),
        )),
    }
}

pub fn eval_regex<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    if args.len() < 2 {
        return Err(QueryError::InvalidFilter(
            "REGEX requires 2-3 arguments".to_string(),
        ));
    }
    let text = args[0].eval_to_comparable(row, ctx)?;
    let pattern = args[1].eval_to_comparable(row, ctx)?;
    let flags = if args.len() > 2 {
        match args[2].eval_to_comparable(row, ctx)? {
            Some(v) => v
                .as_str()
                .map(std::string::ToString::to_string)
                .ok_or_else(|| {
                    QueryError::InvalidFilter("REGEX flags must be a string".to_string())
                })?,
            None => return Ok(None),
        }
    } else {
        String::new()
    };

    match (text, pattern) {
        (Some(ComparableValue::String(t)), Some(ComparableValue::String(p))) => {
            if let Some(prefix) = anchored_literal_regex_prefix(&p, &flags) {
                return Ok(Some(ComparableValue::Bool(t.starts_with(prefix))));
            }
            let re = build_regex_with_flags(&p, &flags)?;
            if let Some(ctx) = ctx {
                ctx.tracker.consume_fuel(1)?;
            }
            Ok(Some(ComparableValue::Bool(re.is_match(&t))))
        }
        (None, _) | (_, None) => Ok(None),
        _ => Err(QueryError::InvalidFilter(
            "REGEX requires string arguments".to_string(),
        )),
    }
}

pub fn eval_concat<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    let mut result = String::new();
    // Per W3C: preserve language tag only if ALL args have the same tag
    let mut common_lang: Option<Option<Arc<str>>> = None;
    for arg in args {
        let lang = extract_lang_tag(arg, row, ctx);
        match &common_lang {
            None => common_lang = Some(lang),
            Some(prev) => {
                if *prev != lang {
                    common_lang = Some(None); // mismatch → no tag
                }
            }
        }
        if let Some(val) = arg.eval_to_comparable(row, ctx)? {
            if let Some(s) = val.as_str() {
                result.push_str(s);
            }
        }
    }
    let lang = common_lang.flatten();
    Ok(Some(string_with_lang(&result, lang)))
}

/// Extract the `(string content, language tag)` of a STRBEFORE/STRAFTER argument.
///
/// Returns `None` when the argument is unbound or not a string-typed literal
/// (e.g. a number or IRI) — both cases make the function raise a type error,
/// which demotes to an unbound result. The language tag comes from a
/// language-tagged `TypedLiteral` value (constants) or, for variable bindings
/// that materialize as a plain string, from [`extract_lang_tag`].
fn str_arg_and_lang<R: RowAccess>(
    expr: &Expression,
    value: Option<ComparableValue>,
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Option<(String, Option<Arc<str>>)> {
    match value? {
        ComparableValue::String(s) => Some((s.to_string(), extract_lang_tag(expr, row, ctx))),
        ComparableValue::TypedLiteral {
            val: FlakeValue::String(s),
            dtc,
        } => {
            let lang = match dtc {
                Some(UnresolvedDatatypeConstraint::LangTag(tag)) => Some(tag),
                // xsd:string (or any non-lang datatype) carries no language tag.
                _ => extract_lang_tag(expr, row, ctx),
            };
            Some((s, lang))
        }
        _ => None,
    }
}

/// SPARQL 1.1 §17.4.3.5 argument compatibility for STRBEFORE/STRAFTER.
///
/// The search string (`arg2`) is compatible when it is a simple literal /
/// `xsd:string` (no language tag), or when it shares `arg1`'s language tag.
/// An incompatible pair raises a type error (→ unbound).
fn args_compatible(lang1: &Option<Arc<str>>, lang2: &Option<Arc<str>>) -> bool {
    match lang2 {
        None => true,
        Some(l2) => lang1.as_deref() == Some(l2.as_ref()),
    }
}

pub fn eval_str_before<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 2, "STRBEFORE")?;
    let arg1 = args[0].eval_to_comparable(row, ctx)?;
    let arg2 = args[1].eval_to_comparable(row, ctx)?;
    let (Some((s, lang1)), Some((sub, lang2))) = (
        str_arg_and_lang(&args[0], arg1, row, ctx),
        str_arg_and_lang(&args[1], arg2, row, ctx),
    ) else {
        return Ok(None);
    };
    if !args_compatible(&lang1, &lang2) {
        return Ok(None);
    }
    match s.find(&sub) {
        // Found (including the empty search string, found at position 0):
        // the result carries arg1's language tag.
        Some(pos) => Ok(Some(string_with_lang(&s[..pos], lang1))),
        // Not found: a plain empty simple literal (no language tag).
        None => Ok(Some(ComparableValue::String(Arc::from("")))),
    }
}

pub fn eval_str_after<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 2, "STRAFTER")?;
    let arg1 = args[0].eval_to_comparable(row, ctx)?;
    let arg2 = args[1].eval_to_comparable(row, ctx)?;
    let (Some((s, lang1)), Some((sub, lang2))) = (
        str_arg_and_lang(&args[0], arg1, row, ctx),
        str_arg_and_lang(&args[1], arg2, row, ctx),
    ) else {
        return Ok(None);
    };
    if !args_compatible(&lang1, &lang2) {
        return Ok(None);
    }
    match s.find(&sub) {
        // Found (the empty search string matches at 0 → the whole string):
        // the result carries arg1's language tag.
        Some(pos) => Ok(Some(string_with_lang(&s[pos + sub.len()..], lang1))),
        // Not found: a plain empty simple literal (no language tag).
        None => Ok(Some(ComparableValue::String(Arc::from("")))),
    }
}

pub fn eval_replace<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    if args.len() < 3 {
        return Err(QueryError::InvalidFilter(
            "REPLACE requires 3-4 arguments".to_string(),
        ));
    }
    let lang = extract_lang_tag(&args[0], row, ctx);
    let input = args[0].eval_to_comparable(row, ctx)?;
    let pattern = args[1].eval_to_comparable(row, ctx)?;
    let replacement = args[2].eval_to_comparable(row, ctx)?;
    let flags = if args.len() > 3 {
        match args[3].eval_to_comparable(row, ctx)? {
            Some(v) => v
                .as_str()
                .map(std::string::ToString::to_string)
                .ok_or_else(|| {
                    QueryError::InvalidFilter("REPLACE flags must be a string".to_string())
                })?,
            None => return Ok(None),
        }
    } else {
        String::new()
    };

    match (input, pattern, replacement) {
        (
            Some(ComparableValue::String(s)),
            Some(ComparableValue::String(p)),
            Some(ComparableValue::String(r)),
        ) => {
            let re = build_regex_with_flags(&p, &flags)?;
            if let Some(ctx) = ctx {
                ctx.tracker.consume_fuel(1)?;
            }
            let replaced = re.replace_all(&s, r.as_ref()).into_owned();
            Ok(Some(string_with_lang(&replaced, lang)))
        }
        (None, _, _) | (_, None, _) | (_, _, None) => Ok(None),
        _ => Ok(None),
    }
}

pub fn eval_substr<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    if args.len() < 2 || args.len() > 3 {
        return Err(QueryError::InvalidFilter(
            "SUBSTR requires 2-3 arguments".to_string(),
        ));
    }
    let lang = extract_lang_tag(&args[0], row, ctx);
    let input = args[0].eval_to_comparable(row, ctx)?;
    let start = args[1].eval_to_comparable(row, ctx)?;
    let length = if args.len() > 2 {
        args[2].eval_to_comparable(row, ctx)?
    } else {
        None
    };

    let s = match input {
        Some(ComparableValue::String(s)) => s,
        None => return Ok(None),
        _ => {
            return Err(QueryError::InvalidFilter(
                "SUBSTR requires a string as first argument".to_string(),
            ))
        }
    };

    let start_1 = match start {
        Some(ComparableValue::Long(n)) => n,
        None => return Ok(None),
        _ => {
            return Err(QueryError::InvalidFilter(
                "SUBSTR requires an integer as second argument".to_string(),
            ))
        }
    };

    let start_0 = if start_1 < 1 {
        0
    } else {
        (start_1 - 1) as usize
    };

    // ASCII fast-path: byte indexing is safe and avoids Vec<char> allocation
    if s.is_ascii() {
        let byte_count = s.len();
        if start_0 >= byte_count {
            return Ok(Some(ComparableValue::String(Arc::from(""))));
        }
        let result = match length {
            Some(ComparableValue::Long(len)) if len > 0 => {
                let end = (start_0 + (len as usize)).min(byte_count);
                &s[start_0..end]
            }
            Some(ComparableValue::Long(_)) => "",
            None => &s[start_0..],
            Some(_) => {
                return Err(QueryError::InvalidFilter(
                    "SUBSTR requires an integer as third argument".to_string(),
                ))
            }
        };
        return Ok(Some(string_with_lang(result, lang)));
    }

    // Multi-byte path: use character-based indexing per W3C SPARQL spec
    let chars: Vec<char> = s.chars().collect();
    let char_count = chars.len();

    if start_0 >= char_count {
        return Ok(Some(ComparableValue::String(Arc::from(""))));
    }

    let result: String = match length {
        Some(ComparableValue::Long(len)) if len > 0 => {
            let end = (start_0 + (len as usize)).min(char_count);
            chars[start_0..end].iter().collect()
        }
        Some(ComparableValue::Long(_)) => String::new(),
        None => chars[start_0..].iter().collect(),
        Some(_) => {
            return Err(QueryError::InvalidFilter(
                "SUBSTR requires an integer as third argument".to_string(),
            ))
        }
    };
    Ok(Some(string_with_lang(&result, lang)))
}

pub fn eval_encode_for_uri<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "ENCODE_FOR_URI")?;
    match args[0].eval_to_comparable(row, ctx)? {
        Some(v) => match v.as_str() {
            Some(s) => Ok(Some(ComparableValue::String(Arc::from(
                utf8_percent_encode(s, NON_ALPHANUMERIC).to_string(),
            )))),
            None => Err(QueryError::InvalidFilter(
                "ENCODE_FOR_URI requires a string argument".to_string(),
            )),
        },
        None => Ok(None),
    }
}

pub fn eval_str_dt<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 2, "STRDT")?;
    // Per W3C SPARQL spec: STRDT requires a simple literal (no language tag,
    // no existing datatype). If the variable binding has a language tag, error.
    if let Expression::Var(var_id) = &args[0] {
        if let Some(Binding::Lit { dtc, .. }) = row.get(*var_id) {
            if dtc.lang_tag().is_some() {
                return Ok(None); // language-tagged → type error → unbound
            }
        }
    }
    let val = args[0].eval_to_comparable(row, ctx)?;
    let dt = args[1].eval_to_comparable(row, ctx)?;
    match (val, dt) {
        (Some(ComparableValue::String(s)), Some(dt_val)) => {
            Ok(Some(ComparableValue::TypedLiteral {
                val: FlakeValue::String(s.to_string()),
                dtc: dt_val
                    .as_str()
                    .map(|s| UnresolvedDatatypeConstraint::Explicit(Arc::from(s))),
            }))
        }
        (Some(_), Some(_)) => Ok(None),
        _ => Ok(None),
    }
}

pub fn eval_str_lang<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 2, "STRLANG")?;
    // Per W3C SPARQL spec: STRLANG requires a simple literal (no language tag).
    if let Expression::Var(var_id) = &args[0] {
        if let Some(Binding::Lit { dtc, .. }) = row.get(*var_id) {
            if dtc.lang_tag().is_some() {
                return Ok(None); // language-tagged → type error → unbound
            }
        }
    }
    let val = args[0].eval_to_comparable(row, ctx)?;
    let lang = args[1].eval_to_comparable(row, ctx)?;
    match (val, lang) {
        (Some(ComparableValue::String(s)), Some(lang_val)) => {
            Ok(Some(ComparableValue::TypedLiteral {
                val: FlakeValue::String(s.to_string()),
                dtc: lang_val
                    .as_str()
                    .map(|s| UnresolvedDatatypeConstraint::LangTag(Arc::from(s))),
            }))
        }
        (Some(_), Some(_)) => Ok(None),
        _ => Ok(None),
    }
}

/// Cypher `replace(original, search, replacement)` — LITERAL (non-regex)
/// replace-all of every occurrence of `search`.
pub fn eval_replace_all<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 3, "replace")?;
    let lang = extract_lang_tag(&args[0], row, ctx);
    let input = args[0].eval_to_comparable(row, ctx)?;
    let search = args[1].eval_to_comparable(row, ctx)?;
    let replacement = args[2].eval_to_comparable(row, ctx)?;
    match (
        input.as_ref().and_then(ComparableValue::as_str),
        search.as_ref().and_then(ComparableValue::as_str),
        replacement.as_ref().and_then(ComparableValue::as_str),
    ) {
        (Some(s), Some(from), Some(to)) => {
            if let Some(ctx) = ctx {
                ctx.tracker.consume_fuel(1)?;
            }
            // Empty search would loop forever in some implementations; std's
            // replace handles it (inserts between chars), but Cypher returns the
            // input unchanged for an empty search.
            let out = if from.is_empty() {
                s.to_string()
            } else {
                s.replace(from, to)
            };
            Ok(Some(string_with_lang(&out, lang)))
        }
        _ if input.is_none() || search.is_none() || replacement.is_none() => Ok(None),
        _ => Err(QueryError::InvalidFilter(
            "replace() requires string arguments".to_string(),
        )),
    }
}

/// Cypher `trim` / `ltrim` / `rtrim` — strip surrounding whitespace.
pub fn eval_trim<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
    side: TrimSide,
) -> Result<Option<ComparableValue>> {
    let name = side.fn_name();
    check_arity(args, 1, name)?;
    let lang = extract_lang_tag(&args[0], row, ctx);
    match args[0].eval_to_comparable(row, ctx)? {
        Some(v) => match v.as_str() {
            Some(s) => {
                let trimmed = match side {
                    TrimSide::Both => s.trim(),
                    TrimSide::Left => s.trim_start(),
                    TrimSide::Right => s.trim_end(),
                };
                Ok(Some(string_with_lang(trimmed, lang)))
            }
            None => Err(QueryError::InvalidFilter(format!(
                "{name}() requires a string argument"
            ))),
        },
        None => Ok(None),
    }
}

/// Cypher `left(s, n)` / `right(s, n)` — first / last `n` characters
/// (character-based; clamps when `n` exceeds the length, returns empty for
/// `n <= 0`).
pub fn eval_left_right<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
    from_left: bool,
) -> Result<Option<ComparableValue>> {
    let name = if from_left { "left" } else { "right" };
    check_arity(args, 2, name)?;
    let lang = extract_lang_tag(&args[0], row, ctx);
    let s = args[0].eval_to_comparable(row, ctx)?;
    let n = args[1].eval_to_comparable(row, ctx)?;
    let (s, n) = match (s, n) {
        (Some(s), Some(n)) => (s, n),
        _ => return Ok(None),
    };
    let Some(s) = s.as_str() else {
        return Err(QueryError::InvalidFilter(format!(
            "{name}() requires a string first argument"
        )));
    };
    let n = match n {
        ComparableValue::Long(n) => n.max(0) as usize,
        _ => {
            return Err(QueryError::InvalidFilter(format!(
                "{name}() requires an integer length"
            )))
        }
    };
    let total = s.chars().count();
    let take = n.min(total);
    let out: String = if from_left {
        s.chars().take(take).collect()
    } else {
        s.chars().skip(total - take).collect()
    };
    Ok(Some(string_with_lang(&out, lang)))
}

/// Which side(s) a [`eval_trim`] call strips.
#[derive(Clone, Copy)]
pub enum TrimSide {
    Both,
    Left,
    Right,
}

impl TrimSide {
    fn fn_name(self) -> &'static str {
        match self {
            TrimSide::Both => "trim",
            TrimSide::Left => "ltrim",
            TrimSide::Right => "rtrim",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::Batch;
    use crate::ir::Function;
    use crate::var_registry::VarId;
    use fluree_db_core::value::FlakeValue;
    use fluree_db_core::Sid;

    fn make_string_batch() -> Batch {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let col = vec![Binding::lit(
            FlakeValue::String("Hello World".to_string()),
            Sid::new(2, "string"),
        )];
        Batch::new(schema, vec![col]).unwrap()
    }

    #[test]
    fn test_strlen() {
        let batch = make_string_batch();
        let row = batch.row_view(0).unwrap();
        let result = eval_strlen::<_>(&[Expression::Var(VarId(0))], &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Long(11)));
    }

    #[test]
    fn test_ucase() {
        let batch = make_string_batch();
        let row = batch.row_view(0).unwrap();
        let result = eval_ucase::<_>(&[Expression::Var(VarId(0))], &row, None).unwrap();
        assert_eq!(
            result,
            Some(ComparableValue::String(Arc::from("HELLO WORLD")))
        );
    }

    #[test]
    fn test_contains() {
        let batch = make_string_batch();
        let row = batch.row_view(0).unwrap();
        let result = eval_contains::<_>(
            &[
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::String("World".to_string())),
            ],
            &row,
            None,
        )
        .unwrap();
        assert_eq!(result, Some(ComparableValue::Bool(true)));
    }

    #[test]
    fn test_anchored_literal_regex_prefix_shortcut() {
        assert_eq!(anchored_literal_regex_prefix("^Hello", ""), Some("Hello"));
        assert_eq!(anchored_literal_regex_prefix("^Hel.o", ""), None);
        assert_eq!(anchored_literal_regex_prefix("^Hello", "i"), None);
    }

    #[test]
    fn test_eval_regex_uses_literal_prefix_semantics() {
        let batch = make_string_batch();
        let row = batch.row_view(0).unwrap();
        let result = eval_regex::<_>(
            &[
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::String("^Hello".to_string())),
            ],
            &row,
            None,
        )
        .unwrap();
        assert_eq!(result, Some(ComparableValue::Bool(true)));
    }

    // STRBEFORE / STRAFTER — SPARQL 1.1 §17.4.3.7/8 datatyping rules.

    fn lang_batch(value: &str, lang: &str) -> Batch {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let col = vec![Binding::lit_lang(
            FlakeValue::String(value.to_string()),
            lang,
        )];
        Batch::new(schema, vec![col]).unwrap()
    }

    fn lang_const(value: &str, lang: &str) -> Expression {
        // `"value"@lang` in expression position lowers to STRLANG(value, lang).
        Expression::Call {
            func: Function::StrLang,
            args: vec![
                Expression::Const(FlakeValue::String(value.to_string())),
                Expression::Const(FlakeValue::String(lang.to_string())),
            ],
        }
    }

    fn s_const(value: &str) -> Expression {
        Expression::Const(FlakeValue::String(value.to_string()))
    }

    #[test]
    fn test_strbefore_found_preserves_lang() {
        let batch = lang_batch("english", "en");
        let row = batch.row_view(0).unwrap();
        let r =
            eval_str_before::<_>(&[Expression::Var(VarId(0)), s_const("s")], &row, None).unwrap();
        assert_eq!(r, Some(string_with_lang("engli", Some(Arc::from("en")))));
    }

    #[test]
    fn test_strbefore_no_match_is_plain_empty() {
        // No match must drop arg1's language tag → plain "".
        let batch = lang_batch("日本語", "ja");
        let row = batch.row_view(0).unwrap();
        let r =
            eval_str_before::<_>(&[Expression::Var(VarId(0)), s_const("s")], &row, None).unwrap();
        assert_eq!(r, Some(ComparableValue::String(Arc::from(""))));
    }

    #[test]
    fn test_strafter_empty_substring_returns_whole_with_lang() {
        let batch = lang_batch("abc", "en");
        let row = batch.row_view(0).unwrap();
        let r = eval_str_after::<_>(&[Expression::Var(VarId(0)), s_const("")], &row, None).unwrap();
        assert_eq!(r, Some(string_with_lang("abc", Some(Arc::from("en")))));
    }

    #[test]
    fn test_strbefore_empty_substring_returns_empty_with_lang() {
        let batch = lang_batch("abc", "en");
        let row = batch.row_view(0).unwrap();
        let r =
            eval_str_before::<_>(&[Expression::Var(VarId(0)), s_const("")], &row, None).unwrap();
        assert_eq!(r, Some(string_with_lang("", Some(Arc::from("en")))));
    }

    #[test]
    fn test_strbefore_incompatible_lang_is_unbound() {
        // arg1 @en, arg2 @cy → incompatible → type error → unbound.
        let batch = lang_batch("abc", "en");
        let row = batch.row_view(0).unwrap();
        let r = eval_str_before::<_>(
            &[Expression::Var(VarId(0)), lang_const("b", "cy")],
            &row,
            None,
        )
        .unwrap();
        assert_eq!(r, None);

        // Same lang → compatible.
        let r2 = eval_str_before::<_>(
            &[Expression::Var(VarId(0)), lang_const("b", "en")],
            &row,
            None,
        )
        .unwrap();
        assert_eq!(r2, Some(string_with_lang("a", Some(Arc::from("en")))));
    }

    #[test]
    fn test_strbefore_simple_arg1_with_lang_arg2_is_unbound() {
        // arg1 simple (no lang), arg2 @en → incompatible.
        let batch = make_string_batch(); // "Hello World", no lang
        let row = batch.row_view(0).unwrap();
        let r = eval_str_before::<_>(
            &[Expression::Var(VarId(0)), lang_const("World", "en")],
            &row,
            None,
        )
        .unwrap();
        assert_eq!(r, None);
    }
}
