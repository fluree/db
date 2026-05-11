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

pub fn eval_str_before<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 2, "STRBEFORE")?;
    let lang = extract_lang_tag(&args[0], row, ctx);
    let arg1 = args[0].eval_to_comparable(row, ctx)?;
    let arg2 = args[1].eval_to_comparable(row, ctx)?;
    match (arg1, arg2) {
        (Some(ComparableValue::String(s)), Some(ComparableValue::String(d))) => {
            let result = s.find(d.as_ref()).map(|pos| &s[..pos]).unwrap_or("");
            Ok(Some(string_with_lang(result, lang)))
        }
        (None, _) | (_, None) => Ok(None),
        _ => Ok(None),
    }
}

pub fn eval_str_after<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 2, "STRAFTER")?;
    let lang = extract_lang_tag(&args[0], row, ctx);
    let arg1 = args[0].eval_to_comparable(row, ctx)?;
    let arg2 = args[1].eval_to_comparable(row, ctx)?;
    match (arg1, arg2) {
        (Some(ComparableValue::String(s)), Some(ComparableValue::String(d))) => {
            let result = s
                .find(d.as_ref())
                .map(|pos| &s[pos + d.len()..])
                .unwrap_or("");
            Ok(Some(string_with_lang(result, lang)))
        }
        (None, _) | (_, None) => Ok(None),
        _ => Ok(None),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::Batch;
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
}
