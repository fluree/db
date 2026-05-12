//! Pattern parser.

use crate::ast::{
    Direction, Label, LengthRange, MapLit, NodePattern, Pattern, PatternPart, RelPattern, RelType,
};
use crate::diag::{DiagCode, Diagnostic};
use crate::lex::TokenKind;

use super::expr::parse_map_lit;
use super::stmt::{parse_ident_or_keyword, parse_var};
use super::stream::TokenStream;

pub fn parse_pattern(s: &mut TokenStream) -> Result<Pattern, Diagnostic> {
    let start = s.peek_span();
    let mut parts = Vec::new();
    loop {
        parts.push(parse_pattern_part(s)?);
        if s.eat(&TokenKind::Comma).is_none() {
            break;
        }
    }
    let end = s.peek_span();
    Ok(Pattern {
        parts,
        span: start.union(end),
    })
}

fn parse_pattern_part(s: &mut TokenStream) -> Result<PatternPart, Diagnostic> {
    let start = s.peek_span();

    // v1 does not bind path variables; if user writes `p = (...)`,
    // reject early with a deferred-feature error.
    if let TokenKind::Ident(_) = s.peek_kind() {
        if matches!(s.peek_at(1), TokenKind::Eq) {
            return Err(Diagnostic {
                code: DiagCode::DeferredPathValue,
                severity: crate::diag::Severity::Error,
                message: "path variables (e.g., `p = (...)`) are deferred — path values are not in v1".to_string(),
                span: s.peek_span(),
                help: Some("rewrite without the path binding".to_string()),
            });
        }
    }

    let head = parse_node_pat(s)?;
    let mut tail = Vec::new();
    loop {
        // Look for a relationship continuation.
        if !is_rel_continuation(s) {
            break;
        }
        let rel = parse_rel_pat(s)?;
        let next = parse_node_pat(s)?;
        tail.push((rel, next));
    }
    let end = s.peek_span();
    Ok(PatternPart {
        path_var: None,
        head,
        tail,
        span: start.union(end),
    })
}

fn is_rel_continuation(s: &TokenStream) -> bool {
    matches!(s.peek_kind(), TokenKind::Minus | TokenKind::LArrowDash)
}

fn parse_node_pat(s: &mut TokenStream) -> Result<NodePattern, Diagnostic> {
    let start = s.expect(&TokenKind::LParen)?;

    // optional var
    let var = if let TokenKind::Ident(_) = s.peek_kind() {
        Some(parse_var(s)?)
    } else {
        None
    };

    // labels
    let mut labels = Vec::new();
    while s.eat(&TokenKind::Colon).is_some() {
        let label_span = s.peek_span();
        let name = parse_ident_or_keyword(s)?;
        labels.push(Label {
            name,
            span: label_span,
        });
    }

    // optional inline property map
    let props = if matches!(s.peek_kind(), TokenKind::LBrace) {
        Some(parse_map_lit(s)?)
    } else {
        None
    };

    let end = s.expect(&TokenKind::RParen)?;
    Ok(NodePattern {
        var,
        labels,
        props,
        span: start.union(end),
    })
}

fn parse_rel_pat(s: &mut TokenStream) -> Result<RelPattern, Diagnostic> {
    // We accept four shapes:
    //   -[r:T {p:v} *1..5]-> b   (Outgoing)
    //   <-[r:T ...]-           b (Incoming)
    //   --                     b (Outgoing untyped, no bracket — rare)
    //   <-                       (Incoming untyped, no bracket — rare)
    //
    // Implementation: read leading direction signal, optional bracket,
    // trailing direction signal.

    let start = s.peek_span();
    let leading_left = s.eat(&TokenKind::LArrowDash).is_some();
    if !leading_left {
        // Must be `-`
        s.expect(&TokenKind::Minus)?;
    }

    // Optional bracketed body
    let mut var = None;
    let mut types = Vec::new();
    let mut length = None;
    let mut props: Option<MapLit> = None;
    if matches!(s.peek_kind(), TokenKind::LBracket) {
        s.advance(); // [
                     // optional variable
        if let TokenKind::Ident(_) = s.peek_kind() {
            var = Some(parse_var(s)?);
        }
        // type alternatives (:T | :T2 ... or :T|T2)
        if s.eat(&TokenKind::Colon).is_some() {
            loop {
                let t_span = s.peek_span();
                let name = parse_ident_or_keyword(s)?;
                types.push(RelType { name, span: t_span });
                if s.eat(&TokenKind::Pipe).is_none() {
                    break;
                }
                // `|:Type` and `|Type` are both accepted by Cypher
                let _ = s.eat(&TokenKind::Colon);
            }
        }
        // Variable-length range
        if s.eat(&TokenKind::Star).is_some() {
            let length_span = s.peek_span();
            let mut min = None;
            let mut max = None;
            // *N or *N..M or *..M or *
            if let TokenKind::Integer(n) = s.peek_kind() {
                min = Some(*n as u32);
                s.advance();
            }
            if s.eat(&TokenKind::DotDot).is_some() {
                if let TokenKind::Integer(n) = s.peek_kind() {
                    max = Some(*n as u32);
                    s.advance();
                }
            } else if min.is_some() {
                // *N alone means exactly N
                max = min;
            }
            length = Some(LengthRange {
                min,
                max,
                span: length_span,
            });
        }
        // Inline property map
        if matches!(s.peek_kind(), TokenKind::LBrace) {
            props = Some(parse_map_lit(s)?);
        }
        s.expect(&TokenKind::RBracket)?;
    }

    // Trailing direction
    let (direction, end_minus_span) = if s.eat(&TokenKind::DashArrowRight).is_some() {
        (Direction::Outgoing, s.peek_span())
    } else if s.eat(&TokenKind::Minus).is_some() {
        if leading_left {
            (Direction::Incoming, s.peek_span())
        } else {
            (Direction::Either, s.peek_span())
        }
    } else {
        return Err(s.error(
            DiagCode::UnexpectedToken,
            "expected `->` or `-` after relationship",
        ));
    };

    Ok(RelPattern {
        var,
        direction,
        types,
        length,
        props,
        span: start.union(end_minus_span),
    })
}
