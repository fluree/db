//! SPARQL Property Path parsing.
//!
//! This module parses property path expressions from tokens.
//!
//! ## Grammar (SPARQL 1.1)
//!
//! ```text
//! PropertyPath ::= PathSequence ( '|' PathSequence )*
//! PathSequence ::= PathEltOrInverse ( '/' PathEltOrInverse )*
//! PathEltOrInverse ::= PathElt | '^' PathElt
//! PathElt ::= PathPrimary PathMod?
//! PathMod ::= '*' | '+' | '?'
//! PathPrimary ::= iri | 'a' | '!' PathNegatedPropertySet | '(' Path ')'
//! PathNegatedPropertySet ::= PathOneInPropertySet | '(' ( PathOneInPropertySet ( '|' PathOneInPropertySet )* )? ')'
//! PathOneInPropertySet ::= iri | 'a' | '^' ( iri | 'a' )
//! ```
//!
//! ## Precedence (lowest to highest)
//!
//! 1. Alternative `|`
//! 2. Sequence `/`
//! 3. Inverse `^`
//! 4. Modifiers `*`, `+`, `?`
//! 5. Primary (IRI, `a`, negated set, grouped)

use crate::ast::path::{NegatedPredicate, PropertyPath};
use crate::ast::term::Iri;
use crate::lex::TokenKind;
use crate::parse::stream::TokenStream;

/// Parse a property path expression.
///
/// This is the top-level entry point for path parsing.
pub fn parse_property_path(tokens: &mut TokenStream) -> Result<PropertyPath, String> {
    parse_path_alternative(tokens)
}

/// Parse path alternatives: `path1 | path2 | ...`
fn parse_path_alternative(tokens: &mut TokenStream) -> Result<PropertyPath, String> {
    let start = tokens.current_span();
    let mut left = parse_path_sequence(tokens)?;

    while tokens.check(&TokenKind::Pipe) {
        tokens.advance(); // consume |
        let right = parse_path_sequence(tokens)?;
        let span = start.union(right.span());
        left = PropertyPath::alternative(left, right, span);
    }

    Ok(left)
}

/// Parse path sequence: `path1 / path2 / ...`
fn parse_path_sequence(tokens: &mut TokenStream) -> Result<PropertyPath, String> {
    let start = tokens.current_span();
    let mut left = parse_path_elt_or_inverse(tokens)?;

    while tokens.check(&TokenKind::Slash) {
        tokens.advance(); // consume /
        let right = parse_path_elt_or_inverse(tokens)?;
        let span = start.union(right.span());
        left = PropertyPath::sequence(left, right, span);
    }

    Ok(left)
}

/// Parse path element or inverse: `^path` or `path`
fn parse_path_elt_or_inverse(tokens: &mut TokenStream) -> Result<PropertyPath, String> {
    if tokens.check(&TokenKind::Caret) {
        let start = tokens.current_span();
        tokens.advance(); // consume ^
        let inner = parse_path_elt(tokens)?;
        let span = start.union(inner.span());
        Ok(PropertyPath::inverse(inner, span))
    } else {
        parse_path_elt(tokens)
    }
}

/// Parse path element with optional modifier: `path*`, `path+`, `path?`
fn parse_path_elt(tokens: &mut TokenStream) -> Result<PropertyPath, String> {
    let start = tokens.current_span();
    let primary = parse_path_primary(tokens)?;

    // Check for modifier
    if tokens.check(&TokenKind::Star) {
        tokens.advance();
        let span = start.union(tokens.previous_span());
        Ok(PropertyPath::zero_or_more(primary, span))
    } else if tokens.check(&TokenKind::Plus) {
        tokens.advance();
        let span = start.union(tokens.previous_span());
        Ok(PropertyPath::one_or_more(primary, span))
    } else if tokens.check(&TokenKind::Question) {
        tokens.advance();
        let span = start.union(tokens.previous_span());
        Ok(PropertyPath::zero_or_one(primary, span))
    } else {
        Ok(primary)
    }
}

/// Parse a primary path element: IRI, `a`, negated set, or grouped path.
fn parse_path_primary(tokens: &mut TokenStream) -> Result<PropertyPath, String> {
    // Check for negated property set: `!`
    if tokens.check(&TokenKind::Bang) {
        return parse_negated_property_set(tokens);
    }

    // Check for grouped path: `(path)`
    if tokens.check(&TokenKind::LParen) {
        let start = tokens.current_span();
        tokens.advance(); // consume (
        let inner = parse_path_alternative(tokens)?;
        if !tokens.match_token(&TokenKind::RParen) {
            return Err(format!(
                "Expected ')' after path at position {}",
                tokens.current_span().start
            ));
        }
        let span = start.union(tokens.previous_span());
        return Ok(PropertyPath::Group {
            path: Box::new(inner),
            span,
        });
    }

    // Check for `a` keyword (rdf:type)
    if tokens.check_keyword(TokenKind::KwA) {
        let span = tokens.current_span();
        tokens.advance();
        return Ok(PropertyPath::A { span });
    }

    // Try to parse an IRI
    if let Some((iri_value, span)) = tokens.consume_iri() {
        return Ok(PropertyPath::Iri(Iri::full(iri_value.as_ref(), span)));
    }

    // Try prefixed name
    if let Some((prefix, local, span)) = tokens.consume_prefixed_name() {
        return Ok(PropertyPath::Iri(Iri::prefixed(
            prefix.as_ref(),
            local.as_ref(),
            span,
        )));
    }

    // Try default prefix (e.g., `:name`)
    if let Some((prefix, span)) = tokens.consume_prefixed_name_ns() {
        // This is `:` followed by local part - handled as prefixed name with empty prefix
        return Ok(PropertyPath::Iri(Iri::prefixed(prefix.as_ref(), "", span)));
    }

    Err(format!(
        "Expected property path (IRI, 'a', or path expression) at position {}",
        tokens.current_span().start
    ))
}

/// Parse a negated property set: `!iri` or `!(iri1|iri2|...)`
fn parse_negated_property_set(tokens: &mut TokenStream) -> Result<PropertyPath, String> {
    let start = tokens.current_span();
    tokens.advance(); // consume !

    // Check for grouped negation: `!(iri1|iri2|...)`
    if tokens.check(&TokenKind::LParen) {
        tokens.advance(); // consume (

        let mut predicates = Vec::new();

        // Empty set is allowed: `!()`
        if !tokens.check(&TokenKind::RParen) {
            predicates.push(parse_path_one_in_property_set(tokens)?);

            while tokens.check(&TokenKind::Pipe) {
                tokens.advance(); // consume |
                predicates.push(parse_path_one_in_property_set(tokens)?);
            }
        }

        if !tokens.match_token(&TokenKind::RParen) {
            return Err(format!(
                "Expected ')' after negated property set at position {}",
                tokens.current_span().start
            ));
        }

        let span = start.union(tokens.previous_span());
        return Ok(PropertyPath::NegatedSet {
            iris: predicates,
            span,
        });
    }

    // Single negated predicate: `!iri` or `!a` or `!^iri` or `!^a`
    let predicate = parse_path_one_in_property_set(tokens)?;
    let span = start.union(predicate.span());
    Ok(PropertyPath::NegatedSet {
        iris: vec![predicate],
        span,
    })
}

/// Parse a single predicate in a negated property set.
///
/// Can be: `iri`, `a`, `^iri`, or `^a`
fn parse_path_one_in_property_set(tokens: &mut TokenStream) -> Result<NegatedPredicate, String> {
    let is_inverse = if tokens.check(&TokenKind::Caret) {
        tokens.advance(); // consume ^
        true
    } else {
        false
    };

    // Check for `a` keyword
    if tokens.check_keyword(TokenKind::KwA) {
        let span = tokens.current_span();
        tokens.advance();
        return Ok(if is_inverse {
            NegatedPredicate::InverseA { span }
        } else {
            NegatedPredicate::ForwardA { span }
        });
    }

    // Try to parse an IRI
    if let Some((iri_value, span)) = tokens.consume_iri() {
        let iri = Iri::full(iri_value.as_ref(), span);
        return Ok(if is_inverse {
            NegatedPredicate::Inverse(iri)
        } else {
            NegatedPredicate::Forward(iri)
        });
    }

    // Try prefixed name
    if let Some((prefix, local, span)) = tokens.consume_prefixed_name() {
        let iri = Iri::prefixed(prefix.as_ref(), local.as_ref(), span);
        return Ok(if is_inverse {
            NegatedPredicate::Inverse(iri)
        } else {
            NegatedPredicate::Forward(iri)
        });
    }

    // Try default prefix
    if let Some((prefix, span)) = tokens.consume_prefixed_name_ns() {
        let iri = Iri::prefixed(prefix.as_ref(), "", span);
        return Ok(if is_inverse {
            NegatedPredicate::Inverse(iri)
        } else {
            NegatedPredicate::Forward(iri)
        });
    }

    Err(format!(
        "Expected IRI or 'a' in negated property set at position {}",
        tokens.current_span().start
    ))
}

/// Check if the current token can start a property path.
pub fn is_path_start(tokens: &TokenStream) -> bool {
    matches!(
        tokens.peek().kind,
        TokenKind::Iri(_)
            | TokenKind::PrefixedName { .. }
            | TokenKind::PrefixedNameNs(_)
            | TokenKind::KwA
            | TokenKind::Caret      // ^path
            | TokenKind::Bang       // !iri
            | TokenKind::LParen // (path)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lex::tokenize;

    fn parse_path(input: &str) -> Result<PropertyPath, String> {
        let tokens = tokenize(input);
        let mut stream = TokenStream::new(tokens);
        parse_property_path(&mut stream)
    }

    #[test]
    fn test_simple_iri() {
        let path = parse_path("<http://example.org/name>").unwrap();
        assert!(matches!(path, PropertyPath::Iri(_)));
        assert!(path.is_simple());
    }

    #[test]
    fn test_prefixed_iri() {
        let path = parse_path("ex:name").unwrap();
        assert!(matches!(path, PropertyPath::Iri(_)));
    }

    #[test]
    fn test_a_keyword() {
        let path = parse_path("a").unwrap();
        assert!(matches!(path, PropertyPath::A { .. }));
        assert!(path.is_simple());
    }

    #[test]
    fn test_inverse() {
        let path = parse_path("^ex:parent").unwrap();
        assert!(matches!(path, PropertyPath::Inverse { .. }));
        assert!(!path.is_simple());
    }

    #[test]
    fn test_zero_or_more() {
        let path = parse_path("ex:parent*").unwrap();
        assert!(matches!(path, PropertyPath::ZeroOrMore { .. }));
    }

    #[test]
    fn test_one_or_more() {
        let path = parse_path("ex:parent+").unwrap();
        assert!(matches!(path, PropertyPath::OneOrMore { .. }));
    }

    #[test]
    fn test_zero_or_one() {
        let path = parse_path("ex:parent?").unwrap();
        assert!(matches!(path, PropertyPath::ZeroOrOne { .. }));
    }

    #[test]
    fn test_sequence() {
        let path = parse_path("ex:parent/ex:child").unwrap();
        assert!(matches!(path, PropertyPath::Sequence { .. }));
    }

    #[test]
    fn test_alternative() {
        let path = parse_path("ex:parent|ex:child").unwrap();
        assert!(matches!(path, PropertyPath::Alternative { .. }));
    }

    #[test]
    fn test_grouped() {
        let path = parse_path("(ex:parent)").unwrap();
        assert!(matches!(path, PropertyPath::Group { .. }));
    }

    #[test]
    fn test_complex_path() {
        // ^ex:parent/ex:child+ - inverse parent, then one-or-more child
        let path = parse_path("^ex:parent/ex:child+").unwrap();
        match path {
            PropertyPath::Sequence { left, right, .. } => {
                assert!(matches!(*left, PropertyPath::Inverse { .. }));
                assert!(matches!(*right, PropertyPath::OneOrMore { .. }));
            }
            _ => panic!("Expected Sequence"),
        }
    }

    #[test]
    fn test_alternative_precedence() {
        // ex:a/ex:b|ex:c should parse as (ex:a/ex:b)|ex:c
        // because sequence has higher precedence than alternative
        let path = parse_path("ex:a/ex:b|ex:c").unwrap();
        match path {
            PropertyPath::Alternative { left, right, .. } => {
                assert!(matches!(*left, PropertyPath::Sequence { .. }));
                assert!(matches!(*right, PropertyPath::Iri(_)));
            }
            _ => panic!("Expected Alternative"),
        }
    }

    #[test]
    fn test_modifier_binds_tightly() {
        // ex:a+/ex:b should parse as (ex:a+)/ex:b
        let path = parse_path("ex:a+/ex:b").unwrap();
        match path {
            PropertyPath::Sequence { left, right, .. } => {
                assert!(matches!(*left, PropertyPath::OneOrMore { .. }));
                assert!(matches!(*right, PropertyPath::Iri(_)));
            }
            _ => panic!("Expected Sequence"),
        }
    }

    #[test]
    fn test_negated_single() {
        let path = parse_path("!ex:hidden").unwrap();
        assert!(matches!(path, PropertyPath::NegatedSet { .. }));
        assert!(path.uses_unsupported_features());
    }

    #[test]
    fn test_negated_set() {
        let path = parse_path("!(ex:a|ex:b)").unwrap();
        match path {
            PropertyPath::NegatedSet { iris, .. } => {
                assert_eq!(iris.len(), 2);
            }
            _ => panic!("Expected NegatedSet"),
        }
    }

    #[test]
    fn test_negated_with_inverse() {
        let path = parse_path("!^ex:parent").unwrap();
        match path {
            PropertyPath::NegatedSet { iris, .. } => {
                assert_eq!(iris.len(), 1);
                assert!(matches!(iris[0], NegatedPredicate::Inverse(_)));
            }
            _ => panic!("Expected NegatedSet"),
        }
    }

    #[test]
    fn test_inverse_of_modifier() {
        // ^ex:parent* - should be ^(ex:parent*) i.e., inverse of transitive
        // But grammar says PathEltOrInverse is ^PathElt, so inverse applies first
        // Actually reading grammar: PathEltOrInverse ::= PathElt | '^' PathElt
        // PathElt ::= PathPrimary PathMod?
        // So ^ex:parent* parses as (^ex:parent)* not ^(ex:parent*)
        // Wait, let me re-read: ^PathElt, and PathElt = PathPrimary PathMod?
        // So it's ^(PathPrimary PathMod?) = ^(ex:parent*) = Inverse(ZeroOrMore(ex:parent))
        // But in our parsing, parse_path_elt_or_inverse calls parse_path_elt for the inner
        // which includes the modifier. So ^ex:parent* is Inverse(ZeroOrMore(ex:parent))
        let path = parse_path("^ex:parent*").unwrap();
        match path {
            PropertyPath::Inverse { path: inner, .. } => {
                assert!(matches!(*inner, PropertyPath::ZeroOrMore { .. }));
            }
            _ => panic!("Expected Inverse"),
        }
    }

    #[test]
    fn test_deeply_nested() {
        // ((ex:a|ex:b)/ex:c)+
        let path = parse_path("((ex:a|ex:b)/ex:c)+").unwrap();
        match path {
            PropertyPath::OneOrMore { path: inner, .. } => {
                assert!(matches!(*inner, PropertyPath::Group { .. }));
            }
            _ => panic!("Expected OneOrMore"),
        }
    }

    #[test]
    fn test_fluree_supported_complex() {
        // A complex but Fluree-supported path: ^ex:parent+/ex:child*|ex:sibling
        let path = parse_path("^ex:parent+/ex:child*|ex:sibling").unwrap();
        assert!(!path.uses_unsupported_features());
    }
}
