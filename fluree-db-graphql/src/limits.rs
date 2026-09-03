//! Resource bounds on an incoming GraphQL document.
//!
//! A derived schema has cyclic types by construction — `Person.knows` is a
//! `[Person]` the moment one subject points at another — so nesting depth is
//! chosen by the caller, not by the schema. Without a ceiling a short document
//! can ask for arbitrarily deep recursion, and a document within the body-size
//! limit can name thousands of aliased root fields that resolve concurrently.

/// Nesting depth beyond which a document is refused.
///
/// Fifteen levels is far past any hand-written query and well short of what
/// threatens the recursive walks (`selection::walk`, lowering, reshaping).
pub const DEFAULT_MAX_DEPTH: usize = 15;

/// Field budget for one document, as async-graphql counts complexity.
///
/// Each field costs 1 by default, so this is effectively a cap on how many
/// fields — across every alias and fragment — one request may select.
pub const DEFAULT_MAX_COMPLEXITY: usize = 1000;

/// What a GraphQL document is allowed to ask for.
///
/// Enforced in two places, because they cover different windows. `max_depth`
/// is checked during `selection::extract`, which runs *before* async-graphql
/// validates — a schema-level limit alone would let a deeply nested document
/// recurse through the extraction walk first. Both are then handed to the
/// schema builder so validation applies them again to execution itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Limits {
    pub max_depth: usize,
    pub max_complexity: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_depth: DEFAULT_MAX_DEPTH,
            max_complexity: DEFAULT_MAX_COMPLEXITY,
        }
    }
}

impl Limits {
    /// Limits that refuse nothing.
    ///
    /// For an embedder running its own trusted documents. A server should not
    /// use this: it is the ceiling every other read surface has.
    pub fn unlimited() -> Self {
        Self {
            max_depth: usize::MAX,
            max_complexity: usize::MAX,
        }
    }
}

/// Brace nesting a document may reach before it is refused unparsed.
///
/// This is not a policy knob, it is a stack guard, and it deliberately matches
/// `async_graphql_parser`'s own `MAX_RECURSION_DEPTH`: a document past this is
/// one that parser would reject anyway. The reason to check first is that its
/// counter runs while *building the AST*, after pest has already descended
/// recursively through the grammar with no limit of its own — so a document a
/// few hundred KB long overflows the stack and aborts the process before the
/// counter is ever consulted. Aborting is not something a caller can catch, so
/// the only defence is not to hand the parser the document at all.
const MAX_PARSE_NESTING: usize = 64;

/// Refuse a document whose brace nesting would drive the parser too deep.
///
/// Counts raw `{`, skipping strings and comments, which is the right proxy: a
/// selection set and an input-object literal both recurse, and neither the
/// grammar nor the stack cares which one it is descending through.
pub fn guard_nesting(document: &str) -> crate::error::Result<()> {
    #[derive(Clone, Copy)]
    enum State {
        Normal,
        Comment,
        Str,
        StrEscape,
        BlockStr,
    }

    let bytes = document.as_bytes();
    let mut state = State::Normal;
    let mut depth = 0usize;
    let mut i = 0;

    while i < bytes.len() {
        let b = bytes[i];
        match state {
            State::Normal => match b {
                b'#' => state = State::Comment,
                b'"' => {
                    if bytes[i..].starts_with(br#"""""#) {
                        state = State::BlockStr;
                        i += 3;
                        continue;
                    }
                    state = State::Str;
                }
                b'{' => {
                    depth += 1;
                    if depth > MAX_PARSE_NESTING {
                        return Err(crate::error::Error::Parse(format!(
                            "document nests more than {MAX_PARSE_NESTING} levels deep"
                        )));
                    }
                }
                b'}' => depth = depth.saturating_sub(1),
                _ => {}
            },
            State::Comment => {
                if b == b'\n' {
                    state = State::Normal;
                }
            }
            State::Str => match b {
                b'\\' => state = State::StrEscape,
                b'"' | b'\n' => state = State::Normal,
                _ => {}
            },
            State::StrEscape => state = State::Str,
            State::BlockStr => {
                if bytes[i..].starts_with(br#"""""#) {
                    state = State::Normal;
                    i += 3;
                    continue;
                }
            }
        }
        i += 1;
    }
    Ok(())
}
