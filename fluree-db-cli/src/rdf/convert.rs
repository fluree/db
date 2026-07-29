//! `fluree rdf convert` — read one syntax, write another.
//!
//! Not built yet: there are no writers. The verb is registered anyway so the
//! surface it will occupy is fixed now — the flags, the TTY guard, the
//! resolver, and the input plumbing are all shared with `check` and `count`
//! and are exercised by them, so what lands later is the writers and nothing
//! else.
//!
//! It refuses with a message that names what is missing rather than "not
//! implemented", because a user who reaches for `convert` needs to know
//! whether to wait or to reach for `riot`.

use crate::cli::RdfCommonArgs;
use crate::error::{CliError, CliResult};
use crate::rdf::input::guard_binary_stdout;
use crate::rdf::syntax::RdfSyntax;
use std::path::Path;

/// The output syntax when `--to` is not given.
///
/// N-Quads, matching riot: it is the only syntax in the set that can hold any
/// dataset without loss and needs no context, which is what makes it the right
/// thing to get when you did not say.
pub const DEFAULT_OUTPUT_SYNTAX: RdfSyntax = RdfSyntax::NQuads;

/// Run `fluree rdf convert`.
pub fn run(
    common: &RdfCommonArgs,
    to: Option<RdfSyntax>,
    output: Option<&Path>,
    pretty: bool,
) -> CliResult<()> {
    let target = to.unwrap_or(DEFAULT_OUTPUT_SYNTAX);

    // Check the invocation before announcing the verb is unbuilt: a user who
    // would have dumped Jelly into their terminal should learn that either
    // way, and these guards are what the writers land behind.
    if output.is_none() {
        guard_binary_stdout(target)?;
    }
    if pretty && target != RdfSyntax::Turtle {
        return Err(CliError::Usage(format!(
            "--pretty applies to turtle output; --to {target} has no pretty form"
        )));
    }
    let _ = common;

    Err(CliError::Usage(format!(
        "`fluree rdf convert` cannot write {target} yet — no serializers have landed.\n  \
         {} `fluree rdf check` and `fluree rdf count` work today; writers for turtle, \
         ntriples, nquads, trig, and jsonld land with the conversion pipeline",
        colored::Colorize::bold(colored::Colorize::cyan("help:")),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> RdfCommonArgs {
        RdfCommonArgs {
            input: None,
            syntax: None,
            base: None,
            time: false,
            profile: None,
            no_hash: false,
        }
    }

    #[test]
    fn the_refusal_names_the_target_syntax_and_what_does_work() {
        let err = run(&args(), Some(RdfSyntax::Turtle), None, false).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("turtle"), "{msg}");
        assert!(msg.contains("check"), "{msg}");
        assert!(msg.contains("count"), "{msg}");
        // Usage, so it exits 2 — this is not "your document is bad".
        assert!(matches!(err, CliError::Usage(_)));
    }

    #[test]
    fn the_default_target_is_nquads_matching_riot() {
        assert_eq!(DEFAULT_OUTPUT_SYNTAX, RdfSyntax::NQuads);
        let msg = run(&args(), None, None, false).unwrap_err().to_string();
        assert!(msg.contains("nquads"), "{msg}");
    }

    #[test]
    fn pretty_is_rejected_for_syntaxes_that_have_no_pretty_form() {
        let msg = run(&args(), Some(RdfSyntax::NTriples), None, true)
            .unwrap_err()
            .to_string();
        assert!(msg.contains("--pretty"), "{msg}");
        // …and accepted for Turtle, which then falls through to the
        // not-yet-written refusal rather than the flag complaint.
        let msg = run(&args(), Some(RdfSyntax::Turtle), None, true)
            .unwrap_err()
            .to_string();
        assert!(!msg.contains("--pretty"), "{msg}");
    }
}
