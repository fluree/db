//! Blank-node output labelling (plan H-6)
//!
//! A blank node's label is scoped to the document that carries it, so a writer
//! is free to choose the labels it emits. Choosing them — rather than echoing
//! the input's — is what riot and Oxigraph both do, and it is what makes
//! parallel output deterministic and label disjointness provable instead of
//! hoped for.
//!
//! The one exception is Fluree's own `_:fdb-…` stable-skolem labels, which are
//! addressable identifiers rather than incidental syntax (#1432). Relabelling
//! them would break `fluree export | fluree rdf convert`, so they pass
//! through.

use fluree_graph_ir::chars::is_blank_node_label;
use fluree_graph_ir::SinkError;

/// Labels that pass through [`BlankNodeLabels::Relabel`] untouched.
const CARVE_OUT: &str = "fdb-";

/// Namespace reserved for anonymous nodes minted under
/// [`BlankNodeLabels::Preserve`].
const PRESERVE_MINT: &str = "fdbw-";

/// How blank-node labels reach the output.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum BlankNodeLabels {
    /// Mint a fresh `b{N}` label for every blank node, except `_:fdb-…`,
    /// which passes through verbatim.
    ///
    /// The mapping is bijective — one output label per distinct input label,
    /// never two inputs onto one output — so no two nodes merge and no node
    /// splits.
    ///
    /// # Why `b{N}` cannot collide
    ///
    /// The output namespace contains exactly two kinds of label: minted
    /// `b{N}`, and passed-through labels, which by construction all begin
    /// `fdb-`. No `b{N}` begins `fdb-`, and the mint counter is monotonic, so
    /// disjointness holds without a runtime check. In particular an input
    /// document's own `_:b0` is *relabelled* like any other, so it cannot
    /// meet a mint.
    ///
    /// # The carve-out is validated too
    ///
    /// A `_:fdb-…` label passes through only if it is a legal
    /// [`BLANK_NODE_LABEL`](fluree_graph_ir::chars::is_blank_node_label). One
    /// that is not was never a Fluree skolem — those are `fdb-<ulid>` and are
    /// always legal — so there is no addressability to preserve, and it is
    /// minted like anything else. The bijection keeps its identity; only its
    /// name changes.
    #[default]
    Relabel,

    /// Emit every *user-written* blank-node label verbatim.
    ///
    /// # What "everything" cannot include
    ///
    /// Two classes of label reach a writer, and only one of them was ever
    /// written by a user.
    ///
    /// A label that is not a `BLANK_NODE_LABEL` is necessarily an *internal*
    /// mint or an externally-sourced identifier: no Turtle document could
    /// contain it, because no parser would have accepted it. The IR's own
    /// anonymous nodes are one example — `-b{N}`, deliberately unlexable so
    /// that in-memory they cannot collide with a user's label — and R2RML,
    /// JSON-LD and skolem sources supply the rest. Preserving one is not
    /// possible (the output would be a document no parser reads, or worse, one
    /// that reads as something else) and not meaningful (a user never chose
    /// it), so it is relabelled into the reserved namespace below. Anonymous
    /// nodes arriving as `term_blank(None)` are the same case and get the same
    /// treatment.
    ///
    /// The test is the whole [`BLANK_NODE_LABEL`](fluree_graph_ir::chars::is_blank_node_label)
    /// production, not an approximation of it. A first-character-only check
    /// let `_:ab.` through, and a reader handed that does not fail — it lexes
    /// `_:ab` and takes the `.` as the statement terminator, silently
    /// renaming the node.
    ///
    /// A label that *does* lex was written by a user, and is emitted
    /// unchanged. The single exception is one already inside the reserved
    /// `fdbw-` namespace: that one is refused rather than emitted, because
    /// preserving it could merge it with a mint, and silently relabelling it
    /// would break the promise the mode exists to make.
    ///
    /// So the guarantee is: output labels are always legal, always disjoint,
    /// and every label a user could have written is either verbatim or a loud
    /// error — never quietly something else.
    Preserve,
}

/// Decides the output label for each blank node, under a [`BlankNodeLabels`].
///
/// # The caller owns the mapping
///
/// This type is deliberately *not* a cache. [`Self::labelled`] decides a label
/// for an input label seen for the **first time** and does not remember it:
/// calling it twice with the same input mints twice. The caller
/// ([`WriterTerms`](super::terms::WriterTerms)) already keys a map from input
/// label to `TermId` in order to honor the protocol's blank-node identity
/// rule, and that map's entry already holds the rewritten term — so a second
/// map here would be written on every first sighting and read never, doubling
/// the per-label memory of a blank-node-heavy document to no purpose.
#[derive(Debug)]
pub(crate) struct BlankLabeler {
    mode: BlankNodeLabels,
    counter: u64,
}

impl BlankLabeler {
    pub(crate) fn new(mode: BlankNodeLabels) -> Self {
        Self { mode, counter: 0 }
    }

    /// Decide the output label for a labelled blank node seen for the first
    /// time. See the type docs: the caller, not this, remembers the answer.
    ///
    /// Fails only under [`BlankNodeLabels::Preserve`], for a label that could
    /// merge with one this writer mints.
    pub(crate) fn labelled(&mut self, input: &str) -> Result<Box<str>, SinkError> {
        match self.mode {
            // The addressability carve-out: `_:fdb-…` are identifiers, not
            // incidental syntax. Stored, not renamed — but only if they can
            // actually be written. A `fdb-` label that is not a
            // `BLANK_NODE_LABEL` is not a Fluree skolem (those are
            // `fdb-<ulid>`, always legal), so there is no addressability to
            // preserve and nothing is lost by minting instead. Passing it
            // through would emit a document no reader accepts, or — for
            // `fdb-x.` — one that reads it back under a different name.
            BlankNodeLabels::Relabel
                if input.starts_with(CARVE_OUT) && is_blank_node_label(input) =>
            {
                Ok(input.into())
            }
            BlankNodeLabels::Relabel => Ok(self.mint()),
            BlankNodeLabels::Preserve => {
                if input.starts_with(PRESERVE_MINT) {
                    return Err(SinkError::rejected(format!(
                        "blank-node label `{input}` is inside the `_:{PRESERVE_MINT}` namespace \
                         this writer reserves for anonymous nodes; preserving it could merge it \
                         with a minted node. Drop --preserve-bnode-labels to relabel instead."
                    )));
                }
                if !is_blank_node_label(input) {
                    // Not a user's label — no document can contain one that
                    // will not lex. It is an internal mint, so it is minted
                    // afresh into the reserved namespace rather than refused:
                    // there is nothing here to preserve.
                    return Ok(self.preserve_mint());
                }
                Ok(input.into())
            }
        }
    }

    /// The output label for an anonymous blank node.
    ///
    /// Infallible: the writer chooses this label, so it is always one the
    /// writer can emit.
    pub(crate) fn anonymous(&mut self) -> Box<str> {
        match self.mode {
            BlankNodeLabels::Relabel => self.mint(),
            BlankNodeLabels::Preserve => self.preserve_mint(),
        }
    }

    fn mint(&mut self) -> Box<str> {
        self.counter += 1;
        format!("b{}", self.counter).into()
    }

    /// A fresh label in the namespace [`BlankNodeLabels::Preserve`] reserves
    /// for nodes whose label the writer chooses.
    fn preserve_mint(&mut self) -> Box<str> {
        self.counter += 1;
        format!("{PRESERVE_MINT}{}", self.counter).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relabelling_is_stable_per_input_label() {
        let mut l = BlankLabeler::new(BlankNodeLabels::Relabel);
        // This type does not dedup — the caller does (see the type docs) —
        // so the property under test is that DISTINCT inputs get distinct
        // labels, and that a carve-out label is decided the same way twice.
        let first = l.labelled("x").unwrap();
        let other = l.labelled("y").unwrap();
        assert_ne!(first, other, "two nodes, two labels");

        let carve = l.labelled("fdb-01ARZ").unwrap();
        assert_eq!(
            carve,
            l.labelled("fdb-01ARZ").unwrap(),
            "the carve-out decision is a function of the label alone"
        );
    }

    /// The disjointness argument in [`BlankNodeLabels::Relabel`], executed: a
    /// document whose own labels are shaped exactly like the mints, plus a
    /// carve-out label, plus anonymous nodes. Every output label must be
    /// distinct — a merge here is the silent data-loss bug the policy exists
    /// to prevent, and one that an isomorphism check could not see.
    #[test]
    fn relabelled_mints_cannot_collide_with_carve_outs_or_mint_shaped_input() {
        let mut l = BlankLabeler::new(BlankNodeLabels::Relabel);
        let mut out: Vec<String> = Vec::new();
        for input in ["b1", "b2", "b3", "fdb-01ARZ", "fdb-b1", "plain"] {
            out.push(l.labelled(input).unwrap().to_string());
        }
        for _ in 0..4 {
            out.push(l.anonymous().to_string());
        }

        let unique: std::collections::HashSet<&String> = out.iter().collect();
        assert_eq!(unique.len(), out.len(), "labels collided: {out:?}");

        // The carve-out passed through untouched…
        assert!(out.contains(&"fdb-01ARZ".to_string()));
        assert!(out.contains(&"fdb-b1".to_string()));
        // …and nothing else did.
        assert!(!out.contains(&"b1".to_string()) || out.iter().filter(|s| *s == "b1").count() == 1);
        assert!(!out.contains(&"plain".to_string()));
    }

    #[test]
    fn preserve_mode_emits_input_labels_verbatim() {
        let mut l = BlankLabeler::new(BlankNodeLabels::Preserve);
        assert_eq!(&*l.labelled("b1").unwrap(), "b1");
        assert_eq!(&*l.labelled("fdb-01ARZ").unwrap(), "fdb-01ARZ");
        assert!(l.anonymous().starts_with(PRESERVE_MINT));
    }

    #[test]
    fn preserve_mode_refuses_a_label_in_its_own_mint_namespace() {
        let mut l = BlankLabeler::new(BlankNodeLabels::Preserve);
        let err = l
            .labelled("fdbw-1")
            .expect_err("a label that could merge with a mint must be refused");
        assert!(matches!(err, SinkError::Rejected(_)), "{err:?}");
        assert!(err.to_string().contains("reserves"), "{err}");
    }

    /// The IR's in-memory anonymous mints are `-b{N}`, deliberately unlexable
    /// so they cannot collide with a user label in memory — and therefore
    /// impossible to serialize.
    ///
    /// A label that cannot lex is *by construction* not a user's, so preserve
    /// mode has nothing to preserve: it mints a fresh legal label instead of
    /// refusing. Refusing would fail a document over a label its author never
    /// wrote.
    #[test]
    fn preserve_mode_relabels_internal_mints_into_a_legal_namespace() {
        let mut l = BlankLabeler::new(BlankNodeLabels::Preserve);
        let first = l.labelled("-b1").unwrap().to_string();
        assert!(first.starts_with(PRESERVE_MINT), "{first}");
        assert!(is_blank_node_label(&first), "{first}");

        // A second internal mint gets its own label. (Stability per input
        // label is the caller's job — `WriterTerms` keys the map — and is
        // covered there.)
        let second = l.labelled("-b2").unwrap().to_string();
        assert_ne!(first, second);

        // Relabelling has no such case to handle — it never emits an input
        // label at all.
        let mut relabel = BlankLabeler::new(BlankNodeLabels::Relabel);
        assert_eq!(&*relabel.labelled("-b1").unwrap(), "b1");
    }

    /// Every label either mode emits must be one a parser can read back.
    #[test]
    fn no_mode_ever_emits_a_label_that_cannot_lex() {
        for mode in [BlankNodeLabels::Relabel, BlankNodeLabels::Preserve] {
            let mut l = BlankLabeler::new(mode);
            let mut emitted = Vec::new();
            for input in ["b1", "fdb-01ARZ", "plain", "-b1", "-b2", "0leading"] {
                if let Ok(label) = l.labelled(input) {
                    emitted.push(label.to_string());
                }
            }
            for _ in 0..3 {
                emitted.push(l.anonymous().to_string());
            }
            for label in &emitted {
                assert!(
                    is_blank_node_label(label),
                    "{mode:?} emitted an unserializable label {label:?}"
                );
            }
            let unique: std::collections::HashSet<&String> = emitted.iter().collect();
            assert_eq!(
                unique.len(),
                emitted.len(),
                "{mode:?} collided: {emitted:?}"
            );
        }
    }

    /// Preserve mode relabels every label the shared production rejects —
    /// not just the ones whose FIRST character is wrong. A first-character
    /// check waved through `ab.` (silently renamed on re-read), the empty
    /// label, embedded spaces and quotes, and every non-ASCII character
    /// outside PN_CHARS_BASE.
    #[test]
    fn preserve_mode_relabels_every_label_the_production_rejects() {
        let cases = [
            "-b1",       // the IR's internal mint
            "ab.",       // trailing '.': lexes as `_:ab` plus a terminator
            "",          // the empty label
            "a b",       // space
            "a\"b",      // quote
            "a\\b",      // backslash
            "a\nb",      // newline
            "a#b",       // starts a comment
            "a,b",       // comma
            "a;b",       // semicolon
            "\u{D7}x",   // MULTIPLICATION SIGN — a PN_CHARS_BASE gap
            "\u{B7}x",   // MIDDLE DOT — PN_CHARS but illegal first
            "\u{300}x",  // COMBINING GRAVE — same
            "\u{FFFE}x", // past FDF0-FFFD
        ];
        let mut l = BlankLabeler::new(BlankNodeLabels::Preserve);
        for input in cases {
            let out = l.labelled(input).expect("relabelled, not refused");
            assert!(
                out.starts_with(PRESERVE_MINT),
                "{input:?} was emitted verbatim as {out:?}"
            );
            assert!(is_blank_node_label(&out), "{out:?} is not emittable");
        }

        // And a legal label is still preserved exactly.
        assert_eq!(&*l.labelled("a.b").unwrap(), "a.b");
        assert_eq!(&*l.labelled("\u{C0}x").unwrap(), "\u{C0}x");
    }
}
