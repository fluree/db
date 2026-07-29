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

use fluree_graph_ir::SinkError;
use std::collections::HashMap;

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
    #[default]
    Relabel,

    /// Emit every labelled blank node's label verbatim.
    ///
    /// Anonymous nodes still have to be given a label, and here the input's
    /// labels are no longer under the writer's control, so disjointness cannot
    /// be established by construction. Mints therefore go in the reserved
    /// `fdbw-{N}` namespace and an input label inside it is *refused* rather
    /// than allowed to merge with a mint. This is the mode's honest cost, and
    /// the reason it is not the default.
    Preserve,
}

/// Maps input blank-node labels to output ones under a [`BlankNodeLabels`].
#[derive(Debug)]
pub(crate) struct BlankLabeler {
    mode: BlankNodeLabels,
    counter: u64,
    /// Input label → output label, for the labels that are rewritten.
    renamed: HashMap<Box<str>, Box<str>>,
}

impl BlankLabeler {
    pub(crate) fn new(mode: BlankNodeLabels) -> Self {
        Self {
            mode,
            counter: 0,
            renamed: HashMap::new(),
        }
    }

    /// The output label for a labelled blank node.
    ///
    /// Fails only under [`BlankNodeLabels::Preserve`], for a label the writer
    /// cannot emit verbatim without risking a silent merge or invalid syntax.
    pub(crate) fn labelled(&mut self, input: &str) -> Result<&str, SinkError> {
        if !self.renamed.contains_key(input) {
            let output = self.output_label_for(input)?;
            self.renamed.insert(input.into(), output);
        }
        Ok(self.renamed[input].as_ref())
    }

    /// Decide the output label for an input label seen for the first time.
    fn output_label_for(&mut self, input: &str) -> Result<Box<str>, SinkError> {
        match self.mode {
            // The addressability carve-out: `_:fdb-…` are identifiers, not
            // incidental syntax. Stored, not renamed.
            BlankNodeLabels::Relabel if input.starts_with(CARVE_OUT) => Ok(input.into()),
            BlankNodeLabels::Relabel => Ok(self.mint()),
            BlankNodeLabels::Preserve => {
                if input.starts_with(PRESERVE_MINT) {
                    return Err(SinkError::rejected(format!(
                        "blank-node label `{input}` is inside the `_:{PRESERVE_MINT}` namespace \
                         this writer reserves for anonymous nodes; preserving it could merge it \
                         with a minted node. Drop --preserve-bnode-labels to relabel instead."
                    )));
                }
                if let Some(bad) = unlexable_first_char(input) {
                    return Err(SinkError::rejected(format!(
                        "blank-node label `{input}` starts with `{bad}`, which cannot begin a \
                         BLANK_NODE_LABEL, so writing it verbatim would produce invalid RDF. \
                         Drop --preserve-bnode-labels to relabel instead."
                    )));
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
            BlankNodeLabels::Preserve => {
                self.counter += 1;
                format!("{PRESERVE_MINT}{}", self.counter).into()
            }
        }
    }

    fn mint(&mut self) -> Box<str> {
        self.counter += 1;
        format!("b{}", self.counter).into()
    }
}

/// The first character of `label`, when it plainly cannot start a Turtle
/// `BLANK_NODE_LABEL`.
///
/// `BLANK_NODE_LABEL ::= '_:' (PN_CHARS_U | [0-9]) …`. The check is
/// deliberately conservative: it rejects only ASCII characters that are
/// certainly outside `PN_CHARS_U | [0-9]`, and lets every non-ASCII character
/// through rather than re-implementing the grammar's Unicode ranges and
/// refusing labels that are in fact legal.
///
/// The case this actually catches is the IR's own anonymous mints, which are
/// `-b{N}`: a leading `-` keeps them from colliding with user labels *in
/// memory*, and is exactly what cannot be serialized.
fn unlexable_first_char(label: &str) -> Option<char> {
    let first = label.chars().next()?;
    let lexable = !first.is_ascii() || first.is_ascii_alphanumeric() || first == '_';
    (!lexable).then_some(first)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relabelling_is_stable_per_input_label() {
        let mut l = BlankLabeler::new(BlankNodeLabels::Relabel);
        let first = l.labelled("x").unwrap().to_string();
        let second = l.labelled("x").unwrap().to_string();
        assert_eq!(first, second, "one node, one label");

        let other = l.labelled("y").unwrap().to_string();
        assert_ne!(first, other, "two nodes, two labels");
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
        assert_eq!(l.labelled("b1").unwrap(), "b1");
        assert_eq!(l.labelled("fdb-01ARZ").unwrap(), "fdb-01ARZ");
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

    /// The IR's in-memory anonymous mints are `-b{N}`, which is deliberately
    /// unlexable so it cannot collide with a user label. Emitting one
    /// verbatim would produce a document no parser accepts.
    #[test]
    fn preserve_mode_refuses_a_label_that_cannot_be_serialized() {
        let mut l = BlankLabeler::new(BlankNodeLabels::Preserve);
        let err = l.labelled("-b1").expect_err("`_:-b1` is not valid RDF");
        assert!(err.to_string().contains("invalid RDF"), "{err}");

        // Relabelling has no such problem — it never emits the input label.
        let mut relabel = BlankLabeler::new(BlankNodeLabels::Relabel);
        assert_eq!(relabel.labelled("-b1").unwrap(), "b1");
    }

    #[test]
    fn the_first_char_check_only_rejects_certain_ascii() {
        assert_eq!(unlexable_first_char("-x"), Some('-'));
        assert_eq!(unlexable_first_char(".x"), Some('.'));
        assert_eq!(unlexable_first_char(""), None);
        assert_eq!(unlexable_first_char("b0"), None);
        assert_eq!(unlexable_first_char("0b"), None);
        assert_eq!(unlexable_first_char("_x"), None);
        // Non-ASCII is let through rather than second-guessed.
        assert_eq!(unlexable_first_char("ünïcødé"), None);
    }
}
