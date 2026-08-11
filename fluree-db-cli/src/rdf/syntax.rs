//! The one RDF syntax table the RDF file verbs resolve against.
//!
//! The CLI already had [`crate::detect::DataFormat`], but it names two things
//! (Turtle, JSON-LD) because that is all `insert`/`upsert` can transact.
//! Conversion has to name every syntax it will ever read or write — including
//! the ones that are not built yet — so that an unsupported input fails with
//! "TriG input is not supported yet" instead of being sniffed into Turtle and
//! producing a parse error thirty lines in.
//!
//! `detect.rs` is deliberately left alone: it still serves `insert`, `upsert`,
//! `validate`, and the server, and unifying them is a follow-up, not part of
//! standing this surface up.

use crate::error::{CliError, CliResult};
use std::path::Path;

/// An RDF serialization the RDF file verbs know the name of.
///
/// Naming a syntax is not the same as supporting it — see
/// [`RdfSyntax::read_support`]. Every variant here is on the roadmap; the ones
/// that have no reader yet exist so the CLI can refuse them by name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, clap::ValueEnum)]
pub enum RdfSyntax {
    /// Turtle. Also the reader for N3-shaped input, which is accepted as an
    /// alias rather than a distinct grammar.
    #[value(name = "turtle", alias = "ttl", alias = "n3")]
    Turtle,
    /// N-Triples — a Turtle subset, read by the same parser.
    #[value(name = "ntriples", alias = "nt", alias = "n-triples")]
    NTriples,
    /// N-Quads.
    #[value(name = "nquads", alias = "nq", alias = "n-quads")]
    NQuads,
    /// TriG.
    #[value(name = "trig")]
    TriG,
    /// JSON-LD.
    #[value(name = "jsonld", alias = "json-ld", alias = "json")]
    JsonLd,
    /// RDF/XML.
    #[value(name = "rdfxml", alias = "rdf-xml", alias = "xml")]
    RdfXml,
    /// RDF/JSON (the triple-map serialization, not JSON-LD).
    #[value(name = "rdfjson", alias = "rdf-json")]
    RdfJson,
    /// Jelly, the binary streaming serialization.
    #[value(name = "jelly")]
    Jelly,
}

/// Whether a syntax can be read today, and if not, what is waiting on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadSupport {
    /// A reader exists and the `rdf` verbs will use it.
    Yes,
    /// No reader yet. The string names the work it lands with, so the error
    /// tells a user *when*, not just *no*.
    NotYet(&'static str),
}

impl RdfSyntax {
    /// Every syntax, in the order they are listed in help and docs.
    pub const ALL: [RdfSyntax; 8] = [
        RdfSyntax::Turtle,
        RdfSyntax::NTriples,
        RdfSyntax::NQuads,
        RdfSyntax::TriG,
        RdfSyntax::JsonLd,
        RdfSyntax::RdfXml,
        RdfSyntax::RdfJson,
        RdfSyntax::Jelly,
    ];

    /// Canonical lowercase name — what `--syntax` accepts and what the
    /// machine-readable outputs emit.
    pub fn as_str(self) -> &'static str {
        match self {
            RdfSyntax::Turtle => "turtle",
            RdfSyntax::NTriples => "ntriples",
            RdfSyntax::NQuads => "nquads",
            RdfSyntax::TriG => "trig",
            RdfSyntax::JsonLd => "jsonld",
            RdfSyntax::RdfXml => "rdfxml",
            RdfSyntax::RdfJson => "rdfjson",
            RdfSyntax::Jelly => "jelly",
        }
    }

    /// Whether this syntax carries named graphs. A quad syntax read into a
    /// triple-only sink has to be refused, not flattened.
    pub fn is_quad_syntax(self) -> bool {
        matches!(
            self,
            RdfSyntax::NQuads | RdfSyntax::TriG | RdfSyntax::JsonLd
        )
    }

    /// Whether serialized output is binary, and so must not be written to a
    /// terminal. See [`crate::rdf::input::guard_binary_stdout`].
    pub fn is_binary(self) -> bool {
        matches!(self, RdfSyntax::Jelly)
    }

    /// Whether a reader exists for this syntax today.
    ///
    /// Four readers, not one grammar wearing four hats. Turtle and TriG share
    /// the token-level parser (TriG is Turtle plus graph blocks); N-Triples
    /// and N-Quads go through the STRICT line scanner, which is a separate
    /// reader on purpose — those grammars are defined by what they refuse,
    /// and reading them with the Turtle parser would accept documents this
    /// CLI is being asked to reject.
    pub fn read_support(self) -> ReadSupport {
        match self {
            RdfSyntax::Turtle | RdfSyntax::NTriples | RdfSyntax::NQuads | RdfSyntax::TriG => {
                ReadSupport::Yes
            }
            RdfSyntax::JsonLd => ReadSupport::NotYet(
                "JSON-LD is read by `fluree insert`, but not yet on the rdf conversion path",
            ),
            RdfSyntax::RdfXml => {
                ReadSupport::NotYet("the RDF/XML reader lands with the XML family")
            }
            RdfSyntax::RdfJson => ReadSupport::NotYet("RDF/JSON lands with the XML family"),
            RdfSyntax::Jelly => ReadSupport::NotYet("Jelly lands last in the format set"),
        }
    }

    /// Reject a syntax with no reader, naming what it is waiting on.
    pub fn require_readable(self) -> CliResult<()> {
        match self.read_support() {
            ReadSupport::Yes => Ok(()),
            ReadSupport::NotYet(why) => Err(CliError::Usage(format!(
                "cannot read {} input yet — {why}\n  {} readable today: turtle, ntriples, \
                 nquads, trig",
                self.as_str(),
                colored::Colorize::bold(colored::Colorize::cyan("help:")),
            ))),
        }
    }

    /// Map a file extension (without the dot, any case) to a syntax.
    ///
    /// Compression suffixes are the caller's problem — strip them with
    /// [`split_compression`] first.
    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext.to_ascii_lowercase().as_str() {
            "ttl" | "turtle" | "n3" => Some(RdfSyntax::Turtle),
            "nt" | "ntriples" => Some(RdfSyntax::NTriples),
            "nq" | "nquads" => Some(RdfSyntax::NQuads),
            "trig" => Some(RdfSyntax::TriG),
            "jsonld" | "json" => Some(RdfSyntax::JsonLd),
            "rdf" | "rdfxml" | "owl" => Some(RdfSyntax::RdfXml),
            "rj" | "rdfjson" => Some(RdfSyntax::RdfJson),
            "jelly" => Some(RdfSyntax::Jelly),
            _ => None,
        }
    }
}

impl std::fmt::Display for RdfSyntax {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A compression layer wrapped around an RDF document.
///
/// Mirrors `fluree_db_api::import`'s private `Compression` — same three cases,
/// same extension table — because the import path's copy is crate-private and
/// this one also has to answer from magic bytes, which stdin needs and a file
/// extension cannot provide.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Compression {
    /// No compression layer.
    #[default]
    None,
    /// gzip, decoded with a multi-member decoder so `pigz`/`bgzip` output works.
    Gzip,
    /// zstd.
    Zstd,
}

impl Compression {
    /// Name used in machine-readable output.
    pub fn as_str(self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Gzip => "gzip",
            Compression::Zstd => "zstd",
        }
    }

    /// Detect a compression layer from a stream's leading bytes.
    ///
    /// This is how stdin gets transparent decompression: a pipe has no name,
    /// so the extension table cannot help and the magic number is the only
    /// evidence there is.
    pub fn from_magic(head: &[u8]) -> Self {
        if head.starts_with(&[0x1f, 0x8b]) {
            Compression::Gzip
        } else if head.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
            Compression::Zstd
        } else {
            Compression::None
        }
    }
}

/// Split a path's outer compression suffix from the RDF extension beneath it.
///
/// `data.ttl.gz` → `(Some("ttl"), Gzip)`; `data.nq` → `(Some("nq"), None)`;
/// `data.gz` → `(None, Gzip)` — a compressed something with no syntax to name.
pub fn split_compression(path: &Path) -> (Option<String>, Compression) {
    let outer = path
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase);
    let comp = match outer.as_deref() {
        Some("gz") => Compression::Gzip,
        Some("zst" | "zstd") => Compression::Zstd,
        _ => return (outer, Compression::None),
    };
    let inner = path
        .file_stem()
        .map(Path::new)
        .and_then(|p| p.extension())
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase);
    (inner, comp)
}

/// Where a resolved syntax came from. Reported in `--profile=json` so a
/// surprising result can be traced to the rule that produced it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyntaxSource {
    /// `--syntax` on the command line.
    Explicit,
    /// The file extension, after stripping any compression suffix.
    Extension,
    /// A look at the leading bytes of the document.
    Sniff,
}

impl SyntaxSource {
    /// Name used in machine-readable output.
    pub fn as_str(self) -> &'static str {
        match self {
            SyntaxSource::Explicit => "explicit",
            SyntaxSource::Extension => "extension",
            SyntaxSource::Sniff => "sniff",
        }
    }
}

/// A resolved input syntax and how it was arrived at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Resolved {
    /// The syntax to parse as.
    pub syntax: RdfSyntax,
    /// The compression layer to strip first.
    pub compression: Compression,
    /// Which rule decided.
    pub source: SyntaxSource,
}

/// Resolve the compression layer: the file suffix if it names one, otherwise
/// the stream's magic bytes.
///
/// The suffix comes first because it is what the user declared, but the magic
/// bytes are not just a fallback for pipes: a gzipped file that nobody named
/// `.gz` decodes too, which costs four bytes of comparison and removes a whole
/// class of "why is my input not valid UTF-8" reports.
pub fn resolve_compression(path: Option<&Path>, magic_head: &[u8]) -> Compression {
    if let Some(p) = path {
        let (_, from_suffix) = split_compression(p);
        if from_suffix != Compression::None {
            return from_suffix;
        }
    }
    Compression::from_magic(magic_head)
}

/// Resolve the input syntax: explicit flag, then file extension, then content.
///
/// The precedence is the same one `detect.rs` uses, and the same one riot
/// uses, because it is the only order in which a user can override a wrong
/// guess. `head` is the leading bytes of the *decompressed* document — pass
/// an empty slice when they are not available yet and resolution will stop at
/// the extension.
///
/// `compression` is resolved separately (by [`resolve_compression`]) and
/// carried through: `--syntax turtle` on `data.ttl.gz` still decompresses,
/// because naming the syntax says nothing about the container.
pub fn resolve(
    path: Option<&Path>,
    explicit: Option<RdfSyntax>,
    head: &[u8],
    compression: Compression,
) -> CliResult<Resolved> {
    let (ext, _) = match path {
        Some(p) => split_compression(p),
        None => (None, Compression::None),
    };

    if let Some(syntax) = explicit {
        return Ok(Resolved {
            syntax,
            compression,
            source: SyntaxSource::Explicit,
        });
    }

    if let Some(syntax) = ext.as_deref().and_then(RdfSyntax::from_extension) {
        return Ok(Resolved {
            syntax,
            compression,
            source: SyntaxSource::Extension,
        });
    }

    match sniff(head) {
        Some(syntax) => Ok(Resolved {
            syntax,
            compression,
            source: SyntaxSource::Sniff,
        }),
        None => Err(CliError::Usage(format!(
            "could not determine the input syntax{}\n  {} pass --syntax (one of: {})",
            path.map_or_else(
                || " of the piped input".to_string(),
                |p| format!(" of '{}'", p.display())
            ),
            colored::Colorize::bold(colored::Colorize::cyan("help:")),
            RdfSyntax::ALL
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", "),
        ))),
    }
}

/// Guess a syntax from a document's leading bytes.
///
/// Deliberately coarse. It separates the three families that are
/// unmistakable — XML, JSON, and everything Turtle-shaped — and does not try
/// to tell Turtle from TriG or N-Triples from N-Quads, because those
/// distinctions live in the middle of a statement and a sniffer that guesses
/// at them is a sniffer that is confidently wrong on the file that matters.
/// `--syntax` is the answer for those.
///
/// Returns `None` when nothing is recognizable, which surfaces as a usage
/// error naming the flag rather than a parse error fifty lines in.
fn sniff(head: &[u8]) -> Option<RdfSyntax> {
    let text = std::str::from_utf8(head).ok()?;
    // A UTF-8 BOM is not part of any RDF grammar, but plenty of editors write
    // one. Stepping over it here only affects *identification*: the parser
    // still sees the BOM and still gets to reject it, which is its call to
    // make. Without this, a BOM'd file with no extension fails as "could not
    // determine the syntax", which points at the wrong problem.
    let trimmed = text.trim_start_matches('\u{feff}').trim_start();
    // An empty document is the empty graph in every syntax — there is nothing
    // to identify and nothing to get wrong. Refusing it would make
    // `fluree parse < /dev/null` exit 2 while an empty `.ttl` file exits 0.
    if trimmed.is_empty() {
        return Some(RdfSyntax::Turtle);
    }
    let mut chars = trimmed.chars();
    match chars.next()? {
        '<' if trimmed.starts_with("<?xml") || trimmed.starts_with("<rdf:") => {
            Some(RdfSyntax::RdfXml)
        }
        // `<http://…>` opens an N-Triples statement; the XML cases above are
        // already claimed, so any other angle bracket is a Turtle-family IRI.
        '<' => Some(RdfSyntax::Turtle),
        '{' | '[' => Some(RdfSyntax::JsonLd),
        // Turtle family, in every shape it can open with: `@prefix`/`@base`
        // directives, SPARQL-style `PREFIX`/`BASE`, blank-node subjects,
        // comments, and bare prefixed names (`ex:alice …`).
        '@' | '_' | '#' => Some(RdfSyntax::Turtle),
        c if c.is_alphabetic() => Some(RdfSyntax::Turtle),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn p(s: &str) -> PathBuf {
        PathBuf::from(s)
    }

    #[test]
    fn compression_suffix_is_stripped_before_the_syntax_is_read() {
        assert_eq!(
            split_compression(&p("data.ttl.gz")),
            (Some("ttl".to_string()), Compression::Gzip)
        );
        assert_eq!(
            split_compression(&p("data.nq.zst")),
            (Some("nq".to_string()), Compression::Zstd)
        );
        assert_eq!(
            split_compression(&p("DATA.TTL.ZSTD")),
            (Some("ttl".to_string()), Compression::Zstd)
        );
        assert_eq!(
            split_compression(&p("data.ttl")),
            (Some("ttl".to_string()), Compression::None)
        );
        // Compressed, but nothing names the syntax underneath.
        assert_eq!(split_compression(&p("data.gz")), (None, Compression::Gzip));
        assert_eq!(split_compression(&p("data")), (None, Compression::None));
    }

    /// One row of the precedence table: the three inputs a rule can see, and
    /// the syntax plus deciding rule they must produce.
    struct Case {
        why: &'static str,
        path: Option<&'static str>,
        explicit: Option<RdfSyntax>,
        head: &'static str,
        syntax: RdfSyntax,
        source: SyntaxSource,
    }

    #[test]
    fn resolver_precedence_is_explicit_then_extension_then_sniff() {
        // The whole precedence table in one place, which is the only way to
        // see that a lower rule never overrides a higher one.
        let cases = [
            Case {
                why: "explicit beats an extension that says otherwise",
                path: Some("data.nt"),
                explicit: Some(RdfSyntax::TriG),
                head: "",
                syntax: RdfSyntax::TriG,
                source: SyntaxSource::Explicit,
            },
            Case {
                why: "explicit beats content that says otherwise",
                path: None,
                explicit: Some(RdfSyntax::NQuads),
                head: r#"{"@id": "x"}"#,
                syntax: RdfSyntax::NQuads,
                source: SyntaxSource::Explicit,
            },
            Case {
                why: "extension beats content that says otherwise",
                path: Some("data.nt"),
                explicit: None,
                head: r#"{"@id": "x"}"#,
                syntax: RdfSyntax::NTriples,
                source: SyntaxSource::Extension,
            },
            Case {
                why: "the extension under a compression suffix still wins",
                path: Some("data.trig.gz"),
                explicit: None,
                head: "@prefix ex: <http://e/> .",
                syntax: RdfSyntax::TriG,
                source: SyntaxSource::Extension,
            },
            Case {
                why: "no extension, so content decides",
                path: Some("dump"),
                explicit: None,
                head: "@prefix ex: <http://e/> .",
                syntax: RdfSyntax::Turtle,
                source: SyntaxSource::Sniff,
            },
            Case {
                why: "stdin has no name, so content decides",
                path: None,
                explicit: None,
                head: "<http://e/s> <http://e/p> <http://e/o> .",
                syntax: RdfSyntax::Turtle,
                source: SyntaxSource::Sniff,
            },
            Case {
                why: "an XML declaration sniffs as RDF/XML, not as a Turtle IRI",
                path: None,
                explicit: None,
                head: r#"<?xml version="1.0"?><rdf:RDF/>"#,
                syntax: RdfSyntax::RdfXml,
                source: SyntaxSource::Sniff,
            },
            Case {
                why: "a JSON array sniffs as JSON-LD",
                path: None,
                explicit: None,
                head: r#"[{"@id": "x"}]"#,
                syntax: RdfSyntax::JsonLd,
                source: SyntaxSource::Sniff,
            },
        ];

        for case in cases {
            let path = case.path.map(p);
            let got = resolve(
                path.as_deref(),
                case.explicit,
                case.head.as_bytes(),
                Compression::None,
            )
            .unwrap_or_else(|e| panic!("{}: {e}", case.why));
            assert_eq!(got.syntax, case.syntax, "{}", case.why);
            assert_eq!(got.source, case.source, "{}", case.why);
        }
    }

    #[test]
    fn an_explicit_syntax_does_not_turn_off_decompression() {
        // Naming the syntax says nothing about the container; a user who
        // passes --syntax turtle for a .ttl.gz still wants the gzip stripped.
        let compression = resolve_compression(Some(&p("data.ttl.gz")), b"");
        assert_eq!(compression, Compression::Gzip);
        let r = resolve(
            Some(&p("data.ttl.gz")),
            Some(RdfSyntax::Turtle),
            b"",
            compression,
        )
        .unwrap();
        assert_eq!(r.compression, Compression::Gzip);
        assert_eq!(r.source, SyntaxSource::Explicit);
    }

    #[test]
    fn compression_comes_from_the_suffix_first_and_the_magic_bytes_second() {
        const GZ: &[u8] = &[0x1f, 0x8b, 0x08];
        const PLAIN: &[u8] = b"@prefix ex: <http://e/> .";

        // A pipe has no name; magic is all there is.
        assert_eq!(resolve_compression(None, GZ), Compression::Gzip);
        assert_eq!(resolve_compression(None, PLAIN), Compression::None);

        // A declared suffix wins outright.
        assert_eq!(
            resolve_compression(Some(&p("data.ttl.zst")), GZ),
            Compression::Zstd
        );

        // No suffix, but the bytes say gzip — decode anyway, rather than
        // handing a compressed blob to the parser as if it were text.
        assert_eq!(resolve_compression(Some(&p("dump")), GZ), Compression::Gzip);
        assert_eq!(
            resolve_compression(Some(&p("data.ttl")), PLAIN),
            Compression::None
        );
    }

    #[test]
    fn magic_bytes_name_the_compression_layer() {
        assert_eq!(
            Compression::from_magic(&[0x1f, 0x8b, 0x08]),
            Compression::Gzip
        );
        assert_eq!(
            Compression::from_magic(&[0x28, 0xb5, 0x2f, 0xfd, 0x00]),
            Compression::Zstd
        );
        assert_eq!(Compression::from_magic(b"@prefix"), Compression::None);
        assert_eq!(Compression::from_magic(&[]), Compression::None);
        // A truncated zstd magic must not be claimed.
        assert_eq!(Compression::from_magic(&[0x28, 0xb5]), Compression::None);
    }

    #[test]
    fn unresolvable_input_names_the_flag_instead_of_guessing() {
        let err = resolve(Some(&p("mystery")), None, &[0xff, 0xfe], Compression::None).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("--syntax"), "{msg}");
        assert!(msg.contains("mystery"), "{msg}");
        // Every nameable syntax is offered, including the unimplemented ones —
        // naming one is how you find out it is not built yet.
        assert!(msg.contains("jelly"), "{msg}");
    }

    #[test]
    fn unreadable_syntaxes_are_refused_by_name_with_what_they_wait_on() {
        for syntax in RdfSyntax::ALL {
            match syntax.read_support() {
                ReadSupport::Yes => assert!(syntax.require_readable().is_ok()),
                ReadSupport::NotYet(_) => {
                    let msg = syntax.require_readable().unwrap_err().to_string();
                    assert!(msg.contains(syntax.as_str()), "{msg}");
                    assert!(
                        msg.contains("turtle, ntriples"),
                        "the refusal must say what does work: {msg}"
                    );
                }
            }
        }
    }

    #[test]
    fn the_four_text_formats_are_readable() {
        // Was "turtle and ntriples are the readable pair", back when
        // N-Triples was read as a Turtle subset by the same parser. It is not
        // any more: the line formats go through the strict scanner, which is
        // what makes their negative-syntax tests meaningful, and TriG shares
        // the token-level parser with Turtle. Two readers, four formats.
        let readable: Vec<RdfSyntax> = RdfSyntax::ALL
            .into_iter()
            .filter(|s| s.read_support() == ReadSupport::Yes)
            .collect();
        assert_eq!(
            readable,
            vec![
                RdfSyntax::Turtle,
                RdfSyntax::NTriples,
                RdfSyntax::NQuads,
                RdfSyntax::TriG
            ]
        );
    }

    #[test]
    fn extension_table_round_trips_the_canonical_names() {
        for syntax in RdfSyntax::ALL {
            assert_eq!(
                RdfSyntax::from_extension(syntax.as_str()),
                Some(syntax),
                "canonical name {} must parse back to itself",
                syntax.as_str()
            );
        }
        assert_eq!(RdfSyntax::from_extension("TTL"), Some(RdfSyntax::Turtle));
        assert_eq!(RdfSyntax::from_extension("csv"), None);
    }

    #[test]
    fn only_jelly_is_binary_and_only_quad_syntaxes_carry_graphs() {
        assert!(RdfSyntax::Jelly.is_binary());
        assert!(!RdfSyntax::Turtle.is_binary());
        assert!(RdfSyntax::NQuads.is_quad_syntax());
        assert!(RdfSyntax::TriG.is_quad_syntax());
        assert!(!RdfSyntax::NTriples.is_quad_syntax());
    }
}
