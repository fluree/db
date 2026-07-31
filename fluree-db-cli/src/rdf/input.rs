//! Getting bytes into the `fluree rdf` verbs: file or stdin, gzip or zstd or
//! neither, with the phases the profiler needs kept apart.
//!
//! The existing [`crate::input::read_input`] is `read_to_string` with a source
//! enum around it — fine for a query string, no use here, where the input can
//! be a compressed pipe and the profiler has to say how much of the run was
//! I/O and how much was the decoder.
//!
//! # What this reads, and what it does not
//!
//! [`open_reader`] hands back a `BufRead` and is the seam the streaming
//! writers will use. What `check` and `count` do today is
//! [`read_all_guarded`] followed by [`decode`]: the whole document into
//! memory, bounded by [`fluree_graph_turtle::error::MAX_INPUT_BYTES`] because
//! the parser addresses token spans with `u32` offsets. Chunked parsing of a
//! stdin stream is real work — it needs a single-pass prelude extractor, since
//! a pipe cannot be reopened at byte 0 — and it lands with the parallel
//! pipeline, not here.

use crate::error::{CliError, CliResult};
use crate::rdf::syntax::Compression;
use fluree_graph_ir::{Phase, PhaseTimings};
use std::io::{BufRead, IsTerminal, Read};
use std::path::{Path, PathBuf};

/// Buffer size for input streams. Matches the import path's `IO_BUF`.
const IO_BUF: usize = 256 * 1024;

/// Where an `rdf` verb's bytes come from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RdfInput {
    /// A named file.
    File(PathBuf),
    /// Standard input — an explicit `-`, or no positional argument at all.
    Stdin,
}

impl RdfInput {
    /// Resolve the positional argument. Absent or `-` means stdin.
    ///
    /// Unlike [`crate::input::resolve_input`] there is no "stdin is a TTY, so
    /// error" check here: `fluree rdf check` with no argument on a terminal
    /// should block on stdin exactly as `cat` does, which is what a user
    /// typing a document by hand expects and what a shell script that pipes
    /// in nothing gets anyway.
    pub fn resolve(positional: Option<&Path>) -> Self {
        match positional {
            None => RdfInput::Stdin,
            Some(p) if p.as_os_str() == "-" => RdfInput::Stdin,
            Some(p) => RdfInput::File(p.to_path_buf()),
        }
    }

    /// The path, when there is one — for extension-based syntax resolution
    /// and for naming the corpus in a profile report.
    pub fn path(&self) -> Option<&Path> {
        match self {
            RdfInput::File(p) => Some(p),
            RdfInput::Stdin => None,
        }
    }

    /// How this input is named in diagnostics.
    pub fn display(&self) -> String {
        match self {
            RdfInput::File(p) => p.display().to_string(),
            RdfInput::Stdin => "<stdin>".to_string(),
        }
    }
}

/// How many bytes the input is expected to hold, when that is knowable.
///
/// `None` for stdin and for anything that is not a regular file — a pipe has
/// no length, and a character device's reported length is not a promise about
/// how much it will produce. Only a regular file's size is used to size the
/// buffer, which keeps a bogus length from turning into a bogus reservation.
pub fn size_hint(input: &RdfInput) -> Option<u64> {
    let RdfInput::File(path) = input else {
        return None;
    };
    let meta = std::fs::metadata(path).ok()?;
    meta.is_file().then_some(meta.len())
}

/// Open the input as a buffered byte stream, undecoded.
///
/// Decompression is deliberately not layered on here: for a pipe the magic
/// bytes are the only evidence a compression layer exists, and they are not
/// available until something has been read.
pub fn open_reader(input: &RdfInput) -> CliResult<Box<dyn BufRead + Send>> {
    match input {
        RdfInput::File(path) => {
            let file = std::fs::File::open(path).map_err(|e| io_error(path, &e))?;
            Ok(Box::new(std::io::BufReader::with_capacity(IO_BUF, file)))
        }
        // `stdin()` is already buffered internally, but the extra wrapper
        // keeps one `BufRead` type for both arms and costs one allocation.
        RdfInput::Stdin => Ok(Box::new(std::io::BufReader::with_capacity(
            IO_BUF,
            std::io::stdin(),
        ))),
    }
}

/// Read the whole stream, refusing anything the parser could not address.
///
/// The cap is enforced with a `take` one byte past the limit rather than a
/// length check afterwards, so an oversized input is refused after reading
/// 4 GiB instead of after allocating however much more was behind it.
///
/// `expected` is the input's length when it has one; it is a sizing hint and
/// never a promise. The `take` wrapper is what makes it necessary: it erases
/// the `File` specialization `read_to_end` uses to reserve the buffer once,
/// leaving the generic doubling path — which on a 280 MB document allocates a
/// 512 MB buffer and peaks at 768 MB copying its way there. Reserving the real
/// length costs two allocations and no copy. A file that grows past its own
/// metadata still reads correctly; the `Vec` simply grows for the tail.
///
/// Time is attributed to [`Phase::Read`].
pub fn read_all_guarded(
    reader: &mut dyn BufRead,
    expected: Option<u64>,
    timings: &mut PhaseTimings,
    what: &str,
) -> CliResult<Vec<u8>> {
    const LIMIT: u64 = fluree_graph_turtle::error::MAX_INPUT_BYTES as u64;

    timings.enter(Phase::Read);
    // A file whose own metadata says it is too big is refused HERE, before a
    // byte is read. The `take` below already guarantees the refusal, but it
    // spends 4 GiB of reading and 4 GiB of buffer to reach it — on a file the
    // caller could be told about immediately. The hint is not trusted for
    // correctness: an input that lies small still hits the `take` cap and is
    // refused there, which is why this is a short-circuit and not the check.
    if let Some(len) = expected {
        if len > LIMIT {
            timings.finish();
            return Err(oversized(what, LIMIT));
        }
    }
    // Clamped at the refusal limit for the same reason in the other direction:
    // a length that is wrong — or a file that is sparse — must not turn into an
    // allocation failure in place of the error message this function produces.
    let mut buf = match expected {
        Some(len) => Vec::with_capacity(len.min(LIMIT + 1) as usize),
        None => Vec::new(),
    };
    let read = reader
        .take(LIMIT + 1)
        .read_to_end(&mut buf)
        .map_err(|e| CliError::Usage(format!("cannot read {what}: {e}")))?;
    timings.finish();

    if read as u64 > LIMIT {
        return Err(oversized(what, LIMIT));
    }
    Ok(buf)
}

/// The one refusal for an input past the parser's addressable limit, shared by
/// the metadata short-circuit and the read that backs it up, so the two cannot
/// drift into telling a user two different things about one condition.
fn oversized(what: &str, limit: u64) -> CliError {
    CliError::Usage(format!(
        "{what} is larger than {limit} bytes, the most this path can parse in one \
         pass (the parser addresses token spans with 32-bit offsets)\n  {} split the \
         input, or wait for the chunked reader that lands with parallel conversion",
        colored::Colorize::bold(colored::Colorize::cyan("help:")),
    ))
}

/// Strip the compression layer and validate UTF-8, producing the text the
/// parser will see.
///
/// Time goes to [`Phase::Decompress`] when there is a layer to strip, and back
/// to [`Phase::Read`] when there is not — an uncompressed input should not
/// report a decompress phase for what is only a UTF-8 scan. Either way the
/// validation is charged with the step that produced the bytes rather than to
/// the parser, which never sees them un-validated.
///
/// Reading the compressed bytes fully before decoding, instead of composing a
/// decoder onto the file handle, is what makes the two phases separable at
/// all: a composed decoder interleaves reads and inflation so finely that no
/// pair of clock reads can tell them apart. The extra memory is the
/// *compressed* size, which is by construction the smaller number.
pub fn decode(
    bytes: Vec<u8>,
    compression: Compression,
    timings: &mut PhaseTimings,
    what: &str,
) -> CliResult<String> {
    timings.enter(if compression == Compression::None {
        Phase::Read
    } else {
        Phase::Decompress
    });
    let decoded = match compression {
        Compression::None => bytes,
        Compression::Gzip => {
            let mut out = Vec::new();
            // Multi-member: `pigz` and `bgzip` emit concatenated streams and a
            // single-member decoder silently stops at the first one.
            flate2::bufread::MultiGzDecoder::new(&bytes[..])
                .read_to_end(&mut out)
                .map_err(|e| CliError::Usage(format!("cannot gunzip {what}: {e}")))?;
            out
        }
        Compression::Zstd => zstd::stream::decode_all(&bytes[..])
            .map_err(|e| CliError::Usage(format!("cannot zstd-decode {what}: {e}")))?,
    };
    let text = String::from_utf8(decoded)
        .map_err(|e| CliError::Usage(format!("{what} is not valid UTF-8: {e}")))?;
    timings.finish();
    Ok(text)
}

/// An input that has been read off the wire and decoded.
#[derive(Debug)]
pub struct Decoded {
    /// The decoded document.
    pub text: String,
    /// Bytes read off the wire — the compressed size, when compressed, and so
    /// the right denominator for an I/O throughput figure.
    pub bytes_on_wire: u64,
    /// The compression layer that was stripped.
    pub compression: Compression,
}

/// Read `input` end to end and decode it, resolving the compression layer
/// from the path suffix or, failing that, the leading bytes.
///
/// The stream is opened exactly once. That is a correctness requirement, not
/// an optimization: a pipe cannot be reopened at byte 0, so a design that
/// peeks through one reader and then reads through another loses however much
/// the first one buffered — silently, and only for stdin.
pub fn read_decoded(input: &RdfInput, timings: &mut PhaseTimings) -> CliResult<Decoded> {
    let what = input.display();
    let mut reader = open_reader(input)?;
    let raw = read_all_guarded(reader.as_mut(), size_hint(input), timings, &what)?;
    // The magic bytes are just the head of what was read — no separate peek.
    let compression = crate::rdf::syntax::resolve_compression(input.path(), &raw);
    let bytes_on_wire = raw.len() as u64;
    let text = decode(raw, compression, timings, &what)?;
    Ok(Decoded {
        text,
        bytes_on_wire,
        compression,
    })
}

/// Whether writing `is_binary` output to this destination would dump binary
/// into somebody's terminal.
///
/// Split out from [`guard_binary_stdout`] so the decision is testable without
/// a controlled TTY.
pub fn would_dump_binary_to_tty(is_binary: bool, stdout_is_tty: bool) -> bool {
    is_binary && stdout_is_tty
}

/// Refuse to write binary output to a terminal.
///
/// Same contract as `fluree export`'s `.flpack` guard: a user who pipes or
/// redirects gets their bytes, and a user who forgot gets an error instead of
/// a scrambled terminal. Wired here now so the writers land behind it rather
/// than in front of it.
pub fn guard_binary_stdout(syntax: crate::rdf::syntax::RdfSyntax) -> CliResult<()> {
    if would_dump_binary_to_tty(syntax.is_binary(), std::io::stdout().is_terminal()) {
        return Err(CliError::Usage(format!(
            "refusing to write binary {} output to a terminal\n  {} pass -o <FILE> or \
             redirect stdout",
            syntax,
            colored::Colorize::bold(colored::Colorize::cyan("help:")),
        )));
    }
    Ok(())
}

/// An I/O failure on a named path.
///
/// Deliberately [`CliError::Usage`], which exits 2: for the `rdf` verbs, exit
/// 1 is reserved for "the document is bad" and everything about *reaching* the
/// document — missing file, unreadable file, bad flag — is the 2 case. Without
/// this a caller could not tell "your Turtle has a syntax error" from "your
/// path was wrong" by exit code alone.
fn io_error(path: &Path, e: &std::io::Error) -> CliError {
    CliError::Usage(format!("cannot read '{}': {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn a_missing_positional_and_a_dash_both_mean_stdin() {
        assert_eq!(RdfInput::resolve(None), RdfInput::Stdin);
        assert_eq!(RdfInput::resolve(Some(Path::new("-"))), RdfInput::Stdin);
        assert_eq!(
            RdfInput::resolve(Some(Path::new("data.ttl"))),
            RdfInput::File(PathBuf::from("data.ttl"))
        );
        assert_eq!(RdfInput::Stdin.display(), "<stdin>");
        assert_eq!(RdfInput::Stdin.path(), None);
    }

    #[test]
    fn binary_output_is_refused_only_when_the_destination_is_a_terminal() {
        // Redirected or piped: the bytes are wanted.
        assert!(!would_dump_binary_to_tty(true, false));
        // A terminal: refuse.
        assert!(would_dump_binary_to_tty(true, true));
        // Text syntaxes are never refused, TTY or not.
        assert!(!would_dump_binary_to_tty(false, true));
        assert!(!would_dump_binary_to_tty(false, false));
    }

    #[test]
    fn the_guard_only_fires_for_binary_syntaxes() {
        use crate::rdf::syntax::RdfSyntax;
        // Under `cargo test` stdout is captured, never a TTY, so the guard
        // must pass for everything here — including Jelly. The decision
        // itself is covered by the predicate test above.
        for syntax in RdfSyntax::ALL {
            assert!(guard_binary_stdout(syntax).is_ok());
        }
    }

    /// Write `bytes` to a file named `name` and read it back through the
    /// real path, compression and all.
    fn read_file(name: &str, bytes: &[u8]) -> CliResult<Decoded> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(name);
        std::fs::File::create(&path)
            .unwrap()
            .write_all(bytes)
            .unwrap();
        read_decoded(&RdfInput::File(path), &mut PhaseTimings::start())
    }

    fn gzip(text: &str) -> Vec<u8> {
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(text.as_bytes()).unwrap();
        gz.finish().unwrap()
    }

    #[test]
    fn plain_gzip_and_zstd_inputs_all_decode_to_the_same_text() {
        let doc = "@prefix ex: <http://example.org/> .\nex:a ex:b \"c\" .\n";

        let plain = read_file("input.ttl", doc.as_bytes()).unwrap();
        assert_eq!(plain.text, doc);
        assert_eq!(plain.compression, Compression::None);

        let gz = read_file("input.ttl.gz", &gzip(doc)).unwrap();
        assert_eq!(gz.text, doc);
        assert_eq!(gz.compression, Compression::Gzip);

        let zst = read_file(
            "input.ttl.zst",
            &zstd::stream::encode_all(doc.as_bytes(), 3).unwrap(),
        )
        .unwrap();
        assert_eq!(zst.text, doc);
        assert_eq!(zst.compression, Compression::Zstd);
    }

    #[test]
    fn a_compressed_file_with_no_suffix_still_decodes() {
        // The magic bytes are read off the front of what was already read,
        // so an unlabelled `.gz` costs four byte comparisons to handle
        // instead of surfacing as a UTF-8 error.
        let doc = "<a> <b> <c> .\n";
        let out = read_file("dump", &gzip(doc)).unwrap();
        assert_eq!(out.text, doc);
        assert_eq!(out.compression, Compression::Gzip);
    }

    #[test]
    fn concatenated_gzip_members_all_decode() {
        // What `pigz` and `bgzip` produce. A single-member decoder returns the
        // first member and reports success, which loses the rest of the file
        // silently — the failure mode this decoder choice exists to avoid.
        let mut combined = Vec::new();
        for part in ["<a> <b> <c> .\n", "<d> <e> <f> .\n"] {
            combined.extend(gzip(part));
        }
        let out = read_file("input.ttl.gz", &combined).unwrap();
        assert_eq!(out.text, "<a> <b> <c> .\n<d> <e> <f> .\n");
    }

    #[test]
    fn read_and_decompress_are_charged_to_separate_phases() {
        let doc = "ex:a ex:b \"c\" .\n".repeat(500);
        let zst = zstd::stream::encode_all(doc.as_bytes(), 3).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("input.ttl.zst");
        std::fs::File::create(&path)
            .unwrap()
            .write_all(&zst)
            .unwrap();

        let mut timings = PhaseTimings::start();
        let out = read_decoded(&RdfInput::File(path), &mut timings).expect("decodes");
        assert_eq!(out.text.len(), doc.len());
        assert_eq!(
            out.bytes_on_wire as usize,
            zst.len(),
            "the wire figure is the compressed size, which is what an I/O rate is per"
        );
        // Both phases ran and neither swallowed the other.
        assert!(timings.elapsed(Phase::Read) > std::time::Duration::ZERO);
        assert!(timings.elapsed(Phase::Decompress) > std::time::Duration::ZERO);
        assert_eq!(timings.elapsed(Phase::Parse), std::time::Duration::ZERO);
    }

    #[test]
    fn an_uncompressed_input_reports_no_decompress_phase_at_all() {
        // There is nothing to decompress; a row for the UTF-8 scan under that
        // name would be a phase breakdown that misnames its own work.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("input.ttl");
        std::fs::File::create(&path)
            .unwrap()
            .write_all("<a> <b> <c> .\n".repeat(500).as_bytes())
            .unwrap();

        let mut timings = PhaseTimings::start();
        read_decoded(&RdfInput::File(path), &mut timings).unwrap();
        assert!(timings.elapsed(Phase::Read) > std::time::Duration::ZERO);
        assert_eq!(
            timings.elapsed(Phase::Decompress),
            std::time::Duration::ZERO
        );
    }

    #[test]
    fn an_oversized_file_is_refused_from_its_metadata_not_after_reading_it() {
        // The `take` cap refuses it either way; what the short-circuit saves is
        // 4 GiB of reading and 4 GiB of buffer to reach a refusal the metadata
        // already implied. Both paths must produce the SAME message, or a user
        // gets a different explanation depending on which one fired.
        const LIMIT: u64 = fluree_graph_turtle::error::MAX_INPUT_BYTES as u64;
        let mut reader = std::io::Cursor::new(b"<a> <b> <c> .\n".as_slice());
        let err = read_all_guarded(
            &mut reader,
            Some(LIMIT + 1),
            &mut PhaseTimings::start(),
            "huge.ttl",
        )
        .expect_err("a file claiming to be past the limit is refused");
        assert!(err.to_string().contains("larger than"), "{err}");
        assert!(err.to_string().contains("huge.ttl"), "{err}");
        // And a hint exactly AT the limit is not refused — the boundary belongs
        // to the readable side, matching the `take(LIMIT + 1)` that backs it.
        let mut reader = std::io::Cursor::new(b"<a> <b> <c> .\n".as_slice());
        assert!(
            read_all_guarded(
                &mut reader,
                Some(LIMIT),
                &mut PhaseTimings::start(),
                "big.ttl"
            )
            .is_ok(),
            "a file exactly at the limit must still be read"
        );
    }

    #[test]
    fn the_size_hint_is_a_hint_and_never_a_promise() {
        // Both directions of wrong, because the hint only sizes the buffer:
        // too small and the `Vec` grows for the tail, too large and the extra
        // capacity is never touched. Either way the bytes are all there.
        let doc = "<a> <b> <c> .\n".repeat(1000);
        for hint in [
            None,
            Some(0),
            Some(1),
            Some(doc.len() as u64),
            Some(1 << 20),
        ] {
            let mut reader = std::io::Cursor::new(doc.as_bytes());
            let out = read_all_guarded(&mut reader, hint, &mut PhaseTimings::start(), "test")
                .expect("reads");
            assert_eq!(out.len(), doc.len(), "hint {hint:?} changed what was read");
            assert_eq!(out, doc.as_bytes(), "hint {hint:?} corrupted the read");
        }
    }

    #[test]
    fn only_a_regular_file_offers_a_size_hint() {
        // Stdin is a pipe: no length to hint with, and a device's reported
        // length is not a promise about what it will produce.
        assert_eq!(size_hint(&RdfInput::Stdin), None);
        assert_eq!(
            size_hint(&RdfInput::File(PathBuf::from("/nonexistent"))),
            None
        );

        let dir = tempfile::tempdir().unwrap();
        assert_eq!(size_hint(&RdfInput::File(dir.path().to_path_buf())), None);

        let path = dir.path().join("input.ttl");
        std::fs::File::create(&path)
            .unwrap()
            .write_all(b"<a> <b> <c> .\n")
            .unwrap();
        assert_eq!(size_hint(&RdfInput::File(path)), Some(14));
    }

    #[test]
    fn a_missing_file_is_a_usage_error_so_it_exits_2_not_1() {
        // Exit 1 means "the RDF is bad". A path that does not exist has to be
        // distinguishable from that by exit code alone.
        let err = read_decoded(
            &RdfInput::File(PathBuf::from("/nonexistent/nope.ttl")),
            &mut PhaseTimings::start(),
        )
        .unwrap_err();
        assert!(matches!(err, CliError::Usage(_)), "{err}");
        assert!(err.to_string().contains("nope.ttl"), "{err}");
    }

    #[test]
    fn invalid_utf8_is_reported_as_such_and_not_as_a_parse_error() {
        let err = read_file("input.ttl", &[0xff, 0xfe, 0x00]).unwrap_err();
        assert!(err.to_string().contains("not valid UTF-8"), "{err}");
    }

    #[test]
    fn corrupt_compressed_input_names_the_decoder_that_refused_it() {
        // The suffix promises gzip and the bytes are not gzip: the suffix is
        // believed, so the failure names the decoder rather than being
        // silently reinterpreted as text.
        let err = read_file("input.ttl.gz", b"not gzip at all").unwrap_err();
        assert!(err.to_string().contains("gunzip"), "{err}");
    }
}
