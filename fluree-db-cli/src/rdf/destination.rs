//! Where converted bytes go, and what may be written there.
//!
//! Two destinations, one rule each. A file is created and truncated; stdout is
//! locked and guarded so binary output never reaches a terminal. Both are
//! wrapped so that a `write` phase can be timed at chunk granularity — see
//! [`TimedWriter`](crate::rdf::writer::TimedWriter).

use crate::error::{CliError, CliResult};
use crate::rdf::syntax::{split_compression, Compression, RdfSyntax};
use crate::rdf::writer::{TimedWriter, WriteClock};
use std::io::{BufWriter, IsTerminal, Write};
use std::path::Path;

/// Buffer between the writer and the destination.
///
/// Large enough that the destination sees chunks rather than statements, which
/// is what keeps the `write` phase's clock reads off the hot path.
const OUT_BUF: usize = 64 * 1024;

/// A destination ready to be written to, plus the clock watching it.
pub struct Destination {
    /// The buffered, timed sink.
    pub out: BufWriter<TimedWriter<Box<dyn Write>>>,
    /// Live handle on time spent in real I/O.
    pub clock: WriteClock,
}

/// Open the output destination for `syntax`.
///
/// `path` of `None` means stdout, which is refused for a binary syntax on a
/// terminal — the same guard `fluree export` applies to `.flpack` archives,
/// and the reason it is checked here rather than at the writer is that the
/// writer has no idea where its bytes are going.
pub fn open(path: Option<&Path>, syntax: RdfSyntax) -> CliResult<Destination> {
    let sink: Box<dyn Write> = match path {
        Some(path) => {
            refuse_compressed_output(path)?;
            let file = std::fs::File::create(path)
                .map_err(|e| CliError::Usage(format!("cannot write '{}': {e}", path.display())))?;
            Box::new(file)
        }
        None => {
            crate::rdf::input::guard_binary_stdout(syntax)?;
            Box::new(std::io::stdout().lock())
        }
    };

    let timed = TimedWriter::new(sink);
    let clock = timed.clock();
    Ok(Destination {
        out: BufWriter::with_capacity(OUT_BUF, timed),
        clock,
    })
}

/// Refuse `-o out.ttl.gz`, which would otherwise write plain Turtle into a
/// file whose name promises gzip.
///
/// Input decompresses transparently, so a user has every reason to expect the
/// output to compress the same way. It does not yet, and silently producing a
/// mislabelled file is the worst of the three available answers.
fn refuse_compressed_output(path: &Path) -> CliResult<()> {
    let (_, compression) = split_compression(path);
    if compression == Compression::None {
        return Ok(());
    }
    Err(CliError::Usage(format!(
        "cannot write compressed output yet: '{}' names a {} file\n  {} write the plain \
         file and compress it, or pipe: `fluree rdf convert in.ttl --to nt | gzip > out.nt.gz`",
        path.display(),
        compression.as_str(),
        colored::Colorize::bold(colored::Colorize::cyan("help:")),
    )))
}

/// Whether stdout is a terminal. Split out so the convert driver can decide
/// whether a completion summary would be noise in a pipe.
pub fn stdout_is_terminal() -> bool {
    std::io::stdout().is_terminal()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_compressed_output_name_is_refused_rather_than_written_plain() {
        // The failure this prevents is silent: a `.gz` full of uncompressed
        // Turtle that every tool downstream refuses.
        for name in ["out.ttl.gz", "out.nt.zst", "OUT.TTL.ZSTD"] {
            let err = refuse_compressed_output(Path::new(name)).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("compressed output"), "{name}: {msg}");
            assert!(msg.contains("gzip"), "{name}: {msg}");
        }
        assert!(refuse_compressed_output(Path::new("out.ttl")).is_ok());
        assert!(refuse_compressed_output(Path::new("out")).is_ok());
    }

    #[test]
    fn opening_a_file_creates_it_and_reports_through_the_clock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.nt");
        let Ok(mut dest) = open(Some(&path), RdfSyntax::NTriples) else {
            panic!("a writable path must open");
        };

        dest.out.write_all(b"<a> <b> <c> .\n").unwrap();
        dest.out.flush().unwrap();
        drop(dest.out);

        assert_eq!(std::fs::read_to_string(&path).unwrap(), "<a> <b> <c> .\n");
        assert!(dest.clock.calls() > 0, "the clock saw the flush");
    }

    #[test]
    fn an_unwritable_path_is_a_usage_error_so_it_exits_2() {
        // `Destination` holds a boxed writer and cannot be Debug, so the
        // error is matched out of the Result by hand.
        let Err(err) = open(
            Some(Path::new("/nonexistent/dir/out.nt")),
            RdfSyntax::NTriples,
        ) else {
            panic!("opening a path under a missing directory must fail");
        };
        assert!(matches!(err, CliError::Usage(_)), "{err}");
        assert!(err.to_string().contains("cannot write"), "{err}");
    }
}
