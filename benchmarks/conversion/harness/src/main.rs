//! Tested helpers for the Tier-2 conversion harness.
//!
//! Everything here is a pure function over text or numbers, wrapped in a
//! subcommand. It lives in Rust rather than in the shell scripts for one
//! reason: these are the parts that are *wrong quietly*. A shell one-liner that
//! mis-parses GNU time's RSS units on one platform produces a plausible number,
//! and a plausible wrong number in a benchmark is worse than a crash.
//!
//! Subcommands:
//!   rss <file>        parse GNU time -v output, print peak RSS in bytes
//!   stats             read numbers on stdin, print {median, mad, n} as JSON
//!   normalize <file>  canonicalize N-Triples for cross-tool diffing
//!   calibrate         verify GNU time's RSS unit empirically on this host

use std::io::Read;
use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (cmd, rest) = match args.split_first() {
        Some((c, r)) => (c.as_str(), r),
        None => {
            eprintln!("usage: bench-harness <rss|stats|normalize|calibrate> [args]");
            return ExitCode::from(2);
        }
    };

    let result = match cmd {
        "rss" => cmd_rss(rest),
        "stats" => cmd_stats(),
        "normalize" => cmd_normalize(rest),
        "calibrate" => cmd_calibrate(),
        other => Err(format!("unknown subcommand '{other}'")),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

// ---------------------------------------------------------------------------
// Peak RSS
// ---------------------------------------------------------------------------

/// Peak resident set size in **bytes**, from GNU time's `-v` output.
///
/// This is the "ONE tested function" the strategy asks for (§6b F7), and it is
/// one function because the unit question here has a widely-believed wrong
/// answer.
///
/// Darwin's `getrusage(2)` returns `ru_maxrss` in **bytes**, where Linux
/// returns **kibibytes** — this is true, well known, and the reason a dozen
/// cross-platform tools carry a `#[cfg(target_os = "macos")]` divide-by-1024.
/// It is also *not what GNU time prints*: GNU time normalizes before emitting,
/// so its `(kbytes)` label is truthful on both platforms.
///
/// Applying the syscall-level correction to GNU time's already-normalized
/// output makes every macOS figure 1024x too small — and "1.5 MB peak RSS for
/// a parser" is plausible enough that nobody would ever question it. So the
/// label is authoritative here, and `calibrate` exists to verify that claim
/// empirically on any host rather than asking the next reader to trust this
/// comment.
fn parse_peak_rss_bytes(gnu_time_output: &str) -> Result<u64, String> {
    for line in gnu_time_output.lines() {
        let line = line.trim();
        let Some(rest) = line.strip_prefix("Maximum resident set size") else {
            continue;
        };
        let Some((unit_part, value_part)) = rest.split_once(':') else {
            continue;
        };
        let value: u64 = value_part
            .trim()
            .parse()
            .map_err(|_| format!("unparseable RSS value in {line:?}"))?;

        let unit = unit_part
            .trim()
            .trim_start_matches('(')
            .trim_end_matches(')');
        return match unit {
            "kbytes" => Ok(value * 1024),
            "bytes" => Ok(value),
            u => Err(format!(
                "unrecognized RSS unit {u:?} — refusing to guess. \
                 Run `bench-harness calibrate` to establish the unit on this host."
            )),
        };
    }
    Err("no 'Maximum resident set size' line in GNU time output — \
         was the command run under `gtime -v` / `/usr/bin/time -v`?"
        .to_string())
}

fn cmd_rss(args: &[String]) -> Result<(), String> {
    let path = args
        .first()
        .ok_or("usage: bench-harness rss <gtime-output>")?;
    let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
    println!("{}", parse_peak_rss_bytes(&text)?);
    Ok(())
}

/// Establish GNU time's RSS unit empirically, on this host, right now.
///
/// Allocates and touches a known amount of memory under `gtime -v` and checks
/// which interpretation of the reported number lands within tolerance. This
/// replaces a portability assumption with a measurement, which matters because
/// the assumption most people would write down here is wrong (see
/// [`parse_peak_rss_bytes`]) and wrong in the direction that looks reasonable.
fn cmd_calibrate() -> Result<(), String> {
    const TARGET_MIB: u64 = 200;
    let gtime = if std::process::Command::new("gtime")
        .arg("--version")
        .output()
        .is_ok()
    {
        "gtime"
    } else {
        "/usr/bin/time"
    };

    let script = format!(
        "b = bytearray({} * 1024 * 1024)\n\
         for i in range(0, len(b), 4096): b[i] = 1\n",
        TARGET_MIB
    );
    let out = std::process::Command::new(gtime)
        .args(["-v", "python3", "-c", &script])
        .output()
        .map_err(|e| format!("could not run {gtime}: {e}"))?;
    let text = String::from_utf8_lossy(&out.stderr);

    let reported = parse_peak_rss_bytes(&text)?;
    let target_bytes = TARGET_MIB * 1024 * 1024;
    let ratio = reported as f64 / target_bytes as f64;

    println!("allocated:  {TARGET_MIB} MiB");
    println!(
        "parsed as:  {} MiB ({reported} bytes)",
        reported / (1024 * 1024)
    );
    println!("ratio:      {ratio:.3}");

    // A correct reading lands a little above 1.0 (interpreter overhead).
    // Reading kibibytes as bytes would land at ~0.001.
    if (0.9..1.6).contains(&ratio) {
        println!("VERDICT:    units correct on this host");
        Ok(())
    } else {
        Err(format!(
            "RSS unit handling is WRONG on this host: a {TARGET_MIB} MiB \
             allocation parsed as {} MiB (ratio {ratio:.4}). A ratio near \
             0.001 means kibibytes are being read as bytes.",
            reported / (1024 * 1024)
        ))
    }
}

// ---------------------------------------------------------------------------
// Median + MAD
// ---------------------------------------------------------------------------

/// Median and median absolute deviation.
///
/// MAD rather than standard deviation because a benchmark's noise is not
/// Gaussian — it is a clean distribution with occasional multi-hundred-percent
/// outliers when something else on the box wakes up. One such sample moves a
/// mean and a standard deviation enough to hide a real regression; it moves a
/// median and a MAD almost not at all.
///
/// The gating rule this feeds (§6b F5) is
/// `flag iff median_delta > max(budget_pct, 3 * MAD / median)`, so a noisy host
/// widens its own tolerance instead of emitting false regressions.
fn median(sorted: &[f64]) -> f64 {
    let n = sorted.len();
    if n == 0 {
        return f64::NAN;
    }
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    }
}

fn median_and_mad(values: &[f64]) -> (f64, f64) {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).expect("no NaNs in timing samples"));
    let med = median(&sorted);
    let mut deviations: Vec<f64> = sorted.iter().map(|v| (v - med).abs()).collect();
    deviations.sort_by(|a, b| a.partial_cmp(b).expect("no NaNs"));
    (med, median(&deviations))
}

fn cmd_stats() -> Result<(), String> {
    let mut input = String::new();
    std::io::stdin()
        .read_to_string(&mut input)
        .map_err(|e| e.to_string())?;
    let values: Vec<f64> = input
        .split_whitespace()
        .map(|t| t.parse::<f64>().map_err(|_| format!("not a number: {t:?}")))
        .collect::<Result<_, _>>()?;
    if values.is_empty() {
        return Err("no samples on stdin".to_string());
    }
    let (med, mad) = median_and_mad(&values);
    let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    println!(
        r#"{{"n":{},"median":{:.6},"mad":{:.6},"min":{:.6},"max":{:.6},"rel_mad_pct":{:.3}}}"#,
        values.len(),
        med,
        mad,
        min,
        max,
        if med == 0.0 { 0.0 } else { 100.0 * mad / med }
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// N-Triples normalization for cross-tool diffing
// ---------------------------------------------------------------------------

/// One normalized statement, plus whether it mentions a blank node.
pub struct Normalized {
    pub text: String,
    pub has_blank: bool,
}

/// Canonicalize one N-Triples line so two conformant tools that spell the same
/// RDF differently compare equal (§6b "Tier-2 normalization rules").
///
/// The line is walked TERM BY TERM rather than character by character, because
/// the rules differ per term and applying any of them across a term boundary is
/// how a normalizer invents equalities that are not there.
///
/// Rules, each a real difference between real tools:
///
/// 1. **`UCHAR` escapes decode to their code point**, in literals *and* in
///    IRIs. `é` and a literal `é` are the same character by the grammar's
///    own definition, and raptor emits the escaped form for non-ASCII where
///    others emit the character. Without this, every non-ASCII term in a real
///    corpus is a false mismatch.
/// 2. **Language tags case-fold.** `@EN-gb` and `@en-GB` are one tag; BCP 47 is
///    case-insensitive.
/// 3. **`"s"^^xsd:string` is `"s"`.** Identical literals in RDF 1.1.
/// 4. **Whitespace BETWEEN terms is not content**, and whitespace *inside* a
///    term is. An earlier version collapsed runs of spaces everywhere, which
///    made `<http://ex/a  b>` and `<http://ex/a b>` compare equal — two
///    different resources, silently fused, on exactly the population of IRIs
///    the escaping rules are about.
///
/// # What it does not do
///
/// Blank-node labels are not canonicalized, and a statement mentioning one is
/// flagged via [`Normalized::has_blank`] so the caller can exempt it. Deciding
/// whether two graphs agree up to blank-node renaming is graph isomorphism, not
/// a line transform; the harness owns that at level 2 with rdflib, and faking
/// it here would produce confident wrong answers on the one question this
/// cannot answer.
pub fn normalize_ntriples_line(line: &str) -> Option<Normalized> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }

    let mut out = String::with_capacity(trimmed.len());
    let mut has_blank = false;
    let mut chars = trimmed.chars().peekable();

    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
            continue;
        }
        if !out.is_empty() {
            out.push(' ');
        }
        match c {
            '<' => out.push_str(&take_iri(&mut chars)),
            '"' => out.push_str(&take_literal(&mut chars)),
            '_' => {
                has_blank = true;
                out.push_str(&take_bare(&mut chars));
            }
            _ => out.push_str(&take_bare(&mut chars)),
        }
    }

    Some(Normalized {
        text: out,
        has_blank,
    })
}

/// Consume `<...>` and return it with `UCHAR` escapes decoded.
///
/// Interior bytes are otherwise untouched — including whitespace, which is what
/// distinguishes two IRIs that differ only by a space.
fn take_iri(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> String {
    let mut raw = String::new();
    chars.next(); // '<'
    for c in chars.by_ref() {
        if c == '>' {
            break;
        }
        raw.push(c);
    }
    format!("<{}>", decode_uchar(&raw))
}

/// Consume a literal and any `@lang` / `^^<datatype>` suffix.
fn take_literal(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> String {
    let mut body = String::new();
    chars.next(); // opening quote
    let mut escaped = false;
    for c in chars.by_ref() {
        if escaped {
            body.push('\\');
            body.push(c);
            escaped = false;
        } else if c == '\\' {
            escaped = true;
        } else if c == '"' {
            break;
        } else {
            body.push(c);
        }
    }
    let mut out = format!("\"{}\"", decode_uchar(&body));

    match chars.peek() {
        Some('@') => {
            chars.next();
            let mut tag = String::new();
            while let Some(&t) = chars.peek() {
                if t.is_ascii_alphanumeric() || t == '-' {
                    tag.push(t);
                    chars.next();
                } else {
                    break;
                }
            }
            // BCP 47 is case-insensitive; lowercase is a canonical form and is
            // only ever compared, never emitted as RDF.
            out.push('@');
            out.push_str(&tag.to_ascii_lowercase());
        }
        Some('^') => {
            chars.next(); // first '^'
            if chars.peek() == Some(&'^') {
                chars.next();
            }
            let datatype = if chars.peek() == Some(&'<') {
                take_iri(chars)
            } else {
                take_bare(chars)
            };
            // RDF 1.1: a bare string literal IS xsd:string.
            if datatype != "<http://www.w3.org/2001/XMLSchema#string>" {
                out.push_str("^^");
                out.push_str(&datatype);
            }
        }
        _ => {}
    }
    out
}

/// Consume a run of non-whitespace: a blank-node label, a bare `.`, anything
/// that is not bracketed or quoted.
fn take_bare(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> String {
    let mut out = String::new();
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            break;
        }
        out.push(c);
        chars.next();
    }
    out
}

/// Decode `\uXXXX` and `\UXXXXXXXX` to their code points, leaving every other
/// escape alone.
///
/// A decoded `"` or `\` is re-escaped, so the result stays an unambiguous
/// comparison key rather than a string whose own delimiters moved.
fn decode_uchar(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        let width = match chars.peek() {
            Some('u') => 4,
            Some('U') => 8,
            // Any other escape is not a UCHAR: pass it through untouched so
            // `\\u0041` stays an escaped backslash followed by "u0041".
            _ => {
                out.push('\\');
                if let Some(next) = chars.next() {
                    out.push(next);
                }
                continue;
            }
        };
        chars.next(); // 'u' or 'U'
        let hex: String = (0..width).filter_map(|_| chars.next()).collect();
        match u32::from_str_radix(&hex, 16).ok().and_then(char::from_u32) {
            Some('"') => out.push_str("\\\""),
            Some('\\') => out.push_str("\\\\"),
            Some(decoded) => out.push(decoded),
            // Not a valid escape: keep the source text rather than guess.
            None => {
                out.push('\\');
                out.push(if width == 4 { 'u' } else { 'U' });
                out.push_str(&hex);
            }
        }
    }
    out
}

fn cmd_normalize(args: &[String]) -> Result<(), String> {
    let path = args
        .first()
        .ok_or("usage: bench-harness normalize <file.nt>")?;
    let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;

    let mut lines: Vec<String> = Vec::new();
    let mut exempt = 0usize;
    for normalized in text.lines().filter_map(normalize_ntriples_line) {
        if normalized.has_blank {
            // Level 2 (rdflib isomorphism) owns blank-node structure.
            exempt += 1;
            continue;
        }
        lines.push(normalized.text);
    }
    // Statement ORDER is not RDF.
    lines.sort_unstable();

    let stdout = std::io::stdout();
    let mut out = std::io::BufWriter::new(stdout.lock());
    use std::io::Write;
    for line in lines {
        writeln!(out, "{line}").map_err(|e| e.to_string())?;
    }
    // Reported on stderr so a caller can surface how much of the file this
    // comparison did NOT cover.
    eprintln!("{exempt}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- peak RSS ---------------------------------------------------------

    /// GNU time normalizes before printing, so the `(kbytes)` label is
    /// truthful on BOTH platforms. Verified empirically on darwin-arm64 with
    /// GNU time 1.10: a 200 MiB resident allocation reports 213312 kbytes.
    /// `bench-harness calibrate` re-establishes this on any other host.
    #[test]
    fn kbytes_are_kibibytes_on_every_platform_gnu_time_runs_on() {
        let linux = "\tMaximum resident set size (kbytes): 204800";
        assert_eq!(parse_peak_rss_bytes(linux).unwrap(), 204_800 * 1024);

        // The measured macOS sample from the calibration run.
        let macos = "\tMaximum resident set size (kbytes): 213312";
        let bytes = parse_peak_rss_bytes(macos).unwrap();
        let mib = bytes / (1024 * 1024);
        assert!(
            (200..=210).contains(&mib),
            "a 200 MiB allocation must parse as ~200 MiB, got {mib} MiB"
        );
    }

    /// The trap this function exists for. Darwin's getrusage(2) really does
    /// return ru_maxrss in BYTES, so the obvious portability fix is to divide
    /// by 1024 on macOS — and it is wrong here, because GNU time already did.
    /// Applying it yields 1.5 MB for a parser: plausible, publishable, and
    /// 1024x too small.
    #[test]
    fn the_macos_bytes_correction_would_be_off_by_1024() {
        let sample = "\tMaximum resident set size (kbytes): 213312";
        let correct = parse_peak_rss_bytes(sample).unwrap();
        let if_we_had_believed_the_syscall_convention = 213_312_u64;
        assert_eq!(correct / if_we_had_believed_the_syscall_convention, 1024);
    }

    #[test]
    fn an_explicit_byte_label_is_believed() {
        assert_eq!(
            parse_peak_rss_bytes("\tMaximum resident set size (bytes): 4096").unwrap(),
            4096
        );
    }

    /// Silence is the failure mode that matters: a missing field must stop the
    /// run, not yield zero and let a "0 MB peak RSS" cell reach a matrix.
    #[test]
    fn missing_rss_line_is_an_error_not_a_zero() {
        let err = parse_peak_rss_bytes("nothing useful here").unwrap_err();
        assert!(err.contains("Maximum resident set size"), "{err}");
    }

    #[test]
    fn an_unknown_unit_is_refused_rather_than_guessed() {
        let err = parse_peak_rss_bytes("\tMaximum resident set size (pages): 512").unwrap_err();
        assert!(
            err.contains("calibrate"),
            "the error must name the remedy: {err}"
        );
    }

    // -- median / MAD -----------------------------------------------------

    #[test]
    fn median_and_mad_ignore_a_single_wild_outlier() {
        // Nine tight samples and one 20x spike, which is what a background
        // process on the bench host looks like.
        let clean = [10.0, 10.1, 9.9, 10.0, 10.2, 9.8, 10.0, 10.1, 9.9];
        let mut noisy = clean.to_vec();
        noisy.push(200.0);

        let (clean_med, _) = median_and_mad(&clean);
        let (noisy_med, noisy_mad) = median_and_mad(&noisy);

        assert!(
            (clean_med - noisy_med).abs() < 0.2,
            "median moved from {clean_med} to {noisy_med}"
        );
        // The mean would have moved from ~10 to ~29.
        let mean: f64 = noisy.iter().sum::<f64>() / noisy.len() as f64;
        assert!(mean > 25.0, "the outlier really is that large: {mean}");
        // MAD stays small: the spread of the bulk, not of the tail.
        assert!(noisy_mad < 0.3, "mad = {noisy_mad}");
    }

    #[test]
    fn median_handles_even_and_odd_sample_counts() {
        assert_eq!(median(&[1.0, 2.0, 3.0]), 2.0);
        assert_eq!(median(&[1.0, 2.0, 3.0, 4.0]), 2.5);
        assert_eq!(median(&[7.0]), 7.0);
    }

    // -- normalization ----------------------------------------------------

    fn norm(line: &str) -> String {
        normalize_ntriples_line(line).expect("a statement").text
    }

    /// We preserve `@EN-gb` losslessly; riot canonicalizes to `@en-GB`. Both
    /// are the same tag, so a byte-diff would report every language-tagged
    /// triple in the corpus as a difference.
    #[test]
    fn language_tags_compare_case_insensitively() {
        let ours = r#"<http://ex/s> <http://ex/p> "hello"@EN-gb ."#;
        let riot = r#"<http://ex/s> <http://ex/p> "hello"@en-GB ."#;
        assert_eq!(norm(ours), norm(riot));
    }

    /// RDF 1.1: a bare string literal IS `xsd:string`. rdflib and rapper spell
    /// it out; we and riot do not.
    #[test]
    fn explicit_xsd_string_equals_the_bare_form() {
        let bare = r#"<http://ex/s> <http://ex/p> "v" ."#;
        let explicit =
            r#"<http://ex/s> <http://ex/p> "v"^^<http://www.w3.org/2001/XMLSchema#string> ."#;
        assert_eq!(norm(bare), norm(explicit));
    }

    #[test]
    fn other_datatypes_are_not_stripped() {
        let integer =
            r#"<http://ex/s> <http://ex/p> "1"^^<http://www.w3.org/2001/XMLSchema#integer> ."#;
        let bare = r#"<http://ex/s> <http://ex/p> "1" ."#;
        assert_ne!(norm(integer), norm(bare));
        assert!(norm(integer).contains("XMLSchema#integer"));
    }

    /// Grammar-defined equivalence, and the one that bites hardest on a real
    /// corpus: raptor emits `UCHAR` escapes for non-ASCII where other tools
    /// emit the character. Without decoding, every accented term in DBLP is a
    /// false mismatch.
    #[test]
    fn uchar_escapes_decode_in_literals_and_in_iris() {
        let escaped = r#"<http://ex/café> <http://ex/p> "résumé" ."#;
        let literal = "<http://ex/café> <http://ex/p> \"résumé\" .";
        assert_eq!(norm(escaped), norm(literal));

        // The 8-hex form too.
        let long = r#"<http://ex/s> <http://ex/p> "\U0001F600" ."#;
        let direct = "<http://ex/s> <http://ex/p> \"\u{1F600}\" .";
        assert_eq!(norm(long), norm(direct));
    }

    /// An escaped backslash followed by text that merely LOOKS like a UCHAR is
    /// not one. Getting this wrong would silently rewrite literal content.
    #[test]
    fn an_escaped_backslash_is_not_the_start_of_a_uchar() {
        let out = norm(r#"<http://ex/s> <http://ex/p> "\\u0041" ."#);
        assert!(out.contains(r"\\u0041"), "{out}");
        assert!(!out.contains('A'), "{out}");
    }

    /// The bug this replaced. Whitespace INSIDE a term is content; only
    /// whitespace BETWEEN terms is not. Collapsing runs of spaces everywhere
    /// made two different resources compare equal — on exactly the population
    /// of IRIs the escaping rules concern.
    #[test]
    fn whitespace_inside_an_iri_is_content_and_two_such_iris_stay_different() {
        let one_space = "<http://ex/a b> <http://ex/p> <http://ex/o> .";
        let two_spaces = "<http://ex/a  b> <http://ex/p> <http://ex/o> .";
        assert_ne!(
            norm(one_space),
            norm(two_spaces),
            "two distinct IRIs were fused by normalization"
        );
        assert!(norm(two_spaces).contains("<http://ex/a  b>"));
    }

    #[test]
    fn inter_term_whitespace_is_not_content() {
        let spaced = "<http://ex/s>    <http://ex/p>\t<http://ex/o>  .";
        let tight = "<http://ex/s> <http://ex/p> <http://ex/o> .";
        assert_eq!(norm(spaced), norm(tight));
    }

    #[test]
    fn literal_content_survives_normalization_intact() {
        let tricky = r#"<http://ex/s> <http://ex/p> "a \" b @en ^^<x>  c" ."#;
        let out = norm(tricky);
        assert!(out.contains(r#"a \" b @en ^^<x>  c"#), "{out}");
    }

    #[test]
    fn comments_and_blank_lines_are_not_statements() {
        assert!(normalize_ntriples_line("").is_none());
        assert!(normalize_ntriples_line("   ").is_none());
        assert!(normalize_ntriples_line("# a comment").is_none());
    }

    /// Blank-node statements are flagged for exemption rather than
    /// canonicalized. Deciding whether `_:b0` here is `_:x` there is graph
    /// isomorphism; level 2 owns it.
    #[test]
    fn blank_node_statements_are_flagged_for_exemption() {
        let with_blank = normalize_ntriples_line("_:b0 <http://ex/p> <http://ex/o> .").unwrap();
        assert!(with_blank.has_blank);

        let object_blank = normalize_ntriples_line("<http://ex/s> <http://ex/p> _:b1 .").unwrap();
        assert!(object_blank.has_blank);

        let no_blank =
            normalize_ntriples_line("<http://ex/s> <http://ex/p> <http://ex/o> .").unwrap();
        assert!(!no_blank.has_blank);

        // A literal that merely CONTAINS "_:" is not a blank node.
        let literal = normalize_ntriples_line(r#"<http://ex/s> <http://ex/p> "_:b0" ."#).unwrap();
        assert!(!literal.has_blank, "a literal is not a blank node");
    }

    /// IRI values are not otherwise rewritten, so riot's validation warnings
    /// about an IRI's *value* cannot become triple-level mismatches.
    #[test]
    fn iri_values_are_left_alone_apart_from_grammar_escapes() {
        let line = r#"<http://ex/a b> <http://ex/p> <http://ex/o> ."#;
        assert!(norm(line).contains("<http://ex/a b>"));
    }
}
