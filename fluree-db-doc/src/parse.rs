//! One document in, a DoCO graph and text projection out.
//!
//! This is the in-process equivalent of `fdoc convert -f doco` and
//! `-f text` from one parse, with the engine's own escalation hook driven by
//! an OpenAI-compatible vision endpoint instead of `fdoc`'s Gemini client.
//! PDF is the geometric path (structure inferred from glyph positions,
//! escalating regions the deterministic pass could not read); the other
//! formats declare their structure and carry no geometry.

use crate::escalate::VlmReader;
use crate::{DocError, Result};
use fluree_doc_model::{to_doco, to_text, DocoOptions, DocumentInfo, Element, Notes, PageSize};
use fluree_doc_pdf::document::AnalyzeOptions;
use fluree_doc_pdf::escalate::{CropHints, Readings};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// The pinned `fluree-doc-parse` revision (see the workspace `Cargo.toml`).
/// Part of every parse-cache fingerprint: bump it with the pin so cached
/// output from an older engine is never served as current.
pub const DOC_PARSE_REV: &str = "38068738caddd3ebe80e0c5e50f372d3c8e82767";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    Pdf,
    Markdown,
    Html,
    Docx,
    Pptx,
    /// A workbook: each sheet is a page, each block of cells a table.
    Xlsx,
    /// Captions or a meeting transcript: one paragraph per speaker turn.
    WebVtt,
    Srt,
    /// An email, its thread split into messages. Attachments are described
    /// on the document node, not read: each is a document of its own.
    Eml,
    Msg,
    /// A raster image. No text layer, so it is readable only through a
    /// configured vision model.
    Image,
}

impl SourceKind {
    pub fn from_path(path: &Path) -> Option<Self> {
        Some(match extension(path)?.as_str() {
            "pdf" => Self::Pdf,
            "md" | "markdown" | "txt" | "text" => Self::Markdown,
            "html" | "htm" | "xhtml" => Self::Html,
            // Word's macro-enabled and template variants are the same OOXML
            // package with a different extension; so are Excel's.
            "docx" | "docm" | "dotx" | "dotm" => Self::Docx,
            "pptx" | "pptm" | "potx" | "potm" | "ppsx" | "ppsm" => Self::Pptx,
            "xlsx" | "xlsm" | "xltx" | "xltm" => Self::Xlsx,
            "vtt" => Self::WebVtt,
            "srt" => Self::Srt,
            "eml" => Self::Eml,
            "msg" => Self::Msg,
            "png" | "jpg" | "jpeg" | "gif" | "webp" => Self::Image,
            _ => return None,
        })
    }

    /// The kind of `path` holding `data`: its name, except that a name
    /// saying only "text" defers to the content. A transcript or a saved
    /// email arrives as `.txt` as often as not: `.vtt` has no registered
    /// type on most systems, and a mail client saves a message under
    /// whatever name it likes. A `.md` is not second-guessed: a memo that
    /// opens with `From:` and `To:` lines is still a memo.
    pub fn detect(path: &Path, data: &[u8]) -> Option<Self> {
        let kind = Self::from_path(path)?;
        if !matches!(extension(path)?.as_str(), "txt" | "text") {
            return Some(kind);
        }
        if let Some(format) = fluree_doc_transcript::Format::sniff(data) {
            return Some(match format {
                fluree_doc_transcript::Format::WebVtt => Self::WebVtt,
                fluree_doc_transcript::Format::Srt => Self::Srt,
            });
        }
        Some(match fluree_doc_email::Format::sniff(data) {
            Some(fluree_doc_email::Format::Eml) => Self::Eml,
            Some(fluree_doc_email::Format::Msg) => Self::Msg,
            None => kind,
        })
    }

    pub fn media_type(self) -> &'static str {
        match self {
            Self::Pdf => "application/pdf",
            Self::Markdown => "text/markdown",
            Self::Html => "text/html",
            Self::Docx => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            Self::Pptx => {
                "application/vnd.openxmlformats-officedocument.presentationml.presentation"
            }
            Self::Xlsx => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            Self::WebVtt => "text/vtt",
            Self::Srt => "application/x-subrip",
            Self::Eml => fluree_doc_email::Format::Eml.mime(),
            Self::Msg => fluree_doc_email::Format::Msg.mime(),
            Self::Image => "image/*",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Pdf => "pdf",
            Self::Markdown => "markdown",
            Self::Html => "html",
            Self::Docx => "docx",
            Self::Pptx => "pptx",
            Self::Xlsx => "xlsx",
            Self::WebVtt => "vtt",
            Self::Srt => "srt",
            Self::Eml => "eml",
            Self::Msg => "msg",
            Self::Image => "image",
        }
    }
}

fn extension(path: &Path) -> Option<String> {
    Some(path.extension()?.to_str()?.to_ascii_lowercase())
}

/// The file extensions [`SourceKind::from_path`] reads, for messages that
/// say what was looked for.
pub const SUPPORTED_EXTENSIONS: &str =
    "pdf, md, txt, html, docx, pptx, xlsx, vtt, srt, eml, msg, png, jpg, gif, webp";

#[derive(Debug, Clone)]
pub struct ParseOptions {
    /// What every minted node IRI starts with: `{base_iri}-element-{n}`,
    /// `{base_iri}-section-{n}` and so on. No delimiter is added, so a
    /// document's nodes share the namespace its IRI is in.
    pub base_iri: String,
    /// Stamped on every node but a table cell as `doc:sourceDocument` — the
    /// retract-on-rerun tag. A cell is reached through its stamped table.
    pub doc_iri: String,
    /// What the document is called, recorded as `doc:sourceName` on its
    /// root: the parser is handed bytes and cannot know it.
    pub source_name: Option<String>,
    /// Reader for pixel-only regions. `None` keeps the deterministic tier.
    pub vlm: Option<Arc<VlmReader>>,
}

impl ParseOptions {
    /// Everything besides the bytes that shapes the output, hashed for the
    /// parse cache.
    pub fn fingerprint(&self) -> String {
        let mut h = Sha256::new();
        h.update(DOC_PARSE_REV.as_bytes());
        h.update([0]);
        h.update(self.base_iri.as_bytes());
        h.update([0]);
        h.update(self.doc_iri.as_bytes());
        h.update([0]);
        h.update(self.source_name.as_deref().unwrap_or_default().as_bytes());
        h.update([0]);
        h.update(self.vlm.as_ref().map_or("none", |v| v.model()).as_bytes());
        h.update([0]);
        // A parse that skipped escalation over the cap must not be served
        // to a later run that raised it.
        h.update(self.vlm.as_ref().map_or(0, |v| v.max_crops()).to_le_bytes());
        hex::encode(h.finalize())[..16].to_string()
    }
}

#[derive(Debug, Clone)]
pub struct ParsedDocument {
    /// DoCO JSON-LD, insertable as-is.
    pub doco: String,
    /// The plain-text projection `nif:beginIndex`/`nif:endIndex` index into.
    pub text: String,
    pub pages: usize,
    pub elements: usize,
    /// Crops the vision model read for this parse (zero when none asked).
    pub escalated_crops: usize,
    /// Why escalation did not happen although the document asked for it:
    /// the deterministic tier stands in, and the caller should say so.
    pub escalation_skipped: Option<String>,
    /// What the output leaves out that the caller should say: pages nothing
    /// transcribed, an email's attachments, a transcript with no cues.
    pub notes: Vec<String>,
    pub from_cache: bool,
}

pub fn parse_bytes(
    data: Vec<u8>,
    kind: SourceKind,
    stem: &str,
    opts: &ParseOptions,
) -> Result<ParsedDocument> {
    let sha256 = fluree_doc_model::sha256_hex(&data);
    let failed = |e: &dyn std::fmt::Display| DocError::Parse(format!("{stem}: {e}"));
    // What a file declares about itself: its title, author and dates.
    let declared = |info: DocumentInfo| Notes {
        info,
        ..Default::default()
    };
    let (elements, notes, mut said) = match kind {
        SourceKind::Pdf => return parse_pdf(data, &sha256, stem, opts),
        SourceKind::Image => return parse_image(&data, &sha256, stem, opts),
        SourceKind::Markdown => {
            let text = String::from_utf8(data)
                .map_err(|e| DocError::Parse(format!("{stem}: not UTF-8: {e}")))?;
            (fluree_doc_markdown::parse(&text), Notes::default(), vec![])
        }
        SourceKind::Html => {
            let text = fluree_doc_html::decode(&data);
            let info = fluree_doc_html::info(&text);
            (fluree_doc_html::parse(&text), declared(info), vec![])
        }
        SourceKind::Docx => {
            let (els, info) = fluree_doc_docx::parse_with_info(&data).map_err(|e| failed(&e))?;
            (els, declared(info), vec![])
        }
        SourceKind::Pptx => {
            let (els, info) = fluree_doc_pptx::parse_with_info(&data).map_err(|e| failed(&e))?;
            (els, declared(info), vec![])
        }
        SourceKind::Xlsx => {
            let (els, info) = fluree_doc_xlsx::parse_with_info(&data).map_err(|e| failed(&e))?;
            (els, declared(info), vec![])
        }
        SourceKind::WebVtt | SourceKind::Srt => {
            let els = fluree_doc_transcript::parse(&data).map_err(|e| failed(&e))?;
            // Otherwise indistinguishable from a file that was not read.
            let said = if els.is_empty() {
                vec!["the transcript holds no cues, so the document is empty".to_string()]
            } else {
                vec![]
            };
            (els, Notes::default(), said)
        }
        SourceKind::Eml | SourceKind::Msg => {
            let email = fluree_doc_email::parse(&data).map_err(|e| failed(&e))?;
            let said = attachments_note(&email);
            let notes = email.notes();
            (email.elements, notes, said)
        }
    };
    let mut parsed = emit(&elements, Vec::new(), notes, &sha256, 0, 0, opts);
    parsed.notes.append(&mut said);
    Ok(parsed)
}

/// An attachment is a document of its own; the email's graph describes it
/// (name, type, size, hash) but does not read it.
fn attachments_note(email: &fluree_doc_email::Email) -> Vec<String> {
    if email.attachments.is_empty() {
        return Vec::new();
    }
    let names: Vec<String> = email
        .attachments
        .iter()
        .map(|a| {
            a.info
                .filename
                .clone()
                .unwrap_or_else(|| a.info.content_type.clone())
        })
        .collect();
    let count = names.len();
    vec![format!(
        "{count} attachment{} described, not read ({}): save and ingest {} as a document of its own",
        if count == 1 { "" } else { "s" },
        names.join(", "),
        if count == 1 { "it" } else { "each" },
    )]
}

fn emit(
    elements: &[Element],
    pages: Vec<PageSize>,
    notes: Notes,
    sha256: &str,
    page_count: usize,
    escalated_crops: usize,
    opts: &ParseOptions,
) -> ParsedDocument {
    let summary = notes.summary();
    let doco_opts = DocoOptions {
        base_iri: opts.base_iri.clone(),
        doc_iri: Some(opts.doc_iri.clone()),
        pages,
        unread: notes.unread,
        running_text: notes.running_text,
        info: notes.info,
        attachments: notes.attachments,
        sha256: Some(sha256.to_string()),
        source_name: opts.source_name.clone(),
    };
    ParsedDocument {
        doco: to_doco(elements, &doco_opts),
        text: to_text(elements),
        pages: page_count,
        elements: elements.len(),
        escalated_crops,
        escalation_skipped: None,
        notes: summary.into_iter().collect(),
        from_cache: false,
    }
}

fn parse_pdf(
    data: Vec<u8>,
    sha256: &str,
    stem: &str,
    opts: &ParseOptions,
) -> Result<ParsedDocument> {
    let raw = hayro_syntax::Pdf::new(Arc::new(data.clone()))
        .map_err(|e| DocError::Parse(format!("{stem}: {e:?}")))?;
    let outline = fluree_doc_pdf::outline::extract(&raw);
    let mut doc =
        fluree_doc_pdf::extract_bytes(data).map_err(|e| DocError::Parse(format!("{stem}: {e}")))?;
    let analyze_opts = AnalyzeOptions {
        // The splice replaces anchors, so they must be there to replace.
        emit_anchors: opts.vlm.is_some(),
        ..AnalyzeOptions::default()
    };
    // Crops are chosen from the document as extracted: analysis changes the
    // pages it reads, and the engine plans from what it was given.
    let extracted = opts.vlm.is_some().then(|| doc.clone());
    let mut analysis = fluree_doc_pdf::document::analyze_with(&mut doc, &outline, &analyze_opts);

    let mut escalated_crops = 0;
    let mut escalation_skipped = None;
    if let (Some(vlm), Some(extracted)) = (&opts.vlm, &extracted) {
        let jobs = fluree_doc_pdf::escalate::plan(
            extracted,
            &outline,
            &analyze_opts,
            &CropHints::default(),
        );
        // Over the cap, the deterministic tier stands: nothing has been
        // spent, the document still lands, and the caller is told so it
        // can raise `--max-crops` on purpose.
        let readings = match vlm.read_pdf(&raw, &doc, jobs) {
            Ok(readings) => readings,
            Err(e @ DocError::CropCap { .. }) => {
                escalation_skipped = Some(e.to_string());
                Readings::from_map(HashMap::new())
            }
            Err(e) => return Err(e),
        };
        escalated_crops = readings.len();
        // Spliced even with no readings: an anchor nothing read is dropped
        // there, and would otherwise reach the text as a `[[VLM…]]` token.
        //
        // The page's own text, so the arbiter can ask whether a reading
        // says anything the page does not.
        let page_text: Vec<Vec<String>> = doc
            .pages
            .iter()
            .map(|p| fluree_doc_pdf::fidelity::page_lines(&p.glyphs))
            .collect();
        fluree_doc_pdf::arbiter::splice_with_page(
            &mut analysis.elements,
            stem,
            &readings,
            None,
            &page_text,
        );
        // The same furniture the deterministic pass already stripped, so
        // a page reads the same whether or not it escalated.
        fluree_doc_pdf::arbiter::scrub_furniture(&mut analysis.elements, &analysis.furniture);
    }

    // After the tiers: an escalated reading replaces the text an anchor has
    // to be found in.
    fluree_doc_pdf::link::attach(
        &mut analysis.elements,
        &fluree_doc_pdf::link::extract(&raw),
        &doc.pages,
    );

    // As displayed: a bbox is placed on the page as rendered, rotation
    // applied.
    let sizes: Vec<PageSize> = doc
        .pages
        .iter()
        .map(|p| {
            let (width, height) = p.display_size();
            PageSize {
                index: p.index,
                width,
                height,
                folio: analysis.folios.get(p.index).cloned().flatten(),
            }
        })
        .collect();
    let notes = Notes {
        unread: fluree_doc_pdf::unread_pages(&doc, &analysis.elements),
        // A bare page number identifies nothing; the rest of the running
        // block is the document's own name for itself.
        running_text: analysis
            .furniture
            .iter()
            .filter(|(text, _)| text.chars().any(char::is_alphabetic))
            .map(|(text, _)| text.clone())
            .collect(),
        info: fluree_doc_pdf::info::read(&raw),
        ..Default::default()
    };
    let page_count = doc.pages.len();
    let mut parsed = emit(
        &analysis.elements,
        sizes,
        notes,
        sha256,
        page_count,
        escalated_crops,
        opts,
    );
    parsed.escalation_skipped = escalation_skipped;
    Ok(parsed)
}

/// A bare image is one page of pixels with no deterministic reading to fall
/// back on: the vision model's Markdown transcription is parsed as the
/// document.
fn parse_image(
    data: &[u8],
    sha256: &str,
    stem: &str,
    opts: &ParseOptions,
) -> Result<ParsedDocument> {
    let Some(vlm) = &opts.vlm else {
        return Err(DocError::Unsupported(format!(
            "{stem}: an image has no text layer; configure `[doc.vlm]` (or `[doc.llm]`) to read it"
        )));
    };
    // The engine knows the formats it renders (PNG, JPEG, TIFF); GIF and
    // WebP are read only here, so the vision model is told what they are.
    let mime = fluree_doc_pdf::image::Format::sniff(data)
        .map(fluree_doc_pdf::image::Format::mime)
        .or_else(|| match image::guess_format(data).ok()? {
            image::ImageFormat::Gif => Some("image/gif"),
            image::ImageFormat::WebP => Some("image/webp"),
            _ => None,
        })
        .ok_or_else(|| DocError::Unsupported(format!("{stem}: not a recognised image")))?;
    let reading = vlm.read_image(data, mime)?.unwrap_or_default();
    let elements = fluree_doc_markdown::parse(&reading);
    Ok(emit(
        &elements,
        Vec::new(),
        Notes::default(),
        sha256,
        1,
        1,
        opts,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ModelEndpoint;

    /// A table PDF from the engine's own fixtures: routed to the vision
    /// model for its table, so a reader is asked for at least one crop.
    const TABLE_PDF: &[u8] = include_bytes!("../testdata/probe-booktabs.pdf");

    /// Over the crop cap nothing is read, and the anchors the analysis
    /// emitted for the splice must still go: they are placeholders, not
    /// text. They used to reach the projection as `[[VLM…]]` tokens.
    #[test]
    fn over_the_crop_cap_no_anchor_reaches_the_text() {
        // Nothing is called: a cap of zero refuses before the first crop.
        let vlm = VlmReader::new(
            ModelEndpoint {
                url: "http://127.0.0.1:9/v1".into(),
                model: "unreachable".into(),
                ..Default::default()
            },
            None,
            0,
        )
        .unwrap();
        let parsed = parse_bytes(
            TABLE_PDF.to_vec(),
            SourceKind::Pdf,
            "table",
            &ParseOptions {
                base_iri: "urn:t".into(),
                doc_iri: "urn:t".into(),
                source_name: None,
                vlm: Some(Arc::new(vlm)),
            },
        )
        .unwrap();
        assert!(
            parsed.escalation_skipped.is_some(),
            "the fixture no longer asks for a crop, so it proves nothing"
        );
        assert_eq!(parsed.escalated_crops, 0);
        assert!(!parsed.text.contains("[[VLM"), "{}", parsed.text);
        assert!(!parsed.doco.contains("[[VLM"));
        assert!(
            !parsed.text.trim().is_empty(),
            "the deterministic reading stands"
        );
    }

    #[test]
    fn a_transcript_or_an_email_saved_as_text_is_read_as_one() {
        let detect = |name: &str, data: &[u8]| SourceKind::detect(Path::new(name), data);
        let vtt = b"WEBVTT\n\n00:00:01.000 --> 00:00:04.000\n<v Ana>Shall we start?\n";
        assert_eq!(detect("call.txt", vtt), Some(SourceKind::WebVtt));
        let eml = b"From: Ana <ana@example.org>\r\nTo: Bo <bo@example.org>\r\n\
                    Subject: Budget\r\nDate: Tue, 7 Oct 2026 10:00:00 +0000\r\n\r\nSee attached.\r\n";
        assert_eq!(detect("budget.TXT", eml), Some(SourceKind::Eml));
        assert_eq!(
            detect("notes.txt", b"# Notes\n\nNothing to see."),
            Some(SourceKind::Markdown)
        );
        // Every other name is trusted, Markdown included: this memo would
        // sniff as an email, and read as one it loses its heading and table.
        let memo = b"From: Ana <ana@example.org>\nTo: Bo <bo@example.org>\nSubject: Q3 plan\n\n\
                     # Q3 plan\n\n| Item | Owner |\n|---|---|\n| Budget | Ana |\n";
        assert_eq!(detect("memo.txt", memo), Some(SourceKind::Eml));
        assert_eq!(detect("memo.md", memo), Some(SourceKind::Markdown));
        assert_eq!(detect("call.pdf", vtt), Some(SourceKind::Pdf));
    }

    #[test]
    fn declared_formats_carry_what_the_file_says_about_itself() {
        let html = b"<html><head><title>Q3 Review</title>\
                     <meta name=\"author\" content=\"Ana\"></head>\
                     <body><p>Revenue rose.</p></body></html>";
        let parsed = parse_bytes(
            html.to_vec(),
            SourceKind::Html,
            "review",
            &ParseOptions {
                base_iri: "urn:fluree:doc:review.html".into(),
                doc_iri: "urn:fluree:doc:review.html".into(),
                source_name: Some("review.html".into()),
                vlm: None,
            },
        )
        .unwrap();
        let graph: serde_json::Value = serde_json::from_str(&parsed.doco).unwrap();
        let root = graph["@graph"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["@type"] == "foaf:Document")
            .expect("a foaf:Document root");
        assert_eq!(root["@id"], "urn:fluree:doc:review.html-element-0");
        assert_eq!(root["dcterms:title"], "Q3 Review");
        assert_eq!(root["dcterms:creator"][0], "Ana");
        assert_eq!(root["doc:sourceName"], "review.html");
        assert_eq!(root["doc:sha256"], fluree_doc_model::sha256_hex(html));
    }

    #[test]
    fn an_email_names_the_attachments_it_does_not_read() {
        let eml = "From: Ana <ana@example.org>\r\n\
                   To: Bo <bo@example.org>\r\n\
                   Subject: Budget\r\n\
                   MIME-Version: 1.0\r\n\
                   Content-Type: multipart/mixed; boundary=\"b\"\r\n\r\n\
                   --b\r\nContent-Type: text/plain\r\n\r\nNumbers attached.\r\n\
                   --b\r\nContent-Type: text/csv\r\n\
                   Content-Disposition: attachment; filename=\"q3.csv\"\r\n\r\na,b\r\n1,2\r\n\
                   --b--\r\n";
        let parsed = parse_bytes(
            eml.as_bytes().to_vec(),
            SourceKind::Eml,
            "budget",
            &ParseOptions {
                base_iri: "urn:t".into(),
                doc_iri: "urn:t".into(),
                source_name: None,
                vlm: None,
            },
        )
        .unwrap();
        assert!(parsed.text.contains("Numbers attached."), "{}", parsed.text);
        assert!(parsed.doco.contains("doc:Message"));
        assert_eq!(parsed.notes.len(), 1, "{:?}", parsed.notes);
        assert!(parsed.notes[0].contains("q3.csv"), "{:?}", parsed.notes);
    }
}
