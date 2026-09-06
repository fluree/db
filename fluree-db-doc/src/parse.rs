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
use fluree_doc_model::{to_doco, to_text, DocoOptions, Notes, PageSize};
use fluree_doc_pdf::document::AnalyzeOptions;
use fluree_doc_pdf::escalate::Readings;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// The pinned `fluree-doc-parse` revision (see the workspace `Cargo.toml`).
/// Part of every parse-cache fingerprint: bump it with the pin so cached
/// output from an older engine is never served as current.
pub const DOC_PARSE_REV: &str = "407daa034aeca0de1a70a53eac7899d94c52bb5d";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    Pdf,
    Markdown,
    Html,
    Docx,
    Pptx,
    /// A raster image. No text layer, so it is readable only through a
    /// configured vision model.
    Image,
}

impl SourceKind {
    pub fn from_path(path: &Path) -> Option<Self> {
        let ext = path.extension()?.to_str()?.to_ascii_lowercase();
        Some(match ext.as_str() {
            "pdf" => Self::Pdf,
            "md" | "markdown" | "txt" | "text" => Self::Markdown,
            "html" | "htm" | "xhtml" => Self::Html,
            // Word's macro-enabled and template variants are the same OOXML
            // package with a different extension.
            "docx" | "docm" | "dotx" | "dotm" => Self::Docx,
            "pptx" | "pptm" | "potx" | "potm" | "ppsx" | "ppsm" => Self::Pptx,
            "png" | "jpg" | "jpeg" | "gif" | "webp" => Self::Image,
            _ => return None,
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
            Self::Image => "image",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ParseOptions {
    /// Prefix for minted element IRIs: `{base_iri}/element/{n}`.
    pub base_iri: String,
    /// Stamped on every element as `doc:sourceDocument` — the retract-on-rerun tag.
    pub doc_iri: String,
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
    pub from_cache: bool,
}

pub fn parse_bytes(
    data: Vec<u8>,
    kind: SourceKind,
    stem: &str,
    opts: &ParseOptions,
) -> Result<ParsedDocument> {
    match kind {
        SourceKind::Pdf => parse_pdf(data, stem, opts),
        SourceKind::Markdown => {
            let text = String::from_utf8(data)
                .map_err(|e| DocError::Parse(format!("{stem}: not UTF-8: {e}")))?;
            Ok(declared(fluree_doc_markdown::parse(&text), opts))
        }
        SourceKind::Html => {
            let text = String::from_utf8_lossy(&data).into_owned();
            Ok(declared(fluree_doc_html::parse(&text), opts))
        }
        SourceKind::Docx => {
            let els = fluree_doc_docx::parse(&data)
                .map_err(|e| DocError::Parse(format!("{stem}: {e}")))?;
            Ok(declared(els, opts))
        }
        SourceKind::Pptx => {
            let els = fluree_doc_pptx::parse(&data)
                .map_err(|e| DocError::Parse(format!("{stem}: {e}")))?;
            Ok(declared(els, opts))
        }
        SourceKind::Image => parse_image(&data, stem, opts),
    }
}

/// Sources that declare their structure: map, don't measure. No geometry,
/// no pages, nothing to escalate.
fn declared(elements: Vec<fluree_doc_model::Element>, opts: &ParseOptions) -> ParsedDocument {
    emit(&elements, Vec::new(), Notes::default(), 0, 0, opts)
}

fn emit(
    elements: &[fluree_doc_model::Element],
    pages: Vec<PageSize>,
    notes: Notes,
    page_count: usize,
    escalated_crops: usize,
    opts: &ParseOptions,
) -> ParsedDocument {
    let doco_opts = DocoOptions {
        base_iri: opts.base_iri.clone(),
        doc_iri: Some(opts.doc_iri.clone()),
        pages,
        unread: notes.unread,
        running_text: notes.running_text,
    };
    ParsedDocument {
        doco: to_doco(elements, &doco_opts),
        text: to_text(elements),
        pages: page_count,
        elements: elements.len(),
        escalated_crops,
        escalation_skipped: None,
        from_cache: false,
    }
}

fn parse_pdf(data: Vec<u8>, stem: &str, opts: &ParseOptions) -> Result<ParsedDocument> {
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
    let mut analysis = fluree_doc_pdf::document::analyze_with(&mut doc, &outline, &analyze_opts);

    let mut escalated_crops = 0;
    let mut escalation_skipped = None;
    if let Some(vlm) = &opts.vlm {
        // Over the cap, the deterministic tier stands: nothing has been
        // spent, the document still lands, and the caller is told so it
        // can raise `--max-crops` on purpose.
        let readings = match vlm.read_pdf(&raw, &doc, &analysis) {
            Ok(readings) => readings,
            Err(e @ DocError::CropCap { .. }) => {
                escalation_skipped = Some(e.to_string());
                Readings::from_map(HashMap::new())
            }
            Err(e) => return Err(e),
        };
        escalated_crops = readings.len();
        if !readings.is_empty() {
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
    }

    // After the tiers: an escalated reading replaces the text an anchor has
    // to be found in.
    fluree_doc_pdf::link::attach(
        &mut analysis.elements,
        &fluree_doc_pdf::link::extract(&raw),
        &doc.pages,
    );

    let sizes: Vec<PageSize> = doc
        .pages
        .iter()
        .map(|p| PageSize {
            index: p.index,
            width: p.width,
            height: p.height,
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
    };
    let page_count = doc.pages.len();
    let mut parsed = emit(
        &analysis.elements,
        sizes,
        notes,
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
fn parse_image(data: &[u8], stem: &str, opts: &ParseOptions) -> Result<ParsedDocument> {
    let Some(vlm) = &opts.vlm else {
        return Err(DocError::Unsupported(format!(
            "{stem}: an image has no text layer; configure `[doc.vlm]` (or `[doc.llm]`) to read it"
        )));
    };
    let mime = fluree_doc_pdf::image::Format::sniff(data)
        .map(fluree_doc_pdf::image::Format::mime)
        .ok_or_else(|| DocError::Unsupported(format!("{stem}: not a recognised image")))?;
    let reading = vlm.read_image(data, mime)?.unwrap_or_default();
    let elements = fluree_doc_markdown::parse(&reading);
    Ok(emit(&elements, Vec::new(), Notes::default(), 1, 1, opts))
}
