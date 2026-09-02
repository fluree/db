//! On-disk caches so a re-run only pays for what changed.
//!
//! Two caches, keyed differently because they are invalidated by different
//! things:
//!
//! - **Parse cache** — keyed on the document's content hash plus a
//!   fingerprint of everything that shapes the output (engine revision,
//!   IRIs, whether crops were read and by which model). Holds the emitted
//!   DoCO graph and the text projection.
//! - **Reading cache** — keyed on the crop's pixels, the prompt and the
//!   model, not on the document. An engine upgrade re-routes pages, but a
//!   crop whose pixels did not change is answered from here without a
//!   model call. This is where the money is.
//!
//! Layout under the root:
//!
//! ```text
//! parse/{sha256}-{fingerprint}/doco.jsonld
//! parse/{sha256}-{fingerprint}/text.txt
//! parse/{sha256}-{fingerprint}/meta.json
//! readings/{key}.txt            (empty file = the model read nothing)
//! ```

use crate::parse::ParsedDocument;
use crate::Result;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct DocCache {
    root: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
struct ParseMeta {
    pages: usize,
    elements: usize,
    escalated_crops: usize,
    #[serde(default)]
    escalation_skipped: Option<String>,
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

impl DocCache {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn parse_dir(&self, sha256: &str, fingerprint: &str) -> PathBuf {
        self.root
            .join("parse")
            .join(format!("{sha256}-{fingerprint}"))
    }

    pub fn load_parse(&self, sha256: &str, fingerprint: &str) -> Option<ParsedDocument> {
        let dir = self.parse_dir(sha256, fingerprint);
        let doco = fs::read_to_string(dir.join("doco.jsonld")).ok()?;
        let text = fs::read_to_string(dir.join("text.txt")).ok()?;
        let meta: ParseMeta =
            serde_json::from_slice(&fs::read(dir.join("meta.json")).ok()?).ok()?;
        Some(ParsedDocument {
            doco,
            text,
            pages: meta.pages,
            elements: meta.elements,
            escalated_crops: meta.escalated_crops,
            escalation_skipped: meta.escalation_skipped,
            from_cache: true,
        })
    }

    pub fn store_parse(&self, sha256: &str, fingerprint: &str, doc: &ParsedDocument) -> Result<()> {
        let dir = self.parse_dir(sha256, fingerprint);
        fs::create_dir_all(&dir)?;
        let meta = ParseMeta {
            pages: doc.pages,
            elements: doc.elements,
            escalated_crops: doc.escalated_crops,
            escalation_skipped: doc.escalation_skipped.clone(),
        };
        // Write into place last so a torn write never reads as a hit.
        fs::write(dir.join("text.txt"), &doc.text)?;
        fs::write(
            dir.join("meta.json"),
            serde_json::to_vec(&meta).expect("meta serializes"),
        )?;
        let tmp = dir.join("doco.jsonld.tmp");
        fs::write(&tmp, &doc.doco)?;
        fs::rename(tmp, dir.join("doco.jsonld"))?;
        Ok(())
    }

    /// Key for one crop reading: the pixels, what was asked, and who was asked.
    pub fn reading_key(model: &str, prompt: &str, png: &[u8]) -> String {
        let mut h = Sha256::new();
        h.update(model.as_bytes());
        h.update([0]);
        h.update(prompt.as_bytes());
        h.update([0]);
        h.update(png);
        hex::encode(h.finalize())
    }

    fn reading_path(&self, key: &str) -> PathBuf {
        self.root.join("readings").join(format!("{key}.txt"))
    }

    /// `Some(None)` is a cached "nothing printed here"; `None` is a miss.
    pub fn load_reading(&self, key: &str) -> Option<Option<String>> {
        let text = fs::read_to_string(self.reading_path(key)).ok()?;
        Some((!text.is_empty()).then_some(text))
    }

    pub fn store_reading(&self, key: &str, reading: Option<&str>) -> Result<()> {
        let path = self.reading_path(key);
        fs::create_dir_all(path.parent().expect("readings dir"))?;
        fs::write(path, reading.unwrap_or_default())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_round_trip() {
        let dir = tempdir();
        let cache = DocCache::new(&dir);
        let doc = ParsedDocument {
            doco: "{}".into(),
            text: "hello".into(),
            pages: 2,
            elements: 3,
            escalated_crops: 1,
            escalation_skipped: None,
            from_cache: false,
        };
        assert!(cache.load_parse("abc", "fp").is_none());
        cache.store_parse("abc", "fp", &doc).unwrap();
        let hit = cache.load_parse("abc", "fp").unwrap();
        assert!(hit.from_cache);
        assert_eq!(hit.text, "hello");
        assert_eq!(hit.pages, 2);
        assert!(cache.load_parse("abc", "other").is_none());
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn empty_reading_is_a_hit() {
        let dir = tempdir();
        let cache = DocCache::new(&dir);
        let key = DocCache::reading_key("m", "p", b"png");
        assert!(cache.load_reading(&key).is_none());
        cache.store_reading(&key, None).unwrap();
        assert_eq!(cache.load_reading(&key), Some(None));
        cache.store_reading(&key, Some("text")).unwrap();
        assert_eq!(cache.load_reading(&key), Some(Some("text".into())));
        fs::remove_dir_all(dir).ok();
    }

    /// Tests run in parallel inside one process, so a name has to come from
    /// a counter: pid + wall clock collided on macOS's clock granularity.
    fn tempdir() -> PathBuf {
        static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let d = std::env::temp_dir().join(format!(
            "fluree-db-doc-cache-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        fs::create_dir_all(&d).unwrap();
        d
    }
}
