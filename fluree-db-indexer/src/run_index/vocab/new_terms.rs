//! The terms a seeded vocabulary merge adds to an existing dictionary, on disk.
//!
//! A seeded merge numbers the terms the dictionary lacks in merge order, so a
//! single stream of them is sorted both by id and by reverse-tree key: the
//! order the forward packs are extended in and the order the reverse tree is
//! merged in. Writing them out rather than collecting them keeps a bulk
//! import's memory independent of how many new terms it brings.
//!
//! Format: `NTM1`, count (u64), total value bytes (u64), then `count` records
//! of id (u64) + value length (u32) + value, all little-endian. Subject ids are
//! sid64s and their values namespace-stripped suffixes; string ids are string
//! ids and their values the strings.

use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAGIC: [u8; 4] = *b"NTM1";
const HEADER_LEN: usize = 20;

/// A file of new terms: where it is and what it holds.
#[derive(Debug, Clone)]
pub struct NewTerms {
    pub path: PathBuf,
    pub count: u64,
    /// Sum of the values' lengths.
    pub value_bytes: u64,
}

impl NewTerms {
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// A reader over the terms, from the first.
    pub fn reader(&self) -> io::Result<NewTermReader> {
        NewTermReader::open(&self.path)
    }
}

/// Appends terms in ascending id order.
pub struct NewTermWriter {
    path: PathBuf,
    file: BufWriter<std::fs::File>,
    count: u64,
    value_bytes: u64,
    last_id: Option<u64>,
}

impl NewTermWriter {
    pub fn create(path: &Path) -> io::Result<Self> {
        let mut file = BufWriter::new(std::fs::File::create(path)?);
        file.write_all(&[0u8; HEADER_LEN])?;
        Ok(Self {
            path: path.to_path_buf(),
            file,
            count: 0,
            value_bytes: 0,
            last_id: None,
        })
    }

    pub fn push(&mut self, id: u64, value: &[u8]) -> io::Result<()> {
        if self.last_id.is_some_and(|last| id <= last) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("new terms out of order: id {id} after {:?}", self.last_id),
            ));
        }
        let len: u32 = value.len().try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "new term longer than 4 GiB")
        })?;
        self.file.write_all(&id.to_le_bytes())?;
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(value)?;
        self.count += 1;
        self.value_bytes += u64::from(len);
        self.last_id = Some(id);
        Ok(())
    }

    pub fn finish(self) -> io::Result<NewTerms> {
        let mut file = self.file.into_inner().map_err(io::Error::from)?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&MAGIC)?;
        file.write_all(&self.count.to_le_bytes())?;
        file.write_all(&self.value_bytes.to_le_bytes())?;
        file.flush()?;
        Ok(NewTerms {
            path: self.path,
            count: self.count,
            value_bytes: self.value_bytes,
        })
    }
}

/// Reads terms back in the order they were written, one at a time.
pub struct NewTermReader {
    file: BufReader<std::fs::File>,
    remaining: u64,
    value: Vec<u8>,
}

impl NewTermReader {
    pub fn open(path: &Path) -> io::Result<Self> {
        let mut file = BufReader::new(std::fs::File::open(path)?);
        let mut header = [0u8; HEADER_LEN];
        file.read_exact(&mut header)?;
        if header[..4] != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} is not a new-terms file", path.display()),
            ));
        }
        let remaining = u64::from_le_bytes(header[4..12].try_into().expect("8 bytes"));
        Ok(Self {
            file,
            remaining,
            value: Vec::new(),
        })
    }

    /// The next term's id and value, or `None` past the last.
    pub fn next_term(&mut self) -> io::Result<Option<(u64, &[u8])>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let mut fixed = [0u8; 12];
        self.file.read_exact(&mut fixed)?;
        let id = u64::from_le_bytes(fixed[..8].try_into().expect("8 bytes"));
        let len = u32::from_le_bytes(fixed[8..].try_into().expect("4 bytes")) as usize;
        self.value.resize(len, 0);
        self.file.read_exact(&mut self.value)?;
        self.remaining -= 1;
        Ok(Some((id, &self.value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terms_read_back_as_written() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.new");
        let mut writer = NewTermWriter::create(&path).unwrap();
        writer.push(3, b"abc").unwrap();
        writer.push(4, b"").unwrap();
        writer.push(9, b"zz").unwrap();
        let terms = writer.finish().unwrap();
        assert_eq!((terms.count, terms.value_bytes), (3, 5));

        let mut reader = terms.reader().unwrap();
        let mut read = Vec::new();
        while let Some((id, value)) = reader.next_term().unwrap() {
            read.push((id, value.to_vec()));
        }
        assert_eq!(
            read,
            vec![(3, b"abc".to_vec()), (4, Vec::new()), (9, b"zz".to_vec())]
        );
    }

    #[test]
    fn ids_must_ascend() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = NewTermWriter::create(&dir.path().join("t.new")).unwrap();
        writer.push(5, b"a").unwrap();
        assert!(writer.push(5, b"b").is_err());
    }
}
