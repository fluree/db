use super::JournalIo;
use fs2::FileExt;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt as UnixFileExt;
use std::path::Path;

/// Unix file adapter. This locks the journal inode, NOT the database root.
/// The parent must already exist durably. Never rename/unlink an open journal;
/// root ownership and protection from legacy writers belong to integration.
pub struct FileIo(File);

impl FileIo {
    pub fn create_new(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        FileExt::try_lock_exclusive(&file)?;
        file.sync_all()?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        File::open(parent)?.sync_all()?;
        Ok(Self(file))
    }

    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        FileExt::try_lock_exclusive(&file)?;
        Ok(Self(file))
    }
}

impl JournalIo for FileIo {
    fn len(&mut self) -> io::Result<u64> {
        Ok(self.0.metadata()?.len())
    }
    fn read_at(&mut self, offset: u64, out: &mut [u8]) -> io::Result<usize> {
        UnixFileExt::read_at(&self.0, out, offset)
    }
    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<usize> {
        UnixFileExt::write_at(&self.0, bytes, offset)
    }
    fn sync_all(&mut self) -> io::Result<()> {
        self.0.sync_all()
    }
}
