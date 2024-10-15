use super::Error;
use memmap::{Mmap, MmapOptions};
use std::fs::{File, OpenOptions};
use std::ops::Deref;
use std::path::Path;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;

/// An input channel that leverages mmap'd files to communicate dictionary entries.
pub struct DictionaryInputChannel {
    // We own the file to keep its lifetime
    f: File,
    data: Mmap,
    name: String,
}
impl DictionaryInputChannel {
    /// Construct a new Dictionary file input using the given path.
    pub fn new(path: &Path) -> Result<DictionaryInputChannel, Error> {
        let f = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(path)?;
        let data = unsafe {
            MmapOptions::new()
                .map(&f)
                .expect("Could not access data from memory mapped file")
        };
        Ok(DictionaryInputChannel {
            f,
            data,
            name: path.display().to_string(),
        })
    }

    // TODO _ Add errors.
    pub fn entry<'a>(&'a self, idx: i64) -> Result<DictionaryEntry<'a>, Error> {
        if idx >= 0 && idx < self.state().num_entries.load(Ordering::Acquire) {
            Ok(DictionaryEntry {
                data: &self.data,
                header: self.state(),
                read_idx: idx as i64,
            })
        } else {
            Err(super::error::OltpMmapError::NotFoundInDictoinary(
                self.name.to_owned(),
                idx,
            ))
        }
    }

    /// Returns the version header of this file.
    pub fn version(&self) -> i64 {
        self.state().version
    }

    fn state(&self) -> &DictionaryHeader {
        unsafe { &*(self.data.as_ref().as_ptr() as *const DictionaryHeader) }
    }
}

pub struct DictionaryEntry<'a> {
    data: &'a Mmap,
    header: &'a DictionaryHeader,
    read_idx: i64,
}
impl<'a> Deref for DictionaryEntry<'a> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        let start_byte_index = (64 + (self.header.entry_size * self.read_idx)) as usize;
        let end_byte_index = (64 + (self.header.entry_size * (self.read_idx + 1))) as usize;
        &self.data[start_byte_index..end_byte_index]
    }
}

/// This first 64 bytes of any ringbuffer in OTLP-MMAP has this format.
/// We use this struct to "reinterpret_cast" and use memory safe primitives for access.
#[repr(C)]
pub(crate) struct DictionaryHeader {
    /// Current data instance version
    version: i64,
    num_entries: AtomicI64,
    entry_size: i64,
    // TODO - just use a fixed size array to ignore bytes.
    ignore: i64,
    ignore_2: i64,
    ignore_3: i64,
    ignore_4: i64,
    ignore_5: i64,
}
