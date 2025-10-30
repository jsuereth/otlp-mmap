use super::Error;
use memmap::{Mmap, MmapOptions};
use moka::future::{Cache, CacheBuilder};
use std::fs::{File, OpenOptions};
use std::ops::Deref;
use std::path::Path;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Async, cached access to OTLP mmap Dictionary Files.
pub struct DictionaryReader<T> {
    input: Mutex<RawDictionaryReader>,
    cache: Cache<i64, Arc<T>>,
}
impl<T> DictionaryReader<T>
where
    T: prost::Message + std::default::Default + 'static,
{
    /// Open a dictionary file and construct an LRU cache against its values.
    pub fn new(p: &Path, max_capacity: u64) -> Result<DictionaryReader<T>, Error> {
        Ok(DictionaryReader {
            input: Mutex::new(RawDictionaryReader::new(p)?),
            cache: CacheBuilder::new(max_capacity).build(),
        })
    }
    /// Reads a value from the dictionary. Returns an error on I/O issues.
    pub async fn get(&self, idx: i64) -> Result<Arc<T>, Arc<Error>> {
        self.cache
            .try_get_with(idx, async move {
                let input = self.input.lock().await;
                let buf = input.entry(idx)?;
                T::decode_length_delimited(buf.deref())
                    .map_err(Error::ProtobufDecodeError)
                    .map(Arc::new)
            })
            .await
    }
    /// Reads the timestamp associated with this dictionary.
    pub async fn version(&self) -> i64 {
        self.input.lock().await.version()
    }
}

/// An input channel that leverages mmap'd files to communicate dictionary entries.
///
/// All access is synchronous.
struct RawDictionaryReader {
    // We own the file to keep its lifetime
    f: File,
    data: Mmap,
    name: String,
}
impl RawDictionaryReader {
    /// Construct a new Dictionary file input using the given path.
    pub fn new(path: &Path) -> Result<RawDictionaryReader, Error> {
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
        Ok(RawDictionaryReader {
            f,
            data,
            name: path.display().to_string(),
        })
    }

    // TODO _ Add errors.
    fn entry<'a>(&'a self, idx: i64) -> Result<RawDictionaryEntry<'a>, Error> {
        if idx >= 0 && idx < self.state().num_entries.load(Ordering::Acquire) {
            Ok(RawDictionaryEntry {
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
    fn version(&self) -> i64 {
        self.state().version
    }

    fn state(&self) -> &RawDictionaryHeader {
        unsafe { &*(self.data.as_ref().as_ptr() as *const RawDictionaryHeader) }
    }
}

struct RawDictionaryEntry<'a> {
    data: &'a Mmap,
    header: &'a RawDictionaryHeader,
    read_idx: i64,
}
impl<'a> Deref for RawDictionaryEntry<'a> {
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
struct RawDictionaryHeader {
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
