//! variable sized dictionary backed by mmap file

use std::sync::atomic::AtomicI64;

use memmap::MmapMut;
use tokio::sync::Mutex;

use crate::oltp_mmap::Error;

/// A thread-safe version of the mmap dictionary
pub struct Dictionary {
    input: Mutex<RawDictionary>,
}

impl Dictionary {
    pub(crate) fn new(data: MmapMut, offset: usize) -> Dictionary {
        Dictionary {
            input: Mutex::new(RawDictionary::new(data, offset)),
        }
    }

    /// Attempts to read a dictionary entry with a given type.
    pub async fn try_read<T: prost::Message + std::default::Default>(
        &self,
        index: i64,
    ) -> Result<T, Error> {
        self.input.lock().await.try_read(index)
    }
}

/// A mmap variable-sized dictionary implementation.
///
/// Note: This is currently designed to only allow ONE consumer
///       but multiple prodcuers.
struct RawDictionary {
    /// The mmap data
    data: MmapMut,
    /// The offset into the mmap data where the dictionary starts.
    offset: usize,
}

impl RawDictionary {
    /// Constructs a new dictionary.
    pub(crate) fn new(data: MmapMut, offset: usize) -> RawDictionary {
        RawDictionary { data, offset }
    }

    /// Attempts to read a message out of the dictionary.
    pub(crate) fn try_read<T: prost::Message + std::default::Default>(
        &self,
        index: i64,
    ) -> Result<T, Error> {
        // TODO - should we first read the length, then limit the buffer?
        let offset = self.offset + 64 + (index as usize);
        if let Some(slice) = self.data.get(offset..) {
            Ok(T::decode_length_delimited(slice)?)
        } else {
            Err(Error::NotFoundInDictionary("<todo>".to_owned(), index))
        }
    }

    fn header(&self) -> &RawDictionaryHeader {
        unsafe { &*(self.data.as_ref().as_ptr().add(self.offset) as *const RawDictionaryHeader) }
    }
}

/// This first 64 bytes of the dictionary in OTLP-MMAP has this format.
/// We use this struct to "reinterpret_cast" and use memory safe primitives for access.
#[repr(C)]
pub(crate) struct RawDictionaryHeader {
    /// Last written location of the dictionary.
    end: AtomicI64,
    /// Number of entries that have been written to the dictionary.
    num_entries: AtomicI64,
}
