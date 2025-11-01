//! variable sized dictionary backed by mmap file

use std::{fs::File, sync::atomic::AtomicI64};

use memmap2::{MmapMut, MmapOptions};
use tokio::sync::Mutex;

use crate::oltp_mmap::Error;

/// A thread-safe version of the mmap dictionary
pub struct Dictionary {
    input: Mutex<RawDictionary>,
}

impl Dictionary {
    pub(crate) fn try_new(f: File, offset: u64) -> Result<Dictionary, Error> {
        Ok(Dictionary {
            input: Mutex::new(RawDictionary::try_new(f, offset)?),
        })
    }

    /// Attempts to read a string from the dictionary.
    pub async fn try_read_string(&self, index: i64) -> Result<String, Error> {
        self.input.lock().await.try_read_string(index)
    }

    /// Attempts to read a proto dictionary entry with a given type.
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
    /// The file we're reading.
    f: File,
    /// The offset into the mmap data where the dictionary starts.
    offset: u64,
}

impl RawDictionary {
    /// Constructs a new dictionary.
    pub(crate) fn try_new(f: File, offset: u64) -> Result<RawDictionary, Error> {
        let file_size = f.metadata()?.len();
        // TODO - default dictionary size here.
        let mut mmap_size = file_size - offset;
        let min_size: u64 = 1024;
        if mmap_size < min_size {
            f.set_len(offset+min_size)?;
            mmap_size = min_size;
        }
        
        let data = unsafe { 
            MmapOptions::new()
            .offset(offset)
            .len(mmap_size as usize)
            .map_mut(&f)? 
        };
        Ok(RawDictionary { data, f, offset })
    }

    // Note: We need to do shenanigans for String to read properly.
    // Prost, by default, serializes "String" type as the google.proto.String message.
    fn try_read_string(&mut self, index: i64) -> Result<String, Error> {
        let offset = (index as u64 - self.offset) as usize;
        if let Some(mut buf) = self.data.get(offset..) {
            let mut result = String::new();
            let ctx = prost::encoding::DecodeContext::default();
            let wire_type = prost::encoding::WireType::LengthDelimited;
            prost::encoding::string::merge(wire_type, &mut result, &mut buf, ctx)?;
            return Ok(result)
        }
        // TODO - Remap the mmap file and retry.
        Err(Error::NotFoundInDictionary(
            "string".to_owned(),
            index,
        ))
    }

    /// Attempts to read a message out of the dictionary.
    pub(crate) fn try_read<T: prost::Message + std::default::Default>(
        &mut self,
        index: i64,
    ) -> Result<T, Error> {
        let offset = (index as u64 - self.offset) as usize;
        if let Some(buf) = self.data.get(offset..) {
            return Ok(T::decode_length_delimited(buf)?)
        }
        // TODO - Remap the mmap file and try again.
        // We were unable to recover here.
        Err(Error::NotFoundInDictionary(
            std::any::type_name::<T>().to_owned(),
            index,
        ))
    }

    // TODO - find ways to check sanity of data.
    pub(crate) fn header(&self) -> &RawDictionaryHeader {
        unsafe { &*(self.data.as_ref().as_ptr() as *const RawDictionaryHeader) }
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
