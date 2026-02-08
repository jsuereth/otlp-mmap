//! variable sized dictionary backed by mmap file

use std::{fs::File, sync::atomic::AtomicI64, sync::Mutex};

use memmap2::{MmapMut, MmapOptions};
use super::Error;

/// A dictionary implementation that allows access to read entries.
pub trait DictionaryInterface {
    /// Reads a string from the dictionary.
    fn try_read_string(&self, index: i64) -> Result<String, Error>;
    /// Reads a protobuf encoded value from the dictionary.
    fn try_read<T: prost::Message + std::default::Default>(
        &self,
        index: i64,
    ) -> Result<T, Error>;
}

/// A thread-safe version of the mmap dictionary
pub struct Dictionary {
    input: Mutex<RawDictionary>,
}
impl DictionaryInterface for Dictionary {
    fn try_read_string(&self, index: i64) -> Result<String, Error> {
        self.input.lock().unwrap().try_read_string(index)
    }

    fn try_read<T: prost::Message + std::default::Default>(
        &self,
        index: i64,
    ) -> Result<T, Error> {
        self.input.lock().unwrap().try_read(index)
    }
}
impl Dictionary {
    pub(crate) fn try_new(f: File, offset: u64) -> Result<Dictionary, Error> {
        Ok(Dictionary {
            input: Mutex::new(RawDictionary::try_new(f, offset)?),
        })
    }
}

struct RawDictionary {
    /// The mmap data
    data: MmapMut,
    /// The file we're reading.
    #[allow(dead_code)]
    f: File,
    /// The offset into the mmap data where the dictionary starts.
    offset: u64,
}

impl RawDictionary {
    /// Constructs a new dictionary.
    pub(crate) fn try_new(f: File, offset: u64) -> Result<RawDictionary, Error> {
        let file_size = f.metadata()?.len();
        let mut mmap_size = file_size - offset;
        let min_size: u64 = 1024;
        if mmap_size < min_size {
            f.set_len(offset + min_size)?;
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

    fn try_read_string(&mut self, index: i64) -> Result<String, Error> {
        if (index as u64) < self.offset {
            return Err(Error::NotFoundInDictionary("string".to_owned(), index));
        }
        let offset = (index as u64 - self.offset) as usize;
        if let Some(mut buf) = self.data.get(offset..) {
            let mut result = String::new();
            let ctx = prost::encoding::DecodeContext::default();
            let wire_type = prost::encoding::WireType::LengthDelimited;
            prost::encoding::string::merge(wire_type, &mut result, &mut buf, ctx)?;
            return Ok(result);
        }
        Err(Error::NotFoundInDictionary("string".to_owned(), index))
    }

    pub(crate) fn try_read<T: prost::Message + std::default::Default>(
        &mut self,
        index: i64,
    ) -> Result<T, Error> {
        if (index as u64) < self.offset {
            return Err(Error::NotFoundInDictionary(
                std::any::type_name::<T>().to_owned(),
                index,
            ));
        }
        let offset = (index as u64 - self.offset) as usize;
        if let Some(buf) = self.data.get(offset..) {
            return Ok(T::decode_length_delimited(buf)?);
        }
        Err(Error::NotFoundInDictionary(
            std::any::type_name::<T>().to_owned(),
            index,
        ))
    }

    pub(crate) fn header(&self) -> &RawDictionaryHeader {
        unsafe { &*(self.data.as_ref().as_ptr() as *const RawDictionaryHeader) }
    }
}

#[repr(C)]
pub(crate) struct RawDictionaryHeader {
    end: AtomicI64,
    num_entries: AtomicI64,
}
