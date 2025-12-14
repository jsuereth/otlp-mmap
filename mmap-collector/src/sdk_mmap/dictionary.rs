//! variable sized dictionary backed by mmap file

use std::{fs::File, sync::atomic::AtomicI64};

use memmap2::{MmapMut, MmapOptions};
use tokio::sync::Mutex;

use crate::sdk_mmap::Error;

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

    // Note: We need to do shenanigans for String to read properly.
    // Prost, by default, serializes "String" type as the google.proto.String message.
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
        // TODO - Remap the mmap file and retry.
        Err(Error::NotFoundInDictionary("string".to_owned(), index))
    }

    /// Attempts to read a message out of the dictionary.
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
        // TODO - debug logs.
        // println!(
        //     "Loading {} from index {}",
        //     std::any::type_name::<T>().to_owned(),
        //     index
        // );
        let offset = (index as u64 - self.offset) as usize;
        if let Some(buf) = self.data.get(offset..) {
            return Ok(T::decode_length_delimited(buf)?);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sdk_mmap::data;
    use prost::Message;
    use std::fs::OpenOptions;
    use std::io::{Seek, Write};
    use std::sync::atomic::Ordering;
    use tempfile::NamedTempFile;

    #[test]
    fn test_new_resizes_file() -> Result<(), Error> {
        let file = NamedTempFile::new()?;
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())?;
        let offset = 64;
        f.set_len(offset)?; // Set file size to be smaller than min_size
        let dict = RawDictionary::try_new(f, offset)?;
        let new_size = dict.f.metadata()?.len();
        assert_eq!(new_size, offset + 1024);
        Ok(())
    }

    #[test]
    fn test_read_header() -> Result<(), Error> {
        let file = NamedTempFile::new()?;
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())?;
        let offset = 0;
        f.set_len(1024)?;

        // Manually write a header
        let end_val: i64 = 123;
        let num_entries_val: i64 = 456;
        f.write_all(&end_val.to_ne_bytes())?;
        f.write_all(&num_entries_val.to_ne_bytes())?;
        f.flush()?;

        let dict_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())?;
        let dict = RawDictionary::try_new(dict_file, offset)?;
        let header = dict.header();

        assert_eq!(header.end.load(Ordering::Relaxed), end_val);
        assert_eq!(header.num_entries.load(Ordering::Relaxed), num_entries_val);
        Ok(())
    }

    #[tokio::test]
    async fn test_read_string_ok() -> Result<(), Error> {
        let file = NamedTempFile::new()?;
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())?;
        let offset = 64;
        f.set_len(offset + 1024)?;

        // Write a prost-encoded string to the file
        let test_string = "hello world".to_string();
        let mut buf = Vec::new();
        // Prost encodes strings as length-delimited
        prost::encoding::string::encode(1, &test_string, &mut buf);
        // We need to strip the tag, try_read_string doesn't expect it
        let encoded_string = &buf[1..];
        f.seek(std::io::SeekFrom::Start(offset + 100))?;
        f.write_all(encoded_string)?;
        f.flush()?;

        let dict_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())?;
        let dict = Dictionary::try_new(dict_file, offset)?;

        let result = dict.try_read_string((offset + 100) as i64).await?;
        assert_eq!(result, test_string);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_string_invalid_index() -> Result<(), Error> {
        let file = NamedTempFile::new()?;
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())?;
        let offset = 64;
        f.set_len(offset + 1024)?;
        let dict = Dictionary::try_new(f, offset)?;

        let result = dict.try_read_string(offset as i64 - 10).await;
        assert!(matches!(result, Err(Error::NotFoundInDictionary(_, _))));

        Ok(())
    }

    #[tokio::test]
    async fn test_read_message_ok() -> Result<(), Error> {
        let file = NamedTempFile::new()?;
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())?;
        let offset = 128;
        f.set_len(offset + 1024)?;

        let resource = data::Resource {
            attributes: vec![],
            dropped_attributes_count: 42,
        };

        let mut buf = Vec::new();
        resource.encode_length_delimited(&mut buf)?;
        f.seek(std::io::SeekFrom::Start(offset + 200))?;
        f.write_all(&buf)?;
        f.flush()?;

        let dict_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())?;
        let dict = Dictionary::try_new(dict_file, offset)?;
        let result: data::Resource = dict.try_read((offset + 200) as i64).await?;

        assert_eq!(result.dropped_attributes_count, 42);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_message_invalid_index() -> Result<(), Error> {
        let file = NamedTempFile::new()?;
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())?;
        let offset = 64;
        f.set_len(offset + 1024)?;
        let dict = Dictionary::try_new(f, offset)?;

        let result: Result<data::Resource, Error> = dict.try_read(10).await;
        assert!(matches!(result, Err(Error::NotFoundInDictionary(_, 10))));

        Ok(())
    }

    #[tokio::test]
    async fn test_read_message_corrupted_data() -> Result<(), Error> {
        let file = NamedTempFile::new()?;
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())?;
        let offset = 0;
        f.set_len(1024)?;
        f.write_all(&[0xDE, 0xAD, 0xBE, 0xEF])?; // Write garbage
        f.flush()?;

        let dict_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())?;
        let dict = Dictionary::try_new(dict_file, offset)?;

        let result: Result<data::Resource, Error> = dict.try_read(offset as i64).await;
        assert!(matches!(result, Err(Error::ProtobufDecodeError(_))));
        Ok(())
    }

    #[tokio::test]
    async fn test_read_beyond_file_bounds() -> Result<(), Error> {
        let file = NamedTempFile::new()?;
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())?;
        let offset = 0;
        // The mmap size is 1024.
        f.set_len(offset + 1024)?;

        let dict = Dictionary::try_new(f, offset)?;

        // Try to read from an index far beyond the end of the mmap.
        let result: Result<data::Resource, Error> = dict.try_read(2048).await;
        assert!(matches!(result, Err(Error::NotFoundInDictionary(_, 2048))));
        Ok(())
    }

    #[tokio::test]
    async fn test_read_malformed_message_fails() -> Result<(), Error> {
        let file = NamedTempFile::new()?;
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())?;
        let offset = 0;
        f.set_len(1024)?;

        // Write a malformed length-delimited message: a length of 100, but only 3 bytes of data.
        let malformed_buf = &[
            100, // varint-encoded length of 100
            1, 2, 3, // Not enough data
        ];
        f.seek(std::io::SeekFrom::Start(offset as u64))?;
        f.write_all(malformed_buf)?;
        f.flush()?;

        let dict_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())?;
        let dict = Dictionary::try_new(dict_file, offset)?;

        // Try to decode it. This should fail because the buffer is unexpectedly short.
        let result: Result<data::Resource, Error> = dict.try_read(offset as i64).await;
        assert!(matches!(result, Err(Error::ProtobufDecodeError(_))));

        Ok(())
    }

    #[tokio::test]
    async fn test_read_entry_exceeding_mmap_bounds_edge() -> Result<(), Error> {
        let file = NamedTempFile::new()?;
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())?;
        let offset = 0;
        let mmap_size = 1024;
        f.set_len(offset + mmap_size)?;

        // Position the entry near the end of the mmap
        let entry_offset = offset + mmap_size - 4; // 4 bytes from the end

        // Write a malformed entry. The length prefix is 10, but only 4 bytes are
        // available in the mmap from this position.
        let malformed_buf = &[
            10, // varint-encoded length of 10
            1, 2, 3, // Only 3 bytes of payload, total of 4 bytes with length
        ];
        f.seek(std::io::SeekFrom::Start(entry_offset))?;
        f.write_all(malformed_buf)?;
        f.flush()?;

        let dict_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())?;
        let dict = Dictionary::try_new(dict_file, offset)?;

        // Try to decode it. This should fail as it tries to read past the mmap boundary.
        let result: Result<data::Resource, Error> = dict.try_read(entry_offset as i64).await;
        assert!(matches!(result, Err(Error::ProtobufDecodeError(_))));

        Ok(())
    }
}
