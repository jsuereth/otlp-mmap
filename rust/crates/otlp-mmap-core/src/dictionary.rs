//! Dictionaries in OTLP MMap

use crate::Error;
use memmap2::{MmapMut, MmapOptions};
use std::{
    cell::UnsafeCell,
    fs::File,
    sync::atomic::{AtomicI64, Ordering},
};

/// A mmap variable-sized dictionary implementation.
///
/// The dictionary points at an offset into the file, and allows indexing entries by their offset.
/// Every entry is expected to be length-delimited, using variable integer specification.
pub struct Dictionary {
    /// The mmap data
    data: UnsafeCell<MmapMut>,
    /// The file we're reading.
    f: File,
    /// The offset into the mmap data where the dictionary starts.
    offset: u64,
}

// We are using memory primitives on MMAP memory to allow multi-thread usage here.
unsafe impl Sync for Dictionary {}

const DICTIONARY_HEADER_SIZE: i64 = 64;
const MIN_DICTIONARY_SIZE: u64 = 1024;

impl Dictionary {
    /// Constructs a new dictionary.
    pub fn try_new(f: File, offset: u64, opt_min_size: Option<u64>) -> Result<Dictionary, Error> {
        // TODO - update this to take an MMAP directly.
        let file_size = f.metadata()?.len();
        // TODO - default dictionary size here.
        let mut mmap_size = file_size - offset;
        let min_size = opt_min_size.unwrap_or(MIN_DICTIONARY_SIZE);
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
        // We set the header offset appropriate, if we're the one writing the dictionary.
        let dictionary = Dictionary {
            data: UnsafeCell::new(data),
            f,
            offset,
        };
        if dictionary.header().end.load(Ordering::Relaxed)
            < (offset as i64 + DICTIONARY_HEADER_SIZE)
        {
            dictionary
                .header()
                .end
                .store(offset as i64 + DICTIONARY_HEADER_SIZE, Ordering::Release);
        }
        Ok(dictionary)
    }

    // Note: We need to do shenanigans for String to read properly.
    // Prost, by default, serializes "String" type as the google.proto.String message.
    pub fn try_read_string(&self, index: i64) -> Result<String, Error> {
        // 0 is special, and always the empty string.
        if index == 0 {
            return Ok("".to_owned());
        }

        if (index as u64) < self.offset {
            return Err(Error::NotFoundInDictionary("string".to_owned(), index));
        }
        let offset = (index as u64 - self.offset) as usize;

        loop {
            let data = unsafe { &*self.data.get() };
            if let Some(mut buf) = data.get(offset..) {
                let mut result = String::new();
                let ctx = prost::encoding::DecodeContext::default();
                let wire_type = prost::encoding::WireType::LengthDelimited;
                match prost::encoding::string::merge(wire_type, &mut result, &mut buf, ctx) {
                    Ok(_) => return Ok(result),
                    Err(e) => {
                        // If we failed to decode, it might be because the buffer was too short.
                        // Try to remap and see if we can read more.
                        if !self.try_remap()? {
                            return Err(e.into());
                        }
                        continue;
                    }
                }
            }
            if !self.try_remap()? {
                break;
            }
        }
        Err(Error::NotFoundInDictionary("string".to_owned(), index))
    }

    /// Attempts to read a message out of the dictionary.
    pub fn try_read<T: prost::Message + std::default::Default>(
        &self,
        index: i64,
    ) -> Result<T, Error> {
        if (index as u64) < self.offset {
            return Err(Error::NotFoundInDictionary(
                std::any::type_name::<T>().to_owned(),
                index,
            ));
        }
        let offset = (index as u64 - self.offset) as usize;
        loop {
            let data = unsafe { &*self.data.get() };
            if let Some(buf) = data.get(offset..) {
                match T::decode_length_delimited(buf) {
                    Ok(msg) => return Ok(msg),
                    Err(e) => {
                        // If we failed to decode, it might be because the buffer was too short.
                        if !self.try_remap()? {
                            return Err(e.into());
                        }
                        continue;
                    }
                }
            }
            if !self.try_remap()? {
                break;
            }
        }
        Err(Error::NotFoundInDictionary(
            std::any::type_name::<T>().to_owned(),
            index,
        ))
    }

    /// Attempts to remap the dictionary to the current file size.
    /// Returns true if the mmap was actually changed.
    fn try_remap(&self) -> Result<bool, Error> {
        let file_size = self.f.metadata()?.len();
        let current_size = unsafe { (&*self.data.get()).len() as u64 };
        let new_mmap_size = file_size - self.offset;

        if new_mmap_size > current_size {
            let data = unsafe {
                MmapOptions::new()
                    .offset(self.offset)
                    .len(new_mmap_size as usize)
                    .map_mut(&self.f)?
            };
            unsafe {
                *self.data.get() = data;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    // TODO - find ways to check sanity of data.
    fn header(&self) -> &RawDictionaryHeader {
        unsafe {
            let data = &*self.data.get();
            &*(data.as_ref().as_ptr() as *const RawDictionaryHeader)
        }
    }

    /// Attempt to write a message to the dictionary.
    pub fn try_write<T: prost::Message>(&self, msg: &T) -> Result<i64, Error> {
        let encoded_len = msg.encoded_len();
        let delimiter_len = prost::length_delimiter_len(encoded_len);
        let total_len = delimiter_len + encoded_len;

        // CAS for bytes to write - This will keep us "thread safe", so it's ok to take a mutable reference to the mmap.
        let current = self
            .header()
            .end
            .fetch_add(total_len as i64, Ordering::Acquire);
        let start = (current as u64 - self.offset) as usize;
        let end = (current as u64 + total_len as u64 - self.offset) as usize;

        self.ensure_capacity(end)?;

        let data = unsafe { &mut *self.data.get() };
        let slice = &mut data[start..end];
        let mut buf = &mut slice[..];
        msg.encode_length_delimited(&mut buf)?;
        // last - update the number of entries.
        self.header().num_entries.fetch_add(1, Ordering::Relaxed);
        Ok(current)
    }
    /// Writes a raw string to the dictionary.
    pub fn try_write_string(&self, s: &str) -> Result<i64, Error> {
        self.try_write_bytes(s.as_bytes())
    }
    fn try_write_bytes(&self, bytes: &[u8]) -> Result<i64, Error> {
        let delimiter_len = prost::length_delimiter_len(bytes.len());
        let total_len = delimiter_len + bytes.len();
        // CAS for bytes to write. This makes it safe for us to pull a mutable reference to MMAP.
        let current = self
            .header()
            .end
            .fetch_add(total_len as i64, Ordering::Acquire);
        let start = (current as u64 - self.offset) as usize;
        let end_delimiter = start + delimiter_len;
        let end = start + total_len;

        self.ensure_capacity(end)?;

        let data = unsafe { &mut *self.data.get() };
        println!("Writing bytes to dictionary. current={current}");
        {
            let mut length_buf = &mut data[start..end_delimiter];
            prost::encoding::encode_varint(bytes.len() as u64, &mut length_buf);
        }
        let buf = &mut data[end_delimiter..end];
        buf.copy_from_slice(bytes);
        // last - update the number of entries.
        self.header().num_entries.fetch_add(1, Ordering::Relaxed);
        Ok(current)
    }

    /// Ensures the dictionary has enough capacity to write up to `end_offset`.
    /// If not, it resizes the file and remaps the memory.
    fn ensure_capacity(&self, end_offset: usize) -> Result<(), Error> {
        let current_size = unsafe { (&*self.data.get()).len() };
        if end_offset > current_size {
            // Double the size or take what's needed, whichever is larger.
            let new_size = std::cmp::max(current_size * 2, end_offset);
            self.f.set_len(self.offset + new_size as u64)?;
            self.try_remap()?;
        }
        Ok(())
    }
}

/// This first 64 bytes of the dictionary in OTLP-MMAP has this format.
/// We use this struct to "reinterpret_cast" and use memory safe primitives for access.
#[repr(C)]
struct RawDictionaryHeader {
    /// Last written location of the dictionary.
    end: AtomicI64,
    /// Number of entries that have been written to the dictionary.
    num_entries: AtomicI64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use prost::Message;
    use std::fs::OpenOptions;
    use std::io::{Seek, Write};
    use std::sync::atomic::Ordering;
    use tempfile::NamedTempFile;

    #[test]
    fn test_new_resizes_file() -> Result<(), Error> {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .expect("Failed to open temp file");
        let offset = 64;
        f.set_len(offset).expect("Failed to set file length"); // Set file size to be smaller than min_size
        let dict = Dictionary::try_new(f, offset, None).expect("Failed to create dictionary");
        let new_size = dict.f.metadata().expect("Failed to get metadata").len();
        assert_eq!(new_size, offset + 1024);
        Ok(())
    }

    #[test]
    fn test_read_header() -> Result<(), Error> {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .expect("Failed to open temp file");
        let offset = 0;
        f.set_len(1024).expect("Failed to set file length");

        // Manually write a header
        let end_val: i64 = 123;
        let num_entries_val: i64 = 456;
        f.write_all(&end_val.to_ne_bytes())
            .expect("Failed to write to file");
        f.write_all(&num_entries_val.to_ne_bytes())
            .expect("Failed to write to file");
        f.flush().expect("Failed to flush file");

        let dict_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .expect("Failed to open temp file");
        let dict =
            Dictionary::try_new(dict_file, offset, None).expect("Failed to create dictionary");
        let header = dict.header();

        assert_eq!(header.end.load(Ordering::Relaxed), end_val);
        assert_eq!(header.num_entries.load(Ordering::Relaxed), num_entries_val);
        Ok(())
    }

    #[test]
    fn test_read_string_ok() -> Result<(), Error> {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .expect("Failed to open temp file");
        let offset = 64;
        f.set_len(offset + 1024).expect("Failed to set file length");

        // Write a prost-encoded string to the file
        let test_string = "hello world".to_string();
        let mut buf = Vec::new();
        // Prost encodes strings as length-delimited
        prost::encoding::string::encode(1, &test_string, &mut buf);
        // We need to strip the tag, try_read_string doesn't expect it
        let encoded_string = &buf[1..];

        // Write header
        f.seek(std::io::SeekFrom::Start(offset))
            .expect("Failed to seek in file");
        let end: i64 = offset as i64 + 200 + encoded_string.len() as i64;
        let num_messages: i64 = 1;
        f.write(&end.to_le_bytes())
            .expect("Failed to write to file");
        f.write(&num_messages.to_le_bytes())
            .expect("Failed to write to file");
        f.seek(std::io::SeekFrom::Start(offset + 100))
            .expect("Failed to seek in file");
        f.write_all(encoded_string)
            .expect("Failed to write to file");
        f.flush().expect("Failed to flush file");

        let dict_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .expect("Failed to open temp file");
        let dict =
            Dictionary::try_new(dict_file, offset, None).expect("Failed to create dictionary");

        let result = dict
            .try_read_string((offset + 100) as i64)
            .expect("Failed to read string");
        assert_eq!(result, test_string);

        Ok(())
    }

    #[test]
    fn test_read_string_invalid_index() -> Result<(), Error> {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .expect("Failed to open temp file");
        let offset = 64;
        f.set_len(offset + 1024).expect("Failed to set file length");
        let dict = Dictionary::try_new(f, offset, None).expect("Failed to create dictionary");

        let result = dict.try_read_string(offset as i64 - 10);
        assert!(matches!(result, Err(Error::NotFoundInDictionary(_, _))));

        Ok(())
    }

    #[test]
    fn test_read_message_ok() -> Result<(), Error> {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .expect("Failed to open temp file");
        let offset = 128;
        f.set_len(offset + 1024).expect("Failed to set file length");

        let resource = otlp_mmap_protocol::Resource {
            attributes: vec![],
            dropped_attributes_count: 42,
        };

        let mut buf = Vec::new();
        resource
            .encode_length_delimited(&mut buf)
            .expect("Failed to encode resource");
        // Write header
        f.seek(std::io::SeekFrom::Start(offset))
            .expect("Failed to seek in file");
        let end: i64 = offset as i64 + 200 + buf.len() as i64;
        let num_messages: i64 = 1;
        f.write(&end.to_le_bytes())
            .expect("Failed to write to file");
        f.write(&num_messages.to_le_bytes())
            .expect("Failed to write to file");
        f.seek(std::io::SeekFrom::Start(offset + 200))
            .expect("Failed to seek in file");
        f.write_all(&buf).expect("Failed to write to file");
        f.flush().expect("Failed to flush file");

        let dict_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .expect("Failed to open temp file");
        let dict =
            Dictionary::try_new(dict_file, offset, None).expect("Failed to create dictionary");
        let result: otlp_mmap_protocol::Resource = dict
            .try_read((offset + 200) as i64)
            .expect("Failed to read resource");

        assert_eq!(result.dropped_attributes_count, 42);

        Ok(())
    }

    #[test]
    fn test_read_message_invalid_index() -> Result<(), Error> {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .expect("Failed to open temp file");
        let offset = 64;
        f.set_len(offset + 1024).expect("Failed to set file length");
        let dict = Dictionary::try_new(f, offset, None).expect("Failed to create dictionary");

        let result: Result<otlp_mmap_protocol::Resource, Error> = dict.try_read(10);
        assert!(matches!(result, Err(Error::NotFoundInDictionary(_, 10))));

        Ok(())
    }

    #[test]
    fn test_read_message_corrupted_data() -> Result<(), Error> {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .expect("Failed to open temp file");
        let offset = 0;
        f.set_len(1024).expect("Failed to set file length");
        f.write_all(&[0xDE, 0xAD, 0xBE, 0xEF])
            .expect("Failed to write to file"); // Write garbage
        f.flush().expect("Failed to flush file");

        let dict_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .expect("Failed to open temp file");
        let dict =
            Dictionary::try_new(dict_file, offset, None).expect("Failed to create dictionary");

        let result: Result<otlp_mmap_protocol::Resource, Error> = dict.try_read(offset as i64);
        assert!(matches!(result, Err(Error::ProtobufDecodeError(_))));
        Ok(())
    }

    #[test]
    fn test_read_beyond_file_bounds() -> Result<(), Error> {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .expect("Failed to open temp file");
        let offset = 0;
        // The mmap size is 1024.
        f.set_len(offset + 1024).expect("Failed to set file length");

        let dict = Dictionary::try_new(f, offset, None).expect("Failed to create dictionary");

        // Try to read from an index far beyond the end of the mmap.
        let result: Result<otlp_mmap_protocol::Resource, Error> = dict.try_read(2048);
        assert!(matches!(result, Err(Error::NotFoundInDictionary(_, 2048))));
        Ok(())
    }

    #[test]
    fn test_read_malformed_message_fails() -> Result<(), Error> {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .expect("Failed to open temp file");
        let offset = 0;
        f.set_len(1024).expect("Failed to set file length");

        // Write a malformed length-delimited message: a length of 100, but only 3 bytes of data.
        let malformed_buf = &[
            100, // varint-encoded length of 100
            1, 2, 3, // Not enough data
        ];
        f.seek(std::io::SeekFrom::Start(offset as u64))
            .expect("Failed to seek in file");
        f.write_all(malformed_buf).expect("Failed to write to file");
        f.flush().expect("Failed to flush file");

        let dict_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .expect("Failed to open temp file");
        let dict =
            Dictionary::try_new(dict_file, offset, None).expect("Failed to create dictionary");

        // Try to decode it. This should fail because the buffer is unexpectedly short.
        let result: Result<otlp_mmap_protocol::Resource, Error> = dict.try_read(offset as i64);
        assert!(matches!(result, Err(Error::ProtobufDecodeError(_))));

        Ok(())
    }

    #[test]
    fn test_read_entry_exceeding_mmap_bounds_edge() -> Result<(), Error> {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .expect("Failed to open temp file");
        let offset = 0;
        let mmap_size = 1024;
        f.set_len(offset + mmap_size)
            .expect("Failed to set file length");

        // Position the entry near the end of the mmap
        let entry_offset = offset + mmap_size - 4; // 4 bytes from the end

        // Write a malformed entry. The length prefix is 10, but only 4 bytes are
        // available in the mmap from this position.
        let malformed_buf = &[
            10, // varint-encoded length of 10
            1, 2, 3, // Only 3 bytes of payload, total of 4 bytes with length
        ];
        f.seek(std::io::SeekFrom::Start(entry_offset))
            .expect("Failed to seek in file");
        f.write_all(malformed_buf).expect("Failed to write to file");
        f.flush().expect("Failed to flush file");

        let dict_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .expect("Failed to open temp file");
        let dict =
            Dictionary::try_new(dict_file, offset, None).expect("Failed to create dictionary");

        // Try to decode it. This should fail as it tries to read past the mmap boundary.
        let result: Result<otlp_mmap_protocol::Resource, Error> =
            dict.try_read(entry_offset as i64);
        assert!(matches!(result, Err(Error::ProtobufDecodeError(_))));

        Ok(())
    }

    #[test]
    fn test_write_then_read_string() -> Result<(), Error> {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .expect("Failed to open temp file");
        let offset = 64;
        f.set_len(offset + 1024).expect("Failed to set file length");

        // Write a prost-encoded string to the file
        let test_string = "hello world".to_owned();
        let dict = Dictionary::try_new(f, offset, None).expect("Failed to create dictionary");
        let idx = dict
            .try_write_string(&test_string)
            .expect("Failed to write string to dictionary");
        let result = dict
            .try_read_string(idx as i64)
            .expect("Failed to read string from dictionary");
        assert_eq!(test_string, result);
        Ok(())
    }
    #[test]
    fn test_write_then_read_proto() -> Result<(), Error> {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .expect("Failed to open temp file");
        let offset = 64;
        f.set_len(offset + 1024).expect("Failed to set file length");

        let msg = otlp_mmap_protocol::Resource {
            attributes: vec![],
            dropped_attributes_count: 42,
        };
        let dict = Dictionary::try_new(f, offset, None).expect("Failed to create dictionary");
        let idx = dict.try_write(&msg).expect("Failed to write resource");
        let result: otlp_mmap_protocol::Resource =
            dict.try_read(idx).expect("Failed to read protocol buffer");
        assert_eq!(result, msg);
        Ok(())
    }

    #[test]
    fn test_dictionary_growth() -> Result<(), Error> {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .expect("Failed to open temp file");
        let offset = 0;
        let initial_size = 128;
        let dict = Dictionary::try_new(f, offset, Some(initial_size))
            .expect("Failed to create dictionary");

        // Write a lot of strings to force growth
        for i in 0..100 {
            let s = format!("string_{}", i);
            let idx = dict.try_write_string(&s).expect("Failed to write string");
            assert_eq!(dict.try_read_string(idx).expect("Failed to read string"), s);
        }

        let final_size = dict.f.metadata().expect("Failed to get metadata").len();
        assert!(final_size > initial_size);
        Ok(())
    }

    #[test]
    fn test_cross_process_growth() -> Result<(), Error> {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let f_writer = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .expect("Failed to open temp file for writer");
        let offset = 0;
        let initial_size = 128;
        let dict_writer = Dictionary::try_new(f_writer, offset, Some(initial_size))
            .expect("Failed to create writer dictionary");

        let f_reader = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .expect("Failed to open temp file for reader");
        let dict_reader = Dictionary::try_new(f_reader, offset, Some(initial_size))
            .expect("Failed to create reader dictionary");

        // 1. Writer writes some data fitting in initial size
        let s1 = "small string";
        let idx1 = dict_writer
            .try_write_string(s1)
            .expect("Failed to write first string");
        assert_eq!(
            dict_reader
                .try_read_string(idx1)
                .expect("Failed to read first string"),
            s1
        );

        // 2. Writer writes a lot of data, forcing file growth
        let mut last_idx = 0;
        let mut last_str = String::new();
        for i in 0..100 {
            last_str = format!("very long string to force growth faster {}", i);
            last_idx = dict_writer
                .try_write_string(&last_str)
                .expect("Failed to write string during growth");
        }

        // 3. Reader tries to read the last entry.
        // This should trigger try_remap because the index is beyond the current reader's mmap size.
        let result = dict_reader
            .try_read_string(last_idx)
            .expect("Reader failed to read grown entry");
        assert_eq!(result, last_str);

        Ok(())
    }

    #[test]
    fn test_reader_edge_case_growth() -> Result<(), Error> {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let initial_size = 1024;

        let f_writer = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .expect("Failed to open temp file for writer");
        let dict_writer = Dictionary::try_new(f_writer, 0, Some(initial_size))
            .expect("Failed to create writer dictionary");

        let f_reader = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .expect("Failed to open temp file for reader");
        let dict_reader = Dictionary::try_new(f_reader, 0, Some(initial_size))
            .expect("Failed to create reader dictionary");

        // 1. Writer writes data until we are near the end of the initial 1024 bytes.
        let mut last_idx = 0;
        loop {
            let s = "short";
            last_idx = dict_writer
                .try_write_string(s)
                .expect("Failed to write string");
            if last_idx > 1000 {
                break;
            }
        }

        // 2. Writer writes a long string that crosses the 1024 boundary.
        // The index will be < 1024, but the payload will end > 1024.
        let long_str = "This string is long enough to definitely cross the 1024 byte boundary from wherever we started near 1000";
        let edge_idx = dict_writer
            .try_write_string(long_str)
            .expect("Failed to write long string at edge");
        assert!(edge_idx < 1024);

        // 3. Reader tries to read the edge entry.
        // If the reader doesn't remap on decoding error, this will FAIL.
        let result = dict_reader
            .try_read_string(edge_idx)
            .expect("Reader failed to read edge-crossing entry");
        assert_eq!(result, long_str);

        Ok(())
    }
}
