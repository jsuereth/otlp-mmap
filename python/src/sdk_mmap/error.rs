use std::array::TryFromSliceError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("OTLP MMap file start time has changed since reading")]
    OtlpMmapOutofData,

    #[error("OTLP mmap version mismatch. Found: {0}, Supported: {1:?}")]
    VersionMismatch(i64, &'static [i64]),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    ProtobufDecodeError(#[from] prost::DecodeError),

    #[error(transparent)]
    ProtobufEncodeError(#[from] prost::EncodeError),

    #[error("Index {1} not found in dictionary {0}")]
    NotFoundInDictionary(String, i64),

    #[error(transparent)]
    ConversionError(#[from] TryFromSliceError),
}
