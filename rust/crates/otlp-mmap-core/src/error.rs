use thiserror::Error;

/// An error from the raw OTLP-MMAP protocol.
#[derive(Error, Debug)]
pub enum Error {
    /// The version header in an OTLP-MMAP file is not longer the same as when it was first read or written.
    #[error("OTLP mmap version mismatch. Found: {0}, Supported: {1:?}")]
    VersionMismatch(i64, &'static [i64]),

    /// A given entry is either not in the available space of the dictionary, or could not be deserialized from the location given.
    #[error("Index {1} not found in dictionary {0}")]
    NotFoundInDictionary(String, i64),

    /// An otherwise-uncategorized I/O error has occured.
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    /// A protocol buffer was unable to be read.
    #[error(transparent)]
    ProtobufDecodeError(#[from] prost::DecodeError),

    /// A protocol buffer was unable to be written.
    #[error(transparent)]
    ProtobufEncodeError(#[from] prost::EncodeError),

    /// We were unabel to write a starting timestamp due to an unforseen system clock issue.
    #[error(transparent)]
    ClockError(#[from] std::time::SystemTimeError),

    /// The configuration found for an OTLP-MMAP file (either in its header or given in the constructor) doesn't abide by invariants.
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),
}
