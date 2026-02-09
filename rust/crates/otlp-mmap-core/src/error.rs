use thiserror::Error;

/// An error from the raw OTLP-MMAP protocol.
#[derive(Error, Debug)]
pub enum Error {
    #[error("Index {1} not found in dictionary {0}")]
    NotFoundInDictionary(String, i64),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    ProtobufDecodeError(#[from] prost::DecodeError),

    #[error(transparent)]
    ProtobufEncodeError(#[from] prost::EncodeError),
}

// TODO - Format errors.
