use std::{array::TryFromSliceError, sync::Arc};
use thiserror::Error;
use tokio::task::JoinError;

#[derive(Error, Debug)]
pub enum Error {
    #[error("OTLP mmap version mismatch. {0} != {1}")]
    VersionMismatch(i64, i64),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    ProtobufDecodeError(#[from] prost::DecodeError),

    #[error("Index {1} not found in dictionary {0}")]
    NotFoundInDictionary(String, i64),

    #[error(transparent)]
    TonicStatus(#[from] tonic::Status),

    #[error(transparent)]
    TonicTransportError(#[from] tonic::transport::Error),

    #[error(transparent)]
    AsyncOltpMmapError(#[from] Arc<Error>),

    #[error(transparent)]
    ConversionError(#[from] TryFromSliceError),

    #[error(transparent)]
    JoinError(#[from] JoinError),

    #[error(transparent)]
    ClapError(#[from] clap::Error),
}

// TODO - Format errors.
