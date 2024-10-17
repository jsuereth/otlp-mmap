use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum OltpMmapError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    ProtobufDecodeError(#[from] prost::DecodeError),

    #[error("Index {1} not found in dictionary {0}")]
    NotFoundInDictoinary(String, i64),

    #[error(transparent)]
    TonicStatus(#[from] tonic::Status),

    #[error(transparent)]
    TonicTransportError(#[from] tonic::transport::Error),

    #[error(transparent)]
    AsyncOltpMmapError(#[from] Arc<OltpMmapError>)
}
