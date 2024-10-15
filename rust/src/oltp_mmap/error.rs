use thiserror::Error;

#[derive(Error, Debug)]
pub enum OltpMmapError {
    #[error("{0}")]
    IoError(#[from] std::io::Error),

    #[error("{0}")]
    ProtobufDecodeError(#[from] prost::DecodeError),

    #[error("Index {1} not found in dictionary {0}")]
    NotFoundInDictoinary(String, i64),
}
