//! Errors for this create.

use thiserror::Error;

/// An error from the OTLP-MMAP collection libary.
#[derive(Error, Debug)]
pub enum Error {
    /// An error in the raw protocol.
    #[error(transparent)]
    OtlpMmapProtocolError(#[from] otlp_mmap_core::Error),
    #[error("Invalid trace id, unable to turn into 16-byte slice")]
    InvalidTraceIdError,
    #[error("Invalid span id, unable to turn into 8-byte slice")]
    InvalidSpanIdError,
    #[error("An error occured reading OTLP-MMAP file")]
    OtlpMmapOutofData,
    #[error(transparent)]
    TonicError(#[from] tonic::Status),
    #[error(transparent)]
    TonicTransportError(#[from] tonic::transport::Error),
    #[error(transparent)]
    ArgumentError(#[from] clap::Error),
}
