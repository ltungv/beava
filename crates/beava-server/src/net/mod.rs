pub mod connection;
pub mod server;

use std::io;

use beava_core::wire::FrameError;
use thiserror::Error;

/// Error from running the server/client
#[derive(Error, Debug)]
pub enum Error {
    /// Error from parsing a frame.
    #[error("Frame error - {0}")]
    Frame(#[from] FrameError),

    /// Error from I/O operations.
    #[error("I/O error - {0}")]
    Io(#[from] io::Error),
}
