
use std::{result, io};

#[derive(Fail, Debug)]
pub enum KvError {
    #[fail(display = "Invalid command line args")]
    InvalidCommandLineArgs {},
    #[fail(display = "Unknown operation {}", op)]
    UnknownOperation {
        op: String
    },
    #[fail(display = "Key {} not found ", op)]
    KeyNotFound {
        op: String
    },
    #[fail(display = "File I/O error")]
    IOError {},
    #[fail(display = "Serde Json error")]
    SerdeError {},
}

impl From<io::Error> for KvError {
    fn from(_error: io::Error) -> Self {
        KvError::IOError {}
    }
}

impl From<serde_json::Error> for KvError {
    fn from(_error: serde_json::Error) -> Self {
        KvError::SerdeError {}
    }
}


pub type Result<T> = result::Result<T, KvError>;