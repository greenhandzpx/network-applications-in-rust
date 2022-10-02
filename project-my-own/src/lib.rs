pub use kv::KvStore;
pub use error::KvError;
pub use error::Result;

extern crate failure;
#[macro_use] extern crate failure_derive;

mod kv;
mod error;
