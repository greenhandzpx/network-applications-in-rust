pub use error::KvError;
pub use error::Result;
pub use kv::KvStore;

extern crate failure;
#[macro_use]
extern crate failure_derive;

mod error;
mod kv;
