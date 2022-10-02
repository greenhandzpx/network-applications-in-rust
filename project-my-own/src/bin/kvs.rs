use clap::{Arg, ArgAction, Command};
use kvs::KvStore;
use std::error::Error;

use kvs::KvError;
use kvs::Result;

fn main() -> Result<()> {
    let matches = Command::new("kvs")
        .arg(Arg::new("operation"))
        .arg(Arg::new("key"))
        .arg(Arg::new("value"))
        .arg(Arg::new("version").short('V').action(ArgAction::SetTrue))
        .get_matches();

    if let Some(v) = matches.get_one::<bool>("version") && *v == true {
        let version: &'static str = env!("CARGO_PKG_VERSION");
        println!("{}", version);
        return Err(KvError::InvalidCommandLineArgs {});
    }
    let op = matches
        .get_one::<String>("operation")
        .expect("must provide an operation");
    let key = matches.get_one::<String>("key");
    let value = matches.get_one::<String>("value");
    let mut kv_store = KvStore::open(&std::env::current_dir()?)?;
    match op as &str {
        "get" => {
            // unimplemented!()
            assert_eq!(value, None);
            if let Some(value) = kv_store.get(key.unwrap().clone())? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        "set" => {
            kv_store.set(key.unwrap().clone(), value.unwrap().clone())?;
        }
        "rm" => {
            match kv_store.remove(key.unwrap().clone()) {
                Err(err) => {
                     println!("Key not found");
                     return Err(err)
                }
                Ok(_) => ()
            }
        }
        _ => return Err(KvError::UnknownOperation { op: op.to_string() })
    }
        
    Ok(())
    
}
