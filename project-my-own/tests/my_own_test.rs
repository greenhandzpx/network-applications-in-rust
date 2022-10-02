use kvs::{KvStore, Result};
use predicates::str::{contains, is_empty, PredicateStrExt};
use std::io::Split;
use std::path;
use std::process::Command;
use tempfile::TempDir;

#[test]
fn test_serde() -> Result<()>{
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path())?;
    // let mut store = KvStore::open(path::Path::new("tmpp"))?;

    store.set("key1".to_owned(), "value1".to_owned())?;
    store.set("key2".to_owned(), "value2".to_owned())?;

    println!("{}", store.get("key1".to_owned()).unwrap().unwrap());
    println!("{}", store.get("key2".to_owned()).unwrap().unwrap());

    Ok(())
}
#[test]
fn test_dummy() -> Result<()> {

    let s = String::from("hh;dd;ll;");

    let sp = s.split(";");

    for spp in sp {
        println!("spp:{}", spp);
    } 

    Ok(())
}