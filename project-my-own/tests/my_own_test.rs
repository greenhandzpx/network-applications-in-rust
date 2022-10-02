use kvs::{KvStore, Result};
use predicates::str::{contains, is_empty, PredicateStrExt};
use std::io::Split;
use std::path;
use std::process::Command;
use tempfile::TempDir;

#[test]
fn serde() -> Result<()> {
    // let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    // let mut store = KvStore::open(temp_dir.path())?;
    let mut store = KvStore::open(&std::env::current_dir()?)?;

    store.set("key1".to_owned(), "value1".to_owned())?;
    println!("{}", store.get("key1".to_owned()).unwrap().unwrap());
    store.set("key1".to_owned(), "value2".to_owned())?;

    println!("{}", store.get("key1".to_owned()).unwrap().unwrap());
    // println!("{}", store.get("key2".to_owned()).unwrap().unwrap());

    Ok(())
}
#[test]
fn dummy() -> Result<()> {
    let s = String::from("hh;dd;ll;");

    let sp = s.split(";");

    for spp in sp {
        println!("spp:{}", spp);
    }

    Ok(())
}

// #[test]
// fn my_own_compaction() -> Result<()> {

//     let mut store = KvStore::open(&std::env::current_dir()?)?;

//     for iter in 0..1000 {
//         for key_id in 0..1000 {
//             let key = format!("key{}", key_id);
//             let value = format!("{}", iter);
//             store.set(key.clone(), value)?;
//             println!("get {:?}", store.get(key)?);
//         }
//         return Ok(())
//         // let new_size = dir_size();
//         // if new_size > current_size {
//         //     current_size = new_size;
//         //     continue;
//         // }
//         // // Compaction triggered.

//         // drop(store);
//         // // reopen and check content.
//         // let mut store = KvStore::open(temp_dir.path())?;
//         // for key_id in 0..1000 {
//         //     let key = format!("key{}", key_id);
//         //     assert_eq!(store.get(key)?, Some(format!("{}", iter)));
//         // }
//         // return Ok(());
//     }

//     Ok(())
// }
