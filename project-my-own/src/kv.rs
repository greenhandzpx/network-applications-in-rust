use std::io::{Write};
use std::{collections::HashMap, path};
use crate::Result;
use std::fs::{File, read_to_string, OpenOptions};
use serde::{Serialize, Deserialize};
use serde_json;
use log::{debug};
use crate::KvError;

const SEPERATOR: &str = ";";


#[derive(Serialize, Deserialize, Debug)]
enum Operation {
    Set,
    // Get,    
    Remove
}

#[derive(Serialize, Deserialize, Debug)]
struct Command {
    op: Operation,
    key: Option<String>,
    value: Option<String>
}

pub struct KvStore {
    kvs: HashMap<String, String>,
    log_path: Option<String>

}

impl KvStore {
    pub fn new(file_path: &path::Path) -> KvStore {
        KvStore {
            kvs: HashMap::new(),
            log_path: Some(file_path.to_str().unwrap().to_string())
        }
    }

    pub fn open(path: &path::Path) -> Result<KvStore> {
        // println!("dir path {:?}", path.to_str());
        let file_path = path.join("data.tmp");
        let kv_store = KvStore::new(&file_path);
        if !file_path.exists() {
            File::create(file_path)?;
        }
        // kv_store.log_path = Some(path.to_str().unwrap().to_string());
        Ok(kv_store)
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // unimplemented!("unimplemented")
        let cmd = Command {
            op: Operation::Set,
            key: Some(key),
            value: Some(value),
        };
        let ser_cmd = serde_json::to_string(&cmd)? + SEPERATOR;

        // println!("path {}", self.log_path.as_ref().unwrap());

        let mut file = OpenOptions::new()
            .write(true)
            .append(true)   
            .open(self.log_path.as_ref().unwrap())?;        
            
        file.write(ser_cmd.as_bytes())?;
        
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        // read the whole log into a long string
        let log = read_to_string(self.log_path.as_ref().unwrap())?;
        // split the long string into many seperated log entry
        let mut ser_cmds: Vec<&str> = log.split(SEPERATOR).collect();
        // println!("log {}", log);
        // because we leave a SEPERATOR at the end, which causes an empty string in the ser__cmds
        ser_cmds.pop();
        // replay the log
        for ser_cmd in ser_cmds {
            // println!("cmd {}", ser_cmd);
            let cmd: Command = serde_json::from_str(ser_cmd)?;
            match cmd.op {
                Operation::Set => self.kvs.insert(cmd.key.unwrap(), cmd.value.unwrap()),
                Operation::Remove => self.kvs.remove(&cmd.key.unwrap())
            };
        }

        if let Some(value) = self.kvs.get(&key) {
            Ok(Some(value.clone()))
        } else {
            Ok(None)
        }
    }

    pub fn remove(&mut self, key: String) -> Result<Option<String>> {
        // first check whether the key exists
        let value = self.get(key.clone())?;

        if let Some(value) = value {

            let cmd = Command {
                op: Operation::Remove,
                key: Some(key),
                value: None,
            };
            let ser_cmd = serde_json::to_string(&cmd)? + SEPERATOR;

            let mut file = OpenOptions::new()
                .write(true)
                .append(true)   
                .open(self.log_path.as_ref().unwrap())?;        

            file.write(ser_cmd.as_bytes())?;

            Ok(Some(value))

        } else {
            return Err(KvError::KeyNotFound { op: key })
        }
    }
}
