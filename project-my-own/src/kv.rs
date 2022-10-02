use crate::KvError;
use crate::Result;
use log::debug;
use serde::{Deserialize, Serialize};
use serde_json;
use std::fs::{read_to_string, File, OpenOptions};
use std::io::Write;
use std::os::unix::prelude::FileExt;
use std::{collections::HashMap, path};

const SEPERATOR: &str = ";";

#[derive(Serialize, Deserialize, Debug)]
enum Operation {
    Set,
    // Get,
    Remove,
}

#[derive(Serialize, Deserialize, Debug)]
struct Command {
    op: Operation,
    key: Option<String>,
    value: Option<String>,
}

struct RecordInfo {
    offset: usize,
    len: usize,
}

pub struct KvStore {
    // kvs: HashMap<String, String>,  
    kvs2: HashMap<String, RecordInfo>,  
    log_path: Option<String>,  // the path of the log file
    // TODO: persist this info into a meta file
    offset: usize,   // the offset(location) of the next write(set or rm)
}

impl KvStore {
    pub fn new(file_path: &path::Path) -> KvStore {
        KvStore {
            // kvs: HashMap::new(),
            kvs2: HashMap::new(),
            log_path: Some(file_path.to_str().unwrap().to_string()),
            offset: 0,
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
            key: Some(key.clone()),
            value: Some(value),
        };
        let ser_cmd = serde_json::to_string(&cmd)? + SEPERATOR;

        // println!("path {}", self.log_path.as_ref().unwrap());
        
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(self.log_path.as_ref().unwrap())?;

        file.write(ser_cmd.as_bytes())?;

        // TODO: persist the offset info into a meta file
        if !self.kvs2.is_empty() {
            // if kvs2 isn't empty, which means a get cmd has been run, so we can start to record the offset
            self.kvs2.insert(key, RecordInfo { offset: self.offset, len: ser_cmd.len() - 1});
            // println!("off1 {} len {}", self.offset, ser_cmd.len());
            // println!("cmd {}", ser_cmd);
            self.offset += ser_cmd.len() + 1;
        }

        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if self.kvs2.is_empty() {
            // kvs2 is empty, which means this is a new process that runs the get cmd
            // so we should replay the log from the log file
            assert_eq!(self.offset, 0);

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
                // println!("cmd len {}", ser_cmd.len());
                let cmd: Command = serde_json::from_str(ser_cmd)?;
                // match cmd.op {
                //     Operation::Set => self.kvs.insert(cmd.key.unwrap(), cmd.value.unwrap()),
                //     Operation::Remove => self.kvs.remove(&cmd.key.unwrap()),
                // };
                self.kvs2.insert(cmd.key.unwrap(), RecordInfo{offset: self.offset, len: ser_cmd.len()});
                self.offset += ser_cmd.len() + 1;
            }
        }

        if let Some(info) = self.kvs2.get(&key) {
            let f = File::open(self.log_path.as_ref().unwrap())?;
            let mut buf = vec![0u8; info.len];
            f.read_exact_at(&mut buf, info.offset.try_into().unwrap())?;
            // println!("buf {}", String::from(buf));
            // println!("off2 {} len {}", info.offset, info.len);
            let cmd: Command = serde_json::from_slice(buf.as_slice())?;
            // println!("cmd value {:?}", cmd.value);
            match cmd.op {
                Operation::Set => Ok(cmd.value),
                Operation::Remove => Ok(None)
            }

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
                key: Some(key.clone()),
                value: None,
            };
            let ser_cmd = serde_json::to_string(&cmd)? + SEPERATOR;

            let mut file = OpenOptions::new()
                .write(true)
                .append(true)
                .open(self.log_path.as_ref().unwrap())?;

            file.write(ser_cmd.as_bytes())?;

            // TODO: persist the offset info into a meta file
            if !self.kvs2.is_empty() {
                self.kvs2.insert(key, RecordInfo { offset: self.offset, len: ser_cmd.len() - 1});
                self.offset += ser_cmd.len() + 1;
            }

            Ok(Some(value))
        } else {
            return Err(KvError::KeyNotFound { op: key });
        }
    }
}
