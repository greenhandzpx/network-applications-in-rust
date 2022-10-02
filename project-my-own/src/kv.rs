use crate::KvError;
use crate::Result;
use log::info;
use serde::{Deserialize, Serialize};
use serde_json;
use std::fs;
use std::fs::{read_to_string, File, OpenOptions};
use std::io::Write;
use std::os::unix::prelude::FileExt;
use std::path::PathBuf;
use std::{collections::HashMap, path};
use filename::file_name;

const SEPERATOR: &str = ";";
const MAX_LOG_FILE_SIZE: u64 = 100000;
const LOG_FILE_NAME: &str = "data.log";
const OLD_LOG_FILE_TMP_NAME: &str= "old_data.tmp";
const NEW_LOG_FILE_TMP_NAME: &str= "new_data.tmp";

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
        // env_logger::init();
        KvStore {
            // kvs: HashMap::new(),
            kvs2: HashMap::new(),
            log_path: Some(file_path.to_str().unwrap().to_string()),
            offset: 0,
        }
    }

    pub fn open(path: &path::Path) -> Result<KvStore> {
        // println!("dir path {:?}", path.to_str());
        let file_path = path.join(LOG_FILE_NAME);
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

        self.write_log_record(key, ser_cmd)

        // println!("path {}", self.log_path.as_ref().unwrap());
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if self.kvs2.is_empty() {
            // kvs2 is empty, which means this is a new process that runs the get cmd
            // so we should replay the log from the log file
            self.construct_mem_map()?;
        }

        if let Some(info) = self.kvs2.get(&key) {
            // println!("get key {} off {} len {}", key, info.offset, info.len);
            let f = File::open(self.log_path.as_ref().unwrap())?;
            let mut buf = vec![0u8; info.len];
            f.read_exact_at(&mut buf, info.offset.try_into().unwrap())?;
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

            self.write_log_record(key, ser_cmd)?;

            Ok(Some(value))

        } else {
            return Err(KvError::KeyNotFound { op: key });
        }
    }

    fn write_log_record(&mut self, key: String, ser_cmd: String) -> Result<()> {
        {
            let mut file = OpenOptions::new()
                .write(true)
                .append(true)
                .open(self.log_path.as_ref().unwrap())?;

            let len = file.write(ser_cmd.as_bytes())?;
            assert_eq!(len, ser_cmd.len());
        }


        // TODO: persist the offset info into a meta file
        if self.kvs2.is_empty() {
            // kvs2 is empty, which means this is a new process that runs the cmd
            // so we should replay the log from the log file
            self.construct_mem_map()?;
        } else {
            // if kvs2 isn't empty, which means a get cmd has been run, so we can start to record the offset
            // println!("write key {} off {} len {}", key, self.offset, ser_cmd.len() - 1);
            self.kvs2.insert(key, RecordInfo { offset: self.offset, len: ser_cmd.len() - 1});
            // println!("off1 {} len {}", self.offset, ser_cmd.len());
            // println!("cmd {}", ser_cmd);
            self.offset += ser_cmd.len();
        }

        if self.check_log_size_too_large()? {
            self.compaction()?;
        }

        Ok(())
    }

    fn construct_mem_map(&mut self) -> Result<()> {
        assert_eq!(self.offset, 0);
        // read the whole log into a long string
        let log = read_to_string(self.log_path.as_ref().unwrap())?;
        // split the long string into many seperated log entry
        let mut ser_cmds: Vec<&str> = log.split(SEPERATOR).collect();
        // because we leave a SEPERATOR at the end, which causes an empty string in the ser__cmds
        ser_cmds.pop();
        // replay the log
        for ser_cmd in ser_cmds {
            let cmd: Command = serde_json::from_str(ser_cmd)?;
            self.kvs2.insert(cmd.key.unwrap(), RecordInfo{offset: self.offset, len: ser_cmd.len()});
            self.offset += ser_cmd.len() + 1;
        }
        Ok(())
    }

    pub fn check_log_size_too_large(&self) -> Result<bool> {
        let f = File::open(self.log_path.as_ref().unwrap())?;

        let size = f.metadata()?.len();

        if size > MAX_LOG_FILE_SIZE {
            println!("log size too large {}", size);
        }
        Ok(size > MAX_LOG_FILE_SIZE)
    }

    pub fn compaction(&mut self) -> Result<()> {
        // Our memory map must keep the latest version of the records
        // so we can create a new compacted log file according to the mem map 
        // and replace the old one with it.
        // TODO replace this trivial method with a more efficient one
        println!("log file too large, start to compaction");

        // let mut new_log_file_name = PathBuf::new();
        let mut offset: usize = 0;
        {
            let f = File::open(self.log_path.as_ref().unwrap())?;

            // let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            let mut temp_file = File::create(NEW_LOG_FILE_TMP_NAME)?;
            // let mut temp_file = tempfile::tempfile()?;

            println!("create a tmp file");
            // new_log_file_name = file_name(&temp_file)?;
            // println!("new file name {:?}", new_log_file_name.to_str());

            for kv in &mut self.kvs2 {
                let mut buf = vec![0u8; kv.1.len + 1]; // include the SEPERATOR
                f.read_exact_at(&mut buf, kv.1.offset.try_into().unwrap())?;
                let len = temp_file.write(buf.as_slice())?;
                assert_eq!(len, buf.len());
                println!("key {} old off {} new off {} len {}", kv.0, kv.1.offset, offset, kv.1.len);

                // modify the mem map info
                kv.1.offset = offset;
                offset += len;
            }
        }
        // ensure both old and new file have been closed
        println!("old log file {}", self.log_path.as_ref().unwrap());
        fs::rename(self.log_path.as_ref().unwrap(), OLD_LOG_FILE_TMP_NAME)?;
        println!("rename old log file");
        // println!("new file name {:?}", new_log_file_name.to_str());
        fs::rename(NEW_LOG_FILE_TMP_NAME, self.log_path.as_ref().unwrap())?;
        println!("rename new log file");
        self.offset = offset;
        fs::remove_file(OLD_LOG_FILE_TMP_NAME)?;
        Ok(())
    }
}
