pub mod replica;
pub mod resp;
pub mod streams;

use resp::{RespHandler, RespValue};
use std::{env, time::Instant};
use streams::Stream;

/// Enum to represent different value types in Redis
#[derive(Debug, Clone)]
pub enum ValueType {
    String(String),
    List(Vec<String>),
    Stream(Stream),
    SortedSet(Vec<(String, f64)>),
}

/// Struct to store a key inside the hashmap. It allows you to set an expiry time (optional)
#[derive(Debug, Clone)]
pub struct KeyWithExpiry {
    pub value: ValueType,
    pub expiry: Option<Instant>,
    pub is_watched: bool,
}

pub struct ServerConfig {
    pub dir: String,
    pub appendonly: bool,
    pub appenddirname: String,
    pub appendfilename: String,
    pub appendfsync: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            dir: env::current_dir().unwrap().to_str().unwrap().to_string(),
            appendonly: false,
            appenddirname: "appendonlydir".to_string(),
            appendfilename: "appendonly.aof".to_string(),
            appendfsync: "everysec".to_string(),
        }
    }
}
