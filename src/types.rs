pub mod replica;
pub mod resp;
pub mod streams;

use resp::{RespHandler, RespValue};
use std::{env, fmt::Display, time::Instant};
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
    pub appendonly: AppendOnly,
    pub appenddirname: String,
    pub appendfilename: String,
    pub appendfsync: String,
    pub is_replica: bool,
    pub dbfilename: String,
    pub port: String,
}

pub enum AppendOnly {
    Yes,
    No,
}

impl From<String> for AppendOnly {
    fn from(s: String) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "yes" => AppendOnly::Yes,
            "no" => AppendOnly::No,
            _ => AppendOnly::No,
        }
    }
}

impl Display for AppendOnly {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppendOnly::Yes => write!(f, "yes"),
            AppendOnly::No => write!(f, "no"),
        }
    }
}
