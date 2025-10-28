pub mod replica;
pub mod resp;
pub mod streams;

use resp::{RespHandler, RespValue};
use std::time::Instant;
use streams::Stream;

/// Enum to represent different value types in Redis
#[derive(Debug, Clone)]
pub enum ValueType {
    String(String),
    List(Vec<String>),
    Stream(Stream),
}

/// Struct to store a key inside the hashmap. It allows you to set an expiry time (optional)
#[derive(Debug, Clone)]
pub struct KeyWithExpiry {
    pub value: ValueType,
    pub expiry: Option<Instant>,
}
