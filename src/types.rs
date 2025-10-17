use crate::parsers::{parse_array, parse_bulk, parse_int, parse_simple};
use anyhow::{anyhow, Result};
use bytes::{Buf, BytesMut};
use std::sync::Arc;
use std::time::Instant;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};

/// Struct to store a key inside the hashmap. It allows you to set an expiry time (optional)
#[derive(Debug, Clone)]
pub struct KeyWithExpiry {
    pub value: String,
    pub expiry: Option<Instant>,
}

/// Struct to represent a connected replica
#[derive(Clone)]
pub struct ReplicaConnection {
    pub stream: Arc<Mutex<TcpStream>>,
}

impl ReplicaConnection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: Arc::new(Mutex::new(stream)),
        }
    }

    pub async fn send_command(&self, command: &RespValue) -> Result<()> {
        let mut stream = self.stream.lock().await;
        stream.write_all(command.encode().as_bytes()).await?;
        Ok(())
    }
}

/// Pretty obvious
pub struct RespHandler {
    stream: TcpStream,
    buf: BytesMut,
}

impl RespHandler {
    /// Use to create a new one to start off with
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buf: BytesMut::with_capacity(4096),
        }
    }

    /// idk what it does (vibe coded)
    pub async fn read_value(&mut self) -> Result<Option<RespValue>> {
        loop {
            if let Ok((v, used)) = parse_msg(&self.buf) {
                self.buf.advance(used);
                return Ok(Some(v));
            }
            let mut tmp = [0u8; 1024];
            let n = self.stream.read(&mut tmp).await?;
            if n == 0 {
                return Ok(None); // client closed
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }

    /// Writes stuff to the stream. it takes a respvalue enum (see below) as argument.
    pub async fn write_value(&mut self, v: RespValue) -> Result<()> {
        self.stream.write_all(v.encode().as_bytes()).await?;
        Ok(())
    }

    pub async fn write_bytes(&mut self, raw_bytes: &[u8]) -> Result<()> {
        self.stream.write_all(raw_bytes).await?;
        Ok(())
    }

    pub fn get_peer_addr(&self) -> Result<std::net::SocketAddr> {
        Ok(self.stream.peer_addr()?)
    }

    /// Split the handler and extract the underlying stream
    pub fn into_stream(self) -> TcpStream {
        self.stream
    }
}

/// A simple check to determine if the incoming data (expected to be a RESP datatype) is of what
/// type
pub fn parse_msg(buf: &[u8]) -> Result<(RespValue, usize)> {
    if buf.is_empty() {
        return Err(anyhow!("need more data"));
    }
    match buf[0] {
        b'+' => parse_simple(buf),
        b'$' => parse_bulk(buf),
        b':' => parse_int(buf),
        b'*' => parse_array(buf),
        _ => Err(anyhow!("unknown type")),
    }
}

#[derive(Debug, Clone)]
pub enum RespValue {
    SimpleString(String),
    BulkString(String),
    NullBulkString,
    Integer(i64),
    Array(Vec<RespValue>),
}

impl RespValue {
    fn encode(&self) -> String {
        match self {
            RespValue::SimpleString(s) => format!("+{s}\r\n"),
            RespValue::BulkString(s) => format!("${}\r\n{s}\r\n", s.len()),
            RespValue::NullBulkString => {
                println!("Encoding NullBulkString as $-1\\r\\n");
                "$-1\r\n".to_string()
            }
            RespValue::Integer(i) => format!(":{i}\r\n"),
            RespValue::Array(items) => {
                let mut out = format!("*{}\r\n", items.len());
                for item in items {
                    out.push_str(&item.encode());
                }
                out
            }
        }
    }

    pub fn as_string(&self) -> Option<String> {
        match self {
            RespValue::BulkString(s) | RespValue::SimpleString(s) => Some(s.clone()),
            _ => None,
        }
    }

    pub fn as_integer(&self) -> Option<i64> {
        match self {
            RespValue::Integer(i) => Some(*i),
            RespValue::BulkString(s) | RespValue::SimpleString(s) => s.parse::<i64>().ok(),
            _ => None,
        }
    }
}
