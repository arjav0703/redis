use crate::parsers::{parse_array, parse_bulk, parse_int, parse_simple};
use anyhow::{anyhow, Result};
use bytes::{Buf, BytesMut};
use std::time::Instant;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

#[derive(Debug, Clone)]
pub struct KeyWithExpiry {
    pub value: String,
    pub expiry: Option<Instant>,
}

pub struct RespHandler {
    stream: TcpStream,
    buf: BytesMut,
}

impl RespHandler {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buf: BytesMut::with_capacity(4096),
        }
    }

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

    pub async fn write_value(&mut self, v: RespValue) -> Result<()> {
        self.stream.write_all(v.encode().as_bytes()).await?;
        Ok(())
    }
}

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
