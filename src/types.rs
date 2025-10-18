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

#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub id: String,
    pub fields: Vec<(String, String)>, // key-value pairs
}

#[derive(Debug, Clone)]
pub struct Stream {
    pub entries: Vec<StreamEntry>,
}

impl Stream {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub async fn add_entry(
        &mut self,
        id: String,
        fields: Vec<(String, String)>,
        handler: &mut RespHandler,
    ) -> (bool, String) {
        let id = self.auto_generate_id(&id);

        if let Err(e) = self.validate_id(&id) {
            let _ = handler
                .write_value(RespValue::SimpleError(format!("{e}")))
                .await;
            return (false, String::new());
        }
        self.entries.push(StreamEntry {
            id: id.clone(),
            fields,
        });
        (true, id)
    }

    fn auto_generate_id(&self, id: &str) -> String {
        let id_parts: Vec<&str> = id.split('-').collect();
        let seq = id_parts[1];

        let largest_id = self.get_largest_id().unwrap_or("0-0");
        let largest_parts: Vec<&str> = largest_id.split('-').collect();
        let largest_ms = largest_parts[0];
        let largest_seq = largest_parts[1];

        if seq == "*" {
            // If the millisecond part is the same as the largest ID, increment the sequence
            if id_parts[0] == largest_ms {
                if let Ok(largest_seq_num) = largest_seq.parse::<u64>() {
                    let id = format!("{}-{}", id_parts[0], largest_seq_num + 1);
                    dbg!(&id);
                    return id;
                }
            } else {
                // If the millisecond part is different, start sequence at 0
                let id = format!("{}-0", id_parts[0]);
                dbg!(&id);
                return id;
            }
        }

        // If no auto-generation is needed, return the original ID
        id.to_string()
    }

    #[allow(dead_code)]
    pub fn get_entries(&self) -> &[StreamEntry] {
        &self.entries
    }

    fn validate_id(&self, id: &str) -> Result<(), anyhow::Error> {
        let id_parts: Vec<&str> = id.split('-').collect();

        let id_ms: u64 = id_parts[0]
            .parse()
            .map_err(|_| anyhow!("Invalid ID format"))?;
        let id_seq: u64 = id_parts[1]
            .parse()
            .map_err(|_| anyhow!("Invalid ID format"))?;

        // Check if ID is 0-0
        if id_ms == 0 && id_seq == 0 {
            return Err(anyhow!(
                "ERR The ID specified in XADD must be greater than 0-0"
            ));
        }

        let largest_id = self.get_largest_id().unwrap_or("0-0");
        dbg!(&largest_id);
        let largest_parts: Vec<&str> = largest_id.split('-').collect();
        let largest_ms: u64 = largest_parts[0].parse().unwrap_or(0);
        let largest_seq: u64 = largest_parts[1].parse().unwrap_or(0);

        if id_ms < largest_ms {
            return Err(anyhow!(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
            ));
        }

        if id_ms == largest_ms && id_seq <= largest_seq {
            return Err(anyhow!(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
            ));
        }

        Ok(())
    }

    fn get_largest_id(&self) -> Option<&str> {
        self.entries.last().map(|entry| entry.id.as_str())
    }
}

impl StreamEntry {
    #[allow(dead_code)]
    pub fn get_id(&self) -> &str {
        &self.id
    }

    #[allow(dead_code)]
    pub fn get_fields(&self) -> &[(String, String)] {
        &self.fields
    }
}

/// Enum to represent different value types in Redis
#[derive(Debug, Clone)]
pub enum ValueType {
    String(String),
    Stream(Stream),
}

/// Struct to store a key inside the hashmap. It allows you to set an expiry time (optional)
#[derive(Debug, Clone)]
pub struct KeyWithExpiry {
    pub value: ValueType,
    pub expiry: Option<Instant>,
}

/// Struct to represent a connected replica
#[derive(Clone)]
pub struct ReplicaConnection {
    pub stream: Arc<Mutex<TcpStream>>,
    pub offset: Arc<Mutex<i64>>,
}

impl ReplicaConnection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: Arc::new(Mutex::new(stream)),
            offset: Arc::new(Mutex::new(0)),
        }
    }

    pub async fn send_command(&self, command: &RespValue) -> Result<()> {
        let encoded = command.encode();
        let bytes_sent = encoded.len();

        let mut stream = self.stream.lock().await;
        stream.write_all(encoded.as_bytes()).await?;

        // Update offset after sending
        let mut offset = self.offset.lock().await;
        *offset += bytes_sent as i64;

        Ok(())
    }

    pub async fn get_offset(&self) -> i64 {
        *self.offset.lock().await
    }

    pub async fn send_getack_and_wait(&self, timeout_ms: u64) -> Result<Option<i64>> {
        let getack_cmd = RespValue::Array(vec![
            RespValue::BulkString("REPLCONF".to_string()),
            RespValue::BulkString("GETACK".to_string()),
            RespValue::BulkString("*".to_string()),
        ]);

        let encoded = getack_cmd.encode();

        // Send GETACK without incrementing offset (GETACK itself is not counted)
        {
            let mut stream = self.stream.lock().await;
            stream.write_all(encoded.as_bytes()).await?;
        }

        // Wait for response with timeout
        let timeout_duration = std::time::Duration::from_millis(timeout_ms);

        match tokio::time::timeout(timeout_duration, self.read_ack_response()).await {
            Ok(Ok(offset)) => Ok(Some(offset)),
            Ok(Err(_)) => Ok(None),
            Err(_) => Ok(None), // Timeout
        }
    }

    async fn read_ack_response(&self) -> Result<i64> {
        let mut stream = self.stream.lock().await;
        let mut buf = BytesMut::with_capacity(512);

        loop {
            if let Ok((v, _)) = parse_msg(&buf) {
                // Expected response: *3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$<len>\r\n<offset>\r\n
                if let RespValue::Array(items) = v {
                    if items.len() == 3 {
                        if let Some(offset_str) = items[2].as_string() {
                            if let Ok(offset) = offset_str.parse::<i64>() {
                                return Ok(offset);
                            }
                        }
                    }
                }
                return Err(anyhow!("Invalid ACK response"));
            }

            let mut tmp = [0u8; 512];
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                return Err(anyhow!("Connection closed"));
            }
            buf.extend_from_slice(&tmp[..n]);
        }
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

    /// Like `read_value`, but also returns the number of bytes consumed from the
    /// tracking the exact byte-length of commands (e.g. for replication ACKs).
    pub async fn read_value_with_size(&mut self) -> Result<Option<(RespValue, usize)>> {
        loop {
            if let Ok((v, used)) = parse_msg(&self.buf) {
                self.buf.advance(used);
                return Ok(Some((v, used)));
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

    /// Read raw bytes from the stream/buffer for RDB file handling
    pub async fn read_raw_bytes(&mut self, count: usize) -> Result<Vec<u8>> {
        let mut result = Vec::new();

        // First, take from existing buffer
        let from_buf = count.min(self.buf.len());
        result.extend_from_slice(&self.buf[..from_buf]);
        self.buf.advance(from_buf);

        // If we need more, read from stream
        let mut remaining = count - from_buf;
        while remaining > 0 {
            let mut tmp = vec![0u8; remaining.min(4096)];
            let n = self.stream.read(&mut tmp).await?;
            if n == 0 {
                return Err(anyhow!("connection closed while reading raw bytes"));
            }
            result.extend_from_slice(&tmp[..n]);
            remaining -= n;
        }

        Ok(result)
    }

    pub fn get_peer_addr(&self) -> Result<std::net::SocketAddr> {
        Ok(self.stream.peer_addr()?)
    }

    /// Split the handler and extract the underlying stream
    pub fn into_stream(self) -> TcpStream {
        self.stream
    }

    /// Get the current buffer size (for debugging)
    #[allow(dead_code)]
    pub fn buffer_len(&self) -> usize {
        self.buf.len()
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
    SimpleError(String),
    BulkString(String),
    NullBulkString,
    Integer(i64),
    Array(Vec<RespValue>),
}

impl RespValue {
    fn encode(&self) -> String {
        match self {
            RespValue::SimpleString(s) => format!("+{s}\r\n"),
            RespValue::SimpleError(s) => format!("-{s}\r\n"),
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
