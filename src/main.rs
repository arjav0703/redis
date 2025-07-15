use anyhow::{anyhow, Result};
use bytes::{Buf, BytesMut};
use std::collections::HashMap;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> Result<()> {
    println!("Mini‑Redis listening on 127.0.0.1:6379");
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(e) = handle_client(stream).await {
                eprintln!("connection error: {e:#}");
            }
        });
    }
}

async fn handle_client(stream: TcpStream) -> Result<()> {
    let mut handler = RespHandler::new(stream);
    let mut db: HashMap<String, String> = HashMap::new();

    while let Some(val) = handler.read_value().await? {
        match val {
            RespValue::Array(items) if !items.is_empty() => {
                if let RespValue::BulkString(cmd) | RespValue::SimpleString(cmd) = &items[0] {
                    let cmd_upper = cmd.to_ascii_uppercase();
                    match cmd_upper.as_str() {
                        "PING" => {
                            let payload = if items.len() > 1 {
                                items[1].as_string().unwrap_or_else(|| "PONG".into())
                            } else {
                                "PONG".into()
                            };
                            handler
                                .write_value(RespValue::SimpleString(payload))
                                .await?;
                        }
                        "ECHO" if items.len() == 2 => {
                            handler.write_value(items[1].clone()).await?;
                        }
                        "SET" if items.len() >= 3 => {
                            let key = items[1].as_string().unwrap_or_default();
                            let val = items[2].as_string().unwrap_or_default();
                            db.insert(key, val);
                            handler
                                .write_value(RespValue::SimpleString("OK".into()))
                                .await?;
                        }
                        "GET" if items.len() == 2 => {
                            let key = items[1].as_string().unwrap_or_default();
                            if let Some(value) = db.get(&key) {
                                handler
                                    .write_value(RespValue::BulkString(value.clone()))
                                    .await?;
                            } else {
                                handler
                                    .write_value(RespValue::BulkString(String::new()))
                                    .await?;
                            }
                        }
                        _ => {
                            handler
                                .write_value(RespValue::SimpleString("ERR unknown command".into()))
                                .await?;
                        }
                    }
                }
            }
            _ => {
                // Anything else -> just PONG
                handler
                    .write_value(RespValue::SimpleString("PONG".into()))
                    .await?;
            }
        }
    }
    Ok(())
}

struct RespHandler {
    stream: TcpStream,
    buf: BytesMut,
}

impl RespHandler {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buf: BytesMut::with_capacity(4096),
        }
    }

    async fn read_value(&mut self) -> Result<Option<RespValue>> {
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

    async fn write_value(&mut self, v: RespValue) -> Result<()> {
        self.stream.write_all(v.encode().as_bytes()).await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum RespValue {
    SimpleString(String),
    BulkString(String),
    Integer(i64),
    Array(Vec<RespValue>),
}

impl RespValue {
    fn encode(&self) -> String {
        match self {
            RespValue::SimpleString(s) => format!("+{s}\r\n"),
            RespValue::BulkString(s) => format!("${}\r\n{s}\r\n", s.len()),
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

    fn as_string(&self) -> Option<String> {
        match self {
            RespValue::BulkString(s) | RespValue::SimpleString(s) => Some(s.clone()),
            _ => None,
        }
    }
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

fn parse_msg(buf: &[u8]) -> Result<(RespValue, usize)> {
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

// PARSERS

fn parse_simple(buf: &[u8]) -> Result<(RespValue, usize)> {
    let end = find_crlf(&buf[1..]).ok_or(anyhow!("incomplete simple"))?;
    let s = std::str::from_utf8(&buf[1..1 + end])?.to_owned();
    Ok((RespValue::SimpleString(s), 1 + end + 2))
}

fn parse_bulk(buf: &[u8]) -> Result<(RespValue, usize)> {
    let len_end = find_crlf(&buf[1..]).ok_or(anyhow!("incomplete bulk len"))?;
    let len: usize = std::str::from_utf8(&buf[1..1 + len_end])?.parse()?;
    let start = 1 + len_end + 2;
    let end = start + len;
    if buf.len() < end + 2 {
        return Err(anyhow!("incomplete bulk body"));
    }
    let s = std::str::from_utf8(&buf[start..end])?.to_owned();
    Ok((RespValue::BulkString(s), end + 2))
}

fn parse_int(buf: &[u8]) -> Result<(RespValue, usize)> {
    let end = find_crlf(&buf[1..]).ok_or(anyhow!("incomplete int"))?;
    let n: i64 = std::str::from_utf8(&buf[1..1 + end])?.parse()?;
    Ok((RespValue::Integer(n), 1 + end + 2))
}

fn parse_array(buf: &[u8]) -> Result<(RespValue, usize)> {
    let len_end = find_crlf(&buf[1..]).ok_or(anyhow!("incomplete array len"))?;
    let count: usize = std::str::from_utf8(&buf[1..1 + len_end])?.parse()?;
    let mut consumed = 1 + len_end + 2;
    let mut items = Vec::with_capacity(count);

    for _ in 0..count {
        let (item, used) = parse_msg(&buf[consumed..])?;
        consumed += used;
        items.push(item);
    }
    Ok((RespValue::Array(items), consumed))
}

// a