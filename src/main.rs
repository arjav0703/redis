use anyhow::Result;
use bytes::BytesMut;
use tokio::{io::AsyncWriteExt, net::TcpListener};

#[tokio::main]
async fn main() {
    println!("Starting Redis server at port 6379...");
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let mut stream = listener.accept().await;

        match stream {
            Ok((mut stream, _)) => {
                //println!("accepted new connection");

                tokio::spawn(async move {
                    handle_req(stream).await;
                });
            }
            Err(e) => {
                println!("error: {e}");
            }
        }
    }
}

async fn handle_req(mut stream: tokio::net::TcpStream) {
    let mut buf = [0; 512];
    let mut handler = RespHandler::new(stream);

    loop {
        let value = handler.read_value();

        handler
            .write_value(RespValue::SimpleString("PONG".to_string()))
            .await;
        match value {
            Some(_value) => {
                println!("Received value");
            }
            _ => {
                println!("No value received, waiting for more data...");
            }
        }

        // let request = resp_string_decode(&buf[..read_count]);
        // println!("Received request: {}", request);
    }
}

struct RespHandler {
    stream: tokio::net::TcpStream,
    buffer: BytesMut,
}

impl RespHandler {
    fn new(stream: tokio::net::TcpStream) -> Self {
        RespHandler {
            stream,
            buffer: BytesMut::with_capacity(1024),
        }
    }

    fn read_value(&mut self) -> Option<RespValue> {
        None
    }

    async fn write_value(&mut self, value: RespValue) {
        let response = value.to_string();
        self.stream
            .write_all(value.to_string().as_bytes())
            .await
            .unwrap();
    }
}

#[allow(dead_code)]
enum RespValue {
    SimpleString(String),
    BulkString(String),
    Integer(i64),
    Array(Vec<RespValue>),
}

impl RespValue {
    fn to_string(&self) -> String {
        match self {
            RespValue::SimpleString(s) => format!("+{s}\r\n"),
            RespValue::BulkString(s) => format!("${}\r\n{s}\r\n", s.len()),
            RespValue::Integer(i) => i.to_string(),
            _ => String::new(),
        }
    }
}

fn read_until_clrf(buffer: &[u8]) -> Option<&[u8]> {
    if let Some(pos) = buffer.windows(2).position(|w| w == b"\r\n") {
        Some(&buffer[..pos])
    } else {
        None
    }
}

fn parse_msg(buffer: BytesMut) -> Result<(RespValue, usize)> {
    match buffer[0] as char {
        '+' => {
            let end =
                read_until_clrf(&buffer[1..]).ok_or_else(|| anyhow::anyhow!("Invalid RESP"))?;
            let value = String::from_utf8(end.to_vec())?;
            Ok((RespValue::SimpleString(value), end.len() + 2))
        }
        '$' => {
            let size_end =
                read_until_clrf(&buffer[1..]).ok_or_else(|| anyhow::anyhow!("Invalid RESP"))?;
            let size: usize = String::from_utf8(size_end.to_vec())?.parse()?;
            if size == 0 {
                return Ok((RespValue::BulkString(String::new()), size_end.len() + 2));
            }
            let start = size_end.len() + 2;
            let end = start + size;
            if end > buffer.len() {
                return Err(anyhow::anyhow!("Buffer too small for bulk string"));
            }
            let value = String::from_utf8(buffer[start..end].to_vec())?;
            Ok((RespValue::BulkString(value), end + 2))
        }
        _ => Err(anyhow::anyhow!("Unsupported RESP type")),
    }
}
