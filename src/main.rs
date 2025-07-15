use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::stream;

#[tokio::main]
async fn main() {
    println!("Starting Redis server at port 6379...");
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let stream = listener.accept().await;

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
        //
        None
    }

    fn write_value(&mut self, value: RespValue) {}
}

#[allow(dead_code)]
enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<String>),
    Array(Vec<RespValue>),
}
