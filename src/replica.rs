use std::env;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug)]
struct ReplicaConfig {
    host: String,
    port: u16,
}

impl ReplicaConfig {
    fn new(host: String, port: String) -> Self {
        Self {
            host,
            port: port.parse().unwrap_or(6379),
        }
    }

    async fn handshake(&self, stream: &mut tokio::net::TcpStream) {
        stream.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
        let _ = stream.read(&mut [0u8; 1024]).await.unwrap();
        let listening_port = env::var("port").unwrap_or_else(|_| "6379".to_string());

        stream
            .write_all(
                format!(
                    "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{listening_port}\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let _ = stream.read(&mut [0u8; 1024]).await.unwrap();

        stream
            .write_all(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
            .await
            .unwrap();
        let _ = stream.read(&mut [0u8; 1024]).await.unwrap();
    }
}

pub async fn replica_handler() {
    let replicaof = env::var("replicaof").unwrap_or_default();

    let (mut host, port) = replicaof.split_once(" ").expect("Invalid replicaof format");

    if host == "localhost" {
        host = "127.0.0.1";
    }

    let master = ReplicaConfig::new(host.to_string(), port.to_string());

    let url = format!("{}:{}", master.host, master.port);

    let mut stream = tokio::net::TcpStream::connect(url).await.unwrap();

    master.handshake(&mut stream).await;
}
