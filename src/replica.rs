use crate::types::KeyWithExpiry;
use crate::types::{RespHandler, RespValue};
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
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
        // Send Ping
        stream.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
        let _ = stream.read(&mut [0u8; 1024]).await.unwrap();

        let listening_port = env::var("port").unwrap_or_else(|_| "6379".to_string());

        // Send REPLCONF listening-port <port>
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

        // Send REPLCONF capa psync2
        stream
            .write_all(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
            .await
            .unwrap();
        let _ = stream.read(&mut [0u8; 1024]).await.unwrap();

        let _replication_id = env::var("replication_id").unwrap_or_default();

        // send psync command
        stream
            .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
            .await
            .unwrap();
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

    let mut handler = RespHandler::new(stream);

    println!("Replica: Listening for commands from master...");

    loop {
        match handler.read_value().await {
            std::result::Result::Ok(Some(val)) => {
                if let RespValue::Array(items) = val {
                    if !items.is_empty() {
                        if let Some(cmd) = items[0].as_string() {
                            let cmd_upper = cmd.to_ascii_uppercase();
                            match cmd_upper.as_str() {
                                "SET" | "DEL" => {
                                    // Process write commands silently (no response to master)
                                    println!("Replica: Received {} command from master", cmd_upper);
                                    // The actual processing will be done in the main handle_client
                                    // when we refactor to share the DB
                                }
                                _ => {
                                    // Ignore non-write commands
                                }
                            }
                        }
                    }
                }
            }
            std::result::Result::Ok(None) => {
                println!("Replica: Master connection closed");
                break;
            }
            Err(e) => {
                eprintln!("Replica: Error reading from master: {}", e);
                break;
            }
        }
    }
}
