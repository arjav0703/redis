use crate::db_handler;
use crate::types::KeyWithExpiry;
use crate::types::{RespHandler, RespValue};
use std::collections::HashMap;
use std::env;
use std::sync::Arc;

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

    async fn handshake(&self, handler: &mut RespHandler) {
        // Send Ping
        println!("Replica: Sending PING");
        handler
            .write_value(RespValue::Array(vec![RespValue::BulkString(
                "PING".to_string(),
            )]))
            .await
            .expect("Failed to send PING");
        let _ = handler.read_value().await.expect("Failed to read PING response");
        println!("Replica: PING OK");

        let listening_port = env::var("port").unwrap_or_else(|_| "6379".to_string());

        // Send REPLCONF listening-port <port>
        println!("Replica: Sending REPLCONF listening-port");
        handler
            .write_value(RespValue::Array(vec![
                RespValue::BulkString("REPLCONF".to_string()),
                RespValue::BulkString("listening-port".to_string()),
                RespValue::BulkString(listening_port),
            ]))
            .await
            .expect("Failed to send REPLCONF listening-port");
        let _ = handler.read_value().await.expect("Failed to read REPLCONF listening-port response");
        println!("Replica: REPLCONF listening-port OK");

        // Send REPLCONF capa psync2
        println!("Replica: Sending REPLCONF capa");
        handler
            .write_value(RespValue::Array(vec![
                RespValue::BulkString("REPLCONF".to_string()),
                RespValue::BulkString("capa".to_string()),
                RespValue::BulkString("psync2".to_string()),
            ]))
            .await
            .expect("Failed to send REPLCONF capa");
        let _ = handler.read_value().await.expect("Failed to read REPLCONF capa response");
        println!("Replica: REPLCONF capa OK");

        // send psync command
        println!("Replica: Sending PSYNC");
        handler
            .write_value(RespValue::Array(vec![
                RespValue::BulkString("PSYNC".to_string()),
                RespValue::BulkString("?".to_string()),
                RespValue::BulkString("-1".to_string()),
            ]))
            .await
            .expect("Failed to send PSYNC");

        println!("Replica: Sent PSYNC, waiting for response...");

        // Read FULLRESYNC response
        let fullresync = handler.read_value().await.expect("Failed to read FULLRESYNC response");
        println!("Replica: Received FULLRESYNC response: {:?}", fullresync);

        // Read and discard the empty RDB file
        // NOTE: The test master sends RDB as $<len>\r\n<data> WITHOUT trailing \r\n
        // This is non-standard RESP, so we must handle it manually
        println!("Replica: Reading empty RDB file header...");
        
        // Manually parse the bulk string header: $<len>\r\n
        // We'll read until we get the full header, then read exactly <len> bytes
        let mut header_buf = Vec::new();
        let mut found_header = false;
        let mut rdb_len = 0usize;
        
        // Read bytes until we find $<number>\r\n
        while !found_header {
            let byte_result = handler.read_raw_bytes(1).await;
            match byte_result {
                Ok(bytes) if !bytes.is_empty() => {
                    header_buf.push(bytes[0]);
                    
                    // Check if we have a complete header
                    if header_buf.len() >= 4 && header_buf[header_buf.len()-2..] == [b'\r', b'\n'] {
                        // Parse the header: $<len>\r\n
                        if header_buf[0] == b'$' {
                            let len_str = String::from_utf8_lossy(&header_buf[1..header_buf.len()-2]);
                            if let Ok(len) = len_str.parse::<usize>() {
                                rdb_len = len;
                                found_header = true;
                                println!("Replica: RDB file size: {} bytes", rdb_len);
                            }
                        }
                    }
                }
                _ => break,
            }
        }
        
        if found_header {
            // Now read exactly rdb_len bytes
            let _rdb_data = handler.read_raw_bytes(rdb_len).await
                .expect("Failed to read RDB data");
            println!("Replica: Read {}-byte RDB file", rdb_len);
        } else {
            panic!("Failed to read RDB header");
        }

        println!("Replica: Handshake complete, ready to receive commands");
        
        // Give a small moment for any pending commands to arrive in the buffer
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
}

pub async fn replica_handler(db: Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>) {
    let replicaof = env::var("replicaof").unwrap_or_default();
    println!("Replica: Starting with replicaof={}", replicaof);

    let (mut host, port) = replicaof.split_once(" ").expect("Invalid replicaof format");

    if host == "localhost" {
        host = "127.0.0.1";
    }

    let master = ReplicaConfig::new(host.to_string(), port.to_string());

    let url = format!("{}:{}", master.host, master.port);
    println!("Replica: Connecting to master at {}", url);

    let stream = match tokio::net::TcpStream::connect(&url).await {
        Ok(s) => {
            println!("Replica: Connected to master");
            s
        }
        Err(e) => {
            eprintln!("Replica: Failed to connect to master: {}", e);
            return;
        }
    };

    let mut handler = RespHandler::new(stream);

    println!("Replica: Starting handshake");
    master.handshake(&mut handler).await;
    println!("Replica: Handshake complete");

    println!("Replica: Listening for commands from master...");

    let mut command_count = 0;
    loop {
        match handler.read_value().await {
            std::result::Result::Ok(Some(val)) => {
                command_count += 1;
                println!("Replica: Received command #{}: {:?}", command_count, val);
                if let RespValue::Array(items) = val {
                    if !items.is_empty() {
                        if let Some(cmd) = items[0].as_string() {
                            let cmd_upper = cmd.to_ascii_uppercase();
                            match cmd_upper.as_str() {
                                "SET" if items.len() >= 3 => {
                                    println!("Replica: Processing SET command");
                                    if let Err(e) = db_handler::set_key_silent(&db, &items).await {
                                        eprintln!("Replica: Error processing SET: {}", e);
                                    } else {
                                        println!("Replica: Successfully processed SET");
                                    }
                                }
                                "DEL" if items.len() >= 2 => {
                                    println!("Replica: Processing DEL command");
                                    if let Err(e) = db_handler::del_key_silent(&db, &items).await {
                                        eprintln!("Replica: Error processing DEL: {}", e);
                                    } else {
                                        println!("Replica: Successfully processed DEL");
                                    }
                                }
                                _ => {
                                    println!("Replica: Ignored command: {}", cmd_upper);
                                }
                            }
                        }
                    }
                } else {
                    println!("Replica: Received non-array value: {:?}", val);
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
