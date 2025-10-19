use crate::types::resp::{parse_msg, RespValue};
use anyhow::{anyhow, Result};
use bytes::BytesMut;
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};

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
