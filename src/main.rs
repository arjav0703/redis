use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::net::{TcpListener, TcpStream};
pub mod parsers;
mod types;
use types::{RespHandler, RespValue};

#[tokio::main]
async fn main() -> Result<()> {
    println!("Mini‑Redis listening on 127.0.0.1:6379");
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    let db = Arc::new(Mutex::new(HashMap::<String, String>::new()));

    loop {
        let (stream, _) = listener.accept().await?;

        let db = db.clone();

        // tokio::spawn(async move {
        if let Err(e) = handle_client(stream, db).await {
            eprintln!("connection error: {e:#}");
        }
        // });
    }
}

async fn handle_client(stream: TcpStream, db: Arc<Mutex<HashMap<String, String>>>) -> Result<()> {
    let mut handler = RespHandler::new(stream);
    // let mut db: HashMap<String, String> = HashMap::new();

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
                            let mut db = db.lock().unwrap();
                            db.insert(key, val);
                            handler
                                .write_value(RespValue::SimpleString("OK".into()))
                                .await?;
                            println!("Current DB state: {:?}", db);
                        }
                        "GET" if items.len() == 2 => {
                            let key = items[1].as_string().unwrap_or_default();
                            println!("GET request for key: {key}");
                            println!("Current DB state: {:?}", db);
                            let db = db.lock().unwrap();
                            if let Some(value) = db.get(&key) {
                                handler
                                    .write_value(RespValue::BulkString(value.clone()))
                                    .await?;
                                println!("Value found: {value}");
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
