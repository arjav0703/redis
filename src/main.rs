use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time;
pub mod parsers;
mod types;
use types::{KeyWithExpiry, RespHandler, RespValue};

#[tokio::main]
async fn main() -> Result<()> {
    println!("Mini‑Redis listening on 127.0.0.1:6379");
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    let db = Arc::new(tokio::sync::Mutex::new(
        HashMap::<String, KeyWithExpiry>::new(),
    ));

    loop {
        let (stream, _) = listener.accept().await?;

        let db = db.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, db).await {
                eprintln!("connection error: {e:#}");
            }
        });
    }
}

async fn handle_client(
    stream: TcpStream,
    db: Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
) -> Result<()> {
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
                            let mut expiry = None;

                            // Handle PX argument for expiry
                            if items.len() >= 5 {
                                let third_option =
                                    items[3].as_string().map(|s| s.to_ascii_uppercase());
                                if third_option == Some("PX".to_string()) {
                                    if let Some(px_ms) = items.get(4).and_then(|v| v.as_integer()) {
                                        if px_ms > 0 {
                                            expiry = Some(
                                                Instant::now()
                                                    + Duration::from_millis(px_ms as u64),
                                            );
                                            println!("PX detected: {}ms", px_ms);
                                        }
                                    }
                                }
                            }

                            {
                                let mut db = db.lock().await;
                                db.insert(key.clone(), KeyWithExpiry { value: val, expiry });
                                handler
                                    .write_value(RespValue::SimpleString("OK".into()))
                                    .await?;
                                println!("Current DB state: {db:?}");
                            }

                            // Start a background task to handle expiry if needed
                            if expiry.is_some() {
                                let db_clone = Arc::clone(&db);
                                let key_clone = key.clone();
                                let expiry_clone = expiry;
                                tokio::spawn(async move {
                                    if let Some(exp_time) = expiry_clone {
                                        let now = Instant::now();
                                        if exp_time > now {
                                            let sleep_duration = exp_time.duration_since(now);
                                            time::sleep(sleep_duration).await;
                                        }

                                        // Delete the key if it still has the same expiry time
                                        let mut db = db_clone.lock().await;
                                        if let Some(entry) = db.get(&key_clone) {
                                            if let Some(entry_expiry) = entry.expiry {
                                                if entry_expiry <= Instant::now() {
                                                    db.remove(&key_clone);
                                                    println!(
                                                        "Key expired and removed: {}",
                                                        key_clone
                                                    );
                                                }
                                            }
                                        }
                                    }
                                });
                            }
                        }
                        "GET" if items.len() == 2 => {
                            let key = items[1].as_string().unwrap_or_default();
                            println!("GET request for key: {key}");
                            println!("Current DB state: {db:?}");
                            let mut db = db.lock().await;

                            // Check if the key exists and hasn't expired
                            if let Some(entry) = db.get(&key) {
                                if let Some(expiry) = entry.expiry {
                                    if expiry <= Instant::now() {
                                        db.remove(&key);
                                        handler.write_value(RespValue::NullBulkString).await?;
                                        println!("Key expired: {key}");
                                        continue;
                                    }
                                }

                                handler
                                    .write_value(RespValue::BulkString(entry.value.clone()))
                                    .await?;
                                println!("Value found: {}", entry.value);
                            } else {
                                handler.write_value(RespValue::NullBulkString).await?;
                            }
                        }
                        "DEL" if items.len() >= 2 => {
                            let mut db = db.lock().await;
                            let mut deleted_count = 0;
                            for i in 1..items.len() {
                                let key = items[i].as_string().unwrap_or_default();
                                if db.remove(&key).is_some() {
                                    deleted_count += 1;
                                    println!("Deleted key: {key}");
                                }
                            }
                            handler
                                .write_value(RespValue::Integer(deleted_count))
                                .await?;
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
