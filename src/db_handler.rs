use crate::types::{
    resp::{RespHandler, RespValue},
    KeyWithExpiry,
};
use anyhow::{Ok, Result};
use std::collections::HashMap;
use std::sync::Arc;

use std::env;
use std::time::{Duration, Instant};
use tokio::time;
pub mod replica_ops;
pub mod stream_ops;

/// Intended to handle the ping command. it the string provided after the ping command or defaults
/// to PONG if nothing ilse is provided
pub async fn send_pong(handler: &mut RespHandler, items: &[RespValue]) -> Result<()> {
    let payload = if items.len() > 1 {
        items[1].as_string().unwrap_or_else(|| "PONG".into())
    } else {
        "PONG".into()
    };
    handler
        .write_value(RespValue::SimpleString(payload))
        .await?;
    Ok(())
}

/// Pretty self explainatory: used to swt a key into the db
pub async fn set_key(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
) -> Result<()> {
    set_key_internal(db, items).await?;
    handler
        .write_value(RespValue::SimpleString("OK".into()))
        .await?;
    Ok(())
}

/// Silent version of set_key for replica processing (no response sent)
pub async fn set_key_silent(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
) -> Result<()> {
    set_key_internal(db, items).await
}

/// Internal implementation of set_key logic without response handling
async fn set_key_internal(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
) -> Result<()> {
    let key = items[1].as_string().unwrap_or_default();
    let val = items[2].as_string().unwrap_or_default();
    let mut expiry = None;

    // Handle PX argument for expiry
    if items.len() >= 5 {
        let third_option = items[3].as_string().map(|s| s.to_ascii_uppercase());
        if third_option == Some("PX".to_string()) {
            if let Some(px_ms) = items.get(4).and_then(|v| v.as_integer()) {
                if px_ms > 0 {
                    expiry = Some(Instant::now() + Duration::from_millis(px_ms as u64));
                    println!("PX detected: {px_ms}ms");
                }
            }
        }
    }

    {
        let mut db = db.lock().await;
        db.insert(
            key.clone(),
            KeyWithExpiry {
                value: crate::types::ValueType::String(val),
                expiry,
            },
        );
        println!("Current DB state: {db:?}");
    }

    // Start a background task to handle expiry if needed
    if expiry.is_some() {
        let db_clone = Arc::clone(db);
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
                            println!("Key expired and removed: {key_clone}");
                        }
                    }
                }
            }
        });
    }
    Ok(())
}

/// Pretty self explainatory: used to get a key from the db
pub async fn get_key(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
) -> Result<()> {
    let key = items[1].as_string().unwrap_or_default();
    println!("GET request for key: {key}");
    println!("Current DB state: {db:?}");
    let mut db = db.lock().await;

    // Check if the key exists and hasn't expired
    if let Some(entry) = db.clone().get(&key) {
        if let Some(expiry) = entry.expiry {
            if expiry <= Instant::now() {
                db.remove(&key);
                handler.write_value(RespValue::NullBulkString).await?;
                println!("Key expired: {key}");
                return Ok(());
            }
        }

        match &entry.value {
            crate::types::ValueType::String(s) => {
                handler
                    .write_value(RespValue::BulkString(s.clone()))
                    .await?;
                println!("Value found: {}", s);
            }
            crate::types::ValueType::Stream(_) => {
                // GET on a stream should return an error or null
                handler
                    .write_value(RespValue::SimpleString(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    ))
                    .await?;
            }
        }
    } else {
        handler.write_value(RespValue::NullBulkString).await?;
    }
    Ok(())
}

/// Pretty self explainatory: used to delete a key from the db
pub async fn del_key(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
) -> Result<()> {
    let deleted_count = del_key_internal(db, items).await?;
    handler
        .write_value(RespValue::Integer(deleted_count))
        .await?;
    Ok(())
}

/// Silent version of del_key for replica processing (no response sent)
pub async fn del_key_silent(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
) -> Result<()> {
    del_key_internal(db, items).await?;
    Ok(())
}

/// Internal implementation of del_key logic without response handling
async fn del_key_internal(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
) -> Result<i64> {
    let mut db = db.lock().await;
    let mut deleted_count = 0;
    for i in 1..items.len() {
        let key = items[i].as_string().unwrap_or_default();
        if db.remove(&key).is_some() {
            deleted_count += 1;
            println!("Deleted key: {key}");
        }
    }
    Ok(deleted_count)
}

/// Still in work, intended to handle the config command
pub async fn handle_config(
    // db: Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
) -> Result<()> {
    let subcommand = items.get(1).and_then(|v| v.as_string());
    match subcommand {
        Some(cmd) if cmd.eq_ignore_ascii_case("GET") => {
            if items.len() == 3 {
                let key = items[2].as_string().unwrap_or_default();
                match key.to_ascii_uppercase().as_str() {
                    "DIR" => {
                        let dir = env::var("dir").unwrap_or_default();
                        let resp_vec = vec![
                            RespValue::BulkString("dir".into()),
                            RespValue::BulkString(dir),
                        ];
                        handler.write_value(RespValue::Array(resp_vec)).await?;
                    }
                    "DBFILENAME" => {
                        let dbfilename = env::var("dbfilename").unwrap_or_default();
                        let resp_vec = vec![
                            RespValue::BulkString("dbfilename".into()),
                            RespValue::BulkString(dbfilename),
                        ];
                        handler.write_value(RespValue::Array(resp_vec)).await?;
                    }
                    _ => {
                        handler.write_value(RespValue::NullBulkString).await?;
                    }
                }
            } else {
                handler.write_value(RespValue::NullBulkString).await?;
            }
        }
        _ => {
            handler
                .write_value(RespValue::SimpleString("OK".into()))
                .await?;
        }
    }

    Ok(())
}

pub async fn handle_key_search(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    pattern: &str,
    handler: &mut RespHandler,
) -> Result<()> {
    let db = db.lock().await;
    let mut keys_found = Vec::new();

    if pattern == "*" {
        for key in db.keys() {
            keys_found.push(RespValue::BulkString(key.clone()));
        }
    } else {
        for key in db.keys() {
            if key.contains(pattern) {
                keys_found.push(RespValue::BulkString(key.clone()));
            }
        }
    }
    if keys_found.is_empty() {
        handler.write_value(RespValue::NullBulkString).await?;
    } else {
        handler.write_value(RespValue::Array(keys_found)).await?;
    }

    Ok(())
}

pub async fn handle_type(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
) -> Result<()> {
    let key = items[1].as_string().unwrap_or_default();
    let db = db.lock().await;

    if let Some(entry) = db.get(&key) {
        match &entry.value {
            crate::types::ValueType::String(_) => {
                handler
                    .write_value(RespValue::SimpleString("string".into()))
                    .await?;
            }
            crate::types::ValueType::Stream(_) => {
                handler
                    .write_value(RespValue::SimpleString("stream".into()))
                    .await?;
            }
        }
    } else {
        handler
            .write_value(RespValue::SimpleString("none".into()))
            .await?;
    }

    Ok(())
}
