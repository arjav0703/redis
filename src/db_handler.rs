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
pub mod geo;
pub mod list_ops;
pub mod pub_sub;
pub mod replica_ops;
pub mod set_key;
pub mod sorted_set;
pub mod stream_ops;
pub mod transactions;

/// Intended to handle the ping command. it the string provided after the ping command or defaults
/// to PONG if nothing ilse is provided
pub async fn send_pong(
    handler: &mut RespHandler,
    items: &[RespValue],
    is_subscribed: bool,
) -> Result<()> {
    // dbg!(&is_subscribed);
    if !is_subscribed {
        let payload = if items.len() > 1 {
            items[1].as_string().unwrap_or_else(|| "PONG".into())
        } else {
            "PONG".into()
        };
        handler
            .write_value(RespValue::SimpleString(payload))
            .await?;
        return Ok(());
    }

    handler
        .write_value(RespValue::Array(vec![
            RespValue::BulkString("pong".into()),
            RespValue::BulkString("".into()),
        ]))
        .await?;
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
            _ => {
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
            crate::types::ValueType::List(_) => {
                handler
                    .write_value(RespValue::SimpleString("list".into()))
                    .await?;
            }
            crate::types::ValueType::SortedSet(_) => {
                handler
                    .write_value(RespValue::SimpleString("set".into()))
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
