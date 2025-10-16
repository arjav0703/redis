use crate::types::{KeyWithExpiry, RespHandler, RespValue};
use anyhow::{Ok, Result};
use std::collections::HashMap;
use std::sync::Arc;

use std::env;
use std::time::{Duration, Instant};
use tokio::time;

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
        db.insert(key.clone(), KeyWithExpiry { value: val, expiry });
        handler
            .write_value(RespValue::SimpleString("OK".into()))
            .await?;
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
            }
        }

        handler
            .write_value(RespValue::BulkString(entry.value.clone()))
            .await?;
        println!("Value found: {}", entry.value);
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
    Ok(())
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

use rand::distr::Alphanumeric;
use rand::Rng;

pub async fn handle_info(handler: &mut RespHandler) -> Result<()> {
    let is_isreplica = !env::var("replicaof").unwrap_or_default().is_empty();
    // dbg!(is_isreplica);
    let role = if is_isreplica { "slave" } else { "master" };
    let info = format!("role:{role}");

    let replication_id = env::var("replication_id").unwrap_or_else(|_| {
        (0..40)
            .map(|_| rand::rng().sample(Alphanumeric) as char)
            .collect()
    });

    let info = format!("{info}\nmaster_replid:{replication_id}\nmaster_repl_offset:0");
    handler.write_value(RespValue::BulkString(info)).await?;
    Ok(())
}

pub async fn handle_replconf(items: &[RespValue], handler: &mut RespHandler) -> Result<()> {
    if items.len() >= 3 {
        let subcommand = items[1].as_string().unwrap_or_default();
        if subcommand.eq_ignore_ascii_case("listening-port") {
            let port = items[2].as_integer().unwrap_or(0);
            println!("REPLCONF listening-port: {port}");
        } else if subcommand.eq_ignore_ascii_case("capa") {
            let capability = items[2].as_string().unwrap_or_default();
            println!("REPLCONF capa: {capability}");
        }
    }

    handler
        .write_value(RespValue::SimpleString("OK".into()))
        .await
        .unwrap_or_else(|e| eprintln!("Failed to send REPLCONF response: {}", e));

    Ok(())
}
