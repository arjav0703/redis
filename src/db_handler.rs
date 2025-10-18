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
        } else if subcommand.eq_ignore_ascii_case("ack") {
            let offset = items[2].as_integer().unwrap_or(0);
            println!("REPLCONF ack: {offset}");
            handler
                .write_bytes(b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n")
                .await?;
        }
    }

    handler
        .write_value(RespValue::SimpleString("OK".into()))
        .await
        .unwrap_or_else(|e| eprintln!("Failed to send REPLCONF response: {}", e));

    Ok(())
}

pub async fn handle_psync(
    items: &[RespValue],
    _db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    handler: &mut RespHandler,
    _replicas: &Arc<tokio::sync::Mutex<Vec<crate::types::ReplicaConnection>>>,
) -> Result<bool> {
    if items.len() >= 2 {
        let replication_id = items[1].as_string().unwrap_or_default();
        let offset = if items.len() >= 3 {
            items[2].as_integer().unwrap_or(0)
        } else {
            0
        };
        println!("PSYNC request: id={replication_id}, offset={offset}");

        // Get the master's replication ID from environment or generate a new one
        let master_replication_id = env::var("replication_id").unwrap_or_else(|_| {
            (0..40)
                .map(|_| rand::rng().sample(Alphanumeric) as char)
                .collect()
        });

        // Respond with FULLRESYNC using the master's replication ID and offset 0
        let response = format!("FULLRESYNC {master_replication_id} 0");
        handler
            .write_value(RespValue::SimpleString(response))
            .await?;
        send_empty_rdb(handler).await?;

        // Return true to indicate this connection should become a replica connection
        Ok(true)
    } else {
        handler
            .write_value(RespValue::SimpleString("ERR invalid PSYNC command".into()))
            .await?;
        Ok(false)
    }
}

async fn send_empty_rdb(handler: &mut RespHandler) -> Result<()> {
    let empty_rdb = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2").unwrap();

    let length_of_file = empty_rdb.len();

    // Send as proper RESP bulk string: $<len>\r\n<data>\r\n
    let header = format!("${}\r\n", length_of_file);
    handler.write_bytes(header.as_bytes()).await?;
    handler.write_bytes(&empty_rdb).await?;

    Ok(())
}

use crate::ReplicaConnection;
pub async fn handle_wait(
    replicas: &Arc<tokio::sync::Mutex<Vec<ReplicaConnection>>>,
    _num_replicas: i64, // Not used - we return the actual count, not the requested count
    timeout_ms: u64,
) -> Result<i64> {
    let replicas_guard = replicas.lock().await;

    // If no replicas connected, return 0
    if replicas_guard.is_empty() {
        return Ok(0);
    }

    // Check if any replica has pending writes (offset > 0)
    let mut has_pending_writes = false;
    for replica in replicas_guard.iter() {
        if replica.get_offset().await > 0 {
            has_pending_writes = true;
            break;
        }
    }

    // If no pending writes, return the number of connected replicas
    if !has_pending_writes {
        return Ok(replicas_guard.len() as i64);
    }

    // Get the current offset for each replica (before sending GETACK)
    let mut expected_offsets = Vec::new();
    for replica in replicas_guard.iter() {
        expected_offsets.push(replica.get_offset().await);
    }

    // Send GETACK to all replicas and collect responses
    let mut ack_futures = Vec::new();
    for (i, replica) in replicas_guard.iter().enumerate() {
        let replica_clone = replica.clone();
        let expected_offset = expected_offsets[i];
        ack_futures.push(tokio::spawn(async move {
            match replica_clone.send_getack_and_wait(timeout_ms).await {
                std::result::Result::Ok(result) => match result {
                    Some(offset) => {
                        // Replica acknowledged if its offset >= expected offset
                        offset >= expected_offset
                    }
                    None => false,
                },
                Err(_) => false,
            }
        }));
    }

    // Wait for all responses (or timeout)
    let mut ack_count = 0i64;
    for future in ack_futures {
        match future.await {
            std::result::Result::Ok(acknowledged) => {
                if acknowledged {
                    ack_count += 1;
                }
            }
            Err(_) => {}
        }
    }

    Ok(ack_count)
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

pub async fn handle_xadd(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
) -> Result<()> {
    // XADD stream_key entry_id field1 value1 [field2 value2 ...]
    // Minimum: XADD key id field value = 5 items
    if items.len() < 5 || (items.len() - 3) % 2 != 0 {
        handler
            .write_value(RespValue::SimpleString(
                "ERR wrong number of arguments for 'xadd' command".into(),
            ))
            .await?;
        return Ok(());
    }

    let key = items[1].as_string().unwrap_or_default();
    let entry_id = items[2].as_string().unwrap_or_default();

    // Parse field-value pairs
    let mut fields = Vec::new();
    let mut i = 3;
    while i < items.len() - 1 {
        let field = items[i].as_string().unwrap_or_default();
        let value = items[i + 1].as_string().unwrap_or_default();
        fields.push((field, value));
        i += 2;
    }

    // get (or create) stream
    let mut db = db.lock().await;
    let entry = db.entry(key.clone()).or_insert_with(|| KeyWithExpiry {
        value: crate::types::ValueType::Stream(crate::types::Stream::new()),
        expiry: None,
    });

    // add the entry
    match &mut entry.value {
        crate::types::ValueType::Stream(stream) => {
            let success = stream.add_entry(entry_id.clone(), fields, handler).await;
            if success {
                handler.write_value(RespValue::BulkString(entry_id)).await?;
            }
        }
        crate::types::ValueType::String(_) => {
            handler
                .write_value(RespValue::SimpleString(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ))
                .await?;
        }
    }

    Ok(())
}
