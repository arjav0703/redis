use crate::types::{
    resp::{RespHandler, RespValue},
    KeyWithExpiry,
};
use anyhow::{Ok, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::broadcast;

static STREAM_NOTIFIER: OnceLock<broadcast::Sender<String>> = OnceLock::new();

fn get_stream_notifier() -> &'static broadcast::Sender<String> {
    STREAM_NOTIFIER.get_or_init(|| {
        let (tx, _) = broadcast::channel(100);
        tx
    })
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
        value: crate::types::ValueType::Stream(crate::types::streams::Stream::new()),
        expiry: None,
    });

    // add the entry
    match &mut entry.value {
        crate::types::ValueType::Stream(stream) => {
            let (success, entry_id) = stream.add_entry(entry_id.clone(), fields, handler).await;
            if success {
                handler.write_value(RespValue::BulkString(entry_id)).await?;
                // Notify any waiting XREAD commands
                let _ = get_stream_notifier().send(key.clone());
            }
        }
        _ => {
            handler
                .write_value(RespValue::SimpleString(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ))
                .await?;
        }
    }

    Ok(())
}

pub async fn handle_xrange(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
) -> Result<()> {
    let stream_key = items[1].as_string().unwrap_or_default();

    let start_id = items[2].as_string().unwrap_or_default();
    let end_id = items[3].as_string().unwrap_or_default();

    let db = db.lock().await;

    let entry: KeyWithExpiry = db.get(&stream_key).unwrap().clone();
    dbg!(&entry);
    let stream = match &entry.value {
        crate::types::ValueType::Stream(s) => s,
        _ => {
            handler
                .write_value(RespValue::SimpleString(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ))
                .await?;
            return Ok(());
        }
    };

    let range = stream.get_range(&start_id, &end_id);
    dbg!(&range);

    let mut resp_array = Vec::new();
    for stream_entry in range {
        let mut entry_array = Vec::new();
        entry_array.push(RespValue::BulkString(stream_entry.id.clone()));

        let mut field_value_array = Vec::new();
        for (field, value) in &stream_entry.fields {
            field_value_array.push(RespValue::BulkString(field.clone()));
            field_value_array.push(RespValue::BulkString(value.clone()));
        }

        entry_array.push(RespValue::Array(field_value_array));
        resp_array.push(RespValue::Array(entry_array));
    }

    handler.write_value(RespValue::Array(resp_array)).await?;

    Ok(())
}

pub async fn handle_xread(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
) -> Result<()> {
    // XREAD [BLOCK milliseconds] STREAMS <key1> <key2> ... <id1> <id2> ...
    // Minimum: XREAD STREAMS key id = 4 items

    let mut block_timeout_ms: Option<u64> = None;
    let mut start_idx = 1;

    if items.len() > 2 {
        if let Some(s) = items[1].as_string() {
            if s.eq_ignore_ascii_case("block") {
                if let Some(timeout_str) = items[2].as_string() {
                    block_timeout_ms = timeout_str.parse().ok();
                    start_idx = 3;
                }
            }
        }
    }

    // Find the STREAMS keyword
    let mut streams_idx = None;
    for (i, item) in items.iter().enumerate().skip(start_idx) {
        if let Some(s) = item.as_string() {
            if s.eq_ignore_ascii_case("streams") {
                streams_idx = Some(i);
                break;
            }
        }
    }

    let streams_idx = match streams_idx {
        Some(idx) => idx,
        None => {
            handler
                .write_value(RespValue::SimpleString(
                    "ERR wrong number of arguments for 'xread' command".into(),
                ))
                .await?;
            return Ok(());
        }
    };

    // Calculate the number of streams
    let total_args = items.len() - streams_idx - 1;
    if total_args < 2 || total_args % 2 != 0 {
        handler
            .write_value(RespValue::SimpleString(
                "ERR wrong number of arguments for 'xread' command".into(),
            ))
            .await?;
        return Ok(());
    }

    let num_streams = total_args / 2;
    let keys_start = streams_idx + 1;
    let ids_start = keys_start + num_streams;

    // Extract stream keys and IDs
    let mut stream_keys = Vec::new();
    let mut stream_ids = Vec::new();

    for i in 0..num_streams {
        let key = items[keys_start + i].as_string().unwrap_or_default();
        let id = items[ids_start + i].as_string().unwrap_or_default();
        stream_keys.push(key);
        stream_ids.push(id);
    }

    // Replace $ with the largest ID in each stream
    {
        let db_lock = db.lock().await;
        for (i, stream_id) in stream_ids.iter_mut().enumerate() {
            if stream_id == "$" {
                let stream_key = &stream_keys[i];

                // Get the current largest ID from the stream
                if let Some(entry) = db_lock.get(stream_key) {
                    match &entry.value {
                        crate::types::ValueType::Stream(s) => {
                            *stream_id = s.get_largest_id().unwrap_or("0-0").to_string();
                        }
                        _ => {}
                    }
                } else {
                    // Stream doesn't exist yet, use "0-0"
                    *stream_id = "0-0".to_string();
                }
            }
        }
    }

    // Helper function to build the response
    let build_response = |db_lock: &HashMap<String, KeyWithExpiry>| -> Result<Vec<RespValue>> {
        let mut result_streams = Vec::new();

        for (i, stream_key) in stream_keys.iter().enumerate() {
            let stream_id = &stream_ids[i];

            let entry = match db_lock.get(stream_key) {
                Some(e) => e.clone(),
                None => {
                    continue;
                }
            };

            let stream = match &entry.value {
                crate::types::ValueType::Stream(s) => s,
                _ => {
                    continue;
                }
            };

            let entries = stream.get_entries_after(stream_id);

            if !entries.is_empty() {
                let mut entries_array = Vec::new();
                for entry in entries {
                    let mut entry_array = Vec::new();
                    entry_array.push(RespValue::BulkString(entry.id.clone()));

                    let mut field_value_array = Vec::new();
                    for (field, value) in &entry.fields {
                        field_value_array.push(RespValue::BulkString(field.clone()));
                        field_value_array.push(RespValue::BulkString(value.clone()));
                    }

                    entry_array.push(RespValue::Array(field_value_array));
                    entries_array.push(RespValue::Array(entry_array));
                }

                // Add this stream to the result (key + entries)
                result_streams.push(RespValue::Array(vec![
                    RespValue::BulkString(stream_key.clone()),
                    RespValue::Array(entries_array),
                ]));
            }
        }

        Ok(result_streams)
    };

    if let Some(timeout_ms) = block_timeout_ms {
        // Subscribe to stream notifications before checking the database
        let mut rx = get_stream_notifier().subscribe();

        loop {
            // Check current state
            let db_lock = db.lock().await;
            let result_streams = build_response(&db_lock)?;
            drop(db_lock);

            // If we have results, return them immediately
            if !result_streams.is_empty() {
                handler
                    .write_value(RespValue::Array(result_streams))
                    .await?;
                return Ok(());
            }

            // No results, wait for notification or timeout
            // If timeout_ms is 0, block indefinitely
            if timeout_ms == 0 {
                // Block indefinitely until notification
                match rx.recv().await {
                    Result::Ok(_notification) => {
                        // New entry added, loop back to check if it's relevant
                        continue;
                    }
                    Err(_) => {
                        // Channel closed, shouldn't happen but handle gracefully
                        break;
                    }
                }
            } else {
                // Block with timeout
                let timeout_duration = std::time::Duration::from_millis(timeout_ms);
                match tokio::time::timeout(timeout_duration, rx.recv()).await {
                    Result::Ok(Result::Ok(_notification)) => {
                        // New entry added, loop back
                        continue;
                    }
                    Result::Ok(Err(_)) => {
                        // Channel closed
                        break;
                    }
                    Err(_) => {
                        // Timeout expired
                        break;
                    }
                }
            }
        }

        // Timeout expired with no results, return null array
        // Note: This should only be reached if timeout_ms > 0 and timeout expires,
        // or if the channel is closed unexpectedly
        handler.write_bytes(b"*-1\r\n").await?;
    } else {
        // Non-blocking: just return current results
        let db_lock = db.lock().await;
        let result_streams = build_response(&db_lock)?;
        handler
            .write_value(RespValue::Array(result_streams))
            .await?;
    }

    Ok(())
}
