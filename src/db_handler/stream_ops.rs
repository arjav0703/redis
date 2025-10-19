use crate::types::{
    resp::{RespHandler, RespValue},
    KeyWithExpiry,
};
use anyhow::{Ok, Result};
use std::collections::HashMap;
use std::sync::Arc;

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
        crate::types::ValueType::String(_) => {
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
    // XREAD STREAMS <key1> <key2> ... <id1> <id2> ...
    // Minimum: XREAD STREAMS key id = 4 items

    // Find the STREAMS keyword
    let mut streams_idx = None;
    for (i, item) in items.iter().enumerate() {
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

    let db = db.lock().await;

    // Build response array for all streams
    let mut result_streams = Vec::new();

    for (i, stream_key) in stream_keys.iter().enumerate() {
        let stream_id = &stream_ids[i];

        // Get the stream from db
        let entry = match db.get(stream_key) {
            Some(e) => e.clone(),
            None => {
                // Stream doesn't exist, skip it
                continue;
            }
        };

        let stream = match &entry.value {
            crate::types::ValueType::Stream(s) => s,
            crate::types::ValueType::String(_) => {
                handler
                    .write_value(RespValue::SimpleString(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    ))
                    .await?;
                return Ok(());
            }
        };

        let entries = stream.get_entries_after(stream_id);

        // Build entries array for this stream
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

    // Send the final response
    handler
        .write_value(RespValue::Array(result_streams))
        .await?;

    Ok(())
}
