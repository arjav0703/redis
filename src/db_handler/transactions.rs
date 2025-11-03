use super::*;
use std::result::Result::Ok;

pub async fn handle_multi(handler: &mut RespHandler, in_transaction: &mut bool) -> Result<()> {
    *in_transaction = true;
    handler
        .write_value(RespValue::SimpleString("OK".to_string()))
        .await?;
    Ok(())
}

pub async fn handle_exec(
    handler: &mut RespHandler,
    in_transaction: &mut bool,
    queued_commands: &mut Vec<RespValue>,
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    replicas: &Arc<tokio::sync::Mutex<Vec<crate::types::replica::ReplicaConnection>>>,
) -> Result<()> {
    if !*in_transaction {
        handler
            .write_value(RespValue::SimpleError("ERR EXEC without MULTI".to_string()))
            .await?;
        return Ok(());
    }

    *in_transaction = false;

    let mut responses = Vec::new();

    for cmd in queued_commands.iter() {
        if let RespValue::Array(items) = cmd {
            let response = execute_command(items, db, replicas).await?;
            responses.push(response);
        }
    }

    queued_commands.clear();
    handler.write_value(RespValue::Array(responses)).await?;
    Ok(())
}

async fn execute_command(
    items: &[RespValue],
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    replicas: &Arc<tokio::sync::Mutex<Vec<crate::types::replica::ReplicaConnection>>>,
) -> Result<RespValue> {
    if items.is_empty() {
        return Ok(RespValue::SimpleError("ERR empty command".to_string()));
    }

    let cmd = match &items[0] {
        RespValue::BulkString(s) | RespValue::SimpleString(s) => s.to_ascii_uppercase(),
        _ => return Ok(RespValue::SimpleError("ERR invalid command".to_string())),
    };

    match cmd.as_str() {
        "SET" if items.len() >= 3 => execute_set(items, db, replicas).await,
        "GET" if items.len() == 2 => execute_get(items, db).await,
        "INCR" if items.len() == 2 => execute_incr(items, db, replicas).await,
        "DEL" if items.len() >= 2 => execute_del(items, db, replicas).await,
        _ => Ok(RespValue::SimpleError(format!(
            "ERR unknown command '{}'",
            cmd
        ))),
    }
}

async fn execute_set(
    items: &[RespValue],
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    _replicas: &Arc<tokio::sync::Mutex<Vec<crate::types::replica::ReplicaConnection>>>,
) -> Result<RespValue> {
    use crate::db_handler::set_key::set_key_internal;
    set_key_internal(db, items).await?;

    Ok(RespValue::SimpleString("OK".to_string()))
}

async fn execute_get(
    items: &[RespValue],
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
) -> Result<RespValue> {
    let key = items[1].as_string().unwrap_or_default();
    let mut db_guard = db.lock().await;

    if let Some(entry) = db_guard.clone().get(&key) {
        if let Some(expiry) = entry.expiry {
            if expiry <= std::time::Instant::now() {
                db_guard.remove(&key);
                return Ok(RespValue::NullBulkString);
            }
        }

        match &entry.value {
            crate::types::ValueType::String(s) => Ok(RespValue::BulkString(s.clone())),
            _ => Ok(RespValue::SimpleError(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            )),
        }
    } else {
        Ok(RespValue::NullBulkString)
    }
}

async fn execute_incr(
    items: &[RespValue],
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    _replicas: &Arc<tokio::sync::Mutex<Vec<crate::types::replica::ReplicaConnection>>>,
) -> Result<RespValue> {
    let key = items[1].as_string().unwrap_or_default();
    let mut db_guard = db.lock().await;

    let new_value = if let Some(entry) = db_guard.get(&key) {
        match &entry.value {
            crate::types::ValueType::String(s) => {
                let num = s
                    .parse::<i64>()
                    .map_err(|_| anyhow::anyhow!("ERR value is not an integer or out of range"))?;
                num + 1
            }
            _ => {
                return Ok(RespValue::SimpleError(
                    "ERR value is not an integer or out of range".to_string(),
                ));
            }
        }
    } else {
        1
    };

    let expiry = db_guard.get(&key).and_then(|e| e.expiry);
    db_guard.insert(
        key.clone(),
        KeyWithExpiry {
            value: crate::types::ValueType::String(new_value.to_string()),
            expiry,
        },
    );

    Ok(RespValue::Integer(new_value))
}

async fn execute_del(
    items: &[RespValue],
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    _replicas: &Arc<tokio::sync::Mutex<Vec<crate::types::replica::ReplicaConnection>>>,
) -> Result<RespValue> {
    let mut db_guard = db.lock().await;
    let mut deleted_count = 0;

    for i in 1..items.len() {
        let key = items[i].as_string().unwrap_or_default();
        if db_guard.remove(&key).is_some() {
            deleted_count += 1;
        }
    }

    Ok(RespValue::Integer(deleted_count))
}

pub async fn incr_key(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
) -> Result<()> {
    let key = items[1].as_string().unwrap();

    let mut db = db.lock().await;
    if let Some(entry) = db.get(&key) {
        match &entry.value {
            crate::types::ValueType::String(s) => {
                if s.parse::<i64>().is_err() {
                    handler
                        .write_value(RespValue::SimpleError(
                            "ERR value is not an integer or out of range".to_string(),
                        ))
                        .await?;
                    return Ok(());
                };

                let num = s.parse::<i64>().unwrap();
                let new_value = num + 1;
                let expiry = entry.expiry;
                db.insert(
                    key.clone(),
                    KeyWithExpiry {
                        value: crate::types::ValueType::String(new_value.to_string()),
                        expiry,
                    },
                );

                handler.write_value(RespValue::Integer(new_value)).await?;
            }
            _ => {
                handler
                    .write_value(RespValue::SimpleError(
                        "ERR value is not an integer or out of range".to_string(),
                    ))
                    .await?;
            }
        }
    } else {
        db.insert(
            key,
            KeyWithExpiry {
                value: crate::types::ValueType::String("1".to_string()),
                expiry: None,
            },
        );
        handler.write_value(RespValue::Integer(1)).await?;
    }
    Ok(())
}
