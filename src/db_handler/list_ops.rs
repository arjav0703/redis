use super::*;
use tokio::sync::mpsc;

pub type BlockedClients = Arc<tokio::sync::Mutex<Vec<crate::BlockedClient>>>;

pub async fn handle_push(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
    push_to_front: bool,
    blocked_clients: &BlockedClients,
) -> Result<()> {
    let list_key = items[1].as_string().unwrap_or_default();

    // add elements to the list
    {
        let mut db_lock = db.lock().await;

        let mut list = if let Some(entry) = db_lock.get(&list_key) {
            match &entry.value {
                crate::types::ValueType::List(l) => l.clone(),
                _ => {
                    handler
                        .write_value(RespValue::SimpleString(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .into(),
                        ))
                        .await?;
                    return Ok(());
                }
            }
        } else {
            vec![]
        };

        if push_to_front {
            for i in 2..items.len() {
                let val = items[i].as_string().unwrap_or_default();
                list.insert(0, val);
            }
        } else {
            for i in 2..items.len() {
                let val = items[i].as_string().unwrap_or_default();
                list.push(val);
            }
        }

        let list_len = list.len() as i64;

        db_lock.insert(
            list_key.clone(),
            KeyWithExpiry {
                value: crate::types::ValueType::List(list),
                expiry: None,
            },
        );

        handler.write_value(RespValue::Integer(list_len)).await?;
    }

    // Check if any clients are blocked on this list
    let mut blocked = blocked_clients.lock().await;
    let client_to_notify = if let Some(pos) = blocked.iter().position(|c| c.list_key == list_key) {
        Some(blocked.remove(pos))
    } else {
        None
    };
    drop(blocked); // Release lock before async operations

    // If we found a blocked client, pop an element for them immediately
    if let Some(client) = client_to_notify {
        let mut db_lock = db.lock().await;
        if let Some(entry) = db_lock.get_mut(&list_key) {
            if let crate::types::ValueType::List(l) = &mut entry.value {
                if let Some(popped_value) = l.first().cloned() {
                    l.remove(0);
                    // Send to the blocked client (key, value)
                    let _ = client.sender.send((list_key.clone(), popped_value)).await;
                }
            }
        }
    }

    Ok(())
}

pub async fn handle_lrange(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
) -> Result<()> {
    let list_key = items[1].as_string().unwrap_or_default();
    let start = items[2].as_integer().unwrap_or(0);
    let end = items[3].as_integer().unwrap_or(0);

    let db = db.lock().await;

    if let Some(entry) = db.get(&list_key) {
        match &entry.value {
            crate::types::ValueType::List(l) => {
                let list_len = l.len() as i64;

                // Handle negative indices
                let start_idx = if start < 0 {
                    (list_len + start).max(0)
                } else {
                    start.min(list_len)
                } as usize;

                let end_idx = if end < 0 {
                    (list_len + end + 1).max(0)
                } else {
                    (end + 1).min(list_len)
                } as usize;

                let slice = if start_idx < end_idx && start_idx < l.len() {
                    &l[start_idx..end_idx.min(l.len())]
                } else {
                    &[]
                };

                let resp_array = RespValue::Array(
                    slice
                        .iter()
                        .map(|s| RespValue::BulkString(s.clone()))
                        .collect(),
                );

                handler.write_value(resp_array).await?;
            }
            _ => {
                handler
                    .write_value(RespValue::SimpleString(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    ))
                    .await?;
            }
        }
    } else {
        handler.write_value(RespValue::Array(vec![])).await?;
    }

    Ok(())
}

pub async fn handle_llen(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
) -> Result<()> {
    let list_key = items[1].as_string().unwrap_or_default();

    let db = db.lock().await;

    if let Some(entry) = db.get(&list_key) {
        match &entry.value {
            crate::types::ValueType::List(l) => {
                let list_len = l.len() as i64;
                handler.write_value(RespValue::Integer(list_len)).await?;
            }
            _ => {
                handler
                    .write_value(RespValue::SimpleString(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    ))
                    .await?;
            }
        }
    } else {
        handler.write_value(RespValue::Integer(0)).await?;
    }

    Ok(())
}

pub async fn handle_lpop(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
) -> Result<()> {
    let list_key = items[1].as_string().unwrap_or_default();

    let elements_to_pop = if items.len() > 2 {
        items[2].as_integer().unwrap_or(1)
    } else {
        1
    };

    let mut db = db.lock().await;

    if let Some(entry) = db.get_mut(&list_key) {
        match &mut entry.value {
            crate::types::ValueType::List(l) => {
                let mut popped_values = vec![];
                let count = elements_to_pop.min(l.len() as i64) as usize;

                for _ in 0..count {
                    if !l.is_empty() {
                        popped_values.push(l.remove(0));
                    }
                }

                if popped_values.is_empty() {
                    handler.write_value(RespValue::NullBulkString).await?;
                } else if elements_to_pop == 1 {
                    handler
                        .write_value(RespValue::BulkString(popped_values[0].clone()))
                        .await?;
                } else {
                    let resp_array = RespValue::Array(
                        popped_values
                            .iter()
                            .map(|s| RespValue::BulkString(s.clone()))
                            .collect(),
                    );
                    handler.write_value(resp_array).await?;
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
    } else {
        handler.write_value(RespValue::NullBulkString).await?;
    }

    Ok(())
}

pub async fn handle_blpop(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
    blocked_clients: &BlockedClients,
) -> Result<()> {
    // BLPOP key [key ...] timeout
    let list_key = items[1].as_string().unwrap_or_default();
    let timeout_secs = items[2].as_string().unwrap_or_default();

    let timeout_float: f64 = timeout_secs.parse().unwrap_or(0.0);

    // check if the list exists and has elements
    {
        let mut db = db.lock().await;
        if let Some(entry) = db.get_mut(&list_key) {
            match &mut entry.value {
                crate::types::ValueType::List(l) => {
                    if !l.is_empty() {
                        // List has elements, pop immediately
                        let popped_value = l.remove(0);
                        let resp_array = RespValue::Array(vec![
                            RespValue::BulkString(list_key),
                            RespValue::BulkString(popped_value),
                        ]);
                        handler.write_value(resp_array).await?;
                        return Ok(());
                    }
                }
                _ => {
                    handler
                        .write_value(RespValue::SimpleString(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .into(),
                        ))
                        .await?;
                    return Ok(());
                }
            }
        }
    }

    // List is empty or doesn't exist, block the client
    // Create a channel to receive notification when element is available
    let (tx, mut rx) = mpsc::channel::<(String, String)>(1);

    // Register this client as blocked
    {
        let mut blocked = blocked_clients.lock().await;
        blocked.push(crate::BlockedClient {
            list_key: list_key.clone(),
            sender: tx,
        });
    }

    if timeout_float > 0.0 {
        let timeout_duration = tokio::time::Duration::from_secs_f64(timeout_float);

        // Wait with timeout
        match tokio::time::timeout(timeout_duration, rx.recv()).await {
            std::result::Result::Ok(Some((key, value))) => {
                let resp_array = RespValue::Array(vec![
                    RespValue::BulkString(key),
                    RespValue::BulkString(value),
                ]);
                handler.write_value(resp_array).await?;
            }
            std::result::Result::Ok(None) | std::result::Result::Err(_) => {
                // Remove from blocked clients list
                let mut blocked = blocked_clients.lock().await;
                blocked.retain(|c| c.list_key != list_key);

                handler.write_bytes(b"*-1\r\n").await?;
            }
        }
    } else {
        // timeout = 0, block indefinitely
        if let Some((key, value)) = rx.recv().await {
            let resp_array = RespValue::Array(vec![
                RespValue::BulkString(key),
                RespValue::BulkString(value),
            ]);
            handler.write_value(resp_array).await?;
        }
    }

    Ok(())
}
