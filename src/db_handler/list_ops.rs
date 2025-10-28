use super::*;

pub async fn handle_push(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
    push_to_front: bool,
) -> Result<()> {
    let list_key = items[1].as_string().unwrap_or_default();

    let mut db = db.lock().await;

    let mut list = if let Some(entry) = db.get(&list_key) {
        match &entry.value {
            crate::types::ValueType::List(l) => l.clone(),
            _ => {
                handler
                    .write_value(RespValue::SimpleString(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    ))
                    .await?;
                return Ok(());
            }
        }
    } else {
        vec![]
    };
    if push_to_front {
        for i in (2..items.len()) {
            let val = items[i].as_string().unwrap_or_default();
            list.insert(0, val);
        }
    } else {
        for i in 2..items.len() {
            let val = items[i].as_string().unwrap_or_default();
            list.push(val);
        }
    }
    // for i in 2..items.len() {
    //     let val = items[i].as_string().unwrap_or_default();
    //     list.push(val);
    // }

    let list_len = list.len() as i64;

    db.insert(
        list_key,
        KeyWithExpiry {
            value: crate::types::ValueType::List(list),
            expiry: None,
        },
    );

    // Return the length of the list as a RESP integer
    handler.write_value(RespValue::Integer(list_len)).await?;

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
