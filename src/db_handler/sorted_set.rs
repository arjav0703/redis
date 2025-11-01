use super::*;

pub async fn zadd(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
) -> Result<()> {
    let key = items[1].as_string().unwrap();
    let score: f64 = items[2]
        .as_string()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let member = items[3].as_string().unwrap_or_default();

    let mut db_lock = db.lock().await;

    if let Some(entry) = db_lock.get_mut(&key) {
        match &mut entry.value {
            crate::types::ValueType::SortedSet(vec) => {
                // Check if member exists
                if let Some(existing) = vec.iter_mut().find(|(m, _)| m == &member) {
                    existing.1 = score;
                    handler.write_value(RespValue::Integer(0)).await?;
                } else {
                    vec.push((member.clone(), score));
                    handler.write_value(RespValue::Integer(1)).await?;
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
        db_lock.insert(
            key.clone(),
            KeyWithExpiry {
                value: crate::types::ValueType::SortedSet(vec![(member.clone(), score)]),
                expiry: None,
            },
        );
        handler.write_value(RespValue::Integer(1)).await?;
    }

    let unsorted_vec = match &db_lock.get(&key).unwrap().value {
        crate::types::ValueType::SortedSet(vec) => vec.clone(),
        _ => vec![],
    };
    let mut sorted_vec = unsorted_vec.clone();
    sort_set(&mut sorted_vec, true).await;
    db_lock.insert(
        key,
        KeyWithExpiry {
            value: crate::types::ValueType::SortedSet(sorted_vec),
            expiry: None,
        },
    );

    Ok(())
}

pub async fn zrank(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
) -> Result<()> {
    let key = items[1].as_string().unwrap();
    let member = items[2].as_string().unwrap_or_default();

    let db_lock = db.lock().await;
    if let Some(entry) = db_lock.get(&key) {
        match &entry.value {
            crate::types::ValueType::SortedSet(vec) => {
                if let Some((index, _)) = vec.iter().enumerate().find(|(_, (m, _))| m == &member) {
                    handler
                        .write_value(RespValue::Integer(index as i64))
                        .await?;
                } else {
                    // Member not found in the sorted set
                    handler.write_value(RespValue::NullBulkString).await?;
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
        // Sorted set key doesn't exist
        handler.write_value(RespValue::NullBulkString).await?;
    }

    Ok(())
}

pub async fn zrange(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
) -> Result<()> {
    let key = items[1].as_string().unwrap();
    let start: isize = items[2]
        .as_string()
        .and_then(|s| s.parse::<isize>().ok())
        .unwrap_or(0);
    let stop: isize = items[3]
        .as_string()
        .and_then(|s| s.parse::<isize>().ok())
        .unwrap_or(-1);

    let db_lock = db.lock().await;
    if let Some(entry) = db_lock.get(&key) {
        match &entry.value {
            crate::types::ValueType::SortedSet(vec) => {
                let len = vec.len() as isize;
                let start_idx = if start < 0 { len + start } else { start }.max(0) as usize;
                let stop_idx = if stop < 0 { len + stop } else { stop }.min(len - 1) as usize;

                let result: Vec<RespValue> = vec[start_idx..=stop_idx]
                    .iter()
                    .map(|(m, _)| RespValue::BulkString(m.clone()))
                    .collect();

                handler.write_value(RespValue::Array(result)).await?;
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

pub async fn zcard(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
) -> Result<()> {
    let key = items[1].as_string().unwrap();

    let db_lock = db.lock().await;
    if let Some(entry) = db_lock.get(&key) {
        match &entry.value {
            crate::types::ValueType::SortedSet(vec) => {
                handler
                    .write_value(RespValue::Integer(vec.len() as i64))
                    .await?;
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

pub async fn zscore(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
) -> Result<()> {
    let key = items[1].as_string().unwrap();
    let member = items[2].as_string().unwrap_or_default();

    let db = db.lock().await;
    if let Some(entry) = db.get(&key) {
        match &entry.value {
            crate::types::ValueType::SortedSet(vec) => {
                if let Some((_, score)) = vec.iter().find(|(m, _)| m == &member) {
                    handler
                        .write_value(RespValue::BulkString(score.to_string()))
                        .await?;
                } else {
                    handler.write_value(RespValue::NullBulkString).await?;
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
    }

    Ok(())
}

pub async fn zrem(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
) -> Result<()> {
    let key = items[1].as_string().unwrap();
    let member = items[2].as_string().unwrap_or_default();

    let mut db_lock = db.lock().await;
    if let Some(entry) = db_lock.get_mut(&key) {
        match &mut entry.value {
            crate::types::ValueType::SortedSet(vec) => {
                let original_len = vec.len();
                vec.retain(|(m, _)| m != &member);
                let removed_count = (original_len - vec.len()) as i64;
                handler
                    .write_value(RespValue::Integer(removed_count))
                    .await?;
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
async fn sort_set(vec: &mut [(String, f64)], ascending: bool) {
    if ascending {
        vec.sort_by(|a, b| {
            // First compare by score
            match a.1.partial_cmp(&b.1) {
                Some(std::cmp::Ordering::Equal) => {
                    // If scores are equal, compare lexicographically by member name
                    a.0.cmp(&b.0)
                }
                other => other.unwrap(),
            }
        });
    } else {
        vec.sort_by(|a, b| {
            // First compare by score (descending)
            match b.1.partial_cmp(&a.1) {
                Some(std::cmp::Ordering::Equal) => {
                    // If scores are equal, compare lexicographically by member name
                    a.0.cmp(&b.0)
                }
                other => other.unwrap(),
            }
        });
    }
}
