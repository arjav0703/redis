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

    Ok(())
}
