use super::*;

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
                let num = s.parse::<i64>().unwrap_or(0);
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
    }
    Ok(())
}
