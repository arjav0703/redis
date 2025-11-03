use super::*;

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
pub async fn set_key_internal(
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
