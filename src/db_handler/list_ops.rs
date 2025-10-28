use super::*;

pub async fn handle_rpush(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
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

    for i in 2..items.len() {
        let val = items[i].as_string().unwrap_or_default();
        list.push(val);
    }

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
