use std::{collections::HashMap, sync::Arc};

use anyhow::Result;

use crate::{
    client_handler::ClientState,
    types::{resp::RespValue, KeyWithExpiry},
};

pub async fn watch_handler(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    state: &mut ClientState,
    items: &[RespValue],
) -> Result<RespValue> {
    if state.in_transaction {
        return Ok(RespValue::SimpleError(
            "ERR WATCH inside MULTI is not allowed".to_string(),
        ));
    }

    let key = items
        .get(1)
        .ok_or_else(|| anyhow::anyhow!("ERR key to watch not found "))?;

    let mut db_guard = db.lock().await;

    let entry = match db_guard.get(&key.as_string().unwrap_or_default()) {
        Some(entry) => entry.clone(),
        None => {
            return Ok(RespValue::SimpleString("OK".to_string()));
        }
    };

    db_guard.insert(
        key.as_string().unwrap_or_default(),
        KeyWithExpiry {
            value: entry.value.clone(),
            expiry: entry.expiry,
            is_watched: true,
        },
    );

    println!(
        "Client is watching key: {}",
        key.as_string().unwrap_or_default()
    );

    Ok(RespValue::SimpleString("OK".to_string()))
}
