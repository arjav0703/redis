use std::{collections::HashMap, sync::Arc};
use tracing::info;

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

    let keys = items.iter().skip(1);

    let mut db_guard = db.lock().await;

    for key in keys {
        let entry = match db_guard.get(&key.as_string().unwrap_or_default()) {
            Some(entry) => entry.clone(),
            None => KeyWithExpiry {
                value: crate::types::ValueType::String("".to_string()),
                expiry: None,
                is_watched: true,
            },
        };

        db_guard.insert(
            key.as_string().unwrap_or_default(),
            KeyWithExpiry {
                value: entry.value.clone(),
                expiry: entry.expiry,
                is_watched: true,
            },
        );

        info!(
            "Client is watching key: {}",
            key.as_string().unwrap_or_default()
        );
    }
    Ok(RespValue::SimpleString("OK".to_string()))
}
