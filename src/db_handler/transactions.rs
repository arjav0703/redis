use super::*;
use std::result::Result::Ok;

mod helper;
use helper::execute_command;

pub async fn multi(handler: &mut RespHandler, in_transaction: &mut bool) -> Result<()> {
    *in_transaction = true;
    handler
        .write_value(RespValue::SimpleString("OK".to_string()))
        .await?;
    Ok(())
}

pub async fn discard(
    handler: &mut RespHandler,
    in_transaction: &mut bool,
    queued_commands: &mut Vec<RespValue>,
) -> Result<()> {
    if !*in_transaction {
        handler
            .write_value(RespValue::SimpleError(
                "ERR DISCARD without MULTI".to_string(),
            ))
            .await?;
        return Ok(());
    }

    *in_transaction = false;
    queued_commands.clear();
    handler
        .write_value(RespValue::SimpleString("OK".to_string()))
        .await?;

    Ok(())
}

pub async fn exec(
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
