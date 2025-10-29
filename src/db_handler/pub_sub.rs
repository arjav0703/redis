use super::*;
use std::collections::HashSet;

pub async fn handle_subscribe(
    items: &[RespValue],
    handler: &mut RespHandler,
    subscribed_channels: &mut HashSet<String>,
    channels_map: &Arc<tokio::sync::Mutex<HashMap<String, usize>>>,
) -> Result<()> {
    let channel_name = items[1].as_string().unwrap_or_default();

    let newly_added = subscribed_channels.insert(channel_name.clone());

    if newly_added {
        let mut map = channels_map.lock().await;
        let counter = map.entry(channel_name.clone()).or_insert(0usize);
        *counter += 1;
    }

    let channel_count = subscribed_channels.len() as i64;

    handler
        .write_value(RespValue::Array(vec![
            RespValue::BulkString("subscribe".into()),
            RespValue::BulkString(channel_name.clone()),
            RespValue::Integer(channel_count),
        ]))
        .await?;
    Ok(())
}

pub async fn handle_publish(
    items: &[RespValue],
    channels_map: &Arc<tokio::sync::Mutex<HashMap<String, usize>>>,
    handler: &mut RespHandler,
) -> Result<()> {
    let channel_name = items[1].as_string().unwrap_or_default();

    let count = {
        let map = channels_map.lock().await;
        map.get(&channel_name).cloned().unwrap_or(0usize)
    } as i64;

    handler.write_value(RespValue::Integer(count)).await?;
    Ok(())
}
