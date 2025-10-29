use super::*;
use std::collections::HashSet;

pub async fn handle_subscribe(
    items: &[RespValue],
    handler: &mut RespHandler,
    subscribed_channels: &mut HashSet<String>,
) -> Result<()> {
    let channel_name = items[1].as_string().unwrap_or_default();

    subscribed_channels.insert(channel_name.clone());

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
