use super::*;

pub async fn handle_subscribe(
    // db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
) -> Result<()> {
    let channel_name = items[1].as_string().unwrap_or_default();

    handler
        .write_value(RespValue::Array(vec![
            RespValue::BulkString("subscribe".into()),
            RespValue::BulkString(channel_name.clone()),
            RespValue::Integer(1),
        ]))
        .await?;
    Ok(())
}
