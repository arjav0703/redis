use super::*;

pub async fn handle_acl_command(
    handler: &mut RespHandler,
    items: &[RespValue],
) -> Result<()> {
    match items.get(1) {
        Some(RespValue::BulkString(command)) if command.to_uppercase() == "WHOAMI" => {
            // For simplicity, we return a fixed username
            handler.write_value(RespValue::BulkString("default".into())).await?;
        }
        Some(RespValue::BulkString(_)) => {
            handler
                .write_value(RespValue::SimpleError("ERR unknown ACL subcommand".to_string()))
                .await?;
        }
        _ => {
            handler
                .write_value(RespValue::SimpleError("ERR invalid ACL command".to_string()))
                .await?;
        }
    }
    Ok(())
}
