use super::*;

pub async fn handle_acl_command(handler: &mut RespHandler, items: &[RespValue]) -> Result<()> {
    match items
        .get(1)
        .unwrap()
        .as_string()
        .unwrap()
        .to_uppercase()
        .as_str()
    {
        "WHOAMI" => {
            handler
                .write_value(RespValue::BulkString("default".into()))
                .await?;
        }
        "GETUSER" => {
            if items[2].as_string().unwrap().as_str() == "default" {
                handler
                    .write_value(RespValue::Array(vec![
                        RespValue::BulkString("flags".into()),
                        RespValue::Array(vec![RespValue::BulkString("nopass".into())]),
                    ]))
                    .await?;
            } else {
                handler
                    .write_value(RespValue::SimpleError("ERR no such user".to_string()))
                    .await?;
            }
        }
        _ => {
            handler
                .write_value(RespValue::SimpleError(
                    "ERR invalid ACL command".to_string(),
                ))
                .await?;
        }
    }
    Ok(())
}
