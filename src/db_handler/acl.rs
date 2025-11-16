use super::*;
use crate::Users;
use sha2::Digest;

pub async fn handle_acl_command(
    handler: &mut RespHandler,
    items: &[RespValue],
    users: &Users,
) -> Result<()> {
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
                let is_nopass = {
                    let users = users.lock().await;
                    users.get("default").unwrap().is_none()
                };
                if is_nopass {
                    handler
                        .write_value(RespValue::Array(vec![
                            RespValue::BulkString("flags".into()),
                            RespValue::Array(vec![RespValue::BulkString("nopass".into())]),
                            RespValue::BulkString("passwords".into()),
                            RespValue::Array(vec![]),
                        ]))
                        .await?;
                } else {
                    let password_hash = {
                        let users = users.lock().await;
                        let pass = users.get("default").unwrap().as_ref().unwrap().to_string();
                        let hash = hex::encode(sha2::Sha256::digest(pass.as_bytes())).to_string();
                        hash
                    };

                    handler
                        .write_value(RespValue::Array(vec![
                            RespValue::BulkString("flags".into()),
                            RespValue::Array(vec![]),
                            RespValue::BulkString("passwords".into()),
                            RespValue::Array(vec![RespValue::BulkString(password_hash)]),
                        ]))
                        .await?;
                }
            } else {
                handler
                    .write_value(RespValue::SimpleError("ERR no such user".to_string()))
                    .await?;
            }
        }
        "SETUSER" => {
            let mut users = users.lock().await;
            let username = items[2].as_string().unwrap();
            let password = items[3].as_string().unwrap()[1..].to_string();
            dbg!(&username, &password);

            users.insert(username, Some(password));

            handler
                .write_value(RespValue::SimpleString("OK".to_string()))
                .await?;
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
