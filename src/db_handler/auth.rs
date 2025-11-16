use super::*;

pub async fn handle_auth(
    handler: &mut RespHandler,
    items: &[RespValue],
    users: &Users,
) -> Result<()> {
    let username = items[1].as_string().unwrap();
    let passowrd = items[2].as_string().unwrap();
    let users = users.lock().await;

    let user_pass = users.get(&username).unwrap();
    let user_pass = user_pass.clone().unwrap_or_default();

    if user_pass == passowrd {
        handler
            .write_value(RespValue::SimpleString("OK".to_string()))
            .await?;
    } else {
        handler
            .write_value(RespValue::SimpleError(
                "WRONGPASS wrong password".to_string(),
            ))
            .await?;
    }
    Ok(())
}
