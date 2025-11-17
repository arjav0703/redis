use super::*;

pub async fn handle_auth(
    handler: &mut RespHandler,
    items: &[RespValue],
    users: &Users,
    authstate: &std::sync::Arc<tokio::sync::Mutex<crate::client_handler::AuthState>>,
) -> Result<()> {
    let username = items.get(1).and_then(|v| v.as_string()).unwrap_or_default();
    let password = items.get(2).and_then(|v| v.as_string()).unwrap_or_default();
    let users = users.lock().await;

    if let Some(user_pass_opt) = users.get(&username) {
        if let Some(expected) = user_pass_opt {
            if expected == &password {
                // mark this connection as authenticated
                let mut auth_lock = authstate.lock().await;
                auth_lock.is_authenticated = true;
                auth_lock.username = username.clone();
                handler
                    .write_value(RespValue::SimpleString("OK".to_string()))
                    .await?;
                return Ok(());
            }
        }
    }

    handler
        .write_value(RespValue::SimpleError("WRONGPASS wrong password".to_string()))
        .await?;
    Ok(())
}
