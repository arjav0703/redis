use anyhow::Result;

use crate::client_handler::{ClientState, SharedResources};
use crate::command_dispatcher::dispatch_command;
use crate::types::resp::RespValue;

/// Process a single command from the client
pub async fn process_command(
    val: RespValue,
    state: &mut ClientState,
    resources: &SharedResources,
) -> Result<bool> {
    match val {
        RespValue::Array(items) if !items.is_empty() => {
            if let RespValue::BulkString(cmd) | RespValue::SimpleString(cmd) = &items[0] {
                let cmd_upper = cmd.to_ascii_uppercase();

                // Check if command is allowed in subscribed mode
                if state.is_subscribed() && !is_allowed_in_subscribed_mode(&cmd_upper) {
                    let err_msg = format!(
                        "ERR Can't execute '{}' in subscribed mode",
                        cmd.to_ascii_lowercase()
                    );
                    state.handler.write_value(RespValue::SimpleError(err_msg)).await?;
                    return Ok(false);
                }

                // Queue commands if in a transaction (except MULTI, EXEC, DISCARD)
                if state.in_transaction && !matches!(cmd_upper.as_str(), "MULTI" | "EXEC" | "DISCARD") {
                    state.queued_commands.push(RespValue::Array(items.clone()));
                    state.handler.write_value(RespValue::SimpleString("QUEUED".to_string())).await?;
                    return Ok(false);
                }

                // Special handling for PSYNC which may convert the connection
                if cmd_upper == "PSYNC" && items.len() >= 2 {
                    if handle_psync_command(state, &items, resources).await? {
                        return Ok(true); // Signal to convert to replica
                    }
                } else {
                    // Dispatch command to appropriate handler
                    dispatch_command(&cmd_upper, &items, state, resources).await?;
                }
            }
        }
        _ => {
            state.handler.write_value(RespValue::SimpleString("PONG".into())).await?;
        }
    }

    Ok(false)
}

/// Check if a command is allowed in subscribed mode
fn is_allowed_in_subscribed_mode(cmd: &str) -> bool {
    matches!(
        cmd,
        "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE" | "PING" | "QUIT" | "RESET"
    )
}

/// Handle PSYNC command and return true if connection should become a replica
async fn handle_psync_command(
    state: &mut ClientState,
    items: &[RespValue],
    resources: &SharedResources,
) -> Result<bool> {
    use crate::db_handler::replica_ops;
    
    let should_become_replica = replica_ops::handle_psync(
        items,
        &resources.db,
        &mut state.handler,
        &resources.replicas,
    ).await?;

    Ok(should_become_replica)
}
