use anyhow::Result;
use std::env;
use std::sync::Arc;

use crate::types::{replica::ReplicaConnection, resp::RespValue};

/// Execute a command and replicate it to replicas
pub async fn execute_and_replicate<F, Fut>(
    command_fn: F,
    items: &[RespValue],
    replicas: &Arc<tokio::sync::Mutex<Vec<ReplicaConnection>>>,
) -> Result<()>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    command_fn().await?;
    propogate_to_replicas(&RespValue::Array(items.to_vec()), replicas).await?;
    Ok(())
}

/// Propagate a command to all connected replicas
pub async fn propogate_to_replicas(
    command: &RespValue,
    replicas: &Arc<tokio::sync::Mutex<Vec<ReplicaConnection>>>,
) -> Result<()> {
    let replica_of = env::var("replicaof").unwrap_or_default();
    if !replica_of.is_empty() {
        return Ok(());
    }

    let replicas_guard = replicas.lock().await;

    for replica in replicas_guard.iter() {
        if let Err(e) = replica.send_command(command).await {
            eprintln!("Failed to propagate command to replica: {}", e);
        }
    }

    Ok(())
}
