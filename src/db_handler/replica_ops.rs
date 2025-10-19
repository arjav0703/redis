use crate::types::{
    resp::{RespHandler, RespValue},
    KeyWithExpiry,
};
use anyhow::{Ok, Result};
use rand::distr::Alphanumeric;
use rand::Rng;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;

pub async fn handle_info(handler: &mut RespHandler) -> Result<()> {
    let is_isreplica = !env::var("replicaof").unwrap_or_default().is_empty();
    // dbg!(is_isreplica);
    let role = if is_isreplica { "slave" } else { "master" };
    let info = format!("role:{role}");

    let replication_id = env::var("replication_id").unwrap_or_else(|_| {
        (0..40)
            .map(|_| rand::rng().sample(Alphanumeric) as char)
            .collect()
    });

    let info = format!("{info}\nmaster_replid:{replication_id}\nmaster_repl_offset:0");
    handler.write_value(RespValue::BulkString(info)).await?;
    Ok(())
}

pub async fn handle_replconf(items: &[RespValue], handler: &mut RespHandler) -> Result<()> {
    if items.len() >= 3 {
        let subcommand = items[1].as_string().unwrap_or_default();

        if subcommand.eq_ignore_ascii_case("listening-port") {
            let port = items[2].as_integer().unwrap_or(0);
            println!("REPLCONF listening-port: {port}");
        } else if subcommand.eq_ignore_ascii_case("capa") {
            let capability = items[2].as_string().unwrap_or_default();
            println!("REPLCONF capa: {capability}");
        } else if subcommand.eq_ignore_ascii_case("ack") {
            let offset = items[2].as_integer().unwrap_or(0);
            println!("REPLCONF ack: {offset}");
            handler
                .write_bytes(b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n")
                .await?;
        }
    }

    handler
        .write_value(RespValue::SimpleString("OK".into()))
        .await
        .unwrap_or_else(|e| eprintln!("Failed to send REPLCONF response: {}", e));

    Ok(())
}

pub async fn handle_psync(
    items: &[RespValue],
    _db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    handler: &mut RespHandler,
    _replicas: &Arc<tokio::sync::Mutex<Vec<ReplicaConnection>>>,
) -> Result<bool> {
    if items.len() >= 2 {
        let replication_id = items[1].as_string().unwrap_or_default();
        let offset = if items.len() >= 3 {
            items[2].as_integer().unwrap_or(0)
        } else {
            0
        };
        println!("PSYNC request: id={replication_id}, offset={offset}");

        // Get the master's replication ID from environment or generate a new one
        let master_replication_id = env::var("replication_id").unwrap_or_else(|_| {
            (0..40)
                .map(|_| rand::rng().sample(Alphanumeric) as char)
                .collect()
        });

        // Respond with FULLRESYNC using the master's replication ID and offset 0
        let response = format!("FULLRESYNC {master_replication_id} 0");
        handler
            .write_value(RespValue::SimpleString(response))
            .await?;
        send_empty_rdb(handler).await?;

        // Return true to indicate this connection should become a replica connection
        Ok(true)
    } else {
        handler
            .write_value(RespValue::SimpleString("ERR invalid PSYNC command".into()))
            .await?;
        Ok(false)
    }
}

async fn send_empty_rdb(handler: &mut RespHandler) -> Result<()> {
    let empty_rdb = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2").unwrap();

    let length_of_file = empty_rdb.len();

    // Send as proper RESP bulk string: $<len>\r\n<data>\r\n
    let header = format!("${}\r\n", length_of_file);
    handler.write_bytes(header.as_bytes()).await?;
    handler.write_bytes(&empty_rdb).await?;

    Ok(())
}

use crate::ReplicaConnection;
pub async fn handle_wait(
    replicas: &Arc<tokio::sync::Mutex<Vec<ReplicaConnection>>>,
    _num_replicas: i64, // Not used - we return the actual count, not the requested count
    timeout_ms: u64,
) -> Result<i64> {
    let replicas_guard = replicas.lock().await;

    // If no replicas connected, return 0
    if replicas_guard.is_empty() {
        return Ok(0);
    }

    // Check if any replica has pending writes (offset > 0)
    let mut has_pending_writes = false;
    for replica in replicas_guard.iter() {
        if replica.get_offset().await > 0 {
            has_pending_writes = true;
            break;
        }
    }

    // If no pending writes, return the number of connected replicas
    if !has_pending_writes {
        return Ok(replicas_guard.len() as i64);
    }

    // Get the current offset for each replica (before sending GETACK)
    let mut expected_offsets = Vec::new();
    for replica in replicas_guard.iter() {
        expected_offsets.push(replica.get_offset().await);
    }

    // Send GETACK to all replicas and collect responses
    let mut ack_futures = Vec::new();
    for (i, replica) in replicas_guard.iter().enumerate() {
        let replica_clone = replica.clone();
        let expected_offset = expected_offsets[i];
        ack_futures.push(tokio::spawn(async move {
            match replica_clone.send_getack_and_wait(timeout_ms).await {
                std::result::Result::Ok(result) => match result {
                    Some(offset) => {
                        // Replica acknowledged if its offset >= expected offset
                        offset >= expected_offset
                    }
                    None => false,
                },
                Err(_) => false,
            }
        }));
    }

    // Wait for all responses (or timeout)
    let mut ack_count = 0i64;
    for future in ack_futures {
        match future.await {
            std::result::Result::Ok(acknowledged) => {
                if acknowledged {
                    ack_count += 1;
                }
            }
            Err(_) => {}
        }
    }

    Ok(ack_count)
}
