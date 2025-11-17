use anyhow::Result;
use std::sync::Arc;

use crate::client_handler::{ClientState, SharedResources};
use crate::db_handler::{
    del_key, geo, get_key, handle_config, handle_key_search, handle_type, list_ops, pub_sub,
    replica_ops, set_key::*, sorted_set, stream_ops, transactions,
};
use crate::replication::execute_and_replicate;
use crate::types::{
    replica::ReplicaConnection,
    resp::{RespHandler, RespValue},
};

/// Dispatch command to the appropriate handler function
pub async fn dispatch_command(
    cmd: &str,
    items: &[RespValue],
    state: &mut ClientState,
    resources: &SharedResources,
) -> Result<()> {
    let is_subscribed = state.is_subscribed();

    let authstate = resources.authstate.lock().await;
    // Require authentication for all commands except AUTH.
    if !authstate.is_authenticated && cmd != "AUTH" {
        state
            .handler
            .write_value(RespValue::SimpleError(
                "NOAUTH Authentication required.".to_string(),
            ))
            .await?;
        return Ok(());
    }

    match cmd {
        "PING" => {
            crate::db_handler::send_pong(&mut state.handler, items, is_subscribed).await?;
        }
        "ECHO" if items.len() == 2 && !is_subscribed => {
            state.handler.write_value(items[1].clone()).await?;
        }

        // Key-value operations
        "SET" if items.len() >= 3 => {
            execute_and_replicate(
                || set_key(&resources.db, items, &mut state.handler),
                items,
                &resources.replicas,
            )
            .await?;
        }
        "GET" if items.len() == 2 => {
            get_key(&resources.db, items, &mut state.handler).await?;
        }
        "DEL" if items.len() >= 2 => {
            execute_and_replicate(
                || del_key(&resources.db, items, &mut state.handler),
                items,
                &resources.replicas,
            )
            .await?;
        }
        "INCR" if items.len() == 2 => {
            execute_and_replicate(
                || transactions::incr_key(&resources.db, items, &mut state.handler),
                items,
                &resources.replicas,
            )
            .await?;
        }
        "TYPE" if items.len() == 2 => {
            handle_type(&resources.db, items, &mut state.handler).await?;
        }

        // Configuration and metadata
        "CONFIG" => {
            handle_config(items, &mut state.handler).await?;
        }
        "KEYS" if items.len() == 2 => {
            let pattern = items[1].as_string().unwrap_or_default();
            handle_key_search(&resources.db, &pattern, &mut state.handler).await?;
        }
        "INFO" if items.len() == 2 => {
            replica_ops::handle_info(&mut state.handler).await?;
        }

        // Replication commands
        "REPLCONF" => {
            handle_replconf_command(&mut state.handler, items).await?;
        }
        "WAIT" if items.len() == 3 => {
            handle_wait_command(&mut state.handler, items, &resources.replicas).await?;
        }

        // Stream operations
        "XADD" if items.len() >= 5 => {
            stream_ops::handle_xadd(&resources.db, items, &mut state.handler).await?;
        }
        "XRANGE" if items.len() >= 4 => {
            stream_ops::handle_xrange(&resources.db, items, &mut state.handler).await?;
        }
        "XREAD" if items.len() >= 4 => {
            stream_ops::handle_xread(&resources.db, items, &mut state.handler).await?;
        }

        // List operations
        "RPUSH" if items.len() >= 3 => {
            execute_and_replicate(
                || {
                    list_ops::handle_push(
                        &resources.db,
                        items,
                        &mut state.handler,
                        false,
                        &resources.blocked_clients,
                    )
                },
                items,
                &resources.replicas,
            )
            .await?;
        }
        "LPUSH" if items.len() >= 3 => {
            execute_and_replicate(
                || {
                    list_ops::handle_push(
                        &resources.db,
                        items,
                        &mut state.handler,
                        true,
                        &resources.blocked_clients,
                    )
                },
                items,
                &resources.replicas,
            )
            .await?;
        }
        "LRANGE" if items.len() == 4 => {
            list_ops::handle_lrange(&resources.db, items, &mut state.handler).await?;
        }
        "LLEN" if items.len() == 2 => {
            list_ops::handle_llen(&resources.db, items, &mut state.handler).await?;
        }
        "LPOP" if items.len() >= 2 => {
            execute_and_replicate(
                || list_ops::handle_lpop(&resources.db, items, &mut state.handler),
                items,
                &resources.replicas,
            )
            .await?;
        }
        "BLPOP" if items.len() >= 3 => {
            list_ops::handle_blpop(
                &resources.db,
                items,
                &mut state.handler,
                &resources.blocked_clients,
            )
            .await?;
        }

        // Pub/Sub operations
        "PUBLISH" if items.len() >= 3 => {
            pub_sub::handle_publish(
                items,
                &resources.channels_map,
                &resources.channel_subscribers,
                &mut state.handler,
            )
            .await?;
        }
        "SUBSCRIBE" if items.len() >= 2 => {
            pub_sub::handle_subscribe(
                items,
                &mut state.handler,
                &mut state.subscribed_channels,
                &resources.channels_map,
                &resources.channel_subscribers,
                state.msg_tx.clone(),
            )
            .await?;
        }
        "UNSUBSCRIBE" if items.len() >= 2 => {
            pub_sub::handle_unsubscribe(
                items,
                &mut state.handler,
                &mut state.subscribed_channels,
                &resources.channels_map,
                &resources.channel_subscribers,
            )
            .await?;
        }

        // Sorted set operations
        "ZADD" if items.len() >= 4 => {
            execute_and_replicate(
                || sorted_set::zadd(&resources.db, items, &mut state.handler),
                items,
                &resources.replicas,
            )
            .await?;
        }
        "ZRANK" if items.len() >= 3 => {
            sorted_set::zrank(&resources.db, items, &mut state.handler).await?;
        }
        "ZRANGE" if items.len() >= 4 => {
            sorted_set::zrange(&resources.db, items, &mut state.handler).await?;
        }
        "ZCARD" if items.len() == 2 => {
            sorted_set::zcard(&resources.db, items, &mut state.handler).await?;
        }
        "ZSCORE" if items.len() == 3 => {
            sorted_set::zscore(&resources.db, items, &mut state.handler).await?;
        }
        "ZREM" if items.len() >= 3 => {
            execute_and_replicate(
                || sorted_set::zrem(&resources.db, items, &mut state.handler),
                items,
                &resources.replicas,
            )
            .await?;
        }

        // Geo operations
        "GEOADD" if items.len() >= 5 => {
            execute_and_replicate(
                || geo::add(&resources.db, items, &mut state.handler),
                items,
                &resources.replicas,
            )
            .await?;
        }
        "GEOPOS" if items.len() >= 3 => {
            geo::pos(&resources.db, items, &mut state.handler).await?;
        }
        "GEODIST" if items.len() >= 3 => {
            geo::dist(&resources.db, items, &mut state.handler).await?;
        }
        "GEOSEARCH" if items.len() >= 5 => {
            geo::search(&resources.db, items, &mut state.handler).await?;
        }

        // Transaction operations
        "MULTI" => {
            transactions::multi(&mut state.handler, &mut state.in_transaction).await?;
        }
        "EXEC" => {
            transactions::exec(
                &mut state.handler,
                &mut state.in_transaction,
                &mut state.queued_commands,
                &resources.db,
                &resources.replicas,
            )
            .await?;
        }
        "DISCARD" => {
            transactions::discard(
                &mut state.handler,
                &mut state.in_transaction,
                &mut state.queued_commands,
            )
            .await?;
        }
        "ACL" => {
            crate::db_handler::acl::handle_acl_command(&mut state.handler, items, &resources.users)
                .await?;
        }
        "AUTH" => {
            crate::db_handler::auth::handle_auth(
                &mut state.handler,
                items,
                &resources.users,
                &resources.authstate,
            )
            .await?;
        }

        _ => {
            state
                .handler
                .write_value(RespValue::SimpleString("ERR unknown command".into()))
                .await?;
        }
    }

    Ok(())
}

/// Handle REPLCONF command
async fn handle_replconf_command(handler: &mut RespHandler, items: &[RespValue]) -> Result<()> {
    let slave_ip = handler.get_peer_addr().unwrap();
    dbg!(&slave_ip);
    std::env::set_var("slave_ip", slave_ip.to_string());
    replica_ops::handle_replconf(items, handler).await
}

/// Handle WAIT command
async fn handle_wait_command(
    handler: &mut RespHandler,
    items: &[RespValue],
    replicas: &Arc<tokio::sync::Mutex<Vec<ReplicaConnection>>>,
) -> Result<()> {
    let num_replicas = items[1].as_integer().unwrap_or(0);
    let timeout_ms = items[2].as_integer().unwrap_or(0) as u64;

    use replica_ops::handle_wait;
    let ack_count = handle_wait(replicas, num_replicas, timeout_ms).await?;

    handler.write_value(RespValue::Integer(ack_count)).await?;
    Ok(())
}
