use anyhow::{Ok, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
pub mod cli;
pub mod parsers;
use std::env;
mod types;
use cli::set_env_vars;
use types::{
    replica::ReplicaConnection,
    resp::{RespHandler, RespValue},
    KeyWithExpiry,
};

mod db_handler;
use db_handler::{
    del_key, geo, get_key, handle_config, handle_key_search, handle_type, list_ops, pub_sub,
    replica_ops, set_key::*, sorted_set, stream_ops, transactions,
};
mod file_handler;
mod replica;

#[derive(Debug)]
pub struct BlockedClient {
    pub list_key: String,
    pub sender: mpsc::Sender<(String, String)>, // Sends (list_key, value) when unblocked
}

pub type BlockedClients = Arc<tokio::sync::Mutex<Vec<BlockedClient>>>;

// Maps channel_name -> list of senders that can receive messages
pub type ChannelSubscribers =
    Arc<tokio::sync::Mutex<HashMap<String, Vec<mpsc::Sender<(String, String)>>>>>;

#[tokio::main]
async fn main() -> Result<()> {
    set_env_vars();
    let (_, _a, port, _isreplica) = cli::getargs();
    let url = format!("127.0.0.1:{port}");

    println!("Mini‑Redis listening on {url}");
    let listener = TcpListener::bind(url).await?;

    let is_isreplica = !env::var("replicaof").unwrap_or_default().is_empty();

    let initial_db = file_handler::read_rdb_file().await?;
    println!("Initial DB state from RDB file: {initial_db:?}");
    let db = Arc::new(tokio::sync::Mutex::new(
        HashMap::<String, KeyWithExpiry>::new(),
    ));
    {
        let mut db_lock = db.lock().await;
        *db_lock = initial_db;
    }

    if is_isreplica {
        let db_clone = Arc::clone(&db);
        tokio::spawn(async move {
            println!("Starting replica handler...");
            replica::replica_handler(db_clone).await;
            println!("Replica handler terminated");
        });
        // Give the replica handler a moment to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    let replicas = Arc::new(tokio::sync::Mutex::new(Vec::<ReplicaConnection>::new()));
    let blocked_clients: BlockedClients = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let channels_map: Arc<tokio::sync::Mutex<HashMap<String, usize>>> =
        Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    let channel_subscribers: ChannelSubscribers = Arc::new(tokio::sync::Mutex::new(HashMap::new()));

    loop {
        let (stream, _) = listener.accept().await?;

        let db = db.clone();
        let replicas = replicas.clone();
        let blocked_clients = blocked_clients.clone();
        let channels_map = channels_map.clone();
        let channel_subscribers = channel_subscribers.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_client(
                stream,
                db,
                replicas,
                blocked_clients,
                channels_map,
                channel_subscribers,
            )
            .await
            {
                eprintln!("connection error: {e:#}");
            }
        });
    }
}

/// Function to handle the requests made (rerun everytime a request is received)
async fn handle_client(
    stream: TcpStream,
    db: Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    replicas: Arc<tokio::sync::Mutex<Vec<ReplicaConnection>>>,
    blocked_clients: BlockedClients,
    channels_map: Arc<tokio::sync::Mutex<HashMap<String, usize>>>,
    channel_subscribers: ChannelSubscribers,
) -> Result<()> {
    let mut handler = RespHandler::new(stream);
    let mut subscribed_channels = std::collections::HashSet::<String>::new();
    let mut is_subscribed: bool;
    let mut in_transaction = false;
    let mut queued_commands: Vec<RespValue> = Vec::new();

    // channel for receiving pub/sub messages
    let (msg_tx, mut msg_rx) = mpsc::channel::<(String, String)>(100);

    loop {
        tokio::select! {
            // incoming commands from client
            val = handler.read_value() => {
                let val = match val? {
                    Some(v) => v,
                    None => break, // disconnected
                };

        match val {
            RespValue::Array(items) if !items.is_empty() => {
                if let RespValue::BulkString(cmd) | RespValue::SimpleString(cmd) = &items[0] {
                    let cmd_upper = cmd.to_ascii_uppercase();

                    is_subscribed = !subscribed_channels.is_empty();
                    if is_subscribed {
                        const ALLOWED: [&str; 7] = [
                            "SUBSCRIBE",
                            "UNSUBSCRIBE",
                            "PSUBSCRIBE",
                            "PUNSUBSCRIBE",
                            "PING",
                            "QUIT",
                            "RESET",
                        ];

                        if !ALLOWED.contains(&cmd_upper.as_str()) {
                            let err_msg = format!(
                                "ERR Can't execute '{}' in subscribed mode",
                                cmd.to_ascii_lowercase()
                            );
                            handler.write_value(RespValue::SimpleError(err_msg)).await?;
                            continue;
                        }
                    }
                    

                    if in_transaction && !matches!(cmd_upper.as_str(), "MULTI" | "EXEC" | "DISCARD") {
                        queued_commands.push(RespValue::Array(items.clone()));
                        handler.write_value(RespValue::SimpleString("QUEUED".to_string())).await?;
                        continue;
                    }
                    
                    match cmd_upper.as_str() {
                        "PING" => {
                            db_handler::send_pong(&mut handler, &items, is_subscribed).await?
                        }
                        "ECHO" if items.len() == 2 && !is_subscribed => {
                            handler.write_value(items[1].clone()).await?;
                        }
                        "SET" if items.len() >= 3 => {
                            set_key(&db, &items, &mut handler).await?;
                            propogate_to_replicas(&RespValue::Array(items.clone()), &replicas)
                                .await?;
                        }
                        "GET" if items.len() == 2 => get_key(&db, &items, &mut handler).await?,
                        "DEL" if items.len() >= 2 => {
                            del_key(&db, &items, &mut handler).await?;
                            propogate_to_replicas(&RespValue::Array(items.clone()), &replicas)
                                .await?;
                        }
                        "CONFIG" => {
                            handle_config(&items, &mut handler).await?;
                        }
                        "KEYS" if items.len() == 2 => {
                            let pattern = items[1].as_string().unwrap_or_default();
                            handle_key_search(&db, &pattern, &mut handler).await?;
                        }
                        "INFO" if items.len() == 2 => {
                            replica_ops::handle_info(&mut handler).await?;
                        }
                        "REPLCONF" => {
                            let slave_ip = handler.get_peer_addr().unwrap();
                            dbg!(&slave_ip);
                            env::set_var("slave_ip", slave_ip.to_string());
                            replica_ops::handle_replconf(&items, &mut handler).await?;
                        }
                        "PSYNC" if items.len() >= 2 => {
                            let should_become_replica =
                                replica_ops::handle_psync(&items, &db, &mut handler, &replicas)
                                    .await?;
                            if should_become_replica {
                                // Convert this connection to a replica connection
                                let stream = handler.into_stream();
                                let replica_conn = ReplicaConnection::new(stream);
                                let mut replicas_guard = replicas.lock().await;
                                replicas_guard.push(replica_conn);

                                let connected_replicas = replicas_guard.len();
                                env::set_var("connected_replicas", connected_replicas.to_string());

                                println!(
                                    "Added new replica connection. Total replicas: {}",
                                    connected_replicas
                                );
                                // Exit the loop - this connection is now a replica connection
                                // and should not process further client commands
                                return Ok(());
                            }
                        }
                        "WAIT" if items.len() == 3 => {
                            let num_replicas = items[1].as_integer().unwrap_or(0);
                            let timeout_ms = items[2].as_integer().unwrap_or(0) as u64;

                            use replica_ops::handle_wait;
                            let ack_count =
                                handle_wait(&replicas, num_replicas, timeout_ms).await?;

                            handler.write_value(RespValue::Integer(ack_count)).await?;
                        }
                        "TYPE" if items.len() == 2 => {
                            handle_type(&db, &items, &mut handler).await?;
                        }
                        "XADD" if items.len() >= 5 => {
                            stream_ops::handle_xadd(&db, &items, &mut handler).await?;
                        }
                        "XRANGE" if items.len() >= 4 => {
                            stream_ops::handle_xrange(&db, &items, &mut handler).await?;
                        }
                        "XREAD" if items.len() >= 4 => {
                            stream_ops::handle_xread(&db, &items, &mut handler).await?;
                        }
                        "RPUSH" if items.len() >= 3 => {
                            list_ops::handle_push(
                                &db,
                                &items,
                                &mut handler,
                                false,
                                &blocked_clients,
                            )
                            .await?;
                            propogate_to_replicas(&RespValue::Array(items.clone()), &replicas)
                                .await?;
                        }
                        "LPUSH" if items.len() >= 3 => {
                            list_ops::handle_push(
                                &db,
                                &items,
                                &mut handler,
                                true,
                                &blocked_clients,
                            )
                            .await?;
                            propogate_to_replicas(&RespValue::Array(items.clone()), &replicas)
                                .await?;
                        }
                        "LRANGE" if items.len() == 4 => {
                            list_ops::handle_lrange(&db, &items, &mut handler).await?;
                        }
                        "LLEN" if items.len() == 2 => {
                            list_ops::handle_llen(&db, &items, &mut handler).await?;
                        }
                        "LPOP" if items.len() >= 2 => {
                            list_ops::handle_lpop(&db, &items, &mut handler).await?;
                            propogate_to_replicas(&RespValue::Array(items.clone()), &replicas)
                                .await?;
                        }
                        "BLPOP" if items.len() >= 3 => {
                            list_ops::handle_blpop(&db, &items, &mut handler, &blocked_clients)
                                .await?;
                        }
                        "PUBLISH" if items.len() >= 3 => {
                            pub_sub::handle_publish(&items, &channels_map, &channel_subscribers, &mut handler).await?;
                        }
                        "SUBSCRIBE" if items.len() >= 2 => {
                            pub_sub::handle_subscribe(
                                &items,
                                &mut handler,
                                &mut subscribed_channels,
                                &channels_map,
                                &channel_subscribers,
                                msg_tx.clone(),
                            )
                            .await?;
                        }
                        "UNSUBSCRIBE" if items.len() >= 2 => {
                            pub_sub::handle_unsubscribe(
                                &items,
                                &mut handler,
                                &mut subscribed_channels,
                                &channels_map,
                                &channel_subscribers,
                            )
                            .await?;
                        }
                        "ZADD" if items.len() >= 4 => {
                            sorted_set::zadd(&db, &items, &mut handler).await?;
                            propogate_to_replicas(&RespValue::Array(items.clone()), &replicas)
                                .await?;
                        }
                        "ZRANK" if items.len() >= 3 => {
                            sorted_set::zrank(&db, &items, &mut handler).await?;
                        }
                        "ZRANGE" if items.len() >= 4 => {
                            sorted_set::zrange(&db, &items, &mut handler).await?;
                        }
                        "ZCARD" if items.len() == 2 => {
                            sorted_set::zcard(&db, &items, &mut handler).await?;
                        }
                        "ZSCORE" if items.len() == 3 => {
                            sorted_set::zscore(&db, &items, &mut handler).await?;
                        }
                        "ZREM" if items.len() >= 3 => {
                            sorted_set::zrem(&db, &items, &mut handler).await?;
                            propogate_to_replicas(&RespValue::Array(items.clone()), &replicas)
                                .await?;
                        }
                        "GEOADD" if items.len() >= 5 => {
                            geo::add(&db, &items, &mut handler).await?;
                            propogate_to_replicas(&RespValue::Array(items.clone()), &replicas)
                                .await?;
                        }
                        "GEOPOS" if items.len() >= 3 => {
                            geo::pos(&db, &items, &mut handler).await?;
                        }
                        "GEODIST" if items.len() >= 3 => {
                            geo::dist(&db, &items, &mut handler).await?;
                        }
                        "GEOSEARCH" if items.len() >= 5 => {
                            geo::search(&db, &items, &mut handler).await?;
                        }
                        "INCR" if items.len() == 2 => {
                            transactions::incr_key(&db, &items, &mut handler).await?;
                            propogate_to_replicas(&RespValue::Array(items.clone()), &replicas)
                                .await?;
                        }
                        "MULTI" => {
                            transactions::handle_multi(&mut handler, &mut in_transaction).await?;
                        }
                        "EXEC" => {
                            transactions::handle_exec(&mut handler, &mut in_transaction, &mut queued_commands, &db, &replicas).await?;
                        }
                        _ => {
                            handler
                                .write_value(RespValue::SimpleString("ERR unknown command".into()))
                                .await?;
                        }
                    }
                }
            }
            _ => {
                handler
                    .write_value(RespValue::SimpleString("PONG".into()))
                    .await?;
            }
        }
            }

            // Handle outgoing pub/sub messages
            Some((channel, message)) = msg_rx.recv() => {
                let resp_array = RespValue::Array(vec![
                    RespValue::BulkString("message".to_string()),
                    RespValue::BulkString(channel),
                    RespValue::BulkString(message),
                ]);
                handler.write_value(resp_array).await?;
            }
        }
    }

    // Cleanup: when the client connection ends, decrement global channel counts
    if !subscribed_channels.is_empty() {
        let mut map = channels_map.lock().await;
        for ch in subscribed_channels.iter() {
            if let Some(counter) = map.get_mut(ch) {
                if *counter > 0 {
                    *counter -= 1;
                }
            }
        }
        // Remove channels with zero subscribers
        map.retain(|_, &mut v| v > 0);
    }

    Ok(())
}

async fn propogate_to_replicas(
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
