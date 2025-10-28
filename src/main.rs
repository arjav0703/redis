use anyhow::{Ok, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
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
use db_handler::{set_key::*, *};
mod file_handler;
mod replica;

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

    loop {
        let (stream, _) = listener.accept().await?;

        let db = db.clone();
        let replicas = replicas.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, db, replicas).await {
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
) -> Result<()> {
    let mut handler = RespHandler::new(stream);
    // let mut db: HashMap<String, String> = HashMap::new();

    while let Some(val) = handler.read_value().await? {
        match val {
            RespValue::Array(items) if !items.is_empty() => {
                if let RespValue::BulkString(cmd) | RespValue::SimpleString(cmd) = &items[0] {
                    let cmd_upper = cmd.to_ascii_uppercase();
                    match cmd_upper.as_str() {
                        "PING" => db_handler::send_pong(&mut handler, &items).await?,
                        "ECHO" if items.len() == 2 => {
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
                            list_ops::handle_push(&db, &items, &mut handler, false).await?;
                            propogate_to_replicas(&RespValue::Array(items.clone()), &replicas)
                                .await?;
                        }
                        "LPUSH" if items.len() >= 3 => {
                            list_ops::handle_push(&db, &items, &mut handler, true).await?;
                            propogate_to_replicas(&RespValue::Array(items.clone()), &replicas)
                                .await?;
                        }
                        "LRANGE" if items.len() == 4 => {
                            list_ops::handle_lrange(&db, &items, &mut handler).await?;
                        }
                        "LLEN" if items.len() == 2 => {
                            list_ops::handle_llen(&db, &items, &mut handler).await?;
                        }
                        "LPOP" if items.len() == 2 => {
                            list_ops::handle_lpop(&db, &items, &mut handler).await?;
                            propogate_to_replicas(&RespValue::Array(items.clone()), &replicas)
                                .await?;
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
