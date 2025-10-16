use anyhow::{Ok, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
pub mod cli;
pub mod parsers;
use std::env;
mod types;
use cli::set_env_vars;
use types::{KeyWithExpiry, RespHandler, RespValue};
mod db_handler;
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

    if is_isreplica {
        replica::replica_handler().await;
    }

    let initial_db = file_handler::read_rdb_file().await?;
    println!("Initial DB state from RDB file: {initial_db:?}");
    let db = Arc::new(tokio::sync::Mutex::new(
        HashMap::<String, KeyWithExpiry>::new(),
    ));
    {
        let mut db_lock = db.lock().await;
        *db_lock = initial_db;
    }
    loop {
        let (stream, _) = listener.accept().await?;

        let db = db.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, db).await {
                eprintln!("connection error: {e:#}");
            }
        });
    }
}

/// Function to handle the requests made (rerun everytime a request is received)
async fn handle_client(
    stream: TcpStream,
    db: Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
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
                            db_handler::set_key(&db, &items, &mut handler).await?
                        }
                        "GET" if items.len() == 2 => {
                            db_handler::get_key(&db, &items, &mut handler).await?
                        }
                        "DEL" if items.len() >= 2 => {
                            db_handler::del_key(&db, &items, &mut handler).await?
                        }
                        "CONFIG" => {
                            db_handler::handle_config(&items, &mut handler).await?;
                        }
                        "KEYS" if items.len() == 2 => {
                            let pattern = items[1].as_string().unwrap_or_default();
                            db_handler::handle_key_search(&db, &pattern, &mut handler).await?;
                        }
                        "INFO" if items.len() == 2 => {
                            db_handler::handle_info(&mut handler).await?;
                        }
                        "REPLCONF" => {
                            db_handler::handle_replconf(&items, &mut handler).await?;
                        }
                        "PSYNC" if items.len() >= 2 => {
                            db_handler::handle_psync(&items, &db, &mut handler).await?;
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
