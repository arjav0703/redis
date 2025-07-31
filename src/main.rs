use anyhow::{Ok, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
pub mod cli;
pub mod parsers;
mod types;
use cli::set_env_vars;
use types::{KeyWithExpiry, RespHandler, RespValue};
mod db_handler;

#[tokio::main]
async fn main() -> Result<()> {
    set_env_vars();
    println!("Mini‑Redis listening on 127.0.0.1:6379");
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    let db = Arc::new(tokio::sync::Mutex::new(
        HashMap::<String, KeyWithExpiry>::new(),
    ));

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
