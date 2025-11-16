use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
pub mod cli;
pub mod parsers;
use std::env;
mod types;
use cli::set_env_vars;
use types::{replica::ReplicaConnection, KeyWithExpiry};

mod client_handler;
mod command_dispatcher;
mod command_processor;
mod db_handler;
mod file_handler;
mod replica;
mod replication;

use client_handler::handle_client;

#[derive(Debug)]
pub struct BlockedClient {
    pub list_key: String,
    pub sender: mpsc::Sender<(String, String)>, // Sends (list_key, value) when unblocked
}

pub type BlockedClients = Arc<tokio::sync::Mutex<Vec<BlockedClient>>>;
pub type Users = Arc<tokio::sync::Mutex<HashMap<String, Option<String>>>>;

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

    let users: Users = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    {
        let mut users_lock = users.lock().await;
        users_lock.insert("default".to_string(), None);
    }

    loop {
        let (stream, _) = listener.accept().await?;

        let db = db.clone();
        let replicas = replicas.clone();
        let blocked_clients = blocked_clients.clone();
        let channels_map = channels_map.clone();
        let channel_subscribers = channel_subscribers.clone();
        let users = users.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_client(
                stream,
                db,
                replicas,
                blocked_clients,
                channels_map,
                channel_subscribers,
                users,
            )
            .await
            {
                eprintln!("connection error: {e:#}");
            }
        });
    }
}
