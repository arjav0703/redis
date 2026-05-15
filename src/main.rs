use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
pub mod cli;
pub mod parsers;
mod types;
use cli::set_env_vars;
use types::{replica::ReplicaConnection, KeyWithExpiry};

mod aof;
mod client_handler;
mod command_dispatcher;
mod command_processor;
mod db_handler;
mod file_handler;
mod replica;
mod replication;

use client_handler::handle_client;

use crate::{
    aof::init_aof,
    client_handler::{AuthState, SharedResources},
    db_handler::acl,
    types::ServerConfig,
};

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
    let server_config: Arc<tokio::sync::Mutex<ServerConfig>> =
        Arc::new(tokio::sync::Mutex::new(ServerConfig::from_cli()));
    let config_lock = server_config.lock().await;
    let port = { config_lock.port.clone() };

    set_env_vars();
    let url = format!("127.0.0.1:{port}");

    println!("Mini‑Redis listening on {url}");
    let listener = TcpListener::bind(url).await?;

    init_aof(&config_lock).await?;

    let initial_db = file_handler::read_rdb_file(&config_lock.dir, &config_lock.dbfilename).await?;
    println!("Initial DB state from RDB file: {initial_db:?}");
    let db = Arc::new(tokio::sync::Mutex::new(
        HashMap::<String, KeyWithExpiry>::new(),
    ));
    {
        let mut db_lock = db.lock().await;
        *db_lock = initial_db;
    }

    let commands = config_lock.get_replay_commands();

    if config_lock.is_replica {
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
    let watch_violated: Arc<tokio::sync::Mutex<bool>> = Arc::new(tokio::sync::Mutex::new(false));

    let users: Users = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    {
        let mut users_lock = users.lock().await;
        users_lock.insert("default".to_string(), None);
    }

    drop(config_lock);
    loop {
        let (stream, _) = listener.accept().await?;

        let db = db.clone();
        let replicas = replicas.clone();
        let blocked_clients = blocked_clients.clone();
        let channels_map = channels_map.clone();
        let channel_subscribers = channel_subscribers.clone();
        let users = users.clone();
        let watch_violated = watch_violated.clone();
        let server_config = server_config.clone();
        // Create a per-connection AuthState so existing connections keep their state
        // when ACL SETUSER changes the default user's password.
        let default_user_nopass = acl::check_nopass_user(&users, "default").await;
        let authstate = Arc::new(tokio::sync::Mutex::new(AuthState {
            is_authenticated: default_user_nopass,
            username: "default".to_string(),
        }));
        tokio::spawn(async move {
            if let Err(e) = handle_client(
                stream,
                SharedResources {
                    db,
                    replicas,
                    blocked_clients,
                    channels_map,
                    channel_subscribers,
                    users,
                    authstate,
                    watch_violated,
                    server_config,
                },
            )
            .await
            {
                eprintln!("connection error: {e:#}");
            }
        });
    }
}
