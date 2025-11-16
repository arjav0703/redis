use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::{
    types::{
        replica::ReplicaConnection,
        resp::{RespHandler, RespValue},
        KeyWithExpiry,
    },
    Users,
};
use crate::{BlockedClients, ChannelSubscribers};

/// Client state for managing connection-level context
pub struct ClientState {
    pub handler: RespHandler,
    pub subscribed_channels: std::collections::HashSet<String>,
    pub in_transaction: bool,
    pub queued_commands: Vec<RespValue>,
    pub msg_tx: mpsc::Sender<(String, String)>,
    pub msg_rx: mpsc::Receiver<(String, String)>,
}

impl ClientState {
    pub fn new(stream: TcpStream) -> Self {
        let (msg_tx, msg_rx) = mpsc::channel::<(String, String)>(100);
        Self {
            handler: RespHandler::new(stream),
            subscribed_channels: std::collections::HashSet::new(),
            in_transaction: false,
            queued_commands: Vec::new(),
            msg_tx,
            msg_rx,
        }
    }

    pub fn is_subscribed(&self) -> bool {
        !self.subscribed_channels.is_empty()
    }
}

/// Shared resources passed to command handlers
pub struct SharedResources {
    pub db: Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    pub replicas: Arc<tokio::sync::Mutex<Vec<ReplicaConnection>>>,
    pub blocked_clients: BlockedClients,
    pub channels_map: Arc<tokio::sync::Mutex<HashMap<String, usize>>>,
    pub channel_subscribers: ChannelSubscribers,
    pub users: Users,
}

/// Function to handle client connections
pub async fn handle_client(
    stream: TcpStream,
    db: Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    replicas: Arc<tokio::sync::Mutex<Vec<ReplicaConnection>>>,
    blocked_clients: BlockedClients,
    channels_map: Arc<tokio::sync::Mutex<HashMap<String, usize>>>,
    channel_subscribers: ChannelSubscribers,
    users: Users,
) -> Result<()> {
    let mut state = ClientState::new(stream);
    let resources = SharedResources {
        db,
        replicas,
        blocked_clients,
        channels_map,
        channel_subscribers,
        users,
    };

    loop {
        tokio::select! {
            // incoming commands from client
            val = state.handler.read_value() => {
                let val = match val? {
                    Some(v) => v,
                    None => break, // disconnected
                };

                let should_become_replica = crate::command_processor::process_command(val, &mut state, &resources).await?;
                if should_become_replica {
                    // Convert this connection to a replica connection
                    let stream = state.handler.into_stream();
                    let replica_conn = ReplicaConnection::new(stream);
                    let mut replicas_guard = resources.replicas.lock().await;
                    replicas_guard.push(replica_conn);

                    let connected_replicas = replicas_guard.len();
                    std::env::set_var("connected_replicas", connected_replicas.to_string());

                    println!("Added new replica connection. Total replicas: {}", connected_replicas);
                    return Ok(());
                }
            }

            // Handle outgoing pub/sub messages
            Some((channel, message)) = state.msg_rx.recv() => {
                send_pubsub_message(&mut state.handler, channel, message).await?;
            }
        }
    }

    cleanup_client(&state, &resources.channels_map).await;
    Ok(())
}

/// Send a pub/sub message to the client
async fn send_pubsub_message(
    handler: &mut RespHandler,
    channel: String,
    message: String,
) -> Result<()> {
    let resp_array = RespValue::Array(vec![
        RespValue::BulkString("message".to_string()),
        RespValue::BulkString(channel),
        RespValue::BulkString(message),
    ]);
    handler.write_value(resp_array).await
}

/// Cleanup client resources when connection ends
async fn cleanup_client(
    state: &ClientState,
    channels_map: &Arc<tokio::sync::Mutex<HashMap<String, usize>>>,
) {
    if !state.subscribed_channels.is_empty() {
        let mut map = channels_map.lock().await;
        for ch in state.subscribed_channels.iter() {
            if let Some(counter) = map.get_mut(ch) {
                if *counter > 0 {
                    *counter -= 1;
                }
            }
        }
        // Remove channels with zero subscribers
        map.retain(|_, &mut v| v > 0);
    }
}
