use super::*;
use std::collections::HashSet;
use tokio::sync::mpsc;

pub async fn handle_subscribe(
    items: &[RespValue],
    handler: &mut RespHandler,
    subscribed_channels: &mut HashSet<String>,
    channels_map: &Arc<tokio::sync::Mutex<HashMap<String, usize>>>,
    channel_subscribers: &Arc<
        tokio::sync::Mutex<HashMap<String, Vec<mpsc::Sender<(String, String)>>>>,
    >,
    msg_tx: mpsc::Sender<(String, String)>,
) -> Result<()> {
    let channel_name = items[1].as_string().unwrap_or_default();

    let newly_added = subscribed_channels.insert(channel_name.clone());

    if newly_added {
        let mut map = channels_map.lock().await;
        let counter = map.entry(channel_name.clone()).or_insert(0usize);
        *counter += 1;

        // Add this client's sender to the channel subscribers
        let mut subs = channel_subscribers.lock().await;
        subs.entry(channel_name.clone())
            .or_insert_with(Vec::new)
            .push(msg_tx);
    }

    let channel_count = subscribed_channels.len() as i64;

    handler
        .write_value(RespValue::Array(vec![
            RespValue::BulkString("subscribe".into()),
            RespValue::BulkString(channel_name.clone()),
            RespValue::Integer(channel_count),
        ]))
        .await?;
    Ok(())
}

pub async fn handle_publish(
    items: &[RespValue],
    channels_map: &Arc<tokio::sync::Mutex<HashMap<String, usize>>>,
    channel_subscribers: &Arc<
        tokio::sync::Mutex<HashMap<String, Vec<mpsc::Sender<(String, String)>>>>,
    >,
    handler: &mut RespHandler,
) -> Result<()> {
    let channel_name = items[1].as_string().unwrap_or_default();
    let message = items[2].as_string().unwrap_or_default();

    let count = {
        let map = channels_map.lock().await;
        map.get(&channel_name).cloned().unwrap_or(0usize)
    } as i64;

    // Send the message to all subscribers
    {
        let mut subs = channel_subscribers.lock().await;
        if let Some(subscribers) = subs.get_mut(&channel_name) {
            // Remove dead channels while sending
            subscribers.retain(|tx| tx.try_send((channel_name.clone(), message.clone())).is_ok());
        }
    }

    handler.write_value(RespValue::Integer(count)).await?;
    Ok(())
}

pub async fn handle_unsubscribe(
    items: &[RespValue],
    handler: &mut RespHandler,
    subscribed_channels: &mut HashSet<String>,
    channels_map: &Arc<tokio::sync::Mutex<HashMap<String, usize>>>,
    channel_subscribers: &Arc<
        tokio::sync::Mutex<HashMap<String, Vec<mpsc::Sender<(String, String)>>>>,
    >,
) -> Result<()> {
    let channel_name = items[1].as_string().unwrap();

    let was_subscribed = subscribed_channels.remove(&channel_name);
    if was_subscribed {
        let mut map = channels_map.lock().await;
        if let Some(counter) = map.get_mut(&channel_name) {
            *counter = counter.saturating_sub(1);
            if *counter == 0 {
                map.remove(&channel_name);
            }
        }

        let mut subs = channel_subscribers.lock().await;
        if let Some(subscribers) = subs.get_mut(&channel_name) {
            subscribers.retain(|tx| !tx.is_closed());
            if subscribers.is_empty() {
                subs.remove(&channel_name);
            }
        }
    }

    let channel_count = subscribed_channels.len() as i64;
    handler
        .write_value(RespValue::Array(vec![
            RespValue::BulkString("unsubscribe".into()),
            RespValue::BulkString(channel_name.clone()),
            RespValue::Integer(channel_count),
        ]))
        .await?;

    Ok(())
}
