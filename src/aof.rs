use std::fs;
use tokio::sync::MutexGuard;

use anyhow::Result;

use crate::types::ServerConfig;

pub async fn init_aof(server_config: &MutexGuard<'_, ServerConfig>) -> Result<()> {
    if !server_config.appendonly.to_bool() {
        println!("AOF is disabled, skipping AOF initialization.");
        return Ok(());
    }
    let aof_dir_path = format!("{}/{}", server_config.dir, server_config.appenddirname,);
    dbg!(&aof_dir_path);
    fs::create_dir(aof_dir_path)?;
    Ok(())
}
