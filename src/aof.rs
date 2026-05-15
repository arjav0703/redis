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
    fs::create_dir(&aof_dir_path)?;

    let aof_file_path = format!(
        "{}/{}.1.incr.aof",
        aof_dir_path, server_config.appendfilename
    );
    dbg!(&aof_file_path);
    fs::write(&aof_file_path, "")?;

    create_aof_manifest(server_config)?;

    Ok(())
}

fn create_aof_manifest(server_config: &ServerConfig) -> Result<()> {
    let manifest_path = format!(
        "{}/{}/{}.manifest",
        server_config.dir, server_config.appenddirname, server_config.appendfilename
    );

    let content = format!(
        "file {}.1.incr.aof seq 1 type i\n",
        server_config.appendfilename
    );
    fs::write(&manifest_path, content)?;
    Ok(())
}
