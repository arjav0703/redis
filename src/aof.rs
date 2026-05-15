use std::fs;
use std::io::Write;
use tokio::sync::MutexGuard;

use anyhow::Result;

use crate::types::{resp::RespValue, ServerConfig};

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

fn parse_aof_manifest(manifest_path: &str) -> Result<String> {
    let content = fs::read_to_string(manifest_path)?;
    for line in content.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 4 && parts[0] == "file" {
            return Ok(parts[1].to_string());
        }
    }

    Err(anyhow::anyhow!("No valid AOF file entry found in manifest"))
}

impl ServerConfig {
    pub fn get_aof_file(&self) -> Result<String> {
        let manifest_path = format!(
            "{}/{}/{}.manifest",
            self.dir, self.appenddirname, self.appendfilename
        );
        parse_aof_manifest(&manifest_path)
    }

    pub fn append_to_aof_file(&self, command: &[RespValue]) -> Result<()> {
        if !self.appendonly.to_bool() {
            return Ok(());
        }
        let aof_file_name = self.get_aof_file()?;
        let aof_file_path = format!("{}/{}/{}", self.dir, self.appenddirname, aof_file_name);

        let mut file = fs::OpenOptions::new().append(true).open(&aof_file_path)?;
        file.write_all(RespValue::Array(command.to_vec()).encode().as_bytes())?;

        Ok(())
    }
}
