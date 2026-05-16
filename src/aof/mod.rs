pub mod replay;

use std::fs;
use std::io::Write;
use tokio::sync::MutexGuard;

use anyhow::Result;

use crate::types::{
    commands::{CommandType, RedisCommand},
    resp::{parse_msg, RespValue},
    ServerConfig,
};
use tracing::info;

pub async fn init_aof(server_config: &MutexGuard<'_, ServerConfig>) -> Result<()> {
    if !server_config.appendonly.to_bool() {
        info!("AOF is disabled, skipping AOF initialization.");
        return Ok(());
    }

    let aof_dir_path = format!("{}/{}", server_config.dir, server_config.appenddirname,);
    info!("AOF dir path: {aof_dir_path}");
    fs::create_dir(&aof_dir_path).or_else(|e| {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            Ok(())
        } else {
            Err(e)
        }
    })?;

    let aof_file_path = format!(
        "{}/{}.1.incr.aof",
        aof_dir_path, server_config.appendfilename
    );
    info!("AOF file path: {aof_file_path}");
    if !std::path::Path::new(&aof_file_path).exists() {
        fs::write(&aof_file_path, "")?;
    }

    let manifest_path = format!(
        "{}/{}/{}.manifest",
        server_config.dir, server_config.appenddirname, server_config.appendfilename
    );
    if !std::path::Path::new(&manifest_path).exists() {
        create_aof_manifest(server_config)?;
    }
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

    pub fn append_to_aof_file(&self, command: &RedisCommand) -> Result<()> {
        if !self.appendonly.to_bool() {
            return Ok(());
        }

        if command.variant == CommandType::Read {
            return Ok(());
        }

        let aof_file_name = self.get_aof_file()?;
        let aof_file_path = format!("{}/{}/{}", self.dir, self.appenddirname, aof_file_name);

        let mut file = fs::OpenOptions::new().append(true).open(&aof_file_path)?;
        file.write_all(RespValue::Array(command.args.clone()).encode().as_bytes())?;

        Ok(())
    }

    pub fn get_replay_commands(&self) -> Vec<RedisCommand> {
        if !self.appendonly.to_bool() {
        info!("AOF is disabled, no commands to replay.");
        return vec![];
    }

        let aof_file_name = match self.get_aof_file() {
            Ok(name) => name,
            Err(e) => {
                eprintln!("Error reading AOF manifest: {e}");
                return vec![];
            }
        };
        let aof_file_path = format!("{}/{}/{}", self.dir, self.appenddirname, aof_file_name);

        let content = match fs::read(&aof_file_path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Error reading AOF file: {e}");
                return vec![];
            }
        };
        let mut commands = Vec::new();
        let mut offset = 0usize;

        while offset < content.len() {
            match parse_msg(&content[offset..]) {
                Ok((value, used)) => {
                    offset += used;
                    if let RespValue::Array(items) = value {
                        if let Some(command) = RedisCommand::from_items(&items) {
                            commands.push(command);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error parsing AOF at offset {offset}: {e}");
                    break;
                }
            }
        }

        commands
    }
}
