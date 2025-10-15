use crate::types::KeyWithExpiry;
use anyhow::{Ok, Result};
use std::collections::HashMap;
use std::env;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::fs::File;
use tokio::io::AsyncReadExt;

pub async fn read_rdb_file() -> Result<HashMap<String, KeyWithExpiry>> {
    let dbfilename = env::var("dbfilename").expect("unexpected env var dbfilename");
    let dir = env::var("dir").expect("unexpected env var dir");
    let path = format!("{dir}/{dbfilename}");
    dbg!(&path);
    if tokio::fs::metadata(&path).await.is_err() {
        return Ok(HashMap::new());
    }
    let mut file = File::open(path).await.expect("failed to open file");
    let mut contents = Vec::new();
    file.read_to_end(&mut contents).await?;

    let db = parse_rdb(&contents)?;

    Ok(db)
}

fn parse_rdb(contents: &[u8]) -> Result<HashMap<String, KeyWithExpiry>> {
    let mut pos = 0;
    let mut db = HashMap::new();

    // check magic string "REDIS"
    if &contents[pos..pos + 5] != b"REDIS" {
        return Err(anyhow::anyhow!(
            "Invalid RDB file: missing REDIS magic string"
        ));
    }
    pos += 5;

    // read version (4 bytes)
    let _version = std::str::from_utf8(&contents[pos..pos + 4])?;
    pos += 4;

    // Parse the file
    while pos < contents.len() {
        let op_code = contents[pos];
        pos += 1;

        match op_code {
            0xFA => {
                // Metadata
                let (_key, new_pos) = read_string(contents, pos)?;
                pos = new_pos;
                let (_value, new_pos) = read_string(contents, pos)?;
                pos = new_pos;
                // skip metadata
            }
            0xFE => {
                // Database selector
                let (_db_number, new_pos) = read_length(contents, pos)?;
                pos = new_pos;
            }
            0xFB => {
                // Resizedb
                let (_hash_table_size, new_pos) = read_length(contents, pos)?;
                pos = new_pos;
                let (_expire_hash_table_size, new_pos) = read_length(contents, pos)?;
                pos = new_pos;
            }
            0xFD => {
                // Expiry in seconds (Unix timestamp)
                let expiry_secs = u32::from_le_bytes([
                    contents[pos],
                    contents[pos + 1],
                    contents[pos + 2],
                    contents[pos + 3],
                ]);
                pos += 4;
                let (key, value, new_pos) = read_key_value_pair(contents, pos)?;
                pos = new_pos;

                // Convert Unix timestamp to Instant
                let expiry_timestamp = UNIX_EPOCH + Duration::from_secs(expiry_secs as u64);
                let now = SystemTime::now();
                
                if let std::result::Result::Ok(duration_until_expiry) = expiry_timestamp.duration_since(now) {
                    // Not expired yet
                    db.insert(
                        key,
                        KeyWithExpiry {
                            value,
                            expiry: Some(Instant::now() + duration_until_expiry),
                        },
                    );
                }
                // If expired, don't insert into db
            }
            0xFC => {
                // Expiry in milliseconds (Unix timestamp)
                let expiry_ms = u64::from_le_bytes([
                    contents[pos],
                    contents[pos + 1],
                    contents[pos + 2],
                    contents[pos + 3],
                    contents[pos + 4],
                    contents[pos + 5],
                    contents[pos + 6],
                    contents[pos + 7],
                ]);
                pos += 8;
                let (key, value, new_pos) = read_key_value_pair(contents, pos)?;
                pos = new_pos;

                // Convert Unix timestamp to Instant
                let expiry_timestamp = UNIX_EPOCH + Duration::from_millis(expiry_ms);
                let now = SystemTime::now();
                
                if let std::result::Result::Ok(duration_until_expiry) = expiry_timestamp.duration_since(now) {
                    // Not expired yet
                    db.insert(
                        key,
                        KeyWithExpiry {
                            value,
                            expiry: Some(Instant::now() + duration_until_expiry),
                        },
                    );
                }
                // If expired, don't insert into db
            }
            0xFF => {
                // End of file
                break;
            }
            0x00 => {
                // String encoding - key-value pair without expiry
                let (key, new_pos) = read_string(contents, pos)?;
                pos = new_pos;
                let (value, new_pos) = read_string(contents, pos)?;
                pos = new_pos;

                db.insert(
                    key,
                    KeyWithExpiry {
                        value,
                        expiry: None,
                    },
                );
            }
            _ => {
                return Err(anyhow::anyhow!("Unknown opcode: 0x{:02X}", op_code));
            }
        }
    }

    Ok(db)
}

fn read_length(contents: &[u8], pos: usize) -> Result<(usize, usize)> {
    let first_byte = contents[pos];
    let encoding_type = (first_byte & 0xC0) >> 6;

    match encoding_type {
        0b00 => {
            // 6-bit length
            Ok(((first_byte & 0x3F) as usize, pos + 1))
        }
        0b01 => {
            // 14-bit length
            let length = (((first_byte & 0x3F) as usize) << 8) | (contents[pos + 1] as usize);
            Ok((length, pos + 2))
        }
        0b10 => {
            // 32-bit length
            let length = u32::from_be_bytes([
                contents[pos + 1],
                contents[pos + 2],
                contents[pos + 3],
                contents[pos + 4],
            ]) as usize;
            Ok((length, pos + 5))
        }
        0b11 => {
            // Special encoding
            Ok((first_byte as usize, pos + 1))
        }
        _ => unreachable!(),
    }
}

fn read_string(contents: &[u8], pos: usize) -> Result<(String, usize)> {
    let first_byte = contents[pos];
    let encoding_type = (first_byte & 0xC0) >> 6;

    if encoding_type == 0b11 {
        // Special integer encoding
        let encoding_format = first_byte & 0x3F;
        match encoding_format {
            0 => {
                // 8-bit integer
                let value = contents[pos + 1] as i8;
                Ok((value.to_string(), pos + 2))
            }
            1 => {
                // 16-bit integer
                let value = i16::from_le_bytes([contents[pos + 1], contents[pos + 2]]);
                Ok((value.to_string(), pos + 3))
            }
            2 => {
                // 32-bit integer
                let value = i32::from_le_bytes([
                    contents[pos + 1],
                    contents[pos + 2],
                    contents[pos + 3],
                    contents[pos + 4],
                ]);
                Ok((value.to_string(), pos + 5))
            }
            _ => Err(anyhow::anyhow!(
                "Unsupported string encoding: {}",
                encoding_format
            )),
        }
    } else {
        // Normal string
        let (length, new_pos) = read_length(contents, pos)?;
        if new_pos + length > contents.len() {
            return Err(anyhow::anyhow!(
                "String length {} exceeds remaining bytes at position {}",
                length,
                new_pos
            ));
        }
        let string = String::from_utf8(contents[new_pos..new_pos + length].to_vec())?;
        Ok((string, new_pos + length))
    }
}

fn read_key_value_pair(contents: &[u8], pos: usize) -> Result<(String, String, usize)> {
    let value_type = contents[pos];
    let new_pos = pos + 1;

    match value_type {
        0x00 => {
            // String encoding
            let (key, pos1) = read_string(contents, new_pos)?;
            let (value, pos2) = read_string(contents, pos1)?;
            Ok((key, value, pos2))
        }
        _ => Err(anyhow::anyhow!(
            "Unsupported value type: 0x{:02X}",
            value_type
        )),
    }
}
