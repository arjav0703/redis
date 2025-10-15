use crate::types::KeyWithExpiry;
use anyhow::{Ok, Result};
use bytes::Buf;
use std::collections::HashMap;
use std::env;
use std::time::Instant;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

trait DecodeHex {
    fn decode_str(&self) -> Result<String>;
}

impl DecodeHex for str {
    fn decode_str(&self) -> Result<String> {
        let str_bytes = hex::decode(self).expect("failed to decode hex");
        let s = String::from_utf8(str_bytes).expect("failed to convert to string");
        Ok(s)
    }
}

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
    // dbg!(&contents);

    let contents = hex::encode(contents);
    // dbg!(&contents);

    let (keys, values) = parse_rdb(&contents).expect("failed to parse rdb file");
    let mut db = HashMap::new();

    // let key = String::from("foo");
    // let value = String::from("bar");
    // let expiry: i64 = 0;
    // db.insert(
    //     key,
    //     KeyWithExpiry {
    //         value,
    //         expiry: if expiry > 0 {
    //             Some(Instant::now() + std::time::Duration::from_millis(expiry as u64))
    // } else {
    //     None
    // },
    // },
    // );

    Ok(db)
}

fn parse_rdb(contents: &str) -> Result<(Vec<String>, Vec<String>)> {
    // strip the magic str
    let contents = &contents[18..];
    dbg!(&contents);

    if contents.starts_with("fa") {
        println!("metadata section started")
    } else {
        return Err(anyhow::anyhow!("invalid rdb file"));
    }

    let contents = contents.strip_prefix("fa").expect("error stripping prefix");

    let metadata = contents[0..20].decode_str()?;
    dbg!(&metadata);

    Ok((vec![], vec![]))
}
