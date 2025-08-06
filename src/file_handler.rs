use crate::types::KeyWithExpiry;
use anyhow::{Ok, Result};
use std::collections::HashMap;
use std::env;
use std::time::Instant;
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
    dbg!(&contents);

    let mut db = HashMap::new();
    let key = String::from("foo");
    let value = String::from("bar");
    let expiry: i64 = 0;
    db.insert(
        key,
        KeyWithExpiry {
            value,
            expiry: if expiry > 0 {
                Some(Instant::now() + std::time::Duration::from_millis(expiry as u64))
            } else {
                None
            },
        },
    );

    Ok(db)
}
