use super::*;

pub async fn zadd(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
) -> Result<()> {
    let key = items[1].as_string().unwrap();
    let score: f64 = items[2].as_string().unwrap().parse().unwrap();
    let member = items[3].as_string().unwrap();

    let set_entry: Vec<(String, f64)> = vec![(member.clone(), score)];

    let mut db = db.lock().await;
    let entry = db.entry(key.clone()).or_insert_with(|| KeyWithExpiry {
        value: crate::types::ValueType::SortedSet(set_entry),
        expiry: None,
    });
    // dbg!(&entry);

    handler.write_value(RespValue::Integer(1)).await?;

    Ok(())
}
