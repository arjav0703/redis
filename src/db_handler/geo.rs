use super::*;
mod decode;
mod encode;

pub async fn add(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
) -> Result<()> {
    // > GEOADD places -0.0884948 51.506479 "London"
    let key = items[1].as_string().unwrap();
    let longitude = items[2].as_string().unwrap().parse::<f64>()?;
    let latitude = items[3].as_string().unwrap().parse::<f64>()?;
    let member = items[4].as_string().unwrap();

    let (longitude_valid, latitude_valid) =
        (validate_longitude(longitude), validate_latitude(latitude));

    if !longitude_valid || !latitude_valid {
        let err_msg = format!(
            "ERR invalid longitude,latitude pair {:.6},{:.6}",
            longitude, latitude
        );
        handler.write_value(RespValue::SimpleError(err_msg)).await?;
        return Ok(());
    }

    let score = encode::encode(latitude, longitude);

    let mut db = db.lock().await;

    if let Some(entry) = db.get_mut(&key) {
        match &mut entry.value {
            crate::types::ValueType::SortedSet(vec) => {
                if let Some(existing) = vec.iter_mut().find(|(m, _)| m == &member) {
                    existing.1 = score as f64;
                    handler.write_value(RespValue::Integer(0)).await?;
                } else {
                    vec.push((member.clone(), score as f64));
                    handler.write_value(RespValue::Integer(1)).await?;
                }
            }
            _ => {
                handler
                    .write_value(RespValue::SimpleError(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    ))
                    .await?;
                return Ok(());
            }
        }
    } else {
        db.insert(
            key.clone(),
            KeyWithExpiry {
                value: crate::types::ValueType::SortedSet(vec![(member.clone(), score as f64)]),
                expiry: None,
            },
        );
        handler.write_value(RespValue::Integer(1)).await?;
    }
    Ok(())
}

fn validate_longitude(longitude: f64) -> bool {
    longitude >= -180.0 && longitude <= 180.0
}

fn validate_latitude(latitude: f64) -> bool {
    latitude >= -85.05112878 && latitude <= 85.05112878
}

pub async fn pos(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
) -> Result<()> {
    // > GEOPOS places London Munich
    // items[1] = key, items[2..] = members
    let key = items[1].as_string().unwrap();

    let db = db.lock().await;
    let mut response_items: Vec<RespValue> = Vec::new();

    if let Some(entry) = db.get(&key) {
        match &entry.value {
            crate::types::ValueType::SortedSet(vec) => {
                // For each requested member, return either ["0","0"] or null array
                for member_item in &items[2..] {
                    let member = member_item.as_string().unwrap_or_default();
                    if let Some((_m, _score)) = vec.iter().find(|(m, _)| m == &member) {
                        // Per instructions we can hardcode lat/long to "0"
                        let mut coord = Vec::new();
                        coord.push(RespValue::BulkString("0".to_string()));
                        coord.push(RespValue::BulkString("0".to_string()));
                        response_items.push(RespValue::Array(coord));
                    } else {
                        // Member not found -> null array
                        response_items.push(RespValue::NullArray);
                    }
                }
            }
            _ => {
                handler
                    .write_value(RespValue::SimpleError(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    ))
                    .await?;
                return Ok(());
            }
        }
    } else {
        // Key doesn't exist -> return null-array for each requested member
        for _ in &items[2..] {
            response_items.push(RespValue::NullArray);
        }
    }

    handler.write_value(RespValue::Array(response_items)).await?;
    Ok(())
}
