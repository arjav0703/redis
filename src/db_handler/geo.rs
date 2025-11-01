use super::*;

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

    let mut db = db.lock().await;

    if let Some(entry) = db.get_mut(&key) {
        match &mut entry.value {
            crate::types::ValueType::SortedSet(vec) => {
                if let Some(existing) = vec.iter_mut().find(|(m, _)| m == &member) {
                    existing.1 = 0.0;
                    handler.write_value(RespValue::Integer(0)).await?;
                } else {
                    vec.push((member.clone(), 0.0));
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
                value: crate::types::ValueType::SortedSet(vec![(member.clone(), 0.0)]),
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
