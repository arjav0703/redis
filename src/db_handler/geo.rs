use crate::db_handler::geo::decode::decode;

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
                // For each requested member, return either [longitude, latitude] or null array
                for member_item in &items[2..] {
                    let member = member_item.as_string().unwrap_or_default();
                    if let Some((_m, score)) = vec.iter().find(|(m, _)| m == &member) {
                        let (latitude, longitude) = decode::decode(*score as u64);
                        let mut coord = Vec::new();
                        coord.push(RespValue::BulkString(longitude.to_string()));
                        coord.push(RespValue::BulkString(latitude.to_string()));
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

    handler
        .write_value(RespValue::Array(response_items))
        .await?;
    Ok(())
}

pub async fn dist(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
) -> Result<()> {
    let key = items[1].as_string().unwrap();
    let point1 = items[2].as_string().unwrap();
    let point2 = items[3].as_string().unwrap();

    let points = vec![point1, point2];
    let mut scores = Vec::new();

    let db = db.lock().await;
    if let Some(entry) = db.get(&key) {
        match &entry.value {
            crate::types::ValueType::SortedSet(vec) => {
                for point in points {
                    if let Some((_member, score)) = vec.iter().find(|(m, _)| m == &point) {
                        scores.push(score);
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
    }

    let p1 = Coordinate::from_score(*scores[0]);
    let p2 = Coordinate::from_score(*scores[1]);

    let dist = p1.dist_from(&p2);

    handler
        .write_value(RespValue::BulkString(dist.to_string()))
        .await?;

    Ok(())
}

struct Coordinate {
    latitude: f64,
    longitude: f64,
}

impl Coordinate {
    pub fn from_score(score: f64) -> Coordinate {
        let (latitude, longitude) = decode(score as u64);

        Coordinate {
            latitude,
            longitude,
        }
    }

    const EARTH_RADIUS: f64 = 6372797.560856;

    pub fn dist_from(&self, point2: &Coordinate) -> f64 {
        // Haversine formula for great circle distance
        let lon1r = self.longitude.to_radians();
        let lon2r = point2.longitude.to_radians();
        let v = ((lon2r - lon1r) / 2.0).sin();

        // If v == 0, longitudes are practically the same
        if v == 0.0 {
            return self.get_lat_distance(point2.latitude);
        }

        let lat1r = self.latitude.to_radians();
        let lat2r = point2.latitude.to_radians();
        let u = ((lat2r - lat1r) / 2.0).sin();
        let a = u * u + lat1r.cos() * lat2r.cos() * v * v;

        2.0 * Self::EARTH_RADIUS * a.sqrt().asin()
    }

    fn get_lat_distance(&self, lat2d: f64) -> f64 {
        // Calculate distance when longitudes are the same
        let lat1r = self.latitude.to_radians();
        let lat2r = lat2d.to_radians();
        let u = ((lat2r - lat1r) / 2.0).sin();

        2.0 * Self::EARTH_RADIUS * (u * u).sqrt().asin()
    }
}

pub async fn search(
    db: &Arc<tokio::sync::Mutex<HashMap<String, KeyWithExpiry>>>,
    items: &[RespValue],
    handler: &mut RespHandler,
) -> Result<()> {
    // > GEOSEARCH places FROMLONLAT 2 48 BYRADIUS 100000 m

    let key = items[1].as_string().unwrap();
    let longitude: f64 = items[3].as_string().unwrap().parse()?;
    let latitude: f64 = items[4].as_string().unwrap().parse()?;
    let radius: f64 = items[6].as_string().unwrap().parse()?;

    let search_coordinates = Coordinate {
        longitude,
        latitude,
    };
    let mut result: Vec<RespValue> = Vec::new();

    let db = db.lock().await;
    if let Some(entry) = db.get(&key) {
        match &entry.value {
            crate::types::ValueType::SortedSet(vec) => {
                for location in vec {
                    let (member, score) = location;
                    let coordinate = Coordinate::from_score(*score);

                    if coordinate.dist_from(&search_coordinates) < radius {
                        result.push(RespValue::BulkString(member.clone()));
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
    }

    handler.write_value(RespValue::Array(result)).await?;
    Ok(())
}
