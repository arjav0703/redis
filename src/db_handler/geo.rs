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

    handler.write_value(RespValue::Integer(1)).await?;
    Ok(())
}
