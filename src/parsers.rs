use crate::types::parse_msg;
use crate::RespValue;
use anyhow::{anyhow, Result};

/// Function used to parse a simple string acc to the RESP conventions
pub fn parse_simple(buf: &[u8]) -> Result<(RespValue, usize)> {
    let end = find_crlf(&buf[1..]).ok_or(anyhow!("incomplete simple"))?;
    let s = std::str::from_utf8(&buf[1..1 + end])?.to_owned();
    Ok((RespValue::SimpleString(s), 1 + end + 2))
}

/// Function used to parse a bulk string
pub fn parse_bulk(buf: &[u8]) -> Result<(RespValue, usize)> {
    let len_end = find_crlf(&buf[1..]).ok_or(anyhow!("incomplete bulk len"))?;
    let len: usize = std::str::from_utf8(&buf[1..1 + len_end])?.parse()?;
    let start = 1 + len_end + 2;
    let end = start + len;
    if buf.len() < end + 2 {
        return Err(anyhow!("incomplete bulk body"));
    }
    let s = std::str::from_utf8(&buf[start..end])?.to_owned();
    Ok((RespValue::BulkString(s), end + 2))
}

/// Function used to parse an integer and return a RespValue
pub fn parse_int(buf: &[u8]) -> Result<(RespValue, usize)> {
    let end = find_crlf(&buf[1..]).ok_or(anyhow!("incomplete int"))?;
    let n: i64 = std::str::from_utf8(&buf[1..1 + end])?.parse()?;
    Ok((RespValue::Integer(n), 1 + end + 2))
}

/// Function used to parse an array
pub fn parse_array(buf: &[u8]) -> Result<(RespValue, usize)> {
    let len_end = find_crlf(&buf[1..]).ok_or(anyhow!("incomplete array len"))?;
    let count: usize = std::str::from_utf8(&buf[1..1 + len_end])?.parse()?;
    let mut consumed = 1 + len_end + 2;
    let mut items = Vec::with_capacity(count);

    for _ in 0..count {
        let (item, used) = parse_msg(&buf[consumed..])?;
        consumed += used;
        items.push(item);
    }
    Ok((RespValue::Array(items), consumed))
}

/// Function used to find if a RESP string contains a crlf (/r/n) and ... (tbh, idk what it does)
fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}
