use crate::types::resp::RespValue;

pub struct RedisCommand {
    pub name: CommandName,
    pub args: Vec<RespValue>,
    pub variant: CommandType,
}

#[derive(PartialEq, Eq)]
pub enum CommandType {
    Read,
    Write,
}

pub enum CommandName {
    Ping,
    Echo,
    Set,
    Get,
    Del,
    Incr,
    Type,
    Config,
    Keys,
    Info,
    Replconf,
    Wait,
    Psync,
    XAdd,
    XRange,
    XRead,
    RPush,
    LPush,
    LRange,
    LLen,
    LPop,
    BLPop,
    Publish,
    Subscribe,
    Unsubscribe,
    ZAdd,
    ZRank,
    ZRange,
    ZCard,
    ZScore,
    ZRem,
    GeoAdd,
    GeoPos,
    GeoDist,
    GeoSearch,
    Multi,
    Exec,
    Discard,
    Acl,
    Auth,
    Watch,
    Unwatch,
    Other,
}

impl CommandName {
    pub fn from_str(s: &str) -> Self {
        match s.to_ascii_uppercase().as_str() {
            "PING" => CommandName::Ping,
            "ECHO" => CommandName::Echo,
            "SET" => CommandName::Set,
            "GET" => CommandName::Get,
            "DEL" => CommandName::Del,
            "INCR" => CommandName::Incr,
            "TYPE" => CommandName::Type,
            "CONFIG" => CommandName::Config,
            "KEYS" => CommandName::Keys,
            "INFO" => CommandName::Info,
            "REPLCONF" => CommandName::Replconf,
            "WAIT" => CommandName::Wait,
            "PSYNC" => CommandName::Psync,
            "XADD" => CommandName::XAdd,
            "XRANGE" => CommandName::XRange,
            "XREAD" => CommandName::XRead,
            "RPUSH" => CommandName::RPush,
            "LPUSH" => CommandName::LPush,
            "LRANGE" => CommandName::LRange,
            "LLEN" => CommandName::LLen,
            "LPOP" => CommandName::LPop,
            "BLPOP" => CommandName::BLPop,
            "PUBLISH" => CommandName::Publish,
            "SUBSCRIBE" => CommandName::Subscribe,
            "UNSUBSCRIBE" => CommandName::Unsubscribe,
            "ZADD" => CommandName::ZAdd,
            "ZRANK" => CommandName::ZRank,
            "ZRANGE" => CommandName::ZRange,
            "ZCARD" => CommandName::ZCard,
            "ZSCORE" => CommandName::ZScore,
            "ZREM" => CommandName::ZRem,
            "GEOADD" => CommandName::GeoAdd,
            "GEOPOS" => CommandName::GeoPos,
            "GEODIST" => CommandName::GeoDist,
            "GEOSEARCH" => CommandName::GeoSearch,
            "MULTI" => CommandName::Multi,
            "EXEC" => CommandName::Exec,
            "DISCARD" => CommandName::Discard,
            "ACL" => CommandName::Acl,
            "AUTH" => CommandName::Auth,
            "WATCH" => CommandName::Watch,
            "UNWATCH" => CommandName::Unwatch,
            _ => CommandName::Other,
        }
    }
}

impl CommandType {
    pub fn from_name(name: &CommandName) -> Self {
        match name {
            CommandName::Set
            | CommandName::Del
            | CommandName::Incr
            | CommandName::XAdd
            | CommandName::RPush
            | CommandName::LPush
            | CommandName::LPop
            | CommandName::BLPop
            | CommandName::ZAdd
            | CommandName::ZRem
            | CommandName::GeoAdd
            | CommandName::Publish
            | CommandName::Multi
            | CommandName::Exec
            | CommandName::Discard
            | CommandName::Watch
            | CommandName::Unwatch => CommandType::Write,
            _ => CommandType::Read,
        }
    }
}

impl RedisCommand {
    pub fn from_items(items: &Vec<RespValue>) -> Option<Self> {
        let cmd_name = items.first()?.as_string()?;
        let name = CommandName::from_str(&cmd_name);
        let variant = CommandType::from_name(&name);

        Some(Self {
            name,
            args: items.clone(),
            variant,
        })
    }
}
