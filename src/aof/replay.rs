use anyhow::Result;

use crate::{
    client_handler::SharedResources,
    db_handler::set_key::set_key_internal,
    types::commands::{CommandName, RedisCommand},
};

pub fn replay_commands(
    items: Vec<RedisCommand>,
    shared_resources: &mut SharedResources,
) -> Result<()> {
    for item in items {
        let db = shared_resources.db.clone();

        tokio::spawn(async move {
            match item.name {
                CommandName::Set => {
                    set_key_internal(&db, &item.args, &mut false).await.unwrap();
                }
                _ => {
                    println!("Unsupported command in AOF replay: {:?}", item.name);
                }
            }
        });
    }
    Ok(())
}
