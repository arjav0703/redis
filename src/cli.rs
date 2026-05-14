use crate::types::{AppendOnly, ServerConfig};
use clap::Parser;
use rand::distr::Alphanumeric;
use rand::Rng;
use std::env;

/// Function to get 'dir' and 'dbfilename' parameters from CLI args and set them as env variables
/// for later use
pub fn set_env_vars() {
    let args = Cli::parse();

    let port = args.port.clone().unwrap_or("6379".to_string());
    env::set_var("port", port);

    let replicaof = args.replicaof.unwrap_or("".to_string());

    if !replicaof.is_empty() {
        env::set_var("replicaof", replicaof);
    }

    let replication_id: String = (0..40)
        .map(|_| rand::rng().sample(Alphanumeric) as char)
        .collect();

    env::set_var("replication_id", replication_id);
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[clap(short, long)]
    dir: Option<String>,
    #[clap(long, default_value = "dump.rdb")]
    dbfilename: Option<String>,
    #[clap(short, long, default_value = "6379")]
    port: Option<String>,
    #[clap(short, long, default_value = "")]
    replicaof: Option<String>,
    #[clap(short, long, default_value = "false")]
    appendonly: Option<String>,
    #[clap(long, default_value = "appendonlydir")]
    appenddirname: Option<String>,
    #[clap(long, default_value = "appendonly.aof")]
    appendfilename: Option<String>,
    #[clap(long, default_value = "everysec")]
    appendfsync: Option<String>,
}

impl ServerConfig {
    pub fn from_cli() -> Self {
        let args = Cli::parse();
        let dir = args.dir;
        let dbfilename = args.dbfilename.unwrap_or("dump.rdb".to_string());
        let port = args.port.unwrap_or("6379".to_string());
        let replicaof = args.replicaof.unwrap_or("".to_string());
        let appendonly = AppendOnly::from(args.appendonly.unwrap());
        let appenddirname = args.appenddirname.unwrap();
        let appendfilename = args.appendfilename.unwrap();
        let appendfsync = args.appendfsync.unwrap();

        ServerConfig {
            dir: dir.unwrap_or_else(|| env::current_dir().unwrap().to_str().unwrap().to_string()),
            appendonly,
            appenddirname,
            appendfilename,
            appendfsync,
            is_replica: !replicaof.is_empty(),
            dbfilename,
            port,
        }
    }
}
