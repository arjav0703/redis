use clap::Parser;
use rand::distr::Alphanumeric;
use rand::Rng;
use std::env;

/// Function to get 'dir' and 'dbfilename' parameters from CLI args and set them as env variables
/// for later use
pub fn set_env_vars() {
    let (dir, dbfilename, port, isreplica) = getargs();
    if !isreplica.is_empty() {
        env::set_var("replicaof", isreplica);
    }

    let replication_id: String = (0..40)
        .map(|_| rand::rng().sample(Alphanumeric) as char)
        .collect();

    env::set_var("replication_id", replication_id);

    env::set_var("port", port);
    env::set_var("dir", dir);
    env::set_var("dbfilename", dbfilename);
}

/// Gets args :)
pub fn getargs() -> (String, String, String, String) {
    let args = Cli::parse();

    let dir = args.dir.unwrap_or(".".to_string());
    let dbfilename = args.dbfilename.unwrap_or("dump.rdb".to_string());
    let port = args.port.unwrap_or("6379".to_string());

    let replicaof = args.replicaof.unwrap_or("".to_string());
    (dir, dbfilename, port, replicaof)
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[clap(short, long, default_value = ".")]
    dir: Option<String>,
    #[clap(long, default_value = "dump.rdb")]
    dbfilename: Option<String>,
    #[clap(short, long, default_value = "6379")]
    port: Option<String>,
    #[clap(short, long, default_value = "")]
    replicaof: Option<String>,
}
