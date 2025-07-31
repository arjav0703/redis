use clap::Parser;
use std::env;

/// Function to get 'dir' and 'dbfilename' parameters from CLI args and set them as env variables
/// for later use
pub fn set_env_vars() {
    let (dir, dbfilename) = getargs();
    env::set_var("dir", dir);
    env::set_var("dbfilename", dbfilename);
}

/// Gets args :)
fn getargs() -> (String, String) {
    let args = Cli::parse();

    let dir = args.dir.unwrap_or(".".to_string());
    let dbfilename = args
        .dbfilename
        .unwrap_or("mini-redis-database.rdb".to_string());

    (dir, dbfilename)
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[clap(short, long, default_value = ".")]
    dir: Option<String>,
    #[clap(long, default_value = "mini-redis-database.rdb")]
    dbfilename: Option<String>,
}
