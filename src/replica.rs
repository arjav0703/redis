use std::env;
use tokio::io::AsyncWriteExt;

#[derive(Debug)]
struct ReplicaConfig {
    host: String,
    port: u16,
}

impl ReplicaConfig {
    fn new(host: String, port: String) -> Self {
        Self {
            host,
            port: port.parse().unwrap_or(6379),
        }
    }

    async fn ping(&self) {
        let url = format!("{}:{}", self.host, self.port);

        let mut stream = tokio::net::TcpStream::connect(url).await.unwrap();

        stream.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    }
}

pub async fn replica_handler() {
    let replicaof = env::var("replicaof").unwrap_or_default();

    let (mut host, port) = replicaof.split_once(" ").expect("Invalid replicaof format");

    if host == "localhost" {
        host = "127.0.0.1";
    }

    let master = ReplicaConfig::new(host.to_string(), port.to_string());
    master.ping().await;
    dbg!(master);
}
