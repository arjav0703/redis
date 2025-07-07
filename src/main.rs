use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    println!("Starting Redis server at port 6379...");
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let stream = listener.accept().await;

        match stream {
            Ok((mut stream, _)) => {
                //println!("accepted new connection");

                tokio::spawn(async move {
                    handle_req(stream).await;
                });
            }
            Err(e) => {
                println!("error: {e}");
            }
        }
    }
}

async fn handle_req(mut stream: tokio::net::TcpStream) {
    let mut buf = [0; 512];
    loop {
        let read_count = stream.read(&mut buf).await.unwrap();
        if read_count == 0 {
            break;
        }

        stream.write(b"+PONG\r\n").await.unwrap();
    }
}
