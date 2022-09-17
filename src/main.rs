use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use mini_redis::Command::{Get, Set};
use mini_redis::{Command, Connection, Frame};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("0:1234").await.unwrap();

    let db = Arc::new(Mutex::new(HashMap::new()));
    println!("Listening");

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let db = db.clone();
        println!("Accepted");
        tokio::spawn(async move {
            process(socket, db).await;
        });
    }
}

async fn process(socket: TcpStream, db: Arc<Mutex<HashMap<String, Bytes>>>) {
    let mut connection = Connection::new(socket);
    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                let mut db = db.lock().unwrap();
                db.insert(cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                let db = db.lock().unwrap();
                if let Some(value) = db.get(cmd.key()) {
                    Frame::Bulk(value.clone())
                } else {
                    Frame::Null
                }
            }
            cmd => {
                panic!("Unimplemented {:?}", cmd);
            }
        };
        connection.write_frame(&response).await.unwrap();
    }
}
