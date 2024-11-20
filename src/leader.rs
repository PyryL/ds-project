use crate::communication::IncomingConnection;
use std::collections::HashMap;
use tokio::sync::mpsc;

pub async fn leader_block(
    mut incoming_connection_stream: mpsc::UnboundedReceiver<(IncomingConnection, Vec<u8>)>,
) {
    let mut leader_storage: HashMap<u64, Vec<u8>> = HashMap::new();

    // some test values, remove later
    leader_storage.insert(42, b"hello world".to_vec());

    while let Some((connection, first_message)) = incoming_connection_stream.recv().await {
        match first_message.first() {
            Some(1) => handle_read_request(connection, first_message, &leader_storage).await,
            _ => panic!(),
        };
    }
}

async fn handle_read_request(connection: IncomingConnection, message: Vec<u8>, storage: &HashMap<u64, Vec<u8>>) {
    // at this point, first byte of connection.message is `1`
    if message.len() != 13 {
        println!("received invalid type=1 message, dropping");
        dbg!(message);
        return;
    }
    let key = u64::from_be_bytes(message[5..13].try_into().unwrap());

    println!("reading value key={} for {}", key, connection.address);

    let default_value = Vec::new();
    let value = storage.get(&key).unwrap_or(&default_value).clone();

    let response_length = 5 + value.len() as u32;
    let response = [vec![0], response_length.to_be_bytes().to_vec(), value].concat();
    connection.respond(&response).await;
}
