use crate::communication::IncomingConnection;
use std::collections::HashMap;
use tokio::sync::mpsc;

pub async fn leader_block(
    mut incoming_connection_stream: mpsc::UnboundedReceiver<IncomingConnection>,
) {
    let mut leader_storage: HashMap<u64, Vec<u8>> = HashMap::new();

    // some test values, remove later
    leader_storage.insert(42, b"hello world".to_vec());

    let default_value = Vec::new();

    while let Some(connection) = incoming_connection_stream.recv().await {
        // TODO: also handle write requests

        // at this point, first byte of connection.message is `1`
        if connection.message.len() != 9 {
            println!("received invalid type=1 message, dropping");
            continue;
        }
        let key = u64::from_be_bytes(connection.message[1..9].try_into().unwrap());

        let value = leader_storage.get(&key).unwrap_or(&default_value).clone();
        let value_length_bytes = (value.len() as u32).to_be_bytes().to_vec();

        let response = [value_length_bytes, value].concat();
        connection.respond(&response).await;
    }
}
