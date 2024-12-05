use crate::communication::IncomingConnection;
use std::collections::HashMap;
use tokio::sync::mpsc;

pub async fn backup_block(
    mut incoming_connection_stream: mpsc::UnboundedReceiver<(IncomingConnection, Vec<u8>)>,
) {
    let mut backup_storage: HashMap<u64, Vec<u8>> = HashMap::new();

    while let Some((connection, message)) = incoming_connection_stream.recv().await {
        match message.first() {
            Some(20) => handle_write_request(connection, message, &mut backup_storage).await,
            Some(32) => handle_transfer_request(connection, message, &mut backup_storage).await,
            _ => {}
        };
    }
}

async fn handle_write_request(
    mut connection: IncomingConnection,
    message: Vec<u8>,
    backup_storage: &mut HashMap<u64, Vec<u8>>,
) {
    // at this point, the first byte of message is 20
    if message.len() < 13 {
        println!("received invalid backup write request, dropping");
        return;
    }

    let key = u64::from_be_bytes(message[5..13].try_into().unwrap());
    let value = &message[13..];

    println!(
        "updating backup key={} value={:?} from {}",
        key, value, connection.address
    );

    backup_storage.insert(key, value.to_vec());

    connection.send_message(&[0, 0, 0, 0, 7, 111, 107]).await;
}

pub async fn handle_transfer_request(
    mut connection: IncomingConnection,
    message: Vec<u8>,
    storage: &mut HashMap<u64, Vec<u8>>,
) {
    // at this point the first byte of message is 32
    if message.len() != 21 {
        println!("received invalid backup transfer request, dropping");
        return;
    }

    let key_lower_bound = u64::from_be_bytes(message[5..13].try_into().unwrap());
    let key_upper_bound = u64::from_be_bytes(message[13..21].try_into().unwrap());

    let keys_to_transfer: Vec<_> = storage.keys().filter(|&&key| key_lower_bound <= key && key <= key_upper_bound).cloned().collect();

    println!("transfering keys {:?} ({}..={}) out from backup", keys_to_transfer, key_lower_bound, key_upper_bound);

    let mut response_payload = Vec::new();

    for key in keys_to_transfer {
        let value = storage.remove(&key).unwrap();

        response_payload.extend_from_slice(&key.to_be_bytes());
        response_payload.extend_from_slice(&(value.len() as u32).to_be_bytes());
        response_payload.extend_from_slice(&value);
    }

    let response_length = response_payload.len() as u32 + 5;
    let response = [vec![0], response_length.to_be_bytes().to_vec(), response_payload].concat();

    connection.send_message(&response).await;
}
