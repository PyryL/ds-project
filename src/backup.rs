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
