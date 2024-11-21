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
            Some(2) => handle_write_request(connection, first_message, &mut leader_storage).await,
            _ => panic!(),
        };
    }
}

async fn handle_read_request(mut connection: IncomingConnection, message: Vec<u8>, storage: &HashMap<u64, Vec<u8>>) {
    // at this point, first byte of connection.message is `1`
    if message.len() != 13 {
        println!("received invalid type=1 message, dropping");
        return;
    }
    let key = u64::from_be_bytes(message[5..13].try_into().unwrap());

    println!("reading value key={} for {}", key, connection.address);

    let default_value = Vec::new();
    let value = storage.get(&key).unwrap_or(&default_value).clone();

    let response_length = 5 + value.len() as u32;
    let response = [vec![0], response_length.to_be_bytes().to_vec(), value].concat();
    connection.send_message(&response).await;
}

async fn handle_write_request(mut connection: IncomingConnection, first_message: Vec<u8>, storage: &mut HashMap<u64, Vec<u8>>) {
    // at this point the first byte of first_message is `2`
    let total_length_header = u32::from_be_bytes(first_message[1..5].try_into().unwrap());
    if first_message.len() != 13 || total_length_header != 13 {
        println!("received invalid type=2 message, dropping");
        return;
    }
    let key = u64::from_be_bytes(first_message[5..13].try_into().unwrap());

    println!("granting write permission for key={} for {}", key, connection.address);

    // send write permission with the current value to the client
    let default_value = Vec::new();
    let old_value = storage.get(&key).unwrap_or(&default_value).clone();

    let permission_msg_length = 5 + old_value.len() as u32;
    let permission_message = [vec![0], permission_msg_length.to_be_bytes().to_vec(), old_value].concat();
    connection.send_message(&permission_message).await;

    // read the new value
    let write_command_message = connection.read_message().await;
    if write_command_message.len() < 5 || write_command_message[0] != 0 {
        println!("received invalid write command message (header), dropping");
        return;
    }
    let new_value_length = u32::from_be_bytes(write_command_message[1..5].try_into().unwrap()) - 5;
    if write_command_message.len() as u32 != new_value_length + 5 {
        println!("received invalid write command message (length), dropping");
        return;
    }
    let new_value = &write_command_message[5..new_value_length as usize+5];

    println!("writing new value={:?} for key={}", new_value, key);

    // write the new value to the storage
    storage.insert(key, new_value.to_vec());

    // respond acknowledgement
    connection.send_message(&[0, 0, 0, 0, 7, 111, 107]).await;
}
