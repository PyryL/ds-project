use super::backup::push_update_to_backups;
use crate::communication::IncomingConnection;
use crate::PeerNode;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub async fn handle_read_request(
    mut connection: IncomingConnection,
    message: Vec<u8>,
    storage: Arc<Mutex<HashMap<u64, Vec<u8>>>>,
) {
    // at this point, first byte of connection.message is `1`
    if message.len() != 13 {
        println!("received invalid type=1 message, dropping");
        return;
    }
    let key = u64::from_be_bytes(message[5..13].try_into().unwrap());

    println!("reading value key={} for {}", key, connection.address);

    let default_value = Vec::new();
    let value;
    {
        let storage_access = storage.lock().await;
        value = storage_access.get(&key).unwrap_or(&default_value).clone();
    }

    let response_length = 5 + value.len() as u32;
    let response = [vec![0], response_length.to_be_bytes().to_vec(), value].concat();
    connection.send_message(&response).await;
}

pub async fn handle_write_request(
    mut connection: IncomingConnection,
    first_message: Vec<u8>,
    storage: Arc<Mutex<HashMap<u64, Vec<u8>>>>,
    this_node_id: u64,
    node_list_arc: Arc<Mutex<Vec<PeerNode>>>,
) {
    // at this point the first byte of first_message is `2`
    let total_length_header = u32::from_be_bytes(first_message[1..5].try_into().unwrap());
    if first_message.len() != 13 || total_length_header != 13 {
        println!("received invalid type=2 message, dropping");
        return;
    }
    let key = u64::from_be_bytes(first_message[5..13].try_into().unwrap());

    println!(
        "granting write permission for key={} for {}",
        key, connection.address
    );

    // TODO: hold mutex for whole write process

    // send write permission with the current value to the client
    let default_value = Vec::new();
    let old_value;
    {
        let storage_access = storage.lock().await;
        old_value = storage_access.get(&key).unwrap_or(&default_value).clone();
    }

    let permission_msg_length = 5 + old_value.len() as u32;
    let permission_message = [
        vec![0],
        permission_msg_length.to_be_bytes().to_vec(),
        old_value,
    ]
    .concat();
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
    let new_value = &write_command_message[5..new_value_length as usize + 5];

    println!("writing new value={:?} for key={}", new_value, key);

    // push the update to backups
    let node_list;
    {
        node_list = node_list_arc.lock().await.clone();
    }
    push_update_to_backups(&node_list, this_node_id, key, new_value.to_vec()).await;

    // write the new value to the storage
    {
        let mut storage_access = storage.lock().await;
        storage_access.insert(key, new_value.to_vec());
    }

    // respond acknowledgement
    connection.send_message(&[0, 0, 0, 0, 7, 111, 107]).await;
}

pub async fn handle_transfer_request(
    mut connection: IncomingConnection,
    message: Vec<u8>,
    storage: Arc<Mutex<HashMap<u64, Vec<u8>>>>,
) {
    // at this point, the first byte of message is 11
    if u32::from_be_bytes(message[1..5].try_into().unwrap()) != 21 || message.len() != 21 {
        println!("receiving invalid transfer request, dropping");
        return;
    }

    let key_lower_bound = u64::from_be_bytes(message[5..13].try_into().unwrap());
    let key_upper_bound = u64::from_be_bytes(message[13..21].try_into().unwrap());

    let mut response_payload = Vec::new();

    let keys_to_transfer: Vec<_>;
    {
        let storage_access = storage.lock().await;
        keys_to_transfer = storage_access
            .keys()
            .filter(|&&k| key_lower_bound <= k && k <= key_upper_bound)
            .cloned()
            .collect();
    }

    println!(
        "transfering leader keys {:?} ({}..={}) to {}",
        keys_to_transfer, key_lower_bound, key_upper_bound, connection.address
    );

    {
        let mut storage_access = storage.lock().await;
        for key in keys_to_transfer {
            if let Some(value) = storage_access.remove(&key) {
                response_payload.extend_from_slice(&key.to_be_bytes());
                response_payload.extend_from_slice(&(value.len() as u32).to_be_bytes());
                response_payload.extend_from_slice(&value);
            }
        }
    }

    let response_length_bytes = (5 + response_payload.len() as u32).to_be_bytes();
    let response = [vec![0], response_length_bytes.to_vec(), response_payload].concat();

    connection.send_message(&response).await;
}

pub async fn handle_backup_request(
    mut connection: IncomingConnection,
    storage: Arc<Mutex<HashMap<u64, Vec<u8>>>>,
) {
    // message was [12, 0, 0, 0, 5]
    let storage_keys: Vec<_>;
    {
        let storage_access = storage.lock().await;
        storage_keys = storage_access.keys().cloned().collect();
    }
    println!(
        "responding leader kv-pairs (keys {:?}) to {} for backup",
        storage_keys, connection.address
    );

    let mut response_payload = Vec::new();

    {
        let storage_access = storage.lock().await;

        for (key, value) in storage_access.iter() {
            response_payload.extend_from_slice(&key.to_be_bytes());
            response_payload.extend_from_slice(&(value.len() as u32).to_be_bytes());
            response_payload.extend_from_slice(value);
        }
    }

    let response_length = response_payload.len() as u32 + 5;

    let response = [
        vec![0],
        response_length.to_be_bytes().to_vec(),
        response_payload,
    ]
    .concat();

    connection.send_message(&response).await;

    println!("backup transfer done");
}

pub async fn handle_fault_tolerance_insertion(
    mut connection: IncomingConnection,
    message: Vec<u8>,
    storage: Arc<Mutex<HashMap<u64, Vec<u8>>>>,
) {
    // at this point the first byte of message is 33
    if message.len() < 5 {
        println!("received invalid fault tolerance insertion request, dropping");
        return;
    }

    let mut keys = Vec::new();

    {
        let mut storage_access = storage.lock().await;

        let mut i = 5;
        while i < message.len() {
            let key = u64::from_be_bytes(message[i..i + 8].try_into().unwrap());
            let value_length = u32::from_be_bytes(message[i + 8..i + 12].try_into().unwrap()) as usize;
            let value = &message[i + 12..i + 12 + value_length];

            storage_access.insert(key, value.to_vec());
            keys.push(key);

            i += value_length + 12;
        }
    }

    println!("inserted fault tolerance keys {:?} to leader storage", keys);

    connection.send_message(&[0, 0, 0, 0, 7, 111, 107]).await;
}
