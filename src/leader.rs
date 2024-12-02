use crate::communication::IncomingConnection;
use crate::PeerNode;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

pub async fn leader_block(
    mut incoming_connection_stream: mpsc::UnboundedReceiver<(IncomingConnection, Vec<u8>)>,
    initial_kv_pairs: Vec<(u64, Vec<u8>)>,
    node_list_arc: Arc<Mutex<Vec<PeerNode>>>,
    this_node_id: u64,
) {
    let mut leader_storage: HashMap<u64, Vec<u8>> = HashMap::new();

    println!(
        "leader block starting with initial kv-pairs {:?}",
        initial_kv_pairs
    );

    for (key, value) in initial_kv_pairs {
        leader_storage.insert(key, value);
    }

    while let Some((connection, first_message)) = incoming_connection_stream.recv().await {
        match first_message.first() {
            Some(1) => handle_read_request(connection, first_message, &leader_storage).await,
            Some(2) => {
                let node_list = node_list_arc.lock().await;
                handle_write_request(
                    connection,
                    first_message,
                    &mut leader_storage,
                    this_node_id,
                    &node_list,
                )
                .await
            }
            Some(11) => {
                handle_transfer_request(connection, first_message, &mut leader_storage).await
            }
            _ => panic!(),
        };
    }
}

async fn handle_read_request(
    mut connection: IncomingConnection,
    message: Vec<u8>,
    storage: &HashMap<u64, Vec<u8>>,
) {
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

async fn handle_write_request(
    mut connection: IncomingConnection,
    first_message: Vec<u8>,
    storage: &mut HashMap<u64, Vec<u8>>,
    this_node_id: u64,
    node_list: &Vec<PeerNode>,
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

    // send write permission with the current value to the client
    let default_value = Vec::new();
    let old_value = storage.get(&key).unwrap_or(&default_value).clone();

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
    let neighbors = find_neighbors(this_node_id, node_list);
    // TODO: parallelize
    for neighbor in neighbors {
        if let Some(neighbor) = neighbor {
            let success = push_update_to_backup(neighbor.ip_address, key, new_value.to_vec()).await;
            if !success {
                // TODO: abort write
                panic!()
            }
        }
    }

    // write the new value to the storage
    storage.insert(key, new_value.to_vec());

    // respond acknowledgement
    connection.send_message(&[0, 0, 0, 0, 7, 111, 107]).await;
}

async fn handle_transfer_request(
    mut connection: IncomingConnection,
    message: Vec<u8>,
    storage: &mut HashMap<u64, Vec<u8>>,
) {
    // at this point, the first byte of message is 11
    if u32::from_be_bytes(message[1..5].try_into().unwrap()) != 21 || message.len() != 21 {
        println!("receiving invalid transfer request, dropping");
        return;
    }

    let key_lower_bound = u64::from_be_bytes(message[5..13].try_into().unwrap());
    let key_upper_bound = u64::from_be_bytes(message[13..21].try_into().unwrap());

    let mut response_payload = Vec::new();

    let keys_to_transfer: Vec<_> = storage
        .keys()
        .filter(|&&k| key_lower_bound <= k && k <= key_upper_bound)
        .cloned()
        .collect();

    println!(
        "transfering leader keys {:?} ({}..={}) to {}",
        keys_to_transfer, key_lower_bound, key_upper_bound, connection.address
    );

    for key in keys_to_transfer {
        if let Some(value) = storage.remove(&key) {
            response_payload.extend_from_slice(&key.to_be_bytes());
            response_payload.extend_from_slice(&(value.len() as u32).to_be_bytes());
            response_payload.extend_from_slice(&value);
        }
    }

    let response_length_bytes = (5 + response_payload.len() as u32).to_be_bytes();
    let response = [vec![0], response_length_bytes.to_vec(), response_payload].concat();

    connection.send_message(&response).await;
}

/// Finds the neighbors of this node and wraps around the ring if necessary.
/// Does not return this node itself in any case.
/// If greater and smaller neighbour would be the same, it is only returned once.
fn find_neighbors(this_node_id: u64, node_list: &Vec<PeerNode>) -> [Option<PeerNode>; 2] {
    // smaller neighbor = node with greatest ID smaller than self
    // if not found, use the greatest node
    let greatest_node = node_list
        .iter()
        .filter(|node| node.id != this_node_id)
        .max_by_key(|node| node.id);

    let smaller_neighbor = node_list
        .iter()
        .filter(|peer| peer.id < this_node_id)
        .max_by_key(|peer| peer.id);

    let smaller_neighbor = match (smaller_neighbor, greatest_node) {
        (Some(neighbor), _) => Some(neighbor.clone()),
        (None, greatest_node) => greatest_node.cloned(),
    };

    // greater neighbor = node with smallest ID greater than self
    // if not found, use the smallest node
    let smallest_node = node_list
        .iter()
        .filter(|node| node.id != this_node_id)
        .min_by_key(|node| node.id);

    let greater_neighbor = node_list
        .iter()
        .filter(|peer| peer.id > this_node_id)
        .min_by_key(|peer| peer.id);

    let greater_neighbor = match (greater_neighbor, smallest_node) {
        (Some(neighbor), _) => Some(neighbor.clone()),
        (None, smallest_node) => smallest_node.cloned(),
    };

    // do not return the same node twice
    match (smaller_neighbor, greater_neighbor) {
        (Some(smaller), Some(greater)) => {
            if smaller.id == greater.id {
                [Some(smaller), None]
            } else {
                [Some(smaller), Some(greater)]
            }
        }
        (smaller, greater) => [smaller, greater],
    }
}

async fn push_update_to_backup(backup_node_ip_address: String, key: u64, value: Vec<u8>) -> bool {
    let request_length = value.len() as u32 + 13;
    let request = [
        vec![20],
        request_length.to_be_bytes().to_vec(),
        key.to_be_bytes().to_vec(),
        value,
    ]
    .concat();

    let mut connection = IncomingConnection::new(backup_node_ip_address, &request)
        .await
        .unwrap();

    let response = connection.read_message().await;

    if response != [0, 0, 0, 0, 7, 111, 107] {
        println!(
            "failed to update backup for key={} at {}, aborting the write",
            key, connection.address
        );
        false
    } else {
        true
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn simple_neighbor_finding() {
        let node_list = vec![
            PeerNode {
                id: 100,
                ip_address: "192.168.0.100".to_string(),
            },
            PeerNode {
                id: 200,
                ip_address: "192.168.0.200".to_string(),
            },
            PeerNode {
                id: 150,
                ip_address: "192.168.0.150".to_string(),
            },
            PeerNode {
                id: 10,
                ip_address: "192.168.0.10".to_string(),
            },
        ];
        let result = find_neighbors(200, &node_list);
        assert_eq!(result[0].clone().unwrap().id, 150);
        assert_eq!(result[1].clone().unwrap().id, 10);
    }

    #[test]
    fn missing_neighbor_finding() {
        let node_list = vec![
            PeerNode {
                id: 100,
                ip_address: "192.168.0.100".to_string(),
            },
            PeerNode {
                id: 200,
                ip_address: "192.168.0.200".to_string(),
            },
        ];
        let result = find_neighbors(200, &node_list);
        assert_eq!(result[0].clone().unwrap().id, 100);
        assert!(result[1].is_none());
    }

    #[test]
    fn alone_neighbor_finding() {
        let node_list = vec![PeerNode {
            id: 200,
            ip_address: "192.168.0.200".to_string(),
        }];
        let result = find_neighbors(200, &node_list);
        assert!(result[0].is_none());
        assert!(result[1].is_none());
    }
}
