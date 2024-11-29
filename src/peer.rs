use crate::{communication::IncomingConnection, PeerNode};
use tokio::sync::{mpsc, Mutex};
use std::sync::Arc;

pub async fn peer_block(
    mut incoming_connection_stream: mpsc::UnboundedReceiver<(IncomingConnection, Vec<u8>)>,
    node_list: Arc<Mutex<Vec<PeerNode>>>,
) {
    while let Some((connection, _message)) = incoming_connection_stream.recv().await {
        // at this point the message is [10, 0, 0, 0, 5]
        let node_list_access = node_list.lock().await;
        handle_node_list_request(connection, &node_list_access).await;
    }
}

async fn handle_node_list_request(mut connection: IncomingConnection, node_list: &Vec<PeerNode>) {
    println!("serving node list to {}", connection.address);

    let mut encoded_node_list = Vec::new();

    for node in node_list {
        let node_id_bytes = node.id.to_be_bytes();
        let node_ip_bytes = ip_address_bytes(&node.ip_address);
        encoded_node_list.extend_from_slice(&node_id_bytes);
        encoded_node_list.extend_from_slice(&node_ip_bytes);
    }
    
    let response_length = (5 + encoded_node_list.len() as u32).to_be_bytes();
    connection.send_message(&[vec![0], response_length.to_vec(), encoded_node_list].concat()).await;
}

/// Returns a list of exactly four integers encoding the given IPv4 address.
/// Panics if the address is invalid.
fn ip_address_bytes(address: &str) -> [u8; 4] {
    let parts: Vec<u8> = address.split('.').map(|s| s.parse().unwrap()).collect();
    if parts.len() != 4 {
        panic!("invalid IPv4 address");
    }
    parts.try_into().unwrap()
}
