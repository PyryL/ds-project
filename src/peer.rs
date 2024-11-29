use crate::{communication::IncomingConnection, PeerNode};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

pub async fn peer_block(
    mut incoming_connection_stream: mpsc::UnboundedReceiver<(IncomingConnection, Vec<u8>)>,
    node_list: Arc<Mutex<Vec<PeerNode>>>,
) {
    while let Some((connection, message)) = incoming_connection_stream.recv().await {
        let mut node_list_access = node_list.lock().await;

        match message.first() {
            Some(10) => handle_node_list_request(connection, &node_list_access).await,
            Some(13) => handle_join_announcement(connection, message, &mut node_list_access).await,
            _ => {}
        };
    }
}

async fn handle_node_list_request(mut connection: IncomingConnection, node_list: &Vec<PeerNode>) {
    // at this point the first byte of message is 10

    println!("serving node list to {}", connection.address);

    let mut encoded_node_list = Vec::new();

    for node in node_list {
        let node_id_bytes = node.id.to_be_bytes();
        let node_ip_bytes = ip_address_bytes(&node.ip_address);
        encoded_node_list.extend_from_slice(&node_id_bytes);
        encoded_node_list.extend_from_slice(&node_ip_bytes);
    }

    let response_length = (5 + encoded_node_list.len() as u32).to_be_bytes();
    connection
        .send_message(&[vec![0], response_length.to_vec(), encoded_node_list].concat())
        .await;
}

async fn handle_join_announcement(
    mut connection: IncomingConnection,
    message: Vec<u8>,
    node_list: &mut Vec<PeerNode>,
) {
    // at this point the first byte of message is 13
    if message.len() != 13 || u32::from_be_bytes(message[1..5].try_into().unwrap()) != 13 {
        println!(
            "received invalid join announcement from {}, dropping",
            connection.address
        );
        return;
    }

    let joining_node_id = u64::from_be_bytes(message[5..13].try_into().unwrap());
    let joining_node_ip = connection.address.ip().to_canonical();

    if !joining_node_ip.is_ipv4() {
        println!("received join announcement from non-IPv4 address, dropping");
        return;
    }

    node_list.push(PeerNode {
        id: joining_node_id,
        ip_address: joining_node_ip.to_string(),
    });

    println!(
        "registered new peer ID={} at {}",
        joining_node_id, joining_node_ip
    );

    connection.send_message(&[0, 0, 0, 0, 7, 111, 107]).await;
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
