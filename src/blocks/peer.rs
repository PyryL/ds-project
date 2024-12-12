use crate::PeerNode;
use crate::helpers::communication::Connection;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

pub async fn peer_block(
    mut incoming_connection_stream: mpsc::UnboundedReceiver<(Connection, Vec<u8>)>,
    node_list: Arc<Mutex<Vec<PeerNode>>>,
) {
    while let Some((connection, message)) = incoming_connection_stream.recv().await {
        let node_list_clone = Arc::clone(&node_list);

        match message.first() {
            Some(10) => handle_node_list_request(connection, node_list_clone).await,
            Some(13) => handle_join_announcement(connection, message, node_list_clone).await,
            _ => {}
        };
    }
}

async fn handle_node_list_request(
    mut connection: Connection,
    node_list_arc: Arc<Mutex<Vec<PeerNode>>>,
) {
    // at this point the first byte of message is 10

    println!("serving node list to {}", connection.address);

    let mut encoded_node_list = Vec::new();

    {
        let node_list = node_list_arc.lock().await;

        for node in node_list.iter() {
            let node_id_bytes = node.id.to_be_bytes();
            let node_ip_bytes = ip_address_bytes(&node.ip_address);
            encoded_node_list.extend_from_slice(&node_id_bytes);
            encoded_node_list.extend_from_slice(&node_ip_bytes);
        }
    }

    let response_length = (5 + encoded_node_list.len() as u32).to_be_bytes();
    connection
        .send_message(&[vec![0], response_length.to_vec(), encoded_node_list].concat())
        .await;
}

async fn handle_join_announcement(
    mut connection: Connection,
    message: Vec<u8>,
    node_list_arc: Arc<Mutex<Vec<PeerNode>>>,
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

    {
        let mut node_list = node_list_arc.lock().await;
        node_list.push(PeerNode {
            id: joining_node_id,
            ip_address: joining_node_ip.to_string(),
        });
    }

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
