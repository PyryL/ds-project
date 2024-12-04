use crate::communication::IncomingConnection;
use crate::PeerNode;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

pub async fn fault_tolerance(
    mut incoming_connection_stream: mpsc::UnboundedReceiver<(IncomingConnection, Vec<u8>)>,
    node_list: Arc<Mutex<Vec<PeerNode>>>,
) {
    while let Some((connection, message)) = incoming_connection_stream.recv().await {
        let node_list_clone = Arc::clone(&node_list);

        tokio::task::spawn(async move {
            match message.first() {
                Some(30) => handle_neighbor_down(connection, message, node_list_clone).await,
                Some(31) => handle_peer_deannouncement(connection, message, node_list_clone).await,
                _ => {}
            };
        });
    }
}

async fn handle_neighbor_down(
    mut connection: IncomingConnection,
    message: Vec<u8>,
    node_list_arc: Arc<Mutex<Vec<PeerNode>>>,
) {
    // at this point the first byte of message is 30
    if message.len() != 13 {
        println!(
            "received invalid type=30 message from {}, dropping",
            connection.address
        );
        return;
    }

    let down_peer_id = u64::from_be_bytes(message[5..13].try_into().unwrap());

    println!(
        "handling down peer ID={} detected by {}",
        down_peer_id, connection.address
    );

    // deannounce the peer
    let node_list;
    {
        node_list = node_list_arc.lock().await.clone();
    }
    deannounce_down_peer(down_peer_id, node_list).await;

    // TODO: move values from backup storage to leader storage

    // TODO: create new backup replicas

    connection.send_message(&[0, 0, 0, 0, 7, 111, 107]).await;
}

async fn handle_peer_deannouncement(
    mut connection: IncomingConnection,
    message: Vec<u8>,
    node_list_arc: Arc<Mutex<Vec<PeerNode>>>,
) {
    // at this point the first byte of message is 31
    if message.len() != 13 {
        println!("received invalid type=31 message, dropping");
        return;
    }

    let down_peer_id = u64::from_be_bytes(message[5..13].try_into().unwrap());

    println!(
        "removing down peer ID={} detected by {}",
        down_peer_id, connection.address
    );

    {
        let mut node_list = node_list_arc.lock().await;
        node_list.retain(|node| node.id != down_peer_id);
    }

    connection.send_message(&[0, 0, 0, 0, 7, 111, 107]).await;
}

async fn deannounce_down_peer(down_peer_id: u64, node_list: Vec<PeerNode>) {
    let message = [vec![31, 0, 0, 0, 13], down_peer_id.to_be_bytes().to_vec()].concat();

    // TODO: parallelize
    for peer in node_list {
        if peer.id == down_peer_id {
            continue;
        }

        let mut connection = IncomingConnection::new(peer.ip_address, &message)
            .await
            .unwrap();

        if connection.read_message().await != [0, 0, 0, 0, 7, 111, 107] {
            println!(
                "failed to deannounce peer ID={} to {}",
                down_peer_id, connection.address
            );
        }
    }
}
