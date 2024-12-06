use crate::communication::IncomingConnection;
use crate::helpers::neighbors::find_neighbors_wrapping;
use crate::PeerNode;
use crate::helpers::neighbors::find_neighbors_nonwrapping;
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

    // this crashed peer is expected to be the smaller neighbor
    // or greater neighbor if it was the greatest node in the ring
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
    deannounce_down_peer(down_peer_id, &node_list).await;

    // move values from backup storage to leader storage
    transfer_from_backup_to_leader(down_peer_id, &node_list).await;

    // create new backup replicas
    create_new_backup_replica(down_peer_id, &node_list).await;

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

    // TODO: if the crashed node was greater neighbor and not the greatest in the ring,
    // replicate new backup to the next greater node

    connection.send_message(&[0, 0, 0, 0, 7, 111, 107]).await;
}

async fn deannounce_down_peer(down_peer_id: u64, node_list: &Vec<PeerNode>) {
    let message = [vec![31, 0, 0, 0, 13], down_peer_id.to_be_bytes().to_vec()].concat();

    // TODO: parallelize
    for peer in node_list {
        if peer.id == down_peer_id {
            continue;
        }

        let mut connection = IncomingConnection::new(peer.ip_address.clone(), &message)
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

/// Node list should still contain the crashed node.
async fn transfer_from_backup_to_leader(down_peer_id: u64, node_list: &Vec<PeerNode>) {
    // find the neighbors of the crashed node
    let (smaller_neighbor, greater_neighbor) = find_neighbors_nonwrapping(down_peer_id, node_list);

    // find the inclusive bounds of the keys that will be transfered
    let transfer_key_lower_bound = match smaller_neighbor {
        Some(node) => node.id + 1,
        None => 0,
    };
    let transfer_key_upper_bound = if greater_neighbor.is_none() { u64::MAX } else { down_peer_id };

    println!("transfering keys {}..={} from backup to leader storage", transfer_key_lower_bound, transfer_key_upper_bound);

    // request backup key-value pairs in the range
    let backup_request = [
        vec![32, 0, 0, 0, 21],
        transfer_key_lower_bound.to_be_bytes().to_vec(),
        transfer_key_upper_bound.to_be_bytes().to_vec(),
    ]
    .concat();

    let mut backup_connection = IncomingConnection::new("127.0.0.1".to_string(), &backup_request).await.unwrap();

    let backup_response = backup_connection.read_message().await;

    if backup_response.len() < 5 || backup_response[0] != 0 {
        println!("received invalid fault tolerance transfer response from backup, dropping");
        return;
    }

    // send request to add leader pairs
    let leader_request = [vec![33], backup_response[1..].to_vec()].concat();

    let mut leader_connection = IncomingConnection::new("127.0.0.1".to_string(), &leader_request)
        .await
        .unwrap();

    let leader_response = leader_connection.read_message().await;

    if leader_response != [0, 0, 0, 0, 7, 111, 107] {
        println!("received invalid fault tolerance transfer response from leader, dropping");
        return;
    }
}

async fn create_new_backup_replica(down_peer_id: u64, node_list: &Vec<PeerNode>) {
    let new_backup_node;

    // if the crashed node was the greatest in the ring
    if find_neighbors_nonwrapping(down_peer_id, &node_list).1.is_none() {
        // new backup node for this node is the smallest in the ring
        new_backup_node = node_list.iter().min_by_key(|node| node.id).unwrap().clone();
    } else {
        // the crashed node was the smaller neighbor of this node
        // new backup node for this node is the smaller neighbor of the crashed node (wrap if necessary)
        new_backup_node = find_neighbors_wrapping(down_peer_id, node_list)[0].clone().unwrap();
    }

    // request the leader key-value pairs from this node itself
    let leader_request = [12, 0, 0, 0, 5];
    let mut leader_connection = IncomingConnection::new("127.0.0.1".to_string(), &leader_request).await.unwrap();

    let leader_response = leader_connection.read_message().await;

    // send the leader pairs of this node to the new backup node
    let backup_request = [vec![21], leader_response[1..].to_vec()].concat();
    let mut backup_connection = IncomingConnection::new(new_backup_node.ip_address, &backup_request).await.unwrap();
    let backup_response = backup_connection.read_message().await;

    if backup_response != [0, 0, 0, 0, 7, 111, 107] {
        println!("failed to create a new backup replica to {}, skipping", backup_connection.address);
    }
}
