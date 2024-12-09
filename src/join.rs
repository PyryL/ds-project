use crate::communication::{resolve_hostname_to_ip_address, IncomingConnection};
use crate::helpers::neighbors::{find_neighbors_nonwrapping, find_neighbors_wrapping};
use crate::PeerNode;
use rand::{thread_rng, Rng};
use std::ops::RangeInclusive;

/// Returns this node ID, node list and initial leader and backup key-value pairs.
pub async fn run_join_procedure(
    known_node_host: Option<&str>,
) -> (u64, Vec<PeerNode>, Vec<(u64, Vec<u8>)>, Vec<(u64, Vec<u8>)>) {
    let known_node_ip_address = match known_node_host {
        Some(host) => {
            if let Some(result) = resolve_hostname_to_ip_address(host) {
                Some(result)
            } else {
                panic!("failed to resolve known host address, aborting");
            }
        }
        None => None,
    };

    let mut node_list = match known_node_ip_address {
        Some(ip_address) => request_node_list(&ip_address).await,
        None => Vec::new(),
    };

    let node_id = thread_rng().gen();

    println!("using node id {}", node_id);

    let (smaller_neighbor, greater_neighbor) = find_neighbors_nonwrapping(node_id, &node_list);

    let leader_kv_pairs = match (smaller_neighbor, greater_neighbor) {
        (None, Some(greater)) => request_primary_kv_pairs(&greater, 0..=node_id).await,
        (Some(smaller), None) => {
            request_primary_kv_pairs(&smaller, (smaller.id + 1)..=u64::MAX).await
        }
        (Some(smaller), Some(greater)) => {
            request_primary_kv_pairs(&greater, (smaller.id + 1)..=node_id).await
        }
        (None, None) => Vec::new(),
    };

    // request backup key-value pairs
    let [smaller_neighbor, greater_neighbor] = find_neighbors_wrapping(node_id, &node_list);
    let mut backup_kv_pairs = Vec::new();
    if let Some(smaller_neighbor) = smaller_neighbor {
        backup_kv_pairs.extend_from_slice(&request_backup_kv_pairs(&smaller_neighbor).await);
    }
    if let Some(greater_neighbor) = greater_neighbor {
        backup_kv_pairs.extend_from_slice(&request_backup_kv_pairs(&greater_neighbor).await);
    }

    // announce every existing node about the join in parallel
    let mut announce_handles = Vec::new();
    for peer_node in node_list.iter() {
        let peer_ip_address = peer_node.ip_address.clone();
        let handle = tokio::task::spawn(async move {
            announce_joining(node_id, peer_ip_address).await;
        });
        announce_handles.push(handle);
    }
    for handle in announce_handles {
        handle.await.unwrap();
    }

    // add this node itself to the list of nodes
    node_list.push(PeerNode {
        id: node_id,
        ip_address: "127.0.0.1".to_string(),
    });

    (node_id, node_list, leader_kv_pairs, backup_kv_pairs)
}

async fn request_node_list(known_node_ip_address: &str) -> Vec<PeerNode> {
    let mut connection =
        IncomingConnection::new(known_node_ip_address.to_string(), &[10, 0, 0, 0, 5])
            .await
            .unwrap();

    let response = connection.read_message().await;

    let response_length = u32::from_be_bytes(response[1..5].try_into().unwrap()) as usize;

    if response[0] != 0 || response_length != response.len() {
        panic!("invalid node list response");
    }

    let mut node_list = Vec::new();

    for i in (5..response_length).step_by(12) {
        let node_id = u64::from_be_bytes(response[i..i + 8].try_into().unwrap());
        let node_ip_address = ip_address_from_bytes(response[i + 8..i + 12].try_into().unwrap());

        node_list.push(PeerNode {
            id: node_id,
            ip_address: if node_ip_address == "127.0.0.1" {
                known_node_ip_address.to_string()
            } else {
                node_ip_address
            },
        });
    }

    println!("received node list {:?}", node_list);

    node_list
}

/// Returns the IPv4 address encoded by the given bytes.
fn ip_address_from_bytes(bytes: &[u8; 4]) -> String {
    bytes.map(|b| b.to_string()).join(".")
}

async fn request_primary_kv_pairs(
    neighbor: &PeerNode,
    key_range: RangeInclusive<u64>,
) -> Vec<(u64, Vec<u8>)> {
    let request = [
        vec![11, 0, 0, 0, 21],
        key_range.start().to_be_bytes().to_vec(),
        key_range.end().to_be_bytes().to_vec(),
    ]
    .concat();

    let mut connection = IncomingConnection::new(neighbor.ip_address.clone(), &request)
        .await
        .unwrap();

    let response = connection.read_message().await;

    if response.len() < 5 || response[0] != 0 {
        panic!("received invalid leader transfer response, aborting");
    }
    let response_length = u32::from_be_bytes(response[1..5].try_into().unwrap()) as usize;
    if response.len() != response_length {
        panic!("received invalid leader transfer response, aborting");
    }

    let mut kv_pairs = Vec::new();

    let mut i = 5;
    while i < response_length {
        let key = u64::from_be_bytes(response[i..i + 8].try_into().unwrap());
        let value_length = u32::from_be_bytes(response[i + 8..i + 12].try_into().unwrap()) as usize;
        let value = &response[i + 12..i + 12 + value_length];
        kv_pairs.push((key, value.to_vec()));
        i += value_length + 12;
    }

    kv_pairs
}

async fn request_backup_kv_pairs(neighbor: &PeerNode) -> Vec<(u64, Vec<u8>)> {
    println!("requesting initial backups from {}", neighbor.ip_address);

    // make request
    let request = [12, 0, 0, 0, 5];
    let mut connection = IncomingConnection::new(neighbor.ip_address.to_string(), &request)
        .await
        .unwrap();

    let response = connection.read_message().await;

    if response.len() < 5 || response[0] != 0 {
        panic!("received invalid backup transfer response, panicing")
    }

    let response_length = u32::from_be_bytes(response[1..5].try_into().unwrap()) as usize;

    let mut kv_pairs = Vec::new();

    let mut i = 5;
    while i < response_length {
        let key = u64::from_be_bytes(response[i..i + 8].try_into().unwrap());
        let value_length = u32::from_be_bytes(response[i + 8..i + 12].try_into().unwrap()) as usize;
        let value = &response[i + 12..i + 12 + value_length];
        kv_pairs.push((key, value.to_vec()));
        i += value_length + 12;
    }

    kv_pairs
}

async fn announce_joining(this_node_id: u64, peer_node_ip_address: String) {
    let request = [vec![13, 0, 0, 0, 13], this_node_id.to_be_bytes().to_vec()].concat();

    let mut connection = IncomingConnection::new(peer_node_ip_address, &request)
        .await
        .unwrap();

    let response = connection.read_message().await;

    if response != vec![0, 0, 0, 0, 7, 111, 107] {
        println!(
            "received invalid ack to join announcement from {}, ignoring",
            connection.address
        );
    }
}
