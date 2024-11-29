use crate::communication::IncomingConnection;
use crate::PeerNode;
use rand::{thread_rng, Rng};
use std::ops::RangeInclusive;

/// Returns node list and initial leader key-value pairs.
pub async fn run_join_procedure(known_node_ip_address: Option<&str>) -> (Vec<PeerNode>, Vec<(u64, Vec<u8>)>) {
    let mut node_list = match known_node_ip_address {
        Some(ip_address) => request_node_list(ip_address).await,
        None => Vec::new(),
    };

    let node_id = thread_rng().gen();

    println!("using node id {}", node_id);

    let (smaller_neighbor, greater_neighbor) = find_neighbors(node_id, &node_list);

    let leader_kv_pairs = match (smaller_neighbor, greater_neighbor) {
        (None, Some(greater)) => request_primary_kv_pairs(&greater, 0..=node_id).await,
        (Some(smaller), None) => request_primary_kv_pairs(&smaller, (smaller.id+1)..=u64::MAX).await,
        (Some(smaller), Some(greater)) => request_primary_kv_pairs(&greater, (smaller.id+1)..=node_id).await,
        (None, None) => Vec::new(),
    };

    // TODO: request backup key-value pairs

    // TODO: parallelize
    for peer_node in node_list.iter() {
        announce_joining(node_id, &peer_node.ip_address).await;
    }

    // add this node itself to the list of nodes
    node_list.push(PeerNode {
        id: node_id,
        ip_address: "127.0.0.1".to_string(),
    });

    (node_list, leader_kv_pairs)
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

/// The neighbors are *not* searched by wrapping around to beginning/end of the ring.
fn find_neighbors(this_node_id: u64, node_list: &[PeerNode]) -> (Option<PeerNode>, Option<PeerNode>) {
    // smaller neighbor = node with greatest ID smaller than self
    let smaller_neighbor = node_list
        .iter()
        .filter(|node| node.id < this_node_id)
        .max_by_key(|node| node.id);

    // greater neighbor = node with smallest ID greater than self
    let greater_neighbor = node_list
        .iter()
        .filter(|node| node.id > this_node_id)
        .min_by_key(|node| node.id);

    (smaller_neighbor.cloned(), greater_neighbor.cloned())
}

async fn request_primary_kv_pairs(neighbor: &PeerNode, key_range: RangeInclusive<u64>) -> Vec<(u64, Vec<u8>)> {
    let request = [vec![11, 0, 0, 0, 21], key_range.start().to_be_bytes().to_vec(), key_range.end().to_be_bytes().to_vec()].concat();

    let mut connection = IncomingConnection::new(neighbor.ip_address.clone(), &request).await.unwrap();

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

async fn announce_joining(this_node_id: u64, peer_node_ip_address: &str) {
    let request = [vec![13, 0, 0, 0, 13], this_node_id.to_be_bytes().to_vec()].concat();

    let mut connection = IncomingConnection::new(peer_node_ip_address.to_string(), &request).await.unwrap();

    let response = connection.read_message().await;

    if response != vec![0, 0, 0, 0, 7, 111, 107] {
        println!("received invalid ack to join announcement from {}, ignoring", peer_node_ip_address);
    }
}
