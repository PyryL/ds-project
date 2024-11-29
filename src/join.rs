use crate::communication::IncomingConnection;
use crate::PeerNode;
use rand::{thread_rng, Rng};

/// Returns node list and initial leader key-value pairs.
pub async fn run_join_procedure(known_node_ip_address: &str) -> (Vec<PeerNode>, Vec<(u64, Vec<u8>)>) {
    let mut node_list = request_node_list(known_node_ip_address).await;

    let node_id = thread_rng().gen();

    println!("using node id {}", node_id);

    let (smaller_neighbor, greater_neighbor) = find_neighbors(node_id, &node_list);

    // add this node itself to the list of nodes
    node_list.push(PeerNode {
        id: node_id,
        ip_address: "127.0.0.1".to_string(),
    });

    let leader_kv_pairs;

    // TODO: handle case where node_list is shorter than 3 but there is still key-value pairs stored

    if node_list.len() >= 3 {
        // TODO: request backups from greater neighbor
        // TODO: requets backups from smaller neighbor
        leader_kv_pairs = request_primary_kv_pairs(node_id, &smaller_neighbor, &greater_neighbor).await;
    } else {
        leader_kv_pairs = Vec::new();
    }

    // TODO: send information to everybody that this node has joined

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

/// Parameter node_list may not be empty.
fn find_neighbors(this_node_id: u64, node_list: &[PeerNode]) -> (PeerNode, PeerNode) {
    // TODO: check this logic when list is short

    let greatest_node = node_list
        .iter()
        .max_by_key(|node| node.id)
        .unwrap();

    let smallest_node = node_list
        .iter()
        .min_by_key(|node| node.id)
        .unwrap();

    // smaller neighbor = node with greatest ID smaller than self
    // if no nodes with smaller ID, use the greatest node
    let smaller_neighbor = node_list
        .iter()
        .filter(|node| node.id < this_node_id)
        .max_by_key(|node| node.id)
        .unwrap_or(greatest_node);

    // greater neighbor = node with smallest ID greater than self
    // if no nodes with greater ID, use the smallest node
    let greater_neighbor = node_list
        .iter()
        .filter(|node| node.id > this_node_id)
        .min_by_key(|node| node.id)
        .unwrap_or(smallest_node);

    (smaller_neighbor.clone(), greater_neighbor.clone())
}

async fn request_primary_kv_pairs(this_node_id: u64, smaller_neighbor: &PeerNode, greater_neighbor: &PeerNode) -> Vec<(u64, Vec<u8>)> {
    let key_lower_bound = smaller_neighbor.id + 1;
    let key_upper_bound = this_node_id;

    let request = [vec![11, 0, 0, 0, 21], key_lower_bound.to_be_bytes().to_vec(), key_upper_bound.to_be_bytes().to_vec()].concat();

    let mut connection = IncomingConnection::new(greater_neighbor.ip_address.clone(), &request).await.unwrap();

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
        i += value_length + 5;
    }

    kv_pairs
}
