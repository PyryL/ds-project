use crate::communication::IncomingConnection;
use crate::PeerNode;
use rand::{thread_rng, Rng};

pub async fn run_join_procedure(known_node_ip_address: &str) -> Vec<PeerNode> {
    let mut node_list = request_node_list(known_node_ip_address).await;

    let node_id = thread_rng().gen();

    println!("using node id {}", node_id);

    // add this node itself to the list of nodes
    node_list.push(PeerNode {
        id: node_id,
        ip_address: "127.0.0.1".to_string(),
    });

    node_list
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
