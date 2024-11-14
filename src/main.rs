use ds_project::{start_node, PeerNode};
use std::env;

#[tokio::main]
async fn main() {
    let node_id: u64 = env::var("DS_NODE_ID").unwrap().parse().unwrap();

    let other_nodes = get_other_nodes();

    start_node(node_id, other_nodes).await;
}

fn get_other_nodes() -> Vec<PeerNode> {
    let other_nodes: Vec<String> = env::var("DS_PEER_NODES")
        .unwrap()
        .split(',')
        .map(|s| s.to_owned())
        .collect();

    // empty env var means no known peer nodes
    if other_nodes.len() == 1 && other_nodes[0] == "" {
        return Vec::new();
    }

    // each node is expected to contain IP address and node id in format `127.0.0.1#1234`
    other_nodes
        .iter()
        .map(|s| {
            let parts: Vec<_> = s.split('#').collect();
            PeerNode {
                id: parts[1].parse().unwrap(),
                ip_address: parts[0].to_owned(),
            }
        })
        .collect()
}
