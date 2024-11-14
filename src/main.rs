use ds_project::{start_node, PeerNode};
use std::env;

#[tokio::main]
async fn main() {
    let node_id: u64 = env::var("DS_NODE_ID").unwrap().parse().unwrap();

    // parse peer node info from comma-separated environment variable
    // each node is expected contain IP address and node id in format `127.0.0.1#1234`
    let other_nodes = env::var("DS_PEER_NODES")
        .unwrap()
        .split(',')
        .map(|s| {
            let parts: Vec<_> = s.split('#').collect();
            PeerNode {
                id: parts[1].parse().unwrap(),
                ip_address: parts[0].to_owned(),
            }
        })
        .collect();

    start_node(node_id, other_nodes).await;
}
