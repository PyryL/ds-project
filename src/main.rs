use ds_project::start_node;
use std::env::args;

#[tokio::main]
async fn main() {
    let node_id: u64 = args().nth(1).unwrap().parse().unwrap();

    let other_nodes = args().skip(2).collect();

    start_node(node_id, other_nodes).await;
}
