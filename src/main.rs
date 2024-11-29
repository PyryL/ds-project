use ds_project::start_node;
use std::env;

#[tokio::main]
async fn main() {
    let known_node_ip_address = env::var("DS_KNOWN_NODE").ok();

    start_node(known_node_ip_address).await;
}
