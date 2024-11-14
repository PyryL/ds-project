#[derive(Debug)]
pub struct PeerNode {
    pub id: u64,
    pub ip_address: String,
}

pub async fn start_node(node_id: u64, node_list: Vec<PeerNode>) {
    println!("starting node ID={} with peers {:?}", node_id, node_list);
}
