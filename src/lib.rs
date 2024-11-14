use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket};

#[derive(Debug)]
pub struct PeerNode {
    pub id: u64,
    pub ip_address: String,
}

pub async fn start_node(node_id: u64, node_list: Vec<PeerNode>) {
    println!("starting node ID={} with peers {:?}", node_id, node_list);

    // say hello to peers
    for peer_node in node_list {
        let client = TcpSocket::new_v4().unwrap();
        let peer_address = format!("{}:52525", peer_node.ip_address).parse().unwrap();
        let mut stream = client.connect(peer_address).await.unwrap();

        let outgoing_message = format!("this is ID={} speaking", node_id);
        stream.write_all(outgoing_message.as_bytes()).await.unwrap();
        stream.shutdown().await.unwrap();

        let mut incoming_message = String::new();
        stream.read_to_string(&mut incoming_message).await.unwrap();
        println!("peer {:?} responded \"{}\"", peer_node, incoming_message);
    }

    println!("all peers greeted, starting to listen others");

    // start own socket
    let listener = TcpListener::bind("0.0.0.0:52525").await.unwrap();

    while let Ok((mut stream, address)) = listener.accept().await {
        let mut incoming_message = String::new();
        stream.read_to_string(&mut incoming_message).await.unwrap();

        println!("received message \"{}\" from {}", incoming_message, address);

        let response = format!(
            "node ID={} received your message",
            node_id
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.shutdown().await.unwrap();
    }
}
