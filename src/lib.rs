use communication::{listen_messages, send_message};
use tokio_stream::StreamExt;

mod communication;

#[derive(Debug)]
pub struct PeerNode {
    pub id: u64,
    pub ip_address: String,
}

pub async fn start_node(node_id: u64, node_list: Vec<PeerNode>) {
    println!("starting node ID={} with peers {:?}", node_id, node_list);

    for peer_node in node_list {
        let response = send_message(peer_node.ip_address, b"can you hear me?")
            .await
            .unwrap();
        let parsed_response = String::from_utf8(response).unwrap();
        println!(
            "received response from peer ID={}: \"{}\"",
            peer_node.id, parsed_response
        );
    }

    println!("starting to listen others");

    let mut incoming_connections_stream = listen_messages().await;

    while let Some(connection) = incoming_connections_stream.next().await {
        let parsed_message = String::from_utf8(connection.message.clone()).unwrap();
        println!("received message \"{}\"", parsed_message);
        connection.respond(b"ack").await;
    }
}
