use communication::{listen_messages, send_message};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

mod communication;
mod leader;

#[derive(Debug)]
pub struct PeerNode {
    pub id: u64,
    pub ip_address: String,
}

pub async fn start_node(node_id: u64, node_list: Vec<PeerNode>) {
    println!("starting node ID={} with peers {:?}", node_id, node_list);

    if let Some(peer_node) = node_list.first() {
        let mut message = vec![1];
        message.extend(&42u64.to_be_bytes().to_vec());
        let response = send_message(peer_node.ip_address.clone(), &message)
            .await
            .unwrap();
        let value = String::from_utf8(response[4..].to_vec()).unwrap();
        println!(
            "read value key=42 from peer id={} and received value=\"{}\"",
            peer_node.id, value
        );
    }

    println!("starting to listen others");

    let (leader_sender, leader_receiver) = mpsc::unbounded_channel();
    tokio::task::spawn(async move {
        leader::leader_block(leader_receiver).await;
    });

    let mut incoming_connections_stream = listen_messages().await;

    while let Some(connection) = incoming_connections_stream.next().await {
        match connection.message.first() {
            Some(1) => leader_sender.send(connection).unwrap(),

            _ => println!("received invalid message, dropping"),
        };
    }
}
