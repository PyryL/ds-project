use std::sync::Arc;
use communication::listen_messages;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

mod client;
mod communication;
mod leader;
mod peer;

#[derive(Debug, Clone)]
pub struct PeerNode {
    pub id: u64,
    pub ip_address: String,
}

pub async fn start_node(node_id: u64, mut node_list: Vec<PeerNode>) {
    println!("starting node ID={} with peers {:?}", node_id, node_list);

    // add this node itself to the list of nodes
    // TODO: convert node list into mutex
    node_list.push(PeerNode {
        id: node_id,
        ip_address: "127.0.0.1".to_string(),
    });

    let (leader_sender, leader_receiver) = mpsc::unbounded_channel();
    let leader_sender = Arc::new(leader_sender);
    tokio::task::spawn(async move {
        leader::leader_block(leader_receiver).await;
    });

    let (client_sender, client_receiver) = mpsc::unbounded_channel();
    let client_sender = Arc::new(client_sender);
    let node_list_clone = node_list.clone();
    tokio::task::spawn(async move {
        client::client_block(client_receiver, &node_list_clone).await;
    });

    let (peer_sender, peer_receiver) = mpsc::unbounded_channel();
    let peer_sender = Arc::new(peer_sender);
    let node_list_clone = node_list.clone();
    tokio::task::spawn(async move {
        peer::peer_block(peer_receiver, &node_list_clone).await;
    });

    let mut incoming_connections_stream = listen_messages().await;

    while let Some(mut connection) = incoming_connections_stream.next().await {
        let leader_sender_clone = Arc::clone(&leader_sender);
        let client_sender_clone = Arc::clone(&client_sender);
        let peer_sender_clone = Arc::clone(&peer_sender);

        tokio::task::spawn(async move {
            let message = connection.read_message().await;

            match message.first() {
                Some(1) | Some(2) => leader_sender_clone.send((connection, message)).unwrap(),
                Some(10) => peer_sender_clone.send((connection, message)).unwrap(),
                Some(200) | Some(202) => client_sender_clone.send((connection, message)).unwrap(),
                _ => println!("received invalid message, dropping"),
            };
        });
    }
}
