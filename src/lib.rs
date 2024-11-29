use communication::listen_messages;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

mod client;
mod communication;
mod join;
mod leader;
mod peer;

#[derive(Debug, Clone)]
pub struct PeerNode {
    pub id: u64,
    pub ip_address: String,
}

pub async fn start_node(known_node_ip_address: Option<String>) {
    // TODO: convert node list into mutex
    let (node_list, initial_leader_kv_pairs) = match known_node_ip_address {
        Some(ip_address) => {
            println!("starting the node...");
            join::run_join_procedure(&ip_address).await
        }
        None => {
            println!("starting without a known node");
            (vec![PeerNode { id: 1234, ip_address: "127.0.0.1".to_string() }], Vec::new())
        }
    };

    // TODO: if starting without known node, node_list does not contain this node itself

    let (leader_sender, leader_receiver) = mpsc::unbounded_channel();
    let leader_sender = Arc::new(leader_sender);
    println!("initial leader kv-pairs {:?}", initial_leader_kv_pairs);
    tokio::task::spawn(async move {
        // TODO: pass initial_leader_kv_pairs to leader block
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
                Some(1) | Some(2) | Some(11) => leader_sender_clone.send((connection, message)).unwrap(),
                Some(10) => peer_sender_clone.send((connection, message)).unwrap(),
                Some(200) | Some(202) => client_sender_clone.send((connection, message)).unwrap(),
                _ => println!("received invalid message, dropping"),
            };
        });
    }
}
