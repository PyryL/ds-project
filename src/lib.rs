use communication::listen_messages;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::StreamExt;

mod backup;
mod client;
mod communication;
mod fault_tolerance;
mod helpers;
mod join;
mod leader;
mod peer;

#[derive(Debug, Clone)]
pub struct PeerNode {
    pub id: u64,
    pub ip_address: String,
}

pub async fn start_node(known_node_ip_address: Option<String>) {
    let (this_node_id, node_list, initial_leader_kv_pairs, _initial_backup_kv_pairs) =
        join::run_join_procedure(known_node_ip_address.as_deref()).await;
    let node_list = Arc::new(Mutex::new(node_list));

    let (leader_sender, leader_receiver) = mpsc::unbounded_channel();
    let leader_sender = Arc::new(leader_sender);
    let node_list_clone = Arc::clone(&node_list);
    tokio::task::spawn(async move {
        leader::leader_block(
            leader_receiver,
            initial_leader_kv_pairs,
            node_list_clone,
            this_node_id,
        )
        .await;
    });

    let (client_sender, client_receiver) = mpsc::unbounded_channel();
    let client_sender = Arc::new(client_sender);
    let node_list_clone = Arc::clone(&node_list);
    tokio::task::spawn(async move {
        client::client_block(client_receiver, node_list_clone).await;
    });

    let (peer_sender, peer_receiver) = mpsc::unbounded_channel();
    let peer_sender = Arc::new(peer_sender);
    let node_list_clone = Arc::clone(&node_list);
    tokio::task::spawn(async move {
        peer::peer_block(peer_receiver, node_list_clone).await;
    });

    let (backup_sender, backup_receiver) = mpsc::unbounded_channel();
    let backup_sender = Arc::new(backup_sender);
    tokio::task::spawn(async move {
        backup::backup_block(backup_receiver).await;
    });

    let (fault_tolerance_sender, fault_tolerance_receiver) = mpsc::unbounded_channel();
    let fault_tolerance_sender = Arc::new(fault_tolerance_sender);
    let node_list_clone = Arc::clone(&node_list);
    tokio::task::spawn(async move {
        fault_tolerance::fault_tolerance(fault_tolerance_receiver, node_list_clone).await;
    });

    let mut incoming_connections_stream = listen_messages().await;

    while let Some(mut connection) = incoming_connections_stream.next().await {
        let leader_sender_clone = Arc::clone(&leader_sender);
        let client_sender_clone = Arc::clone(&client_sender);
        let peer_sender_clone = Arc::clone(&peer_sender);
        let backup_sender_clone = Arc::clone(&backup_sender);
        let fault_tolerance_sender_clone = Arc::clone(&fault_tolerance_sender);

        tokio::task::spawn(async move {
            let message = connection.read_message().await;

            match message.first() {
                Some(1) | Some(2) | Some(11) | Some(12) => {
                    leader_sender_clone.send((connection, message)).unwrap()
                }
                Some(10) | Some(13) => peer_sender_clone.send((connection, message)).unwrap(),
                Some(20) => backup_sender_clone.send((connection, message)).unwrap(),
                Some(30) | Some(31) => fault_tolerance_sender_clone
                    .send((connection, message))
                    .unwrap(),
                Some(200) | Some(202) => client_sender_clone.send((connection, message)).unwrap(),
                _ => println!("received invalid message, dropping"),
            };
        });
    }
}
