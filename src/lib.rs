use communication::listen_messages;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

mod client;
mod communication;
mod leader;

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
    tokio::task::spawn(async move {
        leader::leader_block(leader_receiver).await;
    });

    let (client_sender, client_receiver) = mpsc::unbounded_channel();
    tokio::task::spawn(async move {
        client::client_block(client_receiver, &node_list).await;
    });

    let mut incoming_connections_stream = listen_messages().await;

    while let Some(mut connection) = incoming_connections_stream.next().await {
        let message = connection.read_message().await;

        match message.first() {
            Some(1) | Some(2) => leader_sender.send((connection, message)).unwrap(),

            Some(200) | Some(202) => client_sender.send((connection, message)).unwrap(),

            _ => println!("received invalid message, dropping"),
        };
    }
}
