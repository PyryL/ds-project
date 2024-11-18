use crate::communication::{send_message, IncomingConnection};
use crate::PeerNode;
use tokio::sync::mpsc;

pub async fn client_block(
    mut incoming_connection_stream: mpsc::UnboundedReceiver<IncomingConnection>,
    node_list: &[PeerNode],
) {
    while let Some(client_connection) = incoming_connection_stream.recv().await {
        // TODO: also handle write requests
        // TODO: handle requests in parallel

        // at this point, the first byte of connection.message is `200`
        if client_connection.message.len() != 9 {
            println!("received invalid request from a client, dropping");
            continue;
        }
        let key = u64::from_be_bytes(client_connection.message[1..9].try_into().unwrap());

        // forward the request to the leader node
        let leader_node = leader_node_for_key(node_list, key);
        println!(
            "forwarding read request {} -> {}",
            client_connection.address, leader_node.ip_address
        );
        let forwarded_message = [vec![1u8], key.to_be_bytes().to_vec()].concat();
        let leader_response = send_message(leader_node.ip_address, &forwarded_message)
            .await
            .unwrap();

        // forward response to the client
        client_connection.respond(&leader_response).await;
    }
}

fn leader_node_for_key(node_list: &[PeerNode], key: u64) -> PeerNode {
    // node list always contains at least this node itself
    if node_list.is_empty() {
        panic!("node list should not be empty");
    }

    // find the node with smallest ID that is >= than the key
    // or the largest node if other not found
    let leader_node = node_list
        .iter()
        .filter(|node| node.id >= key)
        .min_by_key(|node| node.id)
        .unwrap_or_else(|| node_list.first().unwrap());

    (*leader_node).clone()
}
