use crate::communication::{send_message, send_message_without_closing, IncomingConnection};
use crate::PeerNode;
use tokio::sync::mpsc;

pub async fn client_block(
    mut incoming_connection_stream: mpsc::UnboundedReceiver<(IncomingConnection, Vec<u8>)>,
    node_list: &[PeerNode],
) {
    while let Some((client_connection, message)) = incoming_connection_stream.recv().await {
        // TODO: make node_list mutex and pass the reference instead of clone
        let node_list_clone = node_list.to_vec();
        tokio::task::spawn(async move {
            match message.first() {
                Some(200) => forward_read_request(client_connection, message, &node_list_clone).await,
                Some(202) => forward_write_request(client_connection, message, &node_list_clone).await,
                _ => {},
            };
        });
    }
}

async fn forward_read_request(client_connection: IncomingConnection, message: Vec<u8>, node_list: &[PeerNode]) {
    // at this point, the first byte of connection.message is `200`
    if message.len() != 13 {
        println!("received invalid request from a client, dropping");
        return;
    }
    let key = u64::from_be_bytes(message[5..13].try_into().unwrap());

    // forward the request to the leader node
    let leader_node = leader_node_for_key(node_list, key);
    println!(
        "forwarding read request {} -> {}",
        client_connection.address, leader_node.ip_address
    );
    let forwarded_message = [vec![1, 0, 0, 0, 13], key.to_be_bytes().to_vec()].concat();
    let leader_response = send_message(leader_node.ip_address, &forwarded_message)
        .await
        .unwrap();

    // forward response to the client
    client_connection.respond(&leader_response).await;
}

async fn forward_write_request(mut client_connection: IncomingConnection, message: Vec<u8>, node_list: &[PeerNode]) {
    // at this point, the first byte of message is `202`
    if message.len() != 13 {
        println!("received invalid type=202 request from a client, dropping");
        return;
    }
    let key = u64::from_be_bytes(message[5..13].try_into().unwrap());

    // forward request to the leader node
    let leader_node = leader_node_for_key(node_list, key);
    println!("forwarding write request {} -> {}", client_connection.address, leader_node.ip_address);
    let forwarded_message = [vec![2, 0, 0, 0, 13], key.to_be_bytes().to_vec()].concat();
    let mut leader_connection = send_message_without_closing(leader_node.ip_address, &forwarded_message).await;

    // wait for and forward the write permission
    let permission_msg = leader_connection.read_message().await;
    client_connection.respond_without_closing(&permission_msg).await;

    // wait for and forward the write command message
    let write_command_message = client_connection.read_message().await;
    leader_connection.respond_without_closing(&write_command_message).await;

    // wait for and forward the acknowledgement
    let ack_message = leader_connection.read_message().await;
    client_connection.respond(&ack_message).await;

    println!("write request forwarding ended");
}

/// From the given node list, returns the node that is the leader for the given key.
fn leader_node_for_key(node_list: &[PeerNode], key: u64) -> PeerNode {
    // node list always contains at least this node itself
    if node_list.is_empty() {
        panic!("node list should not be empty");
    }

    // find the node with smallest ID that is >= than the key
    // or the largest node if other not found
    let leader_node = node_list
        .iter()
        .filter(|node| node.id >= key) // list of nodes with greater id
        .min_by_key(|node| node.id) // take the node with smallest id
        .unwrap_or_else(|| {
            // no nodes with greater id, fall back to the largest node
            node_list.iter().max_by_key(|node| node.id).unwrap()
        });

    (*leader_node).clone()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn leader_node_selection() {
        let node_list = vec![
            PeerNode {
                id: 5,
                ip_address: "192.168.0.5".to_string(),
            },
            PeerNode {
                id: 12,
                ip_address: "192.168.0.12".to_string(),
            },
            PeerNode {
                id: 25,
                ip_address: "192.168.0.25".to_string(),
            },
        ];

        assert_eq!(leader_node_for_key(&node_list, 3).id, 5);
        assert_eq!(leader_node_for_key(&node_list, 5).id, 5);
        assert_eq!(leader_node_for_key(&node_list, 6).id, 12);
        assert_eq!(leader_node_for_key(&node_list, 24).id, 25);
        assert_eq!(leader_node_for_key(&node_list, 25).id, 25);
        assert_eq!(leader_node_for_key(&node_list, 26).id, 25);
    }
}
