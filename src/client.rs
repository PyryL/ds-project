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
