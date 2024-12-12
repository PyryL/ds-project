use crate::PeerNode;
use crate::helpers::communication::Connection;
use crate::blocks::fault_tolerance::send_node_down;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

/// Handles incoming requests from clients wanting to perform operations in the datastore.
pub async fn client_block(
    mut incoming_connection_stream: mpsc::UnboundedReceiver<(Connection, Vec<u8>)>,
    node_list: Arc<Mutex<Vec<PeerNode>>>,
) {
    while let Some((client_connection, message)) = incoming_connection_stream.recv().await {
        let node_list_clone = Arc::clone(&node_list);

        tokio::task::spawn(async move {
            match message.first() {
                Some(200) => {
                    forward_read_request(client_connection, message, node_list_clone).await
                }
                Some(202) => {
                    forward_write_request(client_connection, message, node_list_clone).await
                }
                _ => {}
            };
        });
    }
}

/// Handles an incoming read request from a client
/// by forwarding the conversation between the client and a leader node.
async fn forward_read_request(
    mut client_connection: Connection,
    message: Vec<u8>,
    node_list_arc: Arc<Mutex<Vec<PeerNode>>>,
) {
    // at this point, the first byte of connection.message is `200`
    if message.len() != 13 {
        println!("received invalid request from a client, dropping");
        return;
    }
    let key = u64::from_be_bytes(message[5..13].try_into().unwrap());

    let forwarded_message = [vec![1, 0, 0, 0, 13], key.to_be_bytes().to_vec()].concat();

    // forward the request to the leader node
    let node_list;
    {
        node_list = node_list_arc.lock().await.clone();
    }

    let leader_node = leader_node_for_key(&node_list, key);

    let mut leader_connection =
        match Connection::new(leader_node.ip_address, &forwarded_message).await {
            Ok(connection) => connection,
            Err(_) => {
                // leader node was down, handle fault and retry
                send_node_down(leader_node.id, &node_list).await;

                let node_list;
                {
                    node_list = node_list_arc.lock().await.clone();
                }
                let leader_node = leader_node_for_key(&node_list, key);

                match Connection::new(leader_node.ip_address, &forwarded_message).await {
                    Ok(connection) => connection,
                    Err(_) => {
                        println!("found two crashed nodes during forwarding, dropping");
                        return;
                    }
                }
            }
        };

    println!(
        "forwarding read request {} -> {}",
        client_connection.address, leader_connection.address
    );

    let leader_response = leader_connection.read_message().await;

    // forward response to the client
    client_connection.send_message(&leader_response).await;
}

/// Handles an incoming write request from a client
/// by forwarding the conversation between the client and a leader node.
async fn forward_write_request(
    mut client_connection: Connection,
    message: Vec<u8>,
    node_list_arc: Arc<Mutex<Vec<PeerNode>>>,
) {
    // at this point, the first byte of message is `202`
    if message.len() != 13 {
        println!("received invalid type=202 request from a client, dropping");
        return;
    }
    let key = u64::from_be_bytes(message[5..13].try_into().unwrap());
    let forwarded_message = [vec![2, 0, 0, 0, 13], key.to_be_bytes().to_vec()].concat();

    // forward request to the leader node
    let node_list;
    {
        node_list = node_list_arc.lock().await.clone();
    }

    let leader_node = leader_node_for_key(&node_list, key);

    let mut leader_connection =
        match Connection::new(leader_node.ip_address, &forwarded_message).await {
            Ok(connection) => connection,
            Err(_) => {
                // leader node was down, handle fault and retry
                send_node_down(leader_node.id, &node_list).await;

                let node_list;
                {
                    node_list = node_list_arc.lock().await.clone();
                }
                let leader_node = leader_node_for_key(&node_list, key);

                match Connection::new(leader_node.ip_address, &forwarded_message).await {
                    Ok(connection) => connection,
                    Err(_) => {
                        println!("found two crashed nodes during forwarding, dropping");
                        return;
                    }
                }
            }
        };

    println!(
        "forwarding write request {} -> {}",
        client_connection.address, leader_connection.address
    );

    // wait for and forward the write permission
    let permission_msg = leader_connection.read_message().await;
    client_connection.send_message(&permission_msg).await;

    // wait for and forward the write command message
    let write_command_message = client_connection.read_message().await;
    leader_connection.send_message(&write_command_message).await;

    // wait for and forward the acknowledgement
    let ack_message = leader_connection.read_message().await;
    client_connection.send_message(&ack_message).await;

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
