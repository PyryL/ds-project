use crate::communication::IncomingConnection;
use crate::PeerNode;
use handlers::{handle_backup_request, handle_read_request, handle_transfer_request, handle_write_request};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

mod backup;
mod handlers;

pub async fn leader_block(
    mut incoming_connection_stream: mpsc::UnboundedReceiver<(IncomingConnection, Vec<u8>)>,
    initial_kv_pairs: Vec<(u64, Vec<u8>)>,
    node_list_arc: Arc<Mutex<Vec<PeerNode>>>,
    this_node_id: u64,
) {
    let mut leader_storage: HashMap<u64, Vec<u8>> = HashMap::new();

    println!(
        "leader block starting with initial kv-pairs {:?}",
        initial_kv_pairs
    );

    for (key, value) in initial_kv_pairs {
        leader_storage.insert(key, value);
    }

    while let Some((connection, first_message)) = incoming_connection_stream.recv().await {
        match first_message.first() {
            Some(1) => handle_read_request(connection, first_message, &leader_storage).await,
            Some(2) => {
                let node_list_clone = Arc::clone(&node_list_arc);
                handle_write_request(
                    connection,
                    first_message,
                    &mut leader_storage,
                    this_node_id,
                    node_list_clone,
                )
                .await
            }
            Some(11) => {
                handle_transfer_request(connection, first_message, &mut leader_storage).await
            }
            Some(12) => handle_backup_request(connection, &leader_storage).await,
            _ => panic!(),
        };
    }
}
