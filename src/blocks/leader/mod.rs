use crate::helpers::communication::Connection;
use crate::PeerNode;
use handlers::{
    handle_backup_request, handle_fault_tolerance_insertion, handle_read_request,
    handle_transfer_request, handle_write_request,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

mod backup;
mod handlers;

/// Handles incoming requests related to the primary key-value pairs stored by this node.
pub async fn leader_block(
    mut incoming_connection_stream: mpsc::UnboundedReceiver<(Connection, Vec<u8>)>,
    initial_kv_pairs: Vec<(u64, Vec<u8>)>,
    node_list_arc: Arc<Mutex<Vec<PeerNode>>>,
    this_node_id: u64,
) {
    let leader_storage: Arc<Mutex<HashMap<u64, Vec<u8>>>> = Arc::new(Mutex::new(HashMap::new()));

    println!(
        "leader block starting with initial kv-pairs {:?}",
        initial_kv_pairs
    );

    {
        let mut storage_access = leader_storage.lock().await;
        for (key, value) in initial_kv_pairs {
            storage_access.insert(key, value);
        }
    }

    while let Some((connection, first_message)) = incoming_connection_stream.recv().await {
        let leader_storage_clone = Arc::clone(&leader_storage);
        let node_list_clone = Arc::clone(&node_list_arc);

        tokio::task::spawn(async move {
            match first_message.first() {
                Some(1) => {
                    handle_read_request(connection, first_message, leader_storage_clone).await
                }
                Some(2) => {
                    handle_write_request(
                        connection,
                        first_message,
                        leader_storage_clone,
                        this_node_id,
                        node_list_clone,
                    )
                    .await
                }
                Some(11) => {
                    handle_transfer_request(connection, first_message, leader_storage_clone).await
                }
                Some(12) => handle_backup_request(connection, leader_storage_clone).await,
                Some(33) => {
                    handle_fault_tolerance_insertion(
                        connection,
                        first_message,
                        leader_storage_clone,
                    )
                    .await
                }
                _ => panic!(),
            };
        });
    }
}
