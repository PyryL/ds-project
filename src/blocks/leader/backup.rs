use crate::{
    communication::Connection,
    helpers::neighbors::find_neighbors_wrapping, PeerNode,
};
use crate::blocks::fault_tolerance::send_node_down;

/// Pushes the update to both backup neighbors and handles possible crashed nodes.
/// Returns `true` if the update was propagated to both backups, `false` otherwise.
pub async fn push_update_to_backups(
    node_list: &Vec<PeerNode>,
    this_node_id: u64,
    key: u64,
    value: Vec<u8>,
) -> bool {
    for neighbor_side in 0..2 {
        for retry_counter in 0..2 {
            let neighbor = &find_neighbors_wrapping(this_node_id, node_list)[neighbor_side];

            if let Some(neighbor) = neighbor {
                let success =
                    send_backup_message(neighbor.ip_address.clone(), key, value.clone()).await;

                if !success && retry_counter == 0 {
                    // neighbor is down
                    send_node_down(neighbor.id, node_list).await;
                } else if !success {
                    // two neighbors on the same side were down, failing
                    return false;
                } else {
                    // backup pushed successfully
                    break;
                }
            }
        }
    }

    true
}

async fn send_backup_message(ip_address: String, key: u64, value: Vec<u8>) -> bool {
    let request_length = value.len() as u32 + 13;
    let request = [
        vec![20],
        request_length.to_be_bytes().to_vec(),
        key.to_be_bytes().to_vec(),
        value,
    ]
    .concat();

    let mut connection = match Connection::new(ip_address, &request).await {
        Ok(conn) => conn,
        Err(_) => return false,
    };

    let response = connection.read_message().await;

    if response != [0, 0, 0, 0, 7, 111, 107] {
        println!(
            "failed to update backup for key={} at {}",
            key, connection.address
        );
        false
    } else {
        true
    }
}
