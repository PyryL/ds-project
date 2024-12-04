use crate::communication::IncomingConnection;

pub async fn push_update_to_backup(
    backup_node_ip_address: String,
    key: u64,
    value: Vec<u8>,
) -> bool {
    let request_length = value.len() as u32 + 13;
    let request = [
        vec![20],
        request_length.to_be_bytes().to_vec(),
        key.to_be_bytes().to_vec(),
        value,
    ]
    .concat();

    let mut connection = IncomingConnection::new(backup_node_ip_address, &request)
        .await
        .unwrap();

    let response = connection.read_message().await;

    if response != [0, 0, 0, 0, 7, 111, 107] {
        println!(
            "failed to update backup for key={} at {}, aborting the write",
            key, connection.address
        );
        false
    } else {
        true
    }
}
