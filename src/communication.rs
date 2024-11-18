use std::io::Result;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;

/// Sends the given message to the given peer.
/// Returns the response.
pub async fn send_message(peer_ip_address: String, message: &[u8]) -> Result<Vec<u8>> {
    let peer_address = format!("{}:52525", peer_ip_address).parse().unwrap();

    let client = TcpSocket::new_v4().unwrap();
    let mut stream = client.connect(peer_address).await?;

    stream.write_all(message).await.unwrap();
    stream.shutdown().await.unwrap();

    let mut response: Vec<u8> = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();

    Ok(response)
}

/// Infinitely listens to incoming connections.
/// For every connection, pushes `(message, stream)` tuple to the returned stream
/// where message is the received message and stream is the TCP stream with which
/// response can be sent.
pub async fn listen_messages() -> impl Stream<Item = IncomingConnection> {
    let (tx, rx) = tokio::sync::mpsc::channel(1);

    tokio::task::spawn(async move {
        let listener = TcpListener::bind("0.0.0.0:52525").await.unwrap();

        while let Ok((mut stream, address)) = listener.accept().await {
            let mut incoming_message: Vec<u8> = Vec::new();
            stream.read_to_end(&mut incoming_message).await.unwrap();
            let incoming_connection = IncomingConnection {
                message: incoming_message,
                address,
                stream,
            };
            tx.send(incoming_connection).await.unwrap();
        }
    });

    ReceiverStream::new(rx)
}

pub struct IncomingConnection {
    pub message: Vec<u8>,
    pub address: SocketAddr,
    stream: TcpStream,
}

impl IncomingConnection {
    /// Sends the given response and closes the connection.
    pub async fn respond(mut self, message: &[u8]) {
        self.stream.write_all(message).await.unwrap();
        self.stream.shutdown().await.unwrap();
    }
}
