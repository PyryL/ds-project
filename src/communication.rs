use std::io::Result;
use std::net::{SocketAddr, ToSocketAddrs};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;

/// Sends the given message to the given peer.
/// Returns the response.
pub async fn send_message(peer_ip_address: String, message: &[u8]) -> Result<Vec<u8>> {
    let peer_address = format!("{}:52525", peer_ip_address).to_socket_addrs().unwrap().next().unwrap();

    let client = TcpSocket::new_v4().unwrap();
    let mut stream = client.connect(peer_address).await?;

    stream.write_all(message).await.unwrap();
    stream.shutdown().await.unwrap();

    let mut response: Vec<u8> = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();

    Ok(response)
}

pub async fn send_message_without_closing(peer_ip_address: String, message: &[u8]) -> IncomingConnection {
    let peer_address = format!("{}:52525", peer_ip_address).to_socket_addrs().unwrap().next().unwrap();

    let client = TcpSocket::new_v4().unwrap();
    let mut stream = client.connect(peer_address).await.unwrap();

    stream.write_all(message).await.unwrap();

    IncomingConnection {
        address: peer_address,
        stream,
    }
}

/// Infinitely listens to incoming connections.
/// For every connection, sends `IncomingConnection` to the returned stream.
pub async fn listen_messages() -> impl Stream<Item = IncomingConnection> {
    let (tx, rx) = tokio::sync::mpsc::channel(1);

    tokio::task::spawn(async move {
        let listener = TcpListener::bind("0.0.0.0:52525").await.unwrap();

        while let Ok((stream, address)) = listener.accept().await {
            let incoming_connection = IncomingConnection {
                address,
                stream,
            };
            tx.send(incoming_connection).await.unwrap();
        }
    });

    ReceiverStream::new(rx)
}

pub struct IncomingConnection {
    pub address: SocketAddr,
    stream: TcpStream,
}

impl IncomingConnection {
    /// Reads and returns the next message from the stream.
    /// Panics if there is no message to be read or if it is malformed.
    pub async fn read_message(&mut self) -> Vec<u8> {
        let mut header = [0u8; 5];

        self.stream.read_exact(&mut header).await.unwrap();

        let message_length = u32::from_be_bytes(header[1..5].try_into().unwrap());

        let mut payload = vec![0; (message_length-5) as usize];

        self.stream.read_exact(&mut payload).await.unwrap();

        [header.to_vec(), payload].concat()
    }

    /// Sends the given response and closes the connection.
    pub async fn respond(mut self, message: &[u8]) {
        self.stream.write_all(message).await.unwrap();
        self.stream.shutdown().await.unwrap();
    }

    pub async fn respond_without_closing(&mut self, message: &[u8]) {
        self.stream.write_all(message).await.unwrap();
    }
}
