use std::io::Result;
use std::net::{SocketAddr, ToSocketAddrs};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::Stream;

/// Infinitely listens to incoming connections.
/// For every connection, sends `IncomingConnection` to the returned stream.
pub async fn listen_messages() -> impl Stream<Item = IncomingConnection> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    tokio::task::spawn(async move {
        let listener = TcpListener::bind("0.0.0.0:52525").await.unwrap();

        while let Ok((stream, address)) = listener.accept().await {
            let incoming_connection = IncomingConnection { address, stream };
            tx.send(incoming_connection).unwrap();
        }
    });

    UnboundedReceiverStream::new(rx)
}

pub struct IncomingConnection {
    pub address: SocketAddr,
    stream: TcpStream,
}

impl IncomingConnection {
    /// Open and return a new connection with another process and send the given message.
    pub async fn new(peer_ip_address: String, message: &[u8]) -> Result<IncomingConnection> {
        let peer_address = format!("{}:52525", peer_ip_address)
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap();

        let client = TcpSocket::new_v4().unwrap();
        let mut stream = client.connect(peer_address).await?;

        stream.write_all(message).await.unwrap();

        Ok(IncomingConnection {
            address: peer_address,
            stream,
        })
    }

    /// Reads and returns the next message from the stream.
    /// Panics if there is no message to be read or if it is malformed.
    pub async fn read_message(&mut self) -> Vec<u8> {
        let mut header = [0u8; 5];

        self.stream.read_exact(&mut header).await.unwrap();

        let message_length = u32::from_be_bytes(header[1..5].try_into().unwrap());

        let mut payload = vec![0; (message_length - 5) as usize];

        self.stream.read_exact(&mut payload).await.unwrap();

        [header.to_vec(), payload].concat()
    }

    /// Sends the given message to the connection stream.
    pub async fn send_message(&mut self, message: &[u8]) {
        self.stream.write_all(message).await.unwrap();
    }
}
