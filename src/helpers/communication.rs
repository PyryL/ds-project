use std::io::Result;
use std::net::{SocketAddr, ToSocketAddrs};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::Stream;

/// Infinitely listens to incoming connections.
/// For every connection, sends `Connection` to the returned stream.
pub async fn listen_messages() -> impl Stream<Item = Connection> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    tokio::task::spawn(async move {
        let listener = TcpListener::bind("0.0.0.0:52525").await.unwrap();

        while let Ok((stream, address)) = listener.accept().await {
            let incoming_connection = Connection { address, stream };
            tx.send(incoming_connection).unwrap();
        }
    });

    UnboundedReceiverStream::new(rx)
}

/// Returns the IPv4 address that corresponds to the given hostname, if any.
pub fn resolve_hostname_to_ip_address(hostname: &str) -> Option<String> {
    let addresses = match format!("{}:52525", hostname).to_socket_addrs() {
        Ok(addrs) => addrs,
        Err(_) => return None,
    };

    let ipv4 = addresses
        .map(|address| address.ip())
        .find(|ip| ip.is_ipv4());

    match ipv4 {
        Some(address) => Some(address.to_string()),
        None => None,
    }
}

pub struct Connection {
    pub address: SocketAddr,
    stream: TcpStream,
}

impl Connection {
    /// Open and return a new connection with another process and send the given message.
    pub async fn new(peer_ip_address: String, message: &[u8]) -> Result<Connection> {
        let peer_address = format!("{}:52525", peer_ip_address)
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap();

        let client = TcpSocket::new_v4().unwrap();
        let mut stream = client.connect(peer_address).await?;

        stream.write_all(message).await.unwrap();

        Ok(Connection {
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
