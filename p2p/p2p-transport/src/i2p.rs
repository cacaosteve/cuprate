//! I2P transport using an external i2pd SOCKS5 proxy and a local server tunnel.

use std::{
    io::{self, ErrorKind},
    net::{IpAddr, SocketAddr},
    pin::Pin,
    task::{Context, Poll},
};

use async_trait::async_trait;
use futures::Stream;
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpListener,
};
use tokio_socks::tcp::Socks5Stream;
use tokio_util::codec::{FramedRead, FramedWrite};

use cuprate_p2p_core::{I2p, NetworkZone, Transport};
use cuprate_wire::MoneroWireCodec;

#[derive(Clone, Copy, Debug)]
pub struct I2pSocksClientConfig {
    /// Address of the local i2pd SOCKS5 proxy.
    pub proxy: SocketAddr,
}

#[derive(Clone, Copy, Debug)]
pub struct I2pServerConfig {
    /// Address on which the i2pd server tunnel connects to Cuprate.
    pub ip: IpAddr,
    /// Local port targeted by the i2pd server tunnel.
    pub port: u16,
}

pub struct I2pInboundStream {
    listener: TcpListener,
}

impl Stream for I2pInboundStream {
    type Item = Result<
        (
            Option<<I2p as NetworkZone>::Addr>,
            FramedRead<OwnedReadHalf, MoneroWireCodec>,
            FramedWrite<OwnedWriteHalf, MoneroWireCodec>,
        ),
        io::Error,
    >;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.listener
            .poll_accept(cx)
            .map_ok(|(stream, _)| {
                let (read, write) = stream.into_split();
                (
                    None, // The inbound TCP tunnel does not expose the I2P peer address.
                    FramedRead::new(read, MoneroWireCodec::default()),
                    FramedWrite::new(write, MoneroWireCodec::default()),
                )
            })
            .map(Some)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct I2pSocks;

#[async_trait]
impl Transport<I2p> for I2pSocks {
    type ClientConfig = I2pSocksClientConfig;
    type ServerConfig = I2pServerConfig;
    type Stream = FramedRead<OwnedReadHalf, MoneroWireCodec>;
    type Sink = FramedWrite<OwnedWriteHalf, MoneroWireCodec>;
    type Listener = I2pInboundStream;

    async fn connect_to_peer(
        addr: <I2p as NetworkZone>::Addr,
        config: &Self::ClientConfig,
    ) -> Result<(Self::Stream, Self::Sink), io::Error> {
        let domain = addr.addr_string();
        Socks5Stream::connect(config.proxy, (domain.as_str(), addr.port()))
            .await
            .map_err(|error| io::Error::new(ErrorKind::ConnectionAborted, error.to_string()))
            .map(|stream| {
                let (read, write) = stream.into_inner().into_split();
                (
                    FramedRead::new(read, MoneroWireCodec::default()),
                    FramedWrite::new(write, MoneroWireCodec::default()),
                )
            })
    }

    async fn incoming_connection_listener(
        config: Self::ServerConfig,
    ) -> Result<Self::Listener, io::Error> {
        Ok(I2pInboundStream {
            listener: TcpListener::bind((config.ip, config.port)).await?,
        })
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    use cuprate_p2p_core::Transport;
    use cuprate_wire::I2pAddr;

    use super::{I2pServerConfig, I2pSocks, I2pSocksClientConfig};

    #[tokio::test]
    async fn outbound_socks_sends_i2p_hostname_to_proxy_for_remote_resolution() {
        let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy.local_addr().unwrap();

        let proxy_task = tokio::spawn(async move {
            let (mut stream, _) = proxy.accept().await.unwrap();

            let mut greeting = [0; 2];
            stream.read_exact(&mut greeting).await.unwrap();
            assert_eq!(greeting[0], 5);
            let mut methods = vec![0; greeting[1] as usize];
            stream.read_exact(&mut methods).await.unwrap();
            assert!(methods.contains(&0));
            stream.write_all(&[5, 0]).await.unwrap();

            let mut request = [0; 4];
            stream.read_exact(&mut request).await.unwrap();
            assert_eq!(&request[..3], &[5, 1, 0]);
            assert_eq!(request[3], 3, "SOCKS5 must carry the I2P hostname");

            let mut domain = vec![0; stream.read_u8().await.unwrap() as usize];
            stream.read_exact(&mut domain).await.unwrap();
            let mut port = [0; 2];
            stream.read_exact(&mut port).await.unwrap();

            stream
                .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0])
                .await
                .unwrap();

            (String::from_utf8(domain).unwrap(), u16::from_be_bytes(port))
        });

        let address: I2pAddr = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.b32.i2p:18083"
            .parse()
            .unwrap();
        let config = I2pSocksClientConfig { proxy: proxy_addr };
        let (read, write) = I2pSocks::connect_to_peer(address, &config).await.unwrap();
        drop((read, write));

        let (domain, port) = proxy_task.await.unwrap();
        assert_eq!(domain, address.addr_string());
        assert_eq!(
            port, 1,
            "use Monero's compatibility port, not a local TCP port"
        );
    }

    #[tokio::test]
    async fn inbound_server_tunnel_accepts_anonymous_tcp_connection() {
        let mut inbound = I2pSocks::incoming_connection_listener(I2pServerConfig {
            ip: "127.0.0.1".parse().unwrap(),
            port: 0,
        })
        .await
        .unwrap();
        let listener_addr = inbound.listener.local_addr().unwrap();

        let (client, accepted) = tokio::join!(TcpStream::connect(listener_addr), inbound.next(),);
        let _client = client.unwrap();
        let (peer_addr, _read, _write) = accepted.unwrap().unwrap();
        assert!(peer_addr.is_none());
    }
}
