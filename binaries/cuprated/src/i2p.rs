//! i2pd-backed I2P transport configuration.

use cuprate_p2p::TransportConfig;
use cuprate_p2p_core::I2p;
use cuprate_p2p_transport::{I2pServerConfig, I2pSocks, I2pSocksClientConfig};

use crate::config::Config;

pub(crate) fn transport_config(config: &Config) -> TransportConfig<I2p, I2pSocks> {
    let i2p = &config.p2p.i2p_net;
    let inbound_enabled = i2p.inbound_tunnel && i2p.address.get().is_some();

    TransportConfig {
        client_config: I2pSocksClientConfig {
            proxy: i2p.socks_proxy,
        },
        server_config: inbound_enabled.then_some(I2pServerConfig {
            ip: i2p.listen_addr.ip(),
            port: i2p.listen_addr.port(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monero_destination_port_is_independent_of_local_tunnel_forwarding_port() {
        let mut config = Config::default();
        config.p2p.i2p_net.inbound_tunnel = true;
        config.p2p.i2p_net.address = Some(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.b32.i2p"
                .parse()
                .unwrap(),
        )
        .into();
        config.p2p.i2p_net.listen_addr = "127.0.0.1:28084".parse().unwrap();

        let transport = transport_config(&config);
        assert_eq!(transport.server_config.unwrap().port, 28084);
        assert_eq!(config.p2p.i2p_net.address.get().unwrap().port(), 1);
        assert_eq!(config.i2p_p2p_config().p2p_port, 0);
    }

    #[test]
    fn inbound_without_an_advertised_destination_has_no_listener() {
        let mut config = Config::default();
        config.p2p.i2p_net.inbound_tunnel = true;
        assert!(transport_config(&config).server_config.is_none());
        assert_eq!(config.i2p_p2p_config().max_inbound_connections, 0);
    }
}
