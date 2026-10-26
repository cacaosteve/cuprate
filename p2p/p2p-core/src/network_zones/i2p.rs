//! I2P network zone.

use cuprate_wire::I2pAddr;

use crate::{NetZoneAddress, NetworkZone};

impl NetZoneAddress for I2pAddr {
    const IS_ANONYMITY_NETWORK: bool = true;

    type BanID = [u8; 52];

    // I2P destinations have no Monero TCP port to mutate.
    fn set_port(&mut self, _port: u16) {}

    fn make_canonical(&mut self) {}

    fn ban_id(&self) -> Self::BanID {
        self.domain()
    }

    fn should_add_to_peer_list(&self) -> bool {
        true
    }
}

#[derive(Clone, Copy)]
pub struct I2p;

impl NetworkZone for I2p {
    const NAME: &'static str = "I2P";
    const CHECK_NODE_ID: bool = false;
    const BROADCAST_OWN_ADDR: bool = true;
    type Addr = I2pAddr;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tcp_port_updates_do_not_change_i2p_destination_identity() {
        let mut address: I2pAddr = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.b32.i2p"
            .parse()
            .unwrap();
        let original = address;
        address.set_port(18084);
        address.make_canonical();
        assert_eq!(address, original);
        assert_eq!(address.port(), 1);
    }
}
