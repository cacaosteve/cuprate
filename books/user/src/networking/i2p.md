# I2P P2P (experimental)

Cuprate can connect to Monero peers over I2P for transaction relay. This is a P2P feature, not an I2P wallet-RPC endpoint. It does not synchronize blocks over I2P: start and complete normal synchronization over the configured non-I2P network first. The I2P zone starts after that sync completes.

Cuprate accepts and validates transactions received from I2P peers; those transactions continue through the existing relay policy. Locally submitted transactions have a separate setting, `p2p.i2p_net.local_tx_relay_mode`, which defaults to `clearnet`:

- `clearnet` — use the existing non-I2P route (including the existing Tor preference when configured; otherwise clearnet).
- `i2p_then_clearnet` — try one I2P stem send first. If no I2P interface/peer is available, the send errors, or it has not completed within 15 seconds, start the clearnet Dandelion route. No clearnet send is queued before that decision. If the I2P send succeeds, the transaction stays private and is retried over I2P at embargo expiry; a later failed attempt may fall back to clearnet. Once clearnet fallback succeeds, that transition is recorded and ordinary Dandelion embargo recovery resumes, including eventual public broadcast if a stem peer fails to forward the transaction.
- `i2p_only` — keep a locally submitted transaction private, wait for the I2P interface, and retry I2P sends without clearnet fallback. Once the interface exists, each send attempt has a 15-second deadline; failures are retried after 20 seconds. Successful sends remain private and are retried at embargo expiry until the transaction leaves the pool. This mode requires `p2p.i2p_net.enabled = true` and `offline = false` for new submissions.

The submission origin and chosen local relay policy are saved with each transaction. Changing the configuration affects new submissions, not already queued transactions. Peer echoes (including fluff announcements) do not change a protected local transaction's policy. Removing a transaction from the pool, including block inclusion, cancels pending relay attempts and embargo timers. Bytes already handed to a peer transport cannot be recalled.

On restart, saved local I2P transactions resume their original policy; transactions which already used clearnet fallback resume normal clearnet recovery rather than restarting I2P-first attempts. Peer-origin and ordinary local stem transactions retain standard restart recovery. An older experimental private-pool record without origin metadata is retained privately **without automatic relay**, with a warning, rather than guessing its origin. This does not change the blockchain database or its format. If such legacy entries need to be discarded, stop the node and reset only the transaction-pool data before resubmitting; do not delete the blockchain. Do not downgrade to an older prototype while relying on persisted I2P-only policies: older code does not enforce this metadata.

An I2P send can fail because i2pd/SOCKS is unavailable, tunnels have not built, there is no connected outbound I2P peer, the peer disconnects, or the peer transport write fails/times out. A successful send means the peer connection accepted the protocol write; it is not an end-to-end confirmation that the transaction propagated across I2P or reached a miner. I2P's slower and less predictable peer availability is why the default remains `clearnet`.

## Requirements

- An external `i2pd` instance with its SOCKS5 proxy listening locally (default `127.0.0.1:4447`). Cuprate uses SOCKS5 for outbound peer connections.
- For inbound peers, an `i2pd` server tunnel forwarding to Cuprate's local listener (default `127.0.0.1:18084`). Set the tunnel's I2P destination in `address` and enable `inbound_tunnel`.
- At least one known I2P peer to connect to. This experimental implementation has no built-in I2P seed list; configure `seed_nodes` or arrange discovery through an inbound peer.

The node excludes its own destination from seeds, incoming peer lists, and restored peer lists. For anonymity-network destinations this compares the host, even if a peer advertises a different port. An empty eligible seed list waits for peer discovery instead of attempting a self-connection or stopping the outbound task.

## Configuration

I2P is disabled by default. In `Cuprated.toml`, configure the `[p2p.i2p_net]` section. A minimal inbound example is:

```toml
[p2p.i2p_net]
enabled = true
local_tx_relay_mode = "clearnet"
inbound_tunnel = true
address = "<your-52-character-destination>.b32.i2p"
socks_proxy = "127.0.0.1:4447"
listen_addr = "127.0.0.1:18084"
```

Replace the example destination with the base32 address shown by your `i2pd` server tunnel. Monero I2P addresses identify a destination, not a local TCP port. Cuprate uses compatibility port `1` for SOCKS connections and peer-list serialization, matching `monerod`; legacy numeric `:port` suffixes are accepted but normalized away. Seed addresses use the same host-only format. `listen_addr`, independently, is the actual TCP address to which the server tunnel forwards connections.

For an `i2pd` instance on the same machine/network namespace as Cuprate, add a server tunnel to `tunnels.conf` (or a file in `tunnels.d`):

```ini
[cuprate-p2p]
type = server
host = 127.0.0.1
port = 18084
inport = 0
keys = cuprate-p2p.dat
```

Here `port` must match Cuprate's TCP listener. `inport = 0` accepts the destination's I2P streams regardless of the requested destination port, including Monero's compatibility port `1`. Do not substitute the TCP forwarding port for this setting. Use a dedicated destination/key file for P2P rather than sharing the web or wallet-RPC destination.

If i2pd runs on the host and Cuprate runs inside a VM or container, loopback addresses refer to different machines/namespaces. Set the tunnel's `host` to Cuprate's private VM address and bind `listen_addr` there. Configure `socks_proxy` with a private host/proxy endpoint reachable from Cuprate. Permit only the necessary private paths in both firewalls; do not expose the SOCKS proxy or the anonymous TCP listener on a public interface. Enabling this Cuprate setting does not create Docker port mappings, firewall rules, or an i2pd tunnel automatically.

If inbound is disabled, leave `address` empty and set `inbound_tunnel = false`; outbound connections still need known peers in `seed_nodes` or the address book. There are no built-in I2P seeds in this experiment.

The port/address behavior follows [Monero's I2P address implementation](https://github.com/monero-project/monero/blob/v0.18.4.2/src/net/i2p_address.cpp) and the [Monero i2pd setup guide](https://docs.getmonero.org/running-node/monerod-tori2p/). See also [i2pd tunnel port configuration](https://docs.i2pd.website/en/latest/user-guide/tunnels/).

The I2P P2P zone only permits transaction broadcasts and the protocol's necessary connection-management messages. Block requests, block responses, and block propagation are rejected over this zone.

This feature is experimental. Validation on 2026-10-26 included the affected library regression tests, a real I2P handshake/timed-sync probe against a public Monero peer, and a cross-router probe against a controlled monerod 0.18.5.4 instance. The transport probes passed with i2pd 3.0.0 and with an existing shared i2pd 2.61.0 router. A separate-router inbound handshake/timed-sync probe also passed against the live Cuprate trial built at `f0a760f62d5ffe4c0a98feeb7c278a27da9fe264`; controlled monerod reported its I2P connection to that node in normal state. These tests do not demonstrate end-to-end transaction propagation, validate every relay policy on a live network, or establish anonymity. Broader privacy review and controlled relay testing are still needed before relying on it for production use.

## Opt-in interoperability probe

Once the tunnels are established, run the explicit mainnet handshake/timed-sync probe against a controlled monerod or Cuprate I2P P2P destination:

```sh
CUPRATE_I2P_PEER='<52-character-destination>.b32.i2p' \
CUPRATE_I2P_SOCKS_PROXY=127.0.0.1:4447 \
cargo test --locked -p cuprate-p2p-transport --test i2p_interop -- --ignored --nocapture
```

The test is ignored by default, uses a bounded connection/response timeout, and only exchanges connection-management messages. It does not open a wallet, spend funds, submit transactions, download blocks, or access a local chain database. A passing handshake validates protocol/address interoperability, not transaction propagation or an anonymity guarantee. Funded sends require a separately approved, controlled test.
