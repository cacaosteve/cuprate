# RPC

> **⚠️ Warning ⚠️**
>
> Cuprate is still experimental software.
>
> Consider sandboxing `cuprated` before publicly exposing restricted RPC.

`monerod`'s daemon RPC has 3 kinds of interfaces:
1. [JSON-RPC 2.0](https://www.jsonrpc.org) methods called at the `/json_rpc` endpoint, e.g. [`get_block`](https://www.getmonero.org/resources/developer-guides/daemon-rpc.html#get_block)
1. JSON endpoints, e.g. [`/get_height`](https://www.getmonero.org/resources/developer-guides/daemon-rpc.html#get_height)
1. Binary endpoints, e.g. [`/get_blocks.bin`](https://www.getmonero.org/resources/developer-guides/daemon-rpc.html#get_blocksbin)

<!-- TODO: explain the binary format -->

`cuprated`'s RPC aims to mirror `monerod`'s as much as it can. The end-goal is compatibility with common use-cases such as wallet software.

This section contains the development status of endpoints/methods in `cuprated`.

| Status | Meaning |
|--------|---------|
| 🟢     | Enabled and tested
| 🟣     | Enabled but has differences waiting to be resolved
| 🟠     | Enabled but not fully tested
| ⚪     | Not enabled yet
| ⚫     | Not planned to be supported

<!-- NOTE: Sort methods/endpoints A-Z -->

## JSON-RPC methods
| Method                         | Status | Notes   |
|--------------------------------|--------|---------|
| `add_aux_pow`                  | ⚪     |
| `banned`                       | ⚪     |
| `calc_pow`                     | ⚪     |
| `flush_cache`                  | ⚫     | `cuprated` does not require this method
| `flush_txpool`                 | ⚪     |
| `generateblocks`               | ⚪     |
| `get_alternate_chains`         | 🟠     |
| `get_bans`                     | ⚪     |
| `get_block`                    | 🟠     |
| `get_block_count`              | 🟠     |
| `get_block_header_by_hash`     | 🟠     |
| `get_block_header_by_height`   | 🟠     |
| `get_block_headers_range`      | 🟠     |
| `get_block_template`           | ⚪     |
| `get_coinbase_tx_sum`          | 🟠     |
| `get_connections`              | 🟣     | Admin-only; see peer diagnostics below |
| `get_fee_estimate`             | 🟠     |
| `get_info`                     | 🟣     | Real admin peer counts; bootstrap and RPC-connection fields remain placeholders |
| `get_last_block_header`        | 🟠     |
| `get_miner_data`               | 🟣     |
| `get_output_distribution`      | 🟠     |
| `get_output_histogram`         | 🟠     |
| `get_transaction_counters`     | 🟢     | Cuprate extension; admin-only per-network totals |
| `get_tx_ids_loose`             | ⚫     |
| `get_txpool_backlog`           | 🟠     |
| `get_version`                  | 🟠     |
| `hard_fork_info`               | 🟠     |
| `on_get_block_hash`            | 🟠     |
| `prune_blockchain`             | ⚫     |
| `relay_tx`                     | ⚪     |
| `set_bans`                     | ⚪     |
| `submit_block`                 | 🟠     |
| `sync_info`                    | ⚪     |

## JSON endpoints
Aliases are listed with their canonical endpoint.

| Endpoint                       | Status | Notes |
|--------------------------------|--------|-------|
| `/get_alt_blocks_hashes`       | ⚪     |
| `/get_height`, `/getheight`    | 🟠     |
| `/get_info`, `/getinfo`        | 🟣     | Real admin peer counts; bootstrap and RPC-connection fields remain placeholders |
| `/get_limit`                   | ⚪     |
| `/get_net_stats`               | ⚪     |
| `/get_outs`                    | ⚪     |
| `/get_peer_list`               | 🟣     | Admin-only; last-seen timestamps are not tracked |
| `/get_public_nodes`            | ⚪     |
| `/get_transaction_pool`        | ⚪     |
| `/get_transaction_pool_hashes` | 🟠     |
| `/get_transaction_pool_stats`  | ⚪     |
| `/get_transactions`, `/gettransactions` | 🟣     | `decode_as_json` is unsupported |
| `/in_peers`                    | ⚪     |
| `/is_key_image_spent`          | 🟠     |
| `/mining_status`               | ⚫     | `cuprated` does not mine |
| `/out_peers`                   | ⚪     |
| `/pop_blocks`                  | ⚪     |
| `/save_bc`                     | ⚪     |
| `/send_raw_transaction`, `/sendrawtransaction` | 🟣     | `do_not_relay=true` is explicitly rejected; see submission safeguards below |
| `/set_bootstrap_daemon`        | ⚪     |
| `/set_limit`                   | ⚪     |
| `/set_log_categories`          | ⚪     |
| `/set_log_hash_rate`           | ⚫     | `cuprated` does not mine |
| `/set_log_level`               | ⚪     |
| `/start_mining`                | ⚫     | `cuprated` does not mine |
| `/stop_daemon`                 | 🟠     |
| `/stop_mining`                 | ⚫     | `cuprated` does not mine |
| `/update`                      | ⚫     |

## Transaction submission safeguards

`/send_raw_transaction` and its alias reject `do_not_relay=true` on both the
restricted and administrative listeners, returning a failed response with
`not_relayed=true` and an explanatory reason. No transaction is submitted by
that request. Persisted no-relay storage is not implemented; the flag must never
be silently treated as permission to broadcast.

A successful submission waits for actual insertion into the local pool (or a
confirmed already-known transaction), not merely an internal queue. Pool key-image
conflicts return `double_spend=true`; full-pool and concurrent-verification
failures do not return success. A wallet may retry a concurrent-verification
failure after the other request finishes. Success is not a guarantee of network
delivery, mining, or persistence across a power failure.

`storage.txpool.max_txpool_byte_size` limits serialized transaction bytes across
the public and private pools together, including queued I2P-only transactions.
It does not cap the entire process's RAM or database/compaction overhead. Zero
rejects all new transactions. A full pool rejects new transactions without
evicting existing ones or changing their relay policy. Block inclusion and normal
public-pool expiry free capacity. Private queues remain bounded by the same
global byte limit, but are not automatically expired or broadcast to make room.

If a saved pool exceeds the configured cap, startup fails without deleting or
relaying its contents. Restore a sufficient cap before restarting. There is no
transaction-pool database format change, and queued I2P-only policies remain
unchanged across upgrades and restarts.

## Binary endpoints
Aliases are listed with their canonical endpoint.

| Endpoint                                      | Status | Notes |
|-----------------------------------------------|--------|-------|
| `/get_blocks.bin`, `/getblocks.bin`           | 🟠     |
| `/get_blocks_by_height.bin`, `/getblocks_by_height.bin` | 🟠     |
| `/get_hashes.bin`, `/gethashes.bin`           | 🟠     |
| `/get_output_distribution.bin`                | 🟠     |
| `/get_o_indexes.bin`                          | 🟠     |
| `/get_outs.bin`                               | 🟠     |
| `/get_transaction_pool_hashes.bin`            | 🟠     |

## Private peer diagnostics

Use the **unrestricted administrative RPC** through localhost, a private network,
or an SSH tunnel. Do not publish this listener or proxy these methods to the
internet. Restricted RPC rejects `get_connections` and `/get_peer_list`, and
continues to redact connection counts and peer-list sizes in `get_info`.

On administrative RPC, `get_info` reports the number of handshaken inbound and
outbound connections and the white/gray peer-list sizes across all initialized
zones (clearnet, Tor and I2P). Disabled or not-yet-started zones contribute zero;
zones started after blockchain synchronization become visible automatically.
Disconnected peers stop contributing to connection counts but may remain in
the saved peer lists.

For example, with the administrative listener at `127.0.0.1:18081`:

```sh
curl -fsS http://127.0.0.1:18081/get_info
curl -fsS http://127.0.0.1:18081/json_rpc \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"get_connections"}'
curl -fsS http://127.0.0.1:18081/get_peer_list \
  -H 'Content-Type: application/json' \
  -d '{"public_only":false,"include_blocked":false}'
```

`get_connections` lists active, handshaken connections with their direction,
address type, known address, current peer height, connection age, peer ID,
pruning seed and advertised RPC/support information. Address types are 1=IPv4,
2=IPv6, 3=I2P and 4=Tor. Incoming anonymous connections whose destination is
unknown have empty address/host/IP/port fields; the local proxy address is never
substituted for a peer identity. Peer IDs on anonymous networks are not unique.

The byte counts, rates, idle times and connection UUID fields are **not yet
tracked** and retain the schema's zero sentinels; zero in these fields is not
evidence of an idle connection. `state` is `normal` for handshaken connections;
Cuprate does not use monerod's per-connection synchronization state machine.

`/get_peer_list` lists known peers, not active connections. `public_only` defaults
to `true` (clearnet only); set it to `false` to include Tor and I2P. This option
does not bypass administrative authorization. `include_blocked` defaults to
`false` and filters any retained blocked entries; normal banning purges entries,
so this is not a history of banned peers. Last-seen timestamps are not tracked
and remain zero. No transaction IDs or transaction/peer timing correlations are
added by these diagnostics.

## Private transaction counters (Cuprate extension)

`get_transaction_counters` is available **only on unrestricted administrative
RPC**. Restricted RPC rejects it with JSON-RPC error `-32601`; the totals are
not added to public `get_info` or per-peer connection information.

```sh
curl -fsS http://127.0.0.1:18081/json_rpc \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"get_transaction_counters"}'
```

The result contains `status`, `untrusted`, and `clearnet`, `tor`, `i2p` objects,
each with three integer totals:

- `received`: transaction **blobs**, not message envelopes, in successfully
  decoded P2P `NewTransactions` notifications. Includes duplicates, invalid
  transactions, and notifications ignored while behind the peer. Does not count
  block-sync transaction data, malformed wire envelopes, or local wallet RPC.
- `accepted`: new txpool insertions successfully committed from that source
  zone. Verification or queueing alone does not count; known transactions,
  concurrent duplicates, double spends, and restored pool entries do not count.
  This is not a lifetime unique-hash census: a removed transaction subsequently
  reaccepted counts as a new insertion. The first successful insertion determines
  attribution; an I2P echo of an already accepted clearnet transaction is only a
  receipt, not a second acceptance.
- `relayed`: transaction blobs whose transport send **and flush** succeeded to
  peers in that destination zone, for both stem and fluff. Queued messages,
  failed/timed-out sends and canceled sends do not count. Successful retries and
  fan-out count once per blob per successful peer send. Monero notifications have
  no acknowledgement: this does not prove remote receipt, validation, mining, or
  propagation beyond that peer.

For example, receiving a tx over I2P and passing it to clearnet increases I2P
`received`/`accepted` and clearnet `relayed`, not I2P `relayed`. Local wallet RPC
submissions do not increment any zone's `received`/`accepted`, but their actual
outbound P2P sends increment the destination zone's `relayed` total. Consequently
the totals need not match, and `received - accepted` is **not** a rejection count.

Counters are fixed-size, memory-only, cumulative within each initialized network
zone for the running process. They survive peer disconnects and RPC connections,
reset on node restart, and saturate at `u64::MAX`. Disabled/not-yet-started zones
report zero; late-starting I2P/Tor becomes visible automatically. Each zone's
snapshot is consistent, but snapshots across zones are not a single global
instant. No hashes, identities or per-transaction timestamps are retained. These
diagnostics do not change relay policy; in particular, accepting I2P traffic does
not cause local wallet transactions to switch from clearnet to I2P sending.
