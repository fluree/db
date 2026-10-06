# IPFS Storage

Fluree can use [IPFS](https://ipfs.tech/) as a content-addressed storage backend via the [Kubo](https://github.com/ipfs/kubo) HTTP RPC API. This enables decentralized, content-addressed data storage where every piece of data is identified by its cryptographic hash.

> **Status: experimental, Rust API only.** IPFS storage is available to programs that embed
> Fluree through the Rust API (`fluree-db-api` with the `ipfs` feature). The server and the
> `fluree` CLI cannot use it, and a connection config cannot select it (a storage node with
> `ipfsApiUrl` is rejected). The Rust builder pairs IPFS with an **in-memory nameservice**: the
> blocks persist in IPFS, but the record of each ledger's current commit does not survive a
> restart, so a restarted process no longer finds its ledgers. Treat it as a way to publish and
> fetch content-addressed Fluree data, not as durable primary storage.

## Overview

IPFS storage maps naturally to Fluree's content-addressed architecture. Fluree already identifies every blob (commits, transactions, index nodes) with a CIDv1 content identifier using SHA-256 hashing and Fluree-specific multicodec values. When IPFS is used as the storage backend, these CIDs are stored directly into IPFS via a local Kubo node.

**Key properties:**

- Content-addressed: data is identified by its SHA-256 hash, providing built-in integrity verification
- Immutable: once written, data cannot be modified or deleted (only unpinned for garbage collection)
- Decentralized: data can be replicated across IPFS nodes without centralized coordination
- Compatible: Fluree's native CIDs work directly with IPFS (no translation layer needed)

## Kubo Setup

[Kubo](https://github.com/ipfs/kubo) (formerly go-ipfs) is the reference IPFS implementation. Fluree communicates with Kubo via its HTTP RPC API (default port 5001).

### Install Kubo

**macOS (Homebrew):**

```bash
brew install ipfs
```

**Linux (official binary):**

```bash
wget https://dist.ipfs.tech/kubo/v0.32.1/kubo_v0.32.1_linux-amd64.tar.gz
tar xvfz kubo_v0.32.1_linux-amd64.tar.gz
cd kubo
sudo ./install.sh
```

**Docker:**

```bash
docker run -d \
  --name ipfs \
  -p 4001:4001 \
  -p 5001:5001 \
  -p 8080:8080 \
  -v ipfs_data:/data/ipfs \
  ipfs/kubo:latest
```

### Initialize and Start

```bash
# Initialize IPFS (first time only)
ipfs init

# Start the daemon
ipfs daemon
```

Verify the node is running:

```bash
# Check node identity
curl -s -X POST http://127.0.0.1:5001/api/v0/id | jq .ID
```

### Security Note

The Kubo HTTP RPC API (port 5001) provides full administrative access to the IPFS node. By default, it listens only on `127.0.0.1`. **Do not expose port 5001 to the public internet.** If Fluree and Kubo run on different hosts, use SSH tunneling, a VPN, or a reverse proxy with authentication.

The IPFS gateway (port 8080) is read-only and can be exposed publicly if desired.

## Using IPFS Storage

Enable the `ipfs` feature on `fluree-db-api`:

```toml
[dependencies]
fluree-db-api = { version = "...", features = ["ipfs"] }
```

Build a Fluree instance against a running Kubo node:

```rust
use fluree_db_api::FlureeBuilder;

let fluree = FlureeBuilder::memory().build_ipfs("http://127.0.0.1:5001");
```

`build_ipfs` takes the Kubo HTTP RPC API base URL. It pins every block it writes, enables
background indexing, and keeps the nameservice (ledger heads and branch metadata) in memory.
There is no builder yet that pairs IPFS storage with a persistent nameservice.

## Architecture

```text
┌──────────────────────┐
│   Fluree Process     │
│  ┌────────────────┐  │
│  │  IpfsStorage   │  │
│  │  (HTTP client) │  │
│  └────────┬───────┘  │
└───────────┼──────────┘
            │ HTTP RPC
     ┌──────▼──────┐
     │  Kubo Node  │
     │  (port 5001)│
     └──────┬──────┘
            │ libp2p
     ┌──────▼──────┐
     │  IPFS P2P   │
     │  Network    │
     └─────────────┘
```

Fluree communicates with a local Kubo node via the HTTP RPC API. The Kubo node handles peer-to-peer networking, block storage, and replication with the broader IPFS network.

### API Endpoints Used

| Kubo Endpoint | Purpose |
|---------------|---------|
| `POST /api/v0/block/put` | Store a block with optional codec and hash type |
| `POST /api/v0/block/get` | Retrieve a block by CID |
| `POST /api/v0/block/stat` | Check if a block exists (metadata only) |
| `POST /api/v0/pin/add` | Pin a block to prevent garbage collection |
| `POST /api/v0/id` | Health check (verify node is reachable) |

## Content Addressing

### How Fluree CIDs Map to IPFS

Fluree uses CIDv1 with SHA-256 multihash and private-use multicodec values:

| Content Kind | Multicodec | Hex | Example |
|--------------|------------|-----|---------|
| Commit | `fluree-commit` | `0x300001` | `bafybeig...` |
| Transaction | `fluree-txn` | `0x300002` | `bafybeig...` |
| Index Root | `fluree-index-root` | `0x300003` | `bafybeig...` |
| Index Branch | `fluree-index-branch` | `0x300004` | `bafybeig...` |
| Index Leaf | `fluree-index-leaf` | `0x300005` | `bafybeig...` |
| Dict Blob | `fluree-dict-blob` | `0x300006` | `bafybeig...` |
| Garbage Record | `fluree-garbage` | `0x300007` | `bafybeig...` |
| Ledger Config | `fluree-ledger-config` | `0x300008` | `bafybeig...` |
| Stats Sketch | `fluree-stats-sketch` | `0x300009` | `bafybeig...` |
| Graph Source Snapshot | `fluree-graph-source-snapshot` | `0x30000A` | `bafybeig...` |
| Spatial Index | `fluree-spatial-index` | `0x30000B` | `bafybeig...` |

These are in the multicodec private-use range (`0x300000`+). Kubo accepts them via the `cid-codec` parameter and resolves blocks by multihash regardless of codec. This means Fluree's native CIDs work directly with IPFS without any translation layer.

### Cross-Codec Retrieval

IPFS block storage is keyed by multihash internally. A block stored with codec `0x300001` (Fluree commit) can be retrieved using a CID with codec `0x55` (raw) as long as the SHA-256 digest is the same. This simplifies the address-based `StorageRead` implementation: given a Fluree address containing a hash, we can construct any CID with that hash to fetch the block.

## Pinning

### What is Pinning?

IPFS nodes periodically garbage-collect unpinned blocks to free disk space. Pinning tells the node to keep specific blocks permanently. Without pinning, blocks may be removed from the local node (though they remain available on other nodes that have them).

### Default Behavior

`build_ipfs` pins every block it writes. This ensures that:

- All committed data survives Kubo garbage collection
- The local node serves as a reliable storage backend
- Blocks remain available even if no other node has them

### Pinning Services

For production deployments, consider using a remote pinning service for redundancy:

```bash
# Add a remote pinning service
ipfs pin remote service add pinata https://api.pinata.cloud/psa YOUR_JWT

# Pin a CID to the remote service
ipfs pin remote add --service=pinata bafybeig...
```

## Limitations

### No Prefix Listing

IPFS is a content-addressed store with no concept of directory listing or prefix enumeration. Admin operations that depend on listing (such as the fast path for dropping a ledger) fall back to walking the commit chain by CID, which is slower but correct.

### No Deletion

IPFS content is immutable. When Fluree releases a block (index garbage collection, dropping a ledger), it unpins it:

1. The block is **unpinned** on the local node
2. Kubo's **garbage collector** reclaims the space on its next run
3. The block may still exist on other IPFS nodes, or stay if it is pinned elsewhere

### Nameservice

IPFS stores immutable blocks, but a ledger also needs a mutable pointer to its current commit (the nameservice record). `build_ipfs` keeps those records in memory, so they are lost when the process exits. A future phase may add IPNS or ENS-based nameservices.

### Latency

Writes go through the Kubo HTTP RPC API, adding HTTP overhead compared to direct file I/O. For latency-sensitive workloads, ensure Kubo runs on the same host as Fluree (localhost communication).

### No Encryption

IPFS storage cannot be encrypted at rest. `build_ipfs()` returns an error when the builder carries a key (`AES256Key` or `AES256Keys`), rather than publishing plaintext to a content-addressed network. For confidentiality, use another storage backend.

## Storage Addresses

Fluree addresses for IPFS storage follow the standard format:

```
fluree:ipfs://{ledger_id}/{kind_dir}/{hash_hex}.{ext}
```

Examples:

```
fluree:ipfs://mydb/main/commit/a1b2c3...f6a1b2.fcv2
fluree:ipfs://mydb/main/index/roots/d4e5f6...c3d4e5.json
fluree:ipfs://mydb/main/index/spot/abc123...def456.fli
```

The hash hex in the filename is extracted and used to construct a CID for retrieval from IPFS.

## Operational Considerations

### Disk Usage

Kubo stores blocks in a local datastore (by default, a LevelDB-based flatfs at `~/.ipfs/blocks/`). Monitor disk usage:

```bash
# Check IPFS repo size
ipfs repo stat

# Run garbage collection (removes unpinned blocks)
ipfs repo gc
```

### Network Bandwidth

By default, Kubo participates in the IPFS DHT and may serve blocks to other nodes. For a private deployment:

```bash
# Disable DHT (private node)
ipfs config Routing.Type none

# Or use a private IPFS network with a swarm key
# See: https://github.com/ipfs/kubo/blob/master/docs/experimental-features.md#private-networks
```

### Performance Tuning

```bash
# Increase concurrent connections
ipfs config Swarm.ConnMgr.HighWater 300

# Adjust datastore cache
ipfs config Datastore.BloomFilterSize 1048576

# Disable automatic GC (if using external pinning)
ipfs config --json Datastore.GCPeriod '"0"'
```

### Monitoring

Check Kubo node health:

```bash
# Node identity and version
ipfs id

# Connected peers
ipfs swarm peers | wc -l

# Repo statistics
ipfs repo stat

# Bandwidth usage
ipfs stats bw
```

## Troubleshooting

### Connection Refused

```
IPFS node connection failed: http://127.0.0.1:5001
```

**Causes:**
- Kubo daemon is not running
- Kubo is listening on a different address/port
- Firewall blocking the connection

**Fix:**
```bash
# Start the daemon
ipfs daemon

# Or check what address it's listening on
ipfs config Addresses.API
```

### Block Not Found

```
IPFS block not found: bafybeig...
```

**Causes:**
- Block was never stored on this node
- Block was unpinned and garbage collected
- CID format mismatch

**Fix:**
```bash
# Check if block exists locally
ipfs block stat bafybeig...

# Try fetching from the network
ipfs block get bafybeig... > /dev/null
```

### Slow Writes

**Causes:**
- Kubo node under heavy load
- Network latency (if Kubo is remote)
- Disk I/O bottleneck

**Fix:**
- Run Kubo on the same host as Fluree
- Use SSD storage for the IPFS datastore
- Consider disabling DHT for private deployments

## Future Roadmap

### Phase 2: Decentralized Nameservice

The IPFS storage backend is designed as the foundation for decentralized Fluree deployments. Planned additions:

- **IPNS**: Publish mutable pointers to ledger state (commit head, index root)
- **ENS / L2 chain**: On-chain CID pointers for trustless ledger discovery
- **Two-tier nameservice**: Local nameservice for fast reads with async push to decentralized upstream (similar to `git push`)

### Content Pinning Strategy

Future versions may support:

- Automatic pinning profiles (pin commits only, pin everything, pin nothing)
- Integration with remote pinning services (Pinata, web3.storage)
- Manifest-based tracking for GC and prefix listing

## Related Documentation

- [Storage modes](storage.md) - Overview of all storage backends
- [Rust API](../getting-started/rust-api.md) - Embedding Fluree, including feature flags
- [ContentId and ContentStore](../design/content-id-and-contentstore.md) - Content addressing design
- [Storage traits](../design/storage-traits.md) - Storage backend architecture
