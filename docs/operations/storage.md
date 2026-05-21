# Storage Modes

Fluree supports four storage modes, each optimized for different deployment scenarios. This document provides detailed information about each storage mode and guidance for choosing the right one.

## Storage Modes

### Memory Storage

In-memory storage for development and testing:

```bash
./fluree-db-server --storage memory
```

**Characteristics:**
- Data stored in RAM only
- No persistence (data lost on restart)
- Fastest performance
- No external dependencies

**Use Cases:**
- Local development
- Unit testing
- Temporary/ephemeral databases
- Prototyping

**Limitations:**
- No durability (data lost on crash/restart)
- Limited by available RAM
- Single process only

### File Storage

Local file system storage:

```bash
./fluree-db-server \
  --storage file \
  --data-dir /var/lib/fluree
```

**Characteristics:**
- Data persisted to local disk
- Survives server restarts
- Good performance (SSD recommended)
- Simple setup

**Use Cases:**
- Single-server production
- Development with persistence
- Edge deployments
- Small to medium scale

**Limitations:**
- Single machine only
- No built-in replication
- Limited by disk capacity
- No cross-region support

### AWS Storage

Distributed storage using S3 and DynamoDB:

```bash
./fluree-db-server \
  --storage aws \
  --s3-bucket fluree-prod-data \
  --s3-region us-east-1 \
  --dynamodb-table fluree-nameservice \
  --dynamodb-region us-east-1
```

**Characteristics:**
- Distributed, scalable storage
- Multi-process coordination
- Cross-region replication
- High durability (99.999999999%)

**Use Cases:**
- Multi-server production
- High availability requirements
- Geographic distribution
- Cloud-native applications

**Limitations:**
- Requires AWS account
- Higher latency than local storage
- Usage costs
- More complex setup

### IPFS Storage

Decentralized content-addressed storage via a local Kubo node:

```json
{
  "@context": {"@vocab": "https://ns.flur.ee/system#"},
  "@graph": [{
    "@type": "Connection",
    "indexStorage": {
      "@type": "Storage",
      "ipfsApiUrl": "http://127.0.0.1:5001",
      "ipfsPinOnPut": true
    }
  }]
}
```

**Characteristics:**
- Content-addressed (every blob identified by SHA-256 hash)
- Immutable, tamper-evident storage
- Decentralized replication via IPFS network
- Fluree's native CIDs work directly with IPFS

**Use Cases:**
- Decentralized / censorship-resistant deployments
- Content integrity verification
- Cross-organization data sharing
- Foundation for IPNS/ENS-based ledger discovery

**Limitations:**
- Requires a running Kubo node
- No prefix listing (manifest-based tracking needed)
- No native deletion (unpin + GC)
- Higher write latency than local file I/O

See [IPFS Storage Guide](ipfs-storage.md) for complete setup and configuration.

## Storage Architecture

### Memory Storage

```text
┌──────────────────────┐
│   Fluree Process     │
│  ┌────────────────┐  │
│  │  Hash Map      │  │
│  │  (In Memory)   │  │
│  └────────────────┘  │
└──────────────────────┘
```

All data in process memory.

### File Storage

```text
┌──────────────────────┐
│   Fluree Process     │
│  ┌────────────────┐  │
│  │   File I/O     │  │
│  └────────┬───────┘  │
└───────────┼──────────┘
            │
     ┌──────▼──────┐
     │ File System │
     │  /var/lib/  │
     │   fluree/   │
     └─────────────┘
```

Data persisted to local files.

### AWS Storage

```text
┌──────────────────────┐  ┌──────────────────────┐
│   Fluree Process 1   │  │   Fluree Process 2   │
│  ┌────────────────┐  │  │  ┌────────────────┐  │
│  │  AWS SDK       │  │  │  │  AWS SDK       │  │
│  └────────┬───────┘  │  │  └────────┬───────┘  │
└───────────┼──────────┘  └───────────┼──────────┘
            │                         │
            └────────┬────────────────┘
                     │
          ┌──────────▼──────────┐
          │     AWS Cloud       │
          │  ┌──────┐  ┌──────┐│
          │  │  S3  │  │Dynamo││
          │  └──────┘  └──────┘│
          └─────────────────────┘
```

Multiple processes coordinate via AWS.

### IPFS Storage

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
     │  (IPFS)     │
     └──────┬──────┘
            │ libp2p
     ┌──────▼──────┐
     │  IPFS P2P   │
     │  Network    │
     └─────────────┘
```

Data stored as content-addressed blocks in IPFS via Kubo.

## Storage Encryption

Fluree supports transparent AES-256-GCM encryption for data at rest. When enabled, all data is automatically encrypted before being written to storage.

### Enabling Encryption

```bash
# Generate a 32-byte encryption key
export FLUREE_ENCRYPTION_KEY=$(openssl rand -base64 32)
```

Configure via JSON-LD (file storage):

```json
{
  "@context": {"@vocab": "https://ns.flur.ee/system#"},
  "@graph": [{
    "@type": "Connection",
    "indexStorage": {
      "@type": "Storage",
      "filePath": "/var/lib/fluree",
      "AES256Key": {"envVar": "FLUREE_ENCRYPTION_KEY"}
    }
  }]
}
```

For S3 storage with encryption:

```json
{
  "@context": {"@vocab": "https://ns.flur.ee/system#"},
  "@graph": [{
    "@type": "Connection",
    "indexStorage": {
      "@type": "Storage",
      "s3Bucket": "my-fluree-bucket",
      "s3Endpoint": "https://s3.us-east-1.amazonaws.com",
      "AES256Key": {"envVar": "FLUREE_ENCRYPTION_KEY"}
    }
  }]
}
```

**Key Features:**
- AES-256-GCM authenticated encryption
- Works natively with all storage backends (memory, file, S3)
- Transparent encryption/decryption on read/write
- Portable ciphertext format (encrypted data can be moved between backends)
- Environment variable support for key configuration

See [Storage Encryption](../security/encryption.md) for full documentation.

## File Storage Details

### Directory Structure

```text
/var/lib/fluree/
├── ns@v2/                    # Nameservice records
│   ├── mydb/
│   │   ├── main.json        # Ledger metadata
│   │   └── dev.json
│   └── customers/
│       └── main.json
├── mydb/
│   ├── main/
│   │   ├── commit/          # Commit blobs (*.fcv2)
│   │   ├── txn/             # Transaction metadata (*.json)
│   │   ├── config/          # Ledger config blobs
│   │   └── index/
│   │       ├── roots/       # Index root descriptors (*.fir6)
│   │       ├── objects/
│   │       │   ├── branches/
│   │       │   ├── leaves/
│   │       │   └── history/
│   │       ├── garbage/
│   │       ├── stats/
│   │       └── spatial/
│   ├── dev/
│   │   └── ...
│   └── @shared/
│       └── dicts/           # Dictionaries shared by all branches
└── graph-sources/            # Graph sources
    └── products-search/
        └── main/
            ├── mapping/
            └── snapshots/
```

### File Formats

**Nameservice (JSON):**
```json
{
  "ledger_id": "mydb:main",
  "name": "mydb",
  "branch": "main",
  "commit_t": 150,
  "index_t": 145,
  "commit_head_id": "bafybeig...commitT150",
  "index_head_id": "bafybeig...indexRootT145",
  "retracted": false
}
```

**Commits (Binary):**
- Compressed flake data
- Transaction metadata
- Cryptographic signatures

**Indexes (Binary):**
- Root descriptors, branch manifests, leaf pages, and history sidecars
- Optimized for query performance

**Shared dictionaries (Binary):**
- Cross-branch dictionary blobs under `{ledger}/@shared/dicts/`
- May be referenced by more than one branch of the same ledger

### File System Requirements

**Minimum:**
- 10 GB free space
- SSD recommended (HDD acceptable)
- Sufficient IOPS for workload

**Recommended:**
- 100 GB+ free space
- NVMe SSD
- High IOPS capability
- Regular backups

## AWS Storage Details

### S3 Structure

```text
s3://fluree-prod-data/
├── mydb/
│   ├── main/
│   │   ├── commit/
│   │   ├── txn/
│   │   ├── config/
│   │   └── index/
│   │       ├── roots/
│   │       ├── objects/
│   │       │   ├── branches/
│   │       │   ├── leaves/
│   │       │   └── history/
│   │       ├── garbage/
│   │       ├── stats/
│   │       └── spatial/
│   └── @shared/
│       └── dicts/
└── graph-sources/
    └── products-search/
        └── main/
            ├── mapping/
            └── snapshots/
```

### DynamoDB Schema

The nameservice uses a DynamoDB table with a **composite primary key** (`pk` + `sk`) for ledger and graph source metadata coordination. Each ledger or graph source is stored as multiple items (one per concern) under the same partition key.

See [DynamoDB Nameservice Guide](dynamodb-guide.md) for:
- Complete table schema with composite-key layout
- Table creation scripts (AWS CLI, CloudFormation, Terraform)
- GSI setup for listing by kind
- Local development setup with LocalStack
- Production considerations and troubleshooting

**Quick Reference:**
```text
Table: fluree-nameservice
Primary Key: pk (String, ledger-id) + sk (String, concern)
Sort Key Values: meta, head, index, config, status
GSI1 (gsi1-kind): kind (HASH) + pk (RANGE)
Items per ledger: 5 (meta, head, index, config, status)
Items per graph source: 4 (meta, config, index, status)
```

### AWS Permissions

Required IAM permissions:

**S3:**
```json
{
  "Effect": "Allow",
  "Action": [
    "s3:GetObject",
    "s3:PutObject",
    "s3:ListBucket",
    "s3:DeleteObject"
  ],
  "Resource": [
    "arn:aws:s3:::fluree-prod-data",
    "arn:aws:s3:::fluree-prod-data/*"
  ]
}
```

**DynamoDB:**
```json
{
  "Effect": "Allow",
  "Action": [
    "dynamodb:GetItem",
    "dynamodb:PutItem",
    "dynamodb:UpdateItem",
    "dynamodb:DeleteItem",
    "dynamodb:Query",
    "dynamodb:BatchGetItem",
    "dynamodb:BatchWriteItem"
  ],
  "Resource": [
    "arn:aws:dynamodb:us-east-1:*:table/fluree-nameservice",
    "arn:aws:dynamodb:us-east-1:*:table/fluree-nameservice/index/gsi1-kind"
  ]
}
```

### Cost Considerations

**S3 Costs:**
- Storage: ~$0.023/GB/month (Standard)
- PUT requests: ~$0.005/1000 requests
- GET requests: ~$0.0004/1000 requests

**DynamoDB Costs:**
- Provisioned: ~$0.25/WCU/month + $0.05/RCU/month
- On-Demand: ~$1.25/million writes + $0.25/million reads

**Typical Monthly Costs (medium deployment):**
- S3: $50-200 (depending on data size)
- DynamoDB: $10-50 (depending on traffic)
- Total: $60-250/month

## Choosing a Storage Mode

### Decision Matrix

| Requirement | Memory | File | AWS | IPFS |
|-------------|--------|------|-----|------|
| **Development** | Best | Good | Overkill | Overkill |
| **Single server** | No | Best | Overkill | Good |
| **Multi-server** | No | No | Best | Good |
| **Persistence** | No | Yes | Yes | Yes |
| **Cloud-native** | No | No | Yes | No |
| **Decentralized** | No | No | No | Best |
| **Content integrity** | No | No | No | Best |
| **Cost** | Free | Free | Monthly | Free |
| **Setup complexity** | Trivial | Simple | Complex | Moderate |
| **Performance** | Fastest | Fast | Good | Good |
| **Durability** | None | Local | 11 9's | Network-wide |

### Recommendations

**Use Memory when:**
- Developing locally
- Running tests
- Data is temporary
- Maximum performance needed

**Use File when:**
- Single server deployment
- Local persistence needed
- Simple setup preferred
- Predictable costs important

**Use AWS when:**
- Multiple servers needed
- High availability required
- Geographic distribution needed
- Cloud-native architecture

**Use IPFS when:**
- Decentralized storage required
- Content integrity verification is critical
- Cross-organization data sharing
- Building toward IPNS/ENS-based ledger discovery
- Censorship resistance is a requirement

## Switching Storage Modes

### Memory to File

Export from the running system and import into the new one:

```bash
# Export from memory
curl -X POST http://localhost:8090/export?ledger=mydb:main > mydb-export.jsonld

# Stop memory server, start file server
./fluree-db-server --storage file --data-dir /var/lib/fluree

# Import to file storage
curl -X POST "http://localhost:8090/v1/fluree/insert?ledger=mydb:main" \
  --data-binary @mydb-export.jsonld
```

### File to AWS

Copy files to S3 and create the nameservice table:

```bash
# Copy data directory to S3
aws s3 sync /var/lib/fluree/ s3://fluree-prod-data/

# Create DynamoDB table (see docs/operations/dynamodb-guide.md for full schema)
aws dynamodb create-table \
  --table-name fluree-nameservice \
  --attribute-definitions \
    AttributeName=pk,AttributeType=S \
    AttributeName=sk,AttributeType=S \
    AttributeName=kind,AttributeType=S \
  --key-schema \
    AttributeName=pk,KeyType=HASH \
    AttributeName=sk,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST

# Start AWS-backed server
./fluree-db-server --storage aws --s3-bucket fluree-prod-data
```

### AWS to File

Download from S3:

```bash
# Download data from S3
aws s3 sync s3://fluree-prod-data/ /var/lib/fluree/

# Start file-backed server
./fluree-db-server --storage file --data-dir /var/lib/fluree
```

## Backup and Recovery

### Memory Storage

No native backup (data is ephemeral):

```bash
# Export ledger
curl -X POST http://localhost:8090/export?ledger=mydb:main > backup.jsonld
```

### File Storage

Backup data directory:

```bash
# Stop server (recommended)
systemctl stop fluree

# Backup
tar -czf fluree-backup-$(date +%Y%m%d).tar.gz /var/lib/fluree/

# Start server
systemctl start fluree
```

For online backups, prefer storage-level snapshots or object-store versioning.
The standalone server does not currently expose HTTP read-only toggle endpoints.

### AWS Storage

Use S3 versioning and lifecycle policies:

```bash
# Enable versioning
aws s3api put-bucket-versioning \
  --bucket fluree-prod-data \
  --versioning-configuration Status=Enabled

# Configure lifecycle
aws s3api put-bucket-lifecycle-configuration \
  --bucket fluree-prod-data \
  --lifecycle-configuration file://lifecycle.json
```

DynamoDB backups:

```bash
# Enable point-in-time recovery
aws dynamodb update-continuous-backups \
  --table-name fluree-nameservice \
  --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true
```

## Troubleshooting

### File Storage

**Permission Errors:**
```bash
sudo chown -R fluree:fluree /var/lib/fluree
chmod -R 755 /var/lib/fluree
```

**Disk Full:**
```bash
# Check space
df -h /var/lib/fluree

# Force a full index refresh
curl -X POST http://localhost:8090/v1/fluree/reindex \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb:main"}'
```

### AWS Storage

**Connection Errors:**
- Verify AWS credentials
- Check IAM permissions
- Verify S3 bucket exists
- Check DynamoDB table exists

**Throttling:**
- Increase DynamoDB capacity
- Use provisioned capacity mode
- Implement retry logic

## Related Documentation

- [Configuration](configuration.md) - Configuration options
- [IPFS Storage Guide](ipfs-storage.md) - IPFS/Kubo setup and configuration
- [DynamoDB Nameservice Guide](dynamodb-guide.md) - DynamoDB-specific setup
- [Getting Started: Server](../getting-started/quickstart-server.md) - Initial setup
- [Admin and Health](admin-and-health.md) - Administrative operations
