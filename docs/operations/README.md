# Operations

This section covers operational aspects of running Fluree in production, including configuration, storage backends, monitoring, and administrative operations.

## Operation Guides

### [Configuration](configuration.md)

Server configuration options:
- Command-line flags
- Configuration files
- Environment variables
- Runtime settings
- Tuning parameters

### [Running with Docker](docker.md)

Configuring the official `fluree/server` image:
- Image internals (entrypoint, volumes, runtime user)
- Three configuration approaches: env vars, mounted JSON-LD/TOML config, CLI flags
- Common recipes: LRU cache sizing, background indexing, auth, S3+DynamoDB, query peers
- Full annotated Docker Compose example
- Troubleshooting (volume permissions, `RUST_LOG` vs `FLUREE_LOG_LEVEL`, cache auto-sizing under cgroup limits)

### [Storage Modes](storage.md)

Storage backend options:
- Memory storage (development)
- File system storage (single server)
- AWS S3/DynamoDB (distributed)
- IPFS / Kubo (decentralized)
- Storage selection criteria
- Switching between storage modes

### [Serverless Storage Choices](serverless-storage.md)

Cloud/serverless storage placement guidance:
- Standard S3 vs S3 Express One Zone for index storage
- Why commits should normally remain on Standard S3
- Expected transaction, query, and indexing latency ranges
- Lambda disk cache and S3 concurrency tuning notes

### [IPFS Storage](ipfs-storage.md)

IPFS-specific setup and configuration:
- Kubo node installation and setup
- JSON-LD configuration fields
- Content addressing and CID mapping
- Pinning strategies
- Operational considerations

### [DynamoDB Nameservice](dynamodb-guide.md)

DynamoDB-specific setup and configuration:
- Table creation (CLI, CloudFormation, Terraform)
- Schema reference (v2 attributes)
- AWS credentials and permissions
- Local development with LocalStack
- Production considerations

### [Telemetry and Logging](telemetry.md)

Monitoring and observability:
- Logging configuration
- Metrics collection
- Tracing
- Health monitoring
- Performance metrics
- Integration with monitoring systems

### [Admin, Health, and Stats](admin-and-health.md)

Administrative operations:
- Health check endpoints
- Server statistics
- Manual indexing triggers
- Backup and restore
- Maintenance operations

### [Query peers and replication](query-peers.md)

Run `fluree-server` as a read-only query peer:
- SSE nameservice events (`GET /v1/fluree/events`)
- Peer mode (refresh on stale + write forwarding)
- Storage proxy endpoints (`/v1/fluree/storage/*`) for private-storage deployments

## Deployment Patterns

### Development

Single-process, memory storage:

```bash
./fluree-db-server --storage memory --log-level debug
```

### Single Server Production

File-based storage:

```bash
./fluree-db-server \
  --storage file \
  --data-dir /var/lib/fluree \
  --port 8090 \
  --log-level info
```

### Distributed Production

AWS-backed distributed deployment:

```bash
./fluree-db-server \
  --storage aws \
  --s3-bucket fluree-prod-data \
  --s3-region us-east-1 \
  --dynamodb-table fluree-nameservice \
  --port 8090
```

## Key Configuration Areas

### Server Settings

- Port and host binding
- TLS/SSL certificates
- Request size limits
- Timeout values
- CORS configuration

### Storage Configuration

- Storage mode selection
- Data directory (file mode)
- AWS credentials (S3 mode)
- IPFS / Kubo connection (IPFS mode)
- Connection pooling
- Cache settings

### Indexing Configuration

- Index interval
- Batch size
- Memory allocation
- Number of threads
- Index retention

### Security Configuration

- Authentication mode
- API key requirements
- Signed request validation
- Policy enforcement
- Rate limiting

## Monitoring

### Health Checks

```bash
curl http://localhost:8090/health
```

Response:
```json
{
  "status": "healthy",
  "version": "0.1.0",
  "storage": "file",
  "uptime_ms": 3600000
}
```

### Server Statistics

```bash
curl http://localhost:8090/v1/fluree/stats
```

Response:
```json
{
  "version": "0.1.0",
  "uptime_ms": 3600000,
  "ledgers": 5,
  "queries": {
    "total": 12345,
    "active": 3,
    "avg_duration_ms": 45
  },
  "transactions": {
    "total": 567,
    "avg_duration_ms": 89
  },
  "indexing": {
    "active": true,
    "pending_ledgers": 1,
    "avg_lag_ms": 1500
  }
}
```

### Metrics Collection

Use `GET /v1/fluree/stats` for built-in server statistics. Prometheus-style
`/metrics` export is not currently part of the standalone server API.

## Operational Tasks

### Backup

File storage backup:

```bash
# Backup data directory
tar -czf fluree-backup-$(date +%Y%m%d).tar.gz /var/lib/fluree/
```

AWS storage backup:

```bash
# S3 versioning enabled - automatic backups
aws s3 ls s3://fluree-prod-data/ --recursive

# Point-in-time recovery via S3 versions
```

### Restore

File storage restore:

```bash
# Stop server
systemctl stop fluree

# Restore backup
tar -xzf fluree-backup-20240122.tar.gz -C /

# Start server
systemctl start fluree
```

### Manual Indexing

Trigger indexing manually:

```bash
curl -X POST http://localhost:8090/v1/fluree/reindex \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb:main"}'
```

### Compaction

There is no standalone HTTP compaction endpoint. Reindexing rebuilds index
artifacts when you need to force a full refresh.

## Performance Tuning

### Memory Settings

```bash
./fluree-db-server \
  --query-memory-mb 2048 \
  --cache-size-mb 1024
```

### Indexing Tuning

```bash
fluree-server \
  --indexing-enabled \
  --reindex-min-bytes 100000 \
  --reindex-max-bytes 1000000
```

### Query Tuning

```bash
./fluree-db-server \
  --query-timeout-ms 30000 \
  --max-query-size 1048576 \
  --query-threads 8
```

## High Availability

### Load Balancing

Run multiple Fluree instances behind load balancer:

```text
          ┌─────────────┐
          │   Clients   │
          └──────┬──────┘
                 │
          ┌──────▼──────┐
          │    Load     │
          │  Balancer   │
          └──────┬──────┘
                 │
    ┌────────────┼────────────┐
    │            │            │
┌───▼────┐  ┌───▼────┐  ┌───▼────┐
│Fluree 1│  │Fluree 2│  │Fluree 3│
└───┬────┘  └───┬────┘  └───┬────┘
    │           │           │
    └───────────┼───────────┘
                │
         ┌──────▼──────┐
         │  S3/Dynamo  │
         │  Nameservice│
         └─────────────┘
```

### Failover

Configure health checks in load balancer:

```yaml
health_check:
  path: /health
  interval: 10s
  timeout: 5s
  healthy_threshold: 2
  unhealthy_threshold: 3
```

## Security Hardening

### TLS/SSL

```bash
./fluree-db-server \
  --tls-cert /path/to/cert.pem \
  --tls-key /path/to/key.pem \
  --tls-ca /path/to/ca.pem
```

### Require Authentication

```bash
./fluree-db-server \
  --require-auth \
  --require-signed-requests
```

### Rate Limiting

```bash
./fluree-db-server \
  --rate-limit-queries 100 \
  --rate-limit-transactions 10 \
  --rate-limit-window 60
```

## Best Practices

### 1. Use Appropriate Storage Mode

- Development: memory
- Single server: file
- Production/Distributed: AWS
- Decentralized: IPFS

### 2. Enable Monitoring

Set up monitoring for:
- Health status
- Query latency
- Transaction rate
- Indexing lag
- Error rates

### 3. Regular Backups

Automate backups:

```bash
# Daily backup cron
0 2 * * * /usr/local/bin/backup-fluree.sh
```

### 4. Capacity Planning

Monitor growth:
- Storage usage
- Query volume
- Transaction rate
- Index sizes

### 5. Security Best Practices

- Use TLS in production
- Require authentication
- Enable rate limiting
- Regular security audits

### 6. Log Management

- Rotate logs regularly
- Ship logs to centralized system
- Set appropriate log levels
- Monitor error rates

## Related Documentation

- [Configuration](configuration.md) - Detailed configuration reference
- [Storage](storage.md) - Storage backend details
- [Telemetry](telemetry.md) - Monitoring and metrics
- [Admin and Health](admin-and-health.md) - Administrative operations
- [Getting Started: Server](../getting-started/quickstart-server.md) - Initial setup
