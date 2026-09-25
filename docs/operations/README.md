# Operations

This section covers operational aspects of running Fluree in production, including configuration, storage backends, monitoring, and administrative operations.

## Operation Guides

### [Scaling and Resilience Architecture](scaling-and-resilience.md)

How Fluree's deployment layers fit together:
- Horizontal query scaling with replaceable, cache-warm peers
- Independent transaction, query, and indexing capacity
- Serverless and scale-to-zero deployment boundaries
- Raft write availability within a region
- Multi-cloud read distribution and disaster recovery

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

### [Hardware sizing: CPU vs disk (benchmark)](hardware-benchmarks.md)

A worked benchmark (SPARQLoscope on ~574 M-triple DBLP) showing how hardware maps to performance:
- Import is storage-bound — local NVMe imports faster even on slower CPUs
- Query latency is CPU-bound — served from cache, tracks single-thread speed
- Sizing guidance: when to prioritize disk vs core vs RAM

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

Run the Fluree server as a read-only query peer:
- SSE nameservice events (`GET /v1/fluree/events`)
- Peer mode (refresh on stale + write forwarding)
- Storage proxy endpoints (`/v1/fluree/storage/*`) for private-storage deployments

## Deployment Patterns

### Development

Single process, file storage in `.fluree/storage` under the working directory (the default):

```bash
fluree server run --log-level debug
```

### Single Server Production

File-based storage:

```bash
fluree server run \
  --storage-path /var/lib/fluree \
  --listen-addr 0.0.0.0:8090 \
  --log-level info
```

### Distributed Production

AWS-backed distributed deployment. S3 buckets, DynamoDB table, and region are set in a
JSON-LD connection config file (see [Storage Modes](storage.md) and the
[connection config reference](../reference/connection-config-jsonld.md)); requires a build
with the `aws` feature:

```bash
fluree server run \
  --connection-config /etc/fluree/connection.jsonld \
  --listen-addr 0.0.0.0:8090
```

## Key Configuration Areas

### Server Settings

- Listen address (host and port)
- Request body size limit
- Timeout values
- CORS (on/off)

TLS is not terminated by the server; put a reverse proxy or load balancer in front of it.

### Storage Configuration

- Storage mode selection
- Storage path (file mode)
- AWS credentials (S3 mode)
- IPFS / Kubo connection (IPFS mode)
- Cache settings

### Indexing Configuration

- Novelty thresholds (soft reindex trigger, hard write backpressure)
- Stalled-indexing catch-up sweep interval
- Index retention (GC)

### Security Configuration

- Authentication mode per endpoint group (data, events, admin)
- Trusted token issuers
- Signed request validation
- Policy enforcement

The server has no built-in rate limiting; apply it at a reverse proxy or API gateway.

## Monitoring

### Health Checks

```bash
curl http://localhost:8090/health
```

Response:
```json
{
  "status": "ok",
  "version": "0.1.0"
}
```

### Server Statistics

```bash
curl http://localhost:8090/v1/fluree/stats
```

Response:
```json
{
  "uptime_secs": 3600,
  "storage_type": "file",
  "indexing_enabled": true,
  "cached_ledgers": 3,
  "version": "0.1.0"
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

The in-memory cache budget is global (there is no per-query memory setting):

```bash
fluree server run -- --cache-max-mb 1024
```

### Indexing Tuning

```bash
fluree server run -- \
  --indexing-enabled \
  --reindex-min-bytes 100000 \
  --reindex-max-bytes 1000000
```

### Query Tuning

```bash
fluree server run -- \
  --query-timeout-ms 30000 \
  --body-limit 1048576
```

`--body-limit` caps every request body (queries and transactions alike). There is no
query thread-count setting.

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

The server listens on plain HTTP only. Terminate TLS at a reverse proxy or load balancer
in front of it.

### Require Authentication

```bash
fluree server run -- \
  --data-auth-mode required \
  --data-auth-trusted-issuer did:key:z6Mk... \
  --admin-auth-mode required \
  --events-auth-mode required
```

With `--data-auth-mode required`, data endpoints accept either a Bearer token or a signed
request. See [Configuration](configuration.md) for issuer and audience options.

### Rate Limiting

The server has no built-in rate limiting. Apply it at a reverse proxy, API gateway, or load
balancer.

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

- Use TLS in production (terminated at a reverse proxy)
- Require authentication
- Rate-limit at a reverse proxy or API gateway
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
