# Troubleshooting

This section helps you diagnose and resolve common issues with Fluree deployments.

## Troubleshooting Guides

### [Common Errors](common-errors.md)

Reference for frequently encountered errors:
- Ledger not found
- Invalid IRI errors
- Transaction failures
- Query timeouts
- Permission errors
- Storage issues
- Indexing problems

### [Debugging Queries](debugging-queries.md)

Tools and techniques for query debugging:
- Using EXPLAIN plans
- Query tracing
- Performance profiling
- Identifying slow queries
- Optimizing query patterns

## Quick Diagnostics

### Health Check

First step for any issue:

```bash
curl http://localhost:8090/health
```

Check for unhealthy components.

### Server Status

Check overall server state:

```bash
curl http://localhost:8090/status
```

Look for:
- High error counts
- Active queries/transactions stuck
- High indexing lag
- Memory issues

### Logs

Check server logs:

```bash
# Recent errors
tail -f /var/log/fluree/server.log | grep ERROR

# Recent warnings
tail -f /var/log/fluree/server.log | grep WARN
```

## Common Issue Categories

### Connection Issues

**Symptoms:**
- Cannot connect to server
- Connection refused
- Connection timeout

**Common Causes:**
- Server not running
- Wrong port
- Firewall blocking
- Network issues

**Quick Checks:**
```bash
# Is server running?
ps aux | grep fluree-db-server

# Is port listening?
netstat -an | grep 8090

# Can you reach it?
curl http://localhost:8090/health
```

### Query Issues

**Symptoms:**
- Queries return no results
- Queries timeout
- Unexpected results
- Query errors

**Quick Checks:**
```bash
# Enable explain
curl -X POST http://localhost:8090/v1/fluree/explain \
  -d '{...}'

# Check query stats
curl http://localhost:8090/admin/query-stats
```

See [Debugging Queries](debugging-queries.md).

### Transaction Issues

**Symptoms:**
- Transactions fail
- Validation errors
- Policy denials
- Slow commits

**Quick Checks:**
```bash
# Validate JSON-LD
# Use online validator: json-ld.org/playground

# Check permissions
curl -X POST http://localhost:8090/v1/fluree/update?dryRun=true \
  -d '{...}'

# Check transaction stats
curl http://localhost:8090/admin/transaction-stats
```

### Performance Issues

**Symptoms:**
- Slow queries
- Slow transactions
- High latency
- Timeouts

**Quick Checks:**
```bash
# Check indexing lag
curl http://localhost:8090/ledgers/mydb:main | jq '.commit_t - .index_t'

# Check resource usage
curl http://localhost:8090/admin/memory

# Check active operations
curl http://localhost:8090/status | jq '.queries.active'
```

### Storage Issues

**Symptoms:**
- Cannot write data
- Storage errors
- Disk full
- AWS errors

**Quick Checks:**
```bash
# Check disk space
df -h /var/lib/fluree

# Check AWS connectivity
aws s3 ls s3://fluree-prod-data/

# Check storage stats
curl http://localhost:8090/admin/storage
```

## Error Code Reference

See [Common Errors](common-errors.md) for complete error code reference.

**Most Common:**
- `LEDGER_NOT_FOUND` - Ledger doesn't exist
- `PARSE_ERROR` - Invalid JSON-LD or SPARQL
- `INVALID_IRI` - Malformed IRI
- `QUERY_TIMEOUT` - Query took too long
- `POLICY_DENIED` - Not authorized

## Diagnostic Tools

### Enable Debug Logging

```bash
./fluree-db-server --log-level debug
```

Or at runtime:
```bash
curl -X POST http://localhost:8090/admin/log-level \
  -d '{"level": "debug"}'
```

### Enable Query Tracing

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "X-Fluree-Trace: true" \
  -d '{...}'
```

### Enable Policy Tracing

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "X-Fluree-Policy-Trace: true" \
  -d '{...}'
```

### Get Query Plan

```bash
curl -X POST http://localhost:8090/v1/fluree/explain \
  -d '{...}'
```

## Getting Help

### Diagnostic Information to Collect

When reporting issues, include:

1. **Server version:**
   ```bash
   curl http://localhost:8090/version
   ```

2. **Configuration:**
   ```bash
   ./fluree-db-server --help
   # Include relevant config values
   ```

3. **Error messages:**
   - Complete error response
   - Relevant log entries

4. **Reproduction steps:**
   - Minimal example to reproduce
   - Sample data if needed

5. **Environment:**
   - OS and version
   - Storage mode
   - Available resources (RAM, disk)

### Log Collection

Collect diagnostic logs:

```bash
# Last 1000 lines
tail -n 1000 /var/log/fluree/server.log > fluree-diagnostic.log

# Specific time range
grep "2024-01-22T10:" /var/log/fluree/server.log > issue-logs.log
```

## Best Practices

### 1. Check Logs First

Always check logs before deeper investigation:

```bash
tail -f /var/log/fluree/server.log
```

### 2. Start with Health Check

```bash
curl http://localhost:8090/health
```

### 3. Isolate the Issue

Test components independently:
- Can you connect?
- Can you query?
- Can you transact?

### 4. Use Debug Mode Carefully

Debug logging is verbose:
- Use temporarily
- Disable in production
- May impact performance

### 5. Test on Development

Reproduce on development environment before investigating production.

### 6. Keep Logs

Retain logs for historical analysis:

```bash
# Logrotate config
/var/log/fluree/*.log {
    daily
    rotate 30
    compress
}
```

## Related Documentation

- [Common Errors](common-errors.md) - Error reference
- [Debugging Queries](debugging-queries.md) - Query debugging
- [API Errors](../api/errors.md) - HTTP error codes
- [Operations](../operations/README.md) - Operational guides
