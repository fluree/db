# DynamoDB Nameservice Guide

## Overview

Fluree supports Amazon DynamoDB as a nameservice backend for storing ledger and graph source metadata. The DynamoDB nameservice provides:

- **Item-per-concern independence**: Each concern (commit head, index, status, config) is a separate DynamoDB item, eliminating physical write contention between transactors and indexers
- **Atomic conditional updates**: Reduced logical contention via conditional expressions
- **Strong consistency reads**: Always see the latest data
- **High availability**: DynamoDB's built-in redundancy and durability
- **Unified ledger + graph source support**: Both ledgers and graph sources (BM25, Vector, Iceberg, etc.) share the same table with a composite key

### Why DynamoDB for Nameservice?

The nameservice stores metadata about ledgers and graph sources: commit IDs, index state, status, and configuration. In high-throughput scenarios, transactors and indexers may update this metadata concurrently.

DynamoDB solves this because:

1. **Item-per-concern layout**: Each concern (head, index, status, config) is a separate DynamoDB item under the same partition key, so writes to different concerns never contend at the physical level
2. **Conditional updates**: Each update only proceeds if the new watermark advances monotonically
3. **No read-modify-write cycles (for the write itself)**: Updates are atomic; callers should still expect occasional conditional-update conflicts under contention and retry where appropriate

### Graph Sources (non-ledger)

Graph sources (BM25, Vector, Iceberg, etc.) are stored in the same nameservice table as ledgers. Under the **graph-source-owned manifest** design, the nameservice does **not** store snapshot history for graph sources.

- For ledgers, `index_id` points to a ledger index root.
- For graph sources, `index_id` points to a **graph-source-owned root/manifest** in storage (opaque to nameservice).
- Snapshot history (if any) is stored in storage and managed by the graph source implementation.

This keeps DynamoDB schema stable: **no unbounded "snapshot history" list is stored in the DynamoDB item**.

## Table Setup

### Schema Overview

The table uses a **composite primary key** (`pk` + `sk`) with a Global Secondary Index (GSI) for listing by kind.

- **`pk`** (Partition Key, String): Alias in `name:branch` form (e.g., `mydb:main`)
- **`sk`** (Sort Key, String): Concern discriminator (`meta`, `head`, `index`, `config`, `status`)
- **GSI1** (`gsi1-kind`): Enables efficient listing of all ledgers or all graph sources

### AWS CLI

```bash
aws dynamodb create-table \
  --table-name fluree-nameservice \
  --attribute-definitions \
    AttributeName=pk,AttributeType=S \
    AttributeName=sk,AttributeType=S \
    AttributeName=kind,AttributeType=S \
  --key-schema \
    AttributeName=pk,KeyType=HASH \
    AttributeName=sk,KeyType=RANGE \
  --global-secondary-indexes '[
    {
      "IndexName": "gsi1-kind",
      "KeySchema": [
        {"AttributeName": "kind", "KeyType": "HASH"},
        {"AttributeName": "pk", "KeyType": "RANGE"}
      ],
      "Projection": {
        "ProjectionType": "INCLUDE",
        "NonKeyAttributes": ["name", "branch", "source_type", "dependencies", "retracted"]
      }
    }
  ]' \
  --billing-mode PAY_PER_REQUEST
```

### CloudFormation

```yaml
Resources:
  FlureeNameserviceTable:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: fluree-nameservice
      BillingMode: PAY_PER_REQUEST
      AttributeDefinitions:
        - AttributeName: pk
          AttributeType: S
        - AttributeName: sk
          AttributeType: S
        - AttributeName: kind
          AttributeType: S
      KeySchema:
        - AttributeName: pk
          KeyType: HASH
        - AttributeName: sk
          KeyType: RANGE
      GlobalSecondaryIndexes:
        - IndexName: gsi1-kind
          KeySchema:
            - AttributeName: kind
              KeyType: HASH
            - AttributeName: pk
              KeyType: RANGE
          Projection:
            ProjectionType: INCLUDE
            NonKeyAttributes:
              - name
              - branch
              - source_type
              - dependencies
              - retracted
      PointInTimeRecoverySpecification:
        PointInTimeRecoveryEnabled: true
      Tags:
        - Key: Application
          Value: Fluree
```

### Terraform

```hcl
resource "aws_dynamodb_table" "fluree_nameservice" {
  name         = "fluree-nameservice"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  attribute {
    name = "kind"
    type = "S"
  }

  global_secondary_index {
    name            = "gsi1-kind"
    hash_key        = "kind"
    range_key       = "pk"
    projection_type = "INCLUDE"
    non_key_attributes = [
      "name",
      "branch",
      "source_type",
      "dependencies",
      "retracted",
    ]
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = {
    Application = "Fluree"
  }
}
```

### Programmatic Table Creation

Fluree's `DynamoDbNameService` also provides an `ensure_table()` method that creates the table with the correct schema if it doesn't already exist:

```rust
use fluree_db_storage_aws::dynamodb::DynamoDbNameService;

let ns = DynamoDbNameService::from_client(dynamodb_client, "fluree-nameservice".to_string());
ns.ensure_table().await?;
```

This is used by integration tests and can be used for bootstrapping development environments.

## Table Schema

### Primary Key

| Attribute | Type | Description |
|-----------|------|-------------|
| `pk` | String (Partition Key) | Alias in `name:branch` form (e.g., `mydb:main`) |
| `sk` | String (Sort Key) | Concern discriminator: `meta`, `head`, `index`, `config`, `status` |

### Items per Alias

Each ledger or graph source is represented as multiple items under the same `pk`:

**Ledger (5 items):**

| Sort Key (`sk`) | Description | Key Attributes |
|-----------------|-------------|----------------|
| `meta` | Identity and metadata | `kind`, `name`, `branch`, `retracted`, `schema` |
| `head` | Commit head pointer | `commit_id`, `commit_t` |
| `index` | Index head pointer | `index_id`, `index_t` |
| `config` | Ledger configuration | `default_context_id`, `config_v`, `config_meta` |
| `status` | Operational status | `status`, `status_v`, `status_meta` |

**Graph Source (4 items):**

| Sort Key (`sk`) | Description | Key Attributes |
|-----------------|-------------|----------------|
| `meta` | Identity and metadata | `kind`, `source_type`, `name`, `branch`, `dependencies`, `retracted`, `schema` |
| `config` | Source configuration | `config_json`, `config_v` |
| `index` | Index head pointer | `index_id`, `index_t` |
| `status` | Operational status | `status`, `status_v`, `status_meta` |

### Attribute Reference

All items share these common attributes:

| Attribute | Type | Description |
|-----------|------|-------------|
| `pk` | String | Record address (`name:branch`) |
| `sk` | String | Concern discriminator |
| `schema` | Number | Schema version (always `2`) |
| `updated_at_ms` | Number | Last update timestamp (epoch milliseconds) |

**`meta` item:**

| Attribute | Type | Description |
|-----------|------|-------------|
| `kind` | String | `ledger` or `graph_source` |
| `name` | String | Base name (reserved word — use `#name` in expressions) |
| `branch` | String | Branch name |
| `retracted` | Boolean | Soft-delete flag |
| `source_type` | String (graph source only) | Graph-source type (e.g., `f:Bm25Index`) |
| `dependencies` | List\<String\> (graph source only) | Dependent ledger IDs |

**`head` item (ledgers only):**

| Attribute | Type | Description |
|-----------|------|-------------|
| `commit_id` | String \| null | Latest commit ContentId (CIDv1) |
| `commit_t` | Number | Commit watermark (`t`). `0` = unborn. |

**`index` item (ledgers + graph sources):**

| Attribute | Type | Description |
|-----------|------|-------------|
| `index_id` | String \| null | Latest index ContentId (CIDv1) |
| `index_t` | Number | Index watermark (`t`). `0` = unborn. |

**`config` item:**

| Attribute | Type | Description |
|-----------|------|-------------|
| `default_context_id` | String \| null | Default JSON-LD context ContentId (ledger) |
| `config_json` | String \| null | Opaque JSON config string (graph source) |
| `config_v` | Number | Config version watermark |
| `config_meta` | Map \| null | Extensible config metadata (ledger) |

**`status` item:**

| Attribute | Type | Description |
|-----------|------|-------------|
| `status` | String | Current state (reserved word — use `#st` in expressions) |
| `status_v` | Number | Status version watermark |
| `status_meta` | Map \| null | Extensible status metadata |

### GSI1: `gsi1-kind`

Enables listing all entities of a given kind (ledger or graph source).

| GSI Attribute | Source Attribute | Description |
|---------------|------------------|-------------|
| Partition Key | `kind` | `ledger` or `graph_source` |
| Sort Key | `pk` | Record address |
| Projected | `name`, `branch`, `source_type`, `dependencies`, `retracted` | Meta fields for listing without additional reads |

Only `meta` items carry the `kind` attribute and project into the GSI.

### Initialization Semantics

**All concern items are created atomically at initialization time.** This is a key structural decision:

- `publish_ledger_init` creates all 5 items (`meta`, `head`, `index`, `config`, `status`) via `TransactWriteItems`
- `publish_graph_source` creates all 4 items (`meta`, `config`, `index`, `status`) via `TransactWriteItems`

Subsequent writes usually use `UpdateItem` operations (`compare_and_set_ref`, `publish_index`, `push_status`, `push_config`). The one exception is commit-head CAS on an unknown ledger ID with `expected=None`, where the backend bootstraps the ledger atomically via `TransactWriteItems`.

### How Updates Work

**Commit updates** (transactor):
```
UpdateItem Key: { pk: "mydb:main", sk: "head" }
UpdateExpression: SET commit_id = :cid, commit_t = :t, updated_at_ms = :now
ConditionExpression: attribute_exists(pk) AND commit_t < :t
```

**Index updates** (indexer):
```
UpdateItem Key: { pk: "mydb:main", sk: "index" }
UpdateExpression: SET index_id = :cid, index_t = :t, updated_at_ms = :now
ConditionExpression: attribute_exists(pk) AND index_t < :t
```

Since commit and index updates target different items (different `sk`), they never contend at the DynamoDB physical level.

**Status updates** (CAS):
```
UpdateItem Key: { pk: "mydb:main", sk: "status" }
UpdateExpression: SET #st = :new_state, status_v = :new_v, updated_at_ms = :now
ConditionExpression: status_v = :expected_v AND #st = :expected_state
```

**Config updates** (CAS):
```
UpdateItem Key: { pk: "mydb:main", sk: "config" }
UpdateExpression: SET default_context_id = :ctx, config_v = :new_v, updated_at_ms = :now
ConditionExpression: config_v = :expected_v
```

**RefPublisher updates** (compare-and-set refs):

- `CommitHead` uses strict monotonic guard: `new.t > current.t`
- `IndexHead` allows same-watermark overwrite: `new.t >= current.t` (reindex at same `t`)

When a caller attempts `compare_and_set_ref(expected=None)` on an unknown ledger ID, the DynamoDB backend bootstraps the ledger by creating all 5 ledger concern items via `TransactWriteItems` and pre-setting the target ref to the requested value.

**Retract:**
```
UpdateItem Key: { pk: "mydb:main", sk: "meta" }
UpdateExpression: SET retracted = :true, updated_at_ms = :now
```

### DynamoDB Reserved Words

The attributes `name` and `status` are DynamoDB reserved words. All expressions (reads, updates, projections) must use `ExpressionAttributeNames`:

```
ExpressionAttributeNames: { "#name": "name", "#st": "status" }
```

## Trait Implementations

The DynamoDB nameservice implements all seven nameservice traits:

| Trait | Description |
|-------|-------------|
| `NameService` | Lookup, ledger ID resolution, list all records |
| `Publisher` | Initialize ledgers, publish indexes, retract |
| `AdminPublisher` | Admin index publishing (allows equal-t overwrites) |
| `RefPublisher` | Compare-and-set on commit/index refs |
| `StatusPublisher` | CAS-based status updates |
| `ConfigPublisher` | CAS-based config updates (ledgers only) |
| `GraphSourceLookup` | Read-only graph source discovery: lookup, list all records |
| `GraphSourcePublisher` | Graph source lifecycle (extends `GraphSourceLookup`): create, index, retract |

**Note:** `ConfigPublisher` is scoped to ledgers only. Graph source configuration is managed through `GraphSourcePublisher`, which stores config as an opaque JSON string (`config_json`). `GraphSourceLookup` is a supertrait of `NameService`, so all nameservice implementations automatically support graph source discovery. `GraphSourcePublisher` adds write operations and is required only by APIs that create or drop graph sources.

## Configuration

### JSON-LD Connection Configuration

```json
{
  "@context": {
    "@vocab": "https://ns.flur.ee/system#"
  },
  "@graph": [
    {
      "@id": "s3Storage",
      "@type": "Storage",
      "s3Bucket": "fluree-production-data",
      "s3Endpoint": "https://s3.us-east-1.amazonaws.com",
      "s3Prefix": "ledgers",
      "addressIdentifier": "prod-s3"
    },
    {
      "@id": "dynamodbNs",
      "@type": "Publisher",
      "dynamodbTable": "fluree-nameservice",
      "dynamodbRegion": "us-east-1"
    },
    {
      "@id": "connection",
      "@type": "Connection",
      "parallelism": 4,
      "cacheMaxMb": 1000,
      "commitStorage": {"@id": "s3Storage"},
      "indexStorage": {"@id": "s3Storage"},
      "primaryPublisher": {"@id": "dynamodbNs"}
    }
  ]
}
```

### Configuration Options

| Field | Required | Description | Default |
|-------|----------|-------------|---------|
| `dynamodbTable` | Yes | DynamoDB table name | - |
| `dynamodbRegion` | No | AWS region | `us-east-1` |
| `dynamodbEndpoint` | No | Custom endpoint URL (for LocalStack) | AWS default |
| `dynamodbTimeoutMs` | No | Request timeout in milliseconds | `5000` |

## AWS Credentials

### Authentication Methods

The DynamoDB nameservice uses the standard AWS SDK credential chain:

1. **Environment Variables**
   ```bash
   export AWS_ACCESS_KEY_ID=your_access_key
   export AWS_SECRET_ACCESS_KEY=your_secret_key
   export AWS_REGION=us-east-1
   ```

2. **AWS Credentials File** (`~/.aws/credentials`)
   ```ini
   [default]
   aws_access_key_id = your_access_key
   aws_secret_access_key = your_secret_key
   region = us-east-1
   ```

3. **IAM Roles** (when running on EC2/ECS/Lambda)
   - Automatically uses instance/task role credentials

4. **Session Tokens** (for temporary credentials)
   ```bash
   export AWS_SESSION_TOKEN=your_session_token
   ```

### Required IAM Permissions

Full permissions (recommended):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dynamodb:Query",
        "dynamodb:BatchGetItem"
      ],
      "Resource": [
        "arn:aws:dynamodb:*:*:table/fluree-nameservice",
        "arn:aws:dynamodb:*:*:table/fluree-nameservice/index/gsi1-kind"
      ]
    }
  ]
}
```

If you also use `ensure_table()` for automated table creation (development/testing):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dynamodb:Query",
        "dynamodb:BatchGetItem",
        "dynamodb:CreateTable",
        "dynamodb:DescribeTable"
      ],
      "Resource": [
        "arn:aws:dynamodb:*:*:table/fluree-nameservice",
        "arn:aws:dynamodb:*:*:table/fluree-nameservice/index/gsi1-kind"
      ]
    }
  ]
}
```

Minimal permissions (if not using `all_records`, `all_graph_source_records`, or graph sources):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dynamodb:Query"
      ],
      "Resource": "arn:aws:dynamodb:*:*:table/fluree-nameservice"
    }
  ]
}
```

## Local Development

### Using LocalStack

1. **Start LocalStack**
   ```bash
   docker run -d --name localstack \
     -p 4566:4566 \
     -e SERVICES=dynamodb \
     localstack/localstack
   ```

2. **Create Test Table**
   ```bash
   AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
   aws --endpoint-url=http://localhost:4566 dynamodb create-table \
     --table-name fluree-nameservice \
     --attribute-definitions \
       AttributeName=pk,AttributeType=S \
       AttributeName=sk,AttributeType=S \
       AttributeName=kind,AttributeType=S \
     --key-schema \
       AttributeName=pk,KeyType=HASH \
       AttributeName=sk,KeyType=RANGE \
     --global-secondary-indexes '[
       {
         "IndexName": "gsi1-kind",
         "KeySchema": [
           {"AttributeName": "kind", "KeyType": "HASH"},
           {"AttributeName": "pk", "KeyType": "RANGE"}
         ],
         "Projection": {
           "ProjectionType": "INCLUDE",
           "NonKeyAttributes": ["name", "branch", "source_type", "dependencies", "retracted"]
         }
       }
     ]' \
     --billing-mode PAY_PER_REQUEST
   ```

3. **Configure Fluree**
   ```json
   {
     "@id": "dynamodbNs",
     "@type": "Publisher",
     "dynamodbTable": "fluree-nameservice",
     "dynamodbEndpoint": "http://localhost:4566",
     "dynamodbRegion": "us-east-1"
   }
   ```

4. **Set Environment Variables**
   ```bash
   export AWS_ACCESS_KEY_ID=test
   export AWS_SECRET_ACCESS_KEY=test
   ```

### Using DynamoDB Local

1. **Start DynamoDB Local**
   ```bash
   docker run -d --name dynamodb-local \
     -p 8000:8000 \
     amazon/dynamodb-local
   ```

2. **Create Test Table** (same command as LocalStack, change `--endpoint-url` to `http://localhost:8000`)

## Production Considerations

### Performance

- DynamoDB provides single-digit millisecond latency
- The item-per-concern layout eliminates physical contention between transactors and indexers
- Use on-demand (PAY_PER_REQUEST) billing for variable workloads
- Consider provisioned capacity for predictable high-throughput scenarios
- Enable DynamoDB Accelerator (DAX) if sub-millisecond reads are needed

### Security

- Use IAM roles instead of access keys when possible
- Enable encryption at rest (default for new tables)
- Use VPC endpoints for private DynamoDB access
- Enable CloudTrail for audit logging

### Monitoring

Set up CloudWatch alarms for:

- `ConditionalCheckFailedRequests` - indicates contention (usually normal)
- `ThrottledRequests` - capacity issues
- `SystemErrors` - service issues
- `SuccessfulRequestLatency` - track latency

### Backup and Recovery

```bash
# Enable Point-in-Time Recovery
aws dynamodb update-continuous-backups \
  --table-name fluree-nameservice \
  --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true

# Create on-demand backup
aws dynamodb create-backup \
  --table-name fluree-nameservice \
  --backup-name fluree-ns-backup-$(date +%Y%m%d)
```

### Cost Optimization

- On-demand pricing is cost-effective for variable workloads
- Table data is small (5 items per ledger, 4 per graph source), so costs are minimal
- Typical costs: $1-10/month for small deployments
- GSI storage adds minimal cost (only meta items project into it)

## Troubleshooting

### Authentication Failures

**Symptoms**: Access denied, credential errors

**Solutions**:
- Verify AWS credentials are configured
- Check IAM permissions for the table and GSI
- Test with AWS CLI:
  ```bash
  aws dynamodb describe-table --table-name fluree-nameservice
  ```

### Table Not Found

**Symptoms**: ResourceNotFoundException

**Solutions**:
- Verify table name is correct
- Check table is in the correct region
- Ensure table has finished creating (including GSI)

### Timeout Errors

**Symptoms**: Request timeout

**Solutions**:
- Increase `dynamodbTimeoutMs` configuration
- Check network connectivity to DynamoDB
- Verify endpoint URL is correct (especially for LocalStack)

### Conditional Check Failures

**Symptoms**: High rate of ConditionalCheckFailedException in logs

**Note**: This is usually normal and indicates the system is working correctly. The conditional check prevents overwriting newer data with older data. `publish_index` stale writes are silently ignored (the newer value is preserved). CAS operations (`compare_and_set_ref`, `push_status`, `push_config`) return the current value so the caller can retry or report a conflict.

### Unprocessed Keys (BatchGetItem)

**Symptoms**: Listing graph sources intermittently returns fewer results under load, or logs show throttling.

**Cause**: DynamoDB may return `UnprocessedKeys` in `BatchGetItem` responses under throttling.

**Behavior**: Fluree retries `UnprocessedKeys` with exponential backoff (bounded retries). If retries are exhausted, it returns an error rather than silently dropping items.

### Uninitialized Alias Errors

**Symptoms**: Publish operations fail with "not found" or storage errors

**Cause**: Attempting to `publish_index` or other non-bootstrap writes on a ledger ID that was never initialized with `publish_ledger_init`.

**Solution**: Ensure ledger initialization happens before index/status/config writes. Normal Fluree transaction commit-head publication uses `RefPublisher` CAS and can bootstrap an unknown ledger ID when `expected=None`.

## Related Documentation

- [Storage Modes](storage.md) - Overview of all storage options
- [Configuration](configuration.md) - Full configuration reference
- [Nameservice Schema v2 Design](../design/nameservice-schema-v2.md) - Schema design details
- [Ledgers and the Nameservice](../concepts/ledgers-and-nameservice.md) - Conceptual overview
