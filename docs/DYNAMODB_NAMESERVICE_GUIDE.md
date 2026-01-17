# Fluree DynamoDB Nameservice Configuration Guide

## Overview

Fluree supports Amazon DynamoDB as a nameservice backend for storing ledger
and virtual graph metadata. The DynamoDB nameservice provides:

- **Atomic conditional updates**: No contention between transactors and indexers
- **Strong consistency reads**: Always see the latest data
- **High availability**: DynamoDB's built-in redundancy and durability
- **Scalability**: Handles high throughput without coordination

### Why DynamoDB for Nameservice?

The nameservice stores metadata about ledgers (commit addresses, t-values, index
addresses) and virtual graphs (VG configuration, dependencies). In high-throughput
scenarios, transactors and indexers may update ledger metadata concurrently,
leading to contention with file-based or S3 nameservices.

DynamoDB solves this because:
1. **Separate attributes**: Commit data and index data are stored as separate
   DynamoDB attributes
2. **Conditional updates**: Each update only proceeds if the new t-value is
   greater than the existing one
3. **No read-modify-write cycles**: Updates are atomic, eliminating race
   conditions

## Configuration

### Basic DynamoDB Nameservice Configuration

```json
{
  "@context": {
    "@vocab": "https://ns.flur.ee/system#"
  },
  "@graph": [
    {
      "@id": "dynamodbNs",
      "@type": "Publisher",
      "dynamodbTable": "fluree-nameservice",
      "dynamodbRegion": "us-east-1"
    }
  ]
}
```

### Configuration Options

| Field | Required | Description | Default |
|-------|----------|-------------|---------|
| `dynamodbTable` | Yes | DynamoDB table name | - |
| `dynamodbRegion` | No | AWS region | `us-east-1` |
| `dynamodbEndpoint` | No | Custom endpoint URL (for local dev) | AWS default |
| `dynamodbTimeoutMs` | No | Request timeout in milliseconds | `5000` |

### Complete System Configuration

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

## DynamoDB Table Schema

### Table Creation

Create the DynamoDB table with the following schema:

**AWS CLI:**
```bash
aws dynamodb create-table \
  --table-name fluree-nameservice \
  --attribute-definitions AttributeName=ledger_alias,AttributeType=S \
  --key-schema AttributeName=ledger_alias,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST
```

**CloudFormation:**
```yaml
Resources:
  FlureeNameserviceTable:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: fluree-nameservice
      BillingMode: PAY_PER_REQUEST
      AttributeDefinitions:
        - AttributeName: ledger_alias
          AttributeType: S
      KeySchema:
        - AttributeName: ledger_alias
          KeyType: HASH
      PointInTimeRecoverySpecification:
        PointInTimeRecoveryEnabled: true
```

### Table Attributes

#### Ledger Records

| Attribute | Type | Description |
|-----------|------|-------------|
| `ledger_alias` | String (PK) | Ledger identifier, e.g., `my-ledger:main` |
| `commit_address` | String | Latest commit address |
| `commit_t` | Number | Commit t-value |
| `index_address` | String | Latest index address |
| `index_t` | Number | Index t-value |
| `ledger_name` | String | Ledger name without branch |
| `branch` | String | Branch name |
| `status` | String | Ledger status |

#### Virtual Graph Records

Virtual graphs (VGs) are stored in the same table as ledgers, distinguished by the
`record_type` attribute. No schema changes are required—DynamoDB is schema-less
for non-key attributes.

| Attribute | Type | Description |
|-----------|------|-------------|
| `ledger_alias` | String (PK) | VG identifier, e.g., `my-iceberg-vg:main` |
| `ledger_name` | String | VG name without branch |
| `branch` | String | Branch name |
| `status` | String | VG status (e.g., `ready`) |
| `record_type` | String | `"vg"` to distinguish from ledger records |
| `vg_type` | String | VG type IRI, e.g., `"f:IcebergVirtualGraph"` |
| `vg_config` | String | JSON-stringified VG configuration |
| `dependencies` | List\<String\> | List of dependent ledger aliases |

**Note**: The `record_type` attribute is only present on VG records. Ledger records
do not have this attribute, so absence of `record_type` indicates a ledger record.

### How Updates Work

**Commit updates** (transactor):
```
UpdateExpression: SET commit_address = :addr, commit_t = :t
ConditionExpression: attribute_not_exists(commit_t) OR commit_t < :t
```

**Index updates** (indexer):
```
UpdateExpression: SET index_address = :addr, index_t = :t
ConditionExpression: attribute_not_exists(index_t) OR index_t < :t
```

Since commit and index updates modify different attributes, they never conflict!

## AWS Credentials

### Authentication Methods

The DynamoDB nameservice uses the same credential chain as S3:

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

### Required DynamoDB Permissions

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
        "dynamodb:DeleteItem",
        "dynamodb:Scan"
      ],
      "Resource": "arn:aws:dynamodb:*:*:table/fluree-nameservice"
    }
  ]
}
```

**Minimal permissions** (if not using `all-records`):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem",
        "dynamodb:UpdateItem",
        "dynamodb:DeleteItem"
      ],
      "Resource": "arn:aws:dynamodb:*:*:table/fluree-nameservice"
    }
  ]
}
```

## Implementation Details

### HTTP-Based API Calls

The DynamoDB nameservice uses **direct HTTP calls with AWS Signature V4**
signing, the same approach used by the S3 storage implementation. This means:

- No AWS SDK dependency required
- Lightweight and consistent with existing Fluree patterns
- Uses `fluree.db.util.xhttp` for HTTP requests
- Full control over request/response handling

### Conditional Updates

The implementation uses DynamoDB's conditional expressions to ensure:

1. **Monotonic updates**: A new value is only written if `new_t > existing_t`
2. **No lost updates**: Conditional check prevents overwriting newer data
3. **Graceful conflicts**: `ConditionalCheckFailedException` is handled silently
   (logged at debug level) since it means the data is already newer

### Strong Consistency

All reads use `ConsistentRead: true` to ensure you always see the latest data.
This is important for nameservice lookups where stale data could cause issues.

## Testing and Development

### Local Development with LocalStack

1. **Start LocalStack**
   ```bash
   docker run -p 4566:4566 localstack/localstack
   ```

2. **Create Test Table**
   ```bash
   AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
   aws --endpoint-url=http://localhost:4566 dynamodb create-table \
     --table-name fluree-nameservice \
     --attribute-definitions AttributeName=ledger_alias,AttributeType=S \
     --key-schema AttributeName=ledger_alias,KeyType=HASH \
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

### Manual Testing

```clojure
(require '[fluree.db.nameservice.dynamodb :as dynamodb])
(require '[clojure.core.async :refer [<!!]])

;; Create nameservice
(def ns (dynamodb/start {:table-name "fluree-nameservice"
                         :endpoint "http://localhost:4566"
                         :region "us-east-1"}))

;; Test publish-commit
(<!! (fluree.db.nameservice/publish-commit ns "my-ledger:main" "fluree:commit:abc123" 1))

;; Test publish-index
(<!! (fluree.db.nameservice/publish-index ns "my-ledger:main" "fluree:index:def456" 1))

;; Test lookup
(<!! (fluree.db.nameservice/lookup ns "my-ledger:main"))

;; Test publish-vg (virtual graph)
(<!! (fluree.db.nameservice/publish-vg ns
       {:vg-name "my-iceberg-vg:main"
        :vg-type "f:IcebergVirtualGraph"
        :config {:warehouse-path "/data/warehouse"
                 :mapping "...r2rml..."}
        :dependencies ["source-ledger:main"]}))

;; Lookup returns VG record with :vg-type, :vg-config, :dependencies
(<!! (fluree.db.nameservice/lookup ns "my-iceberg-vg:main"))
```

## Production Considerations

### Performance

- DynamoDB provides single-digit millisecond latency
- Use on-demand (PAY_PER_REQUEST) billing for variable workloads
- Consider provisioned capacity for predictable high-throughput scenarios
- Enable DynamoDB Accelerator (DAX) if sub-millisecond reads are needed

### Security

- Use IAM roles instead of access keys when possible
- Enable encryption at rest (default for new tables)
- Use VPC endpoints for private DynamoDB access
- Enable CloudTrail for audit logging

### Monitoring

- Set up CloudWatch alarms for:
  - `ConditionalCheckFailedRequests` (indicates contention)
  - `ThrottledRequests` (capacity issues)
  - `SystemErrors` (service issues)
- Monitor consumed capacity units
- Track latency metrics

### Backup and Recovery

- Enable Point-in-Time Recovery (PITR)
- Consider DynamoDB Streams for change data capture
- Regular backup verification

### Cost Optimization

- On-demand pricing is cost-effective for variable workloads
- Consider reserved capacity for steady-state workloads
- Table data is small (one item per ledger), so costs are minimal

## Troubleshooting

### Common Issues

#### 1. Authentication Failures
**Symptoms**: Access denied, credential errors
**Solutions**:
- Verify AWS credentials are configured
- Check IAM permissions for the table
- Test with AWS CLI: `aws dynamodb describe-table --table-name fluree-nameservice`

#### 2. Table Not Found
**Symptoms**: ResourceNotFoundException
**Solutions**:
- Verify table name is correct
- Check table is in the correct region
- Ensure table has finished creating

#### 3. Timeout Errors
**Symptoms**: Request timeout
**Solutions**:
- Increase `dynamodbTimeoutMs` configuration
- Check network connectivity to DynamoDB
- Verify endpoint URL is correct

#### 4. Conditional Check Failures
**Symptoms**: High rate of ConditionalCheckFailedException in logs
**Note**: This is usually normal and indicates the system is working correctly.
The conditional check prevents overwriting newer data with older data.

### Debug Logging

Enable debug logging for the DynamoDB nameservice:
```xml
<!-- Add to logback.xml -->
<logger name="fluree.db.nameservice.dynamodb" level="DEBUG"/>
```

## Migration

### From Storage Nameservice to DynamoDB

1. Create DynamoDB table
2. Configure DynamoDB nameservice as secondary publisher initially
3. Verify data is being written to DynamoDB
4. Switch to DynamoDB as primary publisher
5. Remove storage nameservice

### Data Migration Script

```clojure
;; Migrate existing nameservice records to DynamoDB
(require '[fluree.db.nameservice :as ns])
(require '[fluree.db.nameservice.storage :as storage-ns])
(require '[fluree.db.nameservice.dynamodb :as dynamodb-ns])
(require '[clojure.core.async :refer [<!!]])

(defn migrate-records [old-ns new-ns]
  (let [records (<!! (ns/all-records old-ns))]
    (doseq [record records]
      (let [alias (get record "@id")
            commit-addr (get-in record ["f:commit" "@id"])
            commit-t (get record "f:t")
            index-addr (get-in record ["f:index" "@id"])
            index-t (get-in record ["f:index" "f:t"])]
        (when commit-addr
          (<!! (ns/publish-commit new-ns alias commit-addr commit-t)))
        (when index-addr
          (<!! (ns/publish-index new-ns alias index-addr index-t)))))))
```
