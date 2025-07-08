# Fluree S3 Storage Configuration Guide

## Overview

Fluree DB supports Amazon S3 as a storage backend for ledger data, commits, and indexes. This guide covers configuration, usage, and production deployment of S3 storage.

## Configuration

### Basic S3 Configuration

```json
{
  "@context": {
    "@vocab": "https://ns.flur.ee/system#"
  },
  "@graph": [
    {
      "@id": "s3Storage",
      "@type": "Storage",
      "s3Bucket": "your-bucket-name",
      "s3Endpoint": "https://s3.us-east-1.amazonaws.com",
      "s3Prefix": "ledgers",
      "addressIdentifier": "production-s3"
    }
  ]
}
```

### Configuration Options

| Field | Required | Description | Example |
|-------|----------|-------------|---------|
| `s3Bucket` | Yes | S3 bucket name | `"fluree-production-data"` |
| `s3Endpoint` | **Yes** | S3 endpoint URL | `"https://s3.us-east-1.amazonaws.com"` |
| `s3Prefix` | No | Key prefix for all objects | `"ledgers"` |
| `addressIdentifier` | No | Unique identifier for this storage instance | `"prod-s3"` |

> **Note**: As of the latest version, `s3Endpoint` is a required parameter. The API will throw a validation error if not provided.

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
      "@id": "connection",
      "@type": "Connection",
      "parallelism": 4,
      "cacheMaxMb": 1000,
      "commitStorage": {"@id": "s3Storage"},
      "indexStorage": {"@id": "s3Storage"},
      "primaryPublisher": {
        "@type": "Publisher",
        "storage": {"@id": "s3Storage"}
      }
    }
  ]
}
```

## AWS Credentials

### Authentication Methods
Fluree uses the AWS SDK's default credential chain:

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

3. **IAM Roles** (when running on EC2)
   - Automatically uses instance profile credentials

4. **AWS CLI Configuration**
   ```bash
   aws configure
   ```

### Required S3 Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket-name",
        "arn:aws:s3:::your-bucket-name/*"
      ]
    }
  ]
}
```

## API Reference

### Using connect-s3 (Recommended)

```clojure
(require '[fluree.db.api :as fluree])

;; S3 connection for AWS
(def aws-conn
  @(fluree/connect-s3
    {:s3-bucket "my-fluree-bucket"
     :s3-endpoint "https://s3.us-east-1.amazonaws.com"
     :s3-prefix "ledgers/"
     :parallelism 4
     :cache-max-mb 1000}))

;; S3 connection for LocalStack (testing)
(def localstack-conn
  @(fluree/connect-s3
    {:s3-bucket "fluree-test"
     :s3-endpoint "http://localhost:4566"
     :s3-prefix "test/"
     :parallelism 2
     :cache-max-mb 500}))
```

### S3Storage Constructor
```clojure
(s3/open identifier bucket prefix endpoint-override)
```

### Storage Protocols
- `(storage/write-bytes store path data)` - Write raw bytes
- `(storage/read-bytes store path)` - Read raw bytes  
- `(storage/-content-write-bytes store dir data)` - Content-addressed write
- `(storage/-read-json store address keywordize?)` - Read JSON document
- `(storage/location store)` - Get storage location URI
- `(storage/identifiers store)` - Get storage identifier set

### Configuration Schema
See `fluree.db.connection.vocab` for complete configuration vocabulary definitions.

## Production Considerations

### Performance
- Use appropriate AWS instance types with sufficient network bandwidth
- Consider S3 Transfer Acceleration for global deployments
- Monitor S3 request costs and optimize access patterns
- Use S3 Intelligent Tiering for cost optimization

### Security
- Use IAM roles instead of access keys when possible
- Enable S3 bucket encryption
- Implement bucket policies for access control
- Enable S3 access logging for audit trails
- Use VPC endpoints for private S3 access

### Monitoring
- Set up CloudWatch alarms for S3 operations
- Monitor S3 costs and usage patterns
- Track Fluree application metrics
- Implement health checks for S3 connectivity

### Backup and Recovery
- Enable S3 versioning for data protection
- Set up cross-region replication if needed
- Implement regular backup verification
- Document recovery procedures

### Deployment Checklist
- [ ] IAM roles and policies configured
- [ ] S3 bucket created with proper permissions
- [ ] VPC endpoints configured (if needed)
- [ ] CloudWatch monitoring enabled
- [ ] Backup and recovery procedures documented
- [ ] Performance tuning applied
- [ ] Cost optimization enabled (lifecycle policies)

## Migration from Other Storage

### From File Storage to S3
1. Export existing ledger data
2. Configure S3 storage
3. Import data to new S3-backed system
4. Verify data integrity
5. Update application configuration

### Performance Comparison
- File storage: Lower latency, higher IOPS
- S3 storage: Higher durability, infinite scalability
- Choose based on performance vs. durability requirements

## Troubleshooting

### Common Issues

#### 1. Authentication Failures
**Symptoms**: Access denied errors, credential errors
**Solutions**:
- Verify AWS credentials are properly configured
- Check IAM permissions for the bucket
- Ensure AWS region is correctly set
- Test credentials with AWS CLI: `aws s3 ls s3://your-bucket`

#### 2. Bucket Access Issues
**Symptoms**: NoSuchBucket, AccessDenied errors
**Solutions**:
- Verify bucket name is correct and exists
- Check bucket permissions and policies
- Ensure bucket is in the correct region
- Verify network connectivity to S3

#### 3. Network/Connectivity Issues
**Symptoms**: Timeout errors, connection refused
**Solutions**:
- Check firewall rules and security groups
- Verify S3 endpoint configuration
- Test network connectivity: `ping s3.amazonaws.com`
- For custom endpoints, verify service is running

#### 4. Configuration Issues
**Symptoms**: ClassNotFound, protocol errors
**Solutions**:
- Verify S3 dependencies are included in classpath
- Check configuration JSON-LD syntax
- Ensure all required configuration fields are present
- Validate configuration parsing with test utilities

#### 5. Index Loading Issues
**Symptoms**: "Error resolving index node" when loading ledgers from cold start
**Solutions**:
- This was a known bug fixed in recent versions
- Ensure you're using the latest version with the index loading fix
- The issue was caused by improper address resolution in S3Store's `-read-json` method
- If still experiencing issues, verify fluree addresses are properly formatted

#### 6. connect-s3 API Validation Errors
**Symptoms**: "S3 bucket name is required" or "S3 endpoint is required" errors
**Solutions**:
- Ensure both `s3-bucket` and `s3-endpoint` parameters are provided
- `s3-endpoint` is now a required parameter (changed from optional)
- Example: `{:s3-bucket "my-bucket" :s3-endpoint "http://localhost:4566"}`
- For AWS: use `"https://s3.us-east-1.amazonaws.com"` format
- For LocalStack: use `"http://localhost:4566"` format

---

## Implementation Details

### S3 Storage Implementation

#### Features
- **Content-Addressed Storage**: Automatic SHA-256 hashing and addressing
- **JSON Archive Support**: Direct JSON read/write with compression
- **Byte Store**: Raw byte storage and retrieval
- **AWS SDK Integration**: Uses Cognitect AWS SDK for reliable S3 operations
- **Async Operations**: All S3 operations are asynchronous using core.async

#### Storage Protocols
The S3 implementation satisfies all required storage protocols:
- `storage/Addressable` - Provides fluree address generation
- `storage/Identifiable` - Storage identifier management
- `storage/JsonArchive` - JSON document storage
- `storage/ContentAddressedStore` - Hash-based content storage
- `storage/ByteStore` - Raw byte operations

#### S3 File Structure
- Ledger metadata: `<prefix>/<ledger-name>.json`
- Commit files: `<prefix>/<ledger-name>/commit/<hash>.json`
- Index files: `<prefix>/<ledger-name>/index/{root,post,spot,tspo,opst}/<hash>.json`

## Testing and Development

### Test Organization

The S3 storage tests are organized into three categories:

1. **Unit Tests** (`s3_unit_test.clj`)
   - Pure unit tests without external dependencies
   - Protocol compliance verification
   - Basic S3Store creation and configuration
   - API parameter validation
   - No LocalStack or real S3 required

2. **Integration Tests** (`s3_test.clj`)
   - Basic S3 integration with LocalStack
   - Real S3 read/write operations
   - End-to-end ledger workflows
   - Requires LocalStack running

3. **Indexing Tests** (`s3_indexing_test.clj`)
   - Comprehensive indexing functionality
   - Index creation and storage validation
   - Cold start index loading
   - Query functionality with indexes
   - Requires LocalStack running

### Test Environment Setup

#### Option 1: LocalStack (Recommended for Development)

1. **Start LocalStack**
   ```bash
   docker run -p 4566:4566 localstack/localstack
   ```

2. **Set Environment Variables**
   ```bash
   export S3_TEST_ENDPOINT=http://localhost:4566
   export S3_TEST_BUCKET=fluree-test-bucket
   export AWS_ACCESS_KEY_ID=test
   export AWS_SECRET_ACCESS_KEY=test
   export AWS_REGION=us-east-1
   ```

3. **Run Tests**
   ```bash
   # Unit tests (no LocalStack required)
   clojure -M:dev:cljtest -e "(require 'fluree.db.storage.s3-unit-test) (clojure.test/run-tests 'fluree.db.storage.s3-unit-test)"
   
   # Integration tests (requires LocalStack)
   clojure -M:dev:cljtest -e "(require 'fluree.db.storage.s3-test) (clojure.test/run-tests 'fluree.db.storage.s3-test)"
   
   # Indexing tests (requires LocalStack)
   clojure -M:dev:cljtest -e "(require 'fluree.db.storage.s3-indexing-test) (clojure.test/run-tests 'fluree.db.storage.s3-indexing-test)"
   ```

#### Option 2: Real AWS S3

1. **Set AWS Credentials** (see Authentication Methods above)

2. **Create Test Bucket**
   ```bash
   aws s3 mb s3://fluree-test-bucket
   ```

3. **Set Environment Variables**
   ```bash
   export S3_TEST_BUCKET=fluree-test-bucket
   export S3_TEST_PREFIX=test-data
   # Don't set S3_TEST_ENDPOINT for real AWS
   ```

4. **Run Integration Tests**
   ```bash
   # Integration tests
   clojure -M:dev:cljtest -e "(require 'fluree.db.storage.s3-test) (clojure.test/run-tests 'fluree.db.storage.s3-test)"
   
   # Indexing tests
   clojure -M:dev:cljtest -e "(require 'fluree.db.storage.s3-indexing-test) (clojure.test/run-tests 'fluree.db.storage.s3-indexing-test)"
   ```

### Running Tests

```bash
# All S3 tests
make test  # Runs all tests including S3

# Unit tests only (no external dependencies)
clojure -M:dev:cljtest -e "(require 'fluree.db.storage.s3-unit-test) (clojure.test/run-tests 'fluree.db.storage.s3-unit-test)"

# Integration tests only (requires LocalStack)
clojure -M:dev:cljtest -e "(require 'fluree.db.storage.s3-test) (clojure.test/run-tests 'fluree.db.storage.s3-test)"

# Indexing tests only (requires LocalStack)
clojure -M:dev:cljtest -e "(require 'fluree.db.storage.s3-indexing-test) (clojure.test/run-tests 'fluree.db.storage.s3-indexing-test)"
```

### Debugging Tools

#### Enable S3 Debug Logging
```xml
<!-- Add to logback.xml -->
<logger name="fluree.db.storage.s3" level="DEBUG"/>
<logger name="cognitect.aws" level="DEBUG"/>
<logger name="fluree.db.api" level="INFO"/>
<logger name="f.db.flake.index.novelty" level="INFO"/>
<logger name="f.db.nameservice.storage" level="INFO"/>
```

> **Note**: S3 tests now use proper logging via `fluree.db.util.log` instead of `println` for better control over log levels.

#### Manual S3 Operations
```clojure
(require '[fluree.db.storage.s3 :as s3])
(require '[fluree.db.storage :as storage])
(require '[clojure.core.async :refer [<!]])

;; Create store with endpoint
(def store (s3/open "test" "your-bucket" "test-prefix" "https://s3.us-east-1.amazonaws.com"))

;; Test write
(def result (<! (storage/write-bytes store "test.txt" "Hello, S3!")))

;; Test read
(def content (<! (storage/read-bytes store "test.txt")))
```

#### Test Utilities

The `fluree.db.test-utils` namespace provides helpful utilities for S3 testing:

```clojure
(require '[fluree.db.test-utils :as test-utils])
(require '[fluree.db.util.log :as log])

;; Check LocalStack availability
(test-utils/s3-available?) ; Returns true if LocalStack S3 is running at localhost:4566

;; Example usage in tests
(deftest s3-integration-test
  (testing "S3 operations"
    (if-not (test-utils/s3-available?)
      (log/info "⏭️  Skipping S3 test - LocalStack not available")
      (do-s3-integration-test))))
```
