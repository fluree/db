# File Storage Configuration Guide

This guide covers how to configure and use Fluree's local file storage backend, including the optional AES-256 encryption feature.

## Basic Configuration

### Simple File Storage

```clojure
(require '[fluree.db.api :as fluree])

;; Basic file storage with default settings
@(fluree/connect-file {})

;; Custom storage path
@(fluree/connect-file {:storage-path "./my-ledger-data"})
```

### Full Configuration Options

```clojure
@(fluree/connect-file {:storage-path "./data"        ; Directory for file storage
                       :parallelism 8                ; Number of parallel operations
                       :cache-max-mb 2000           ; Max memory cache in MB
                       :defaults {...}})            ; Default ledger options
```

## Encryption Configuration

Fluree supports optional AES-256 encryption for file storage, providing data-at-rest encryption for your ledger data.

### Basic Encryption Setup

```clojure
;; Enable encryption with a 32-byte key
@(fluree/connect-file {:storage-path "./secure-data"
                       :aes256-key "my-secret-32-byte-encryption-key!"})
```

### Environment Variable Configuration

For production environments, store the encryption key in environment variables:

```clojure
;; In your application
(def encryption-key (System/getenv "FLUREE_ENCRYPTION_KEY"))

@(fluree/connect-file {:storage-path "./data"
                       :aes256-key encryption-key})
```

```bash
# Set environment variable
export FLUREE_ENCRYPTION_KEY="my-secret-32-byte-encryption-key!"
```

### Advanced: JSON-LD Configuration with Environment Variables

For more complex configurations, you can use the low-level `connect` function with JSON-LD configuration:

```clojure
;; Using JSON-LD config with environment variable and fallback
@(fluree/connect
  {"@context" {"@base"  "https://ns.flur.ee/config/connection/"
               "@vocab" "https://ns.flur.ee/system#"}
   "@id"      "encrypted-file-connection"
   "@graph"   [{"@id"        "fileStorage"
                "@type"      "Storage"
                "filePath"   "data/encrypted"
                "AES256Key"  {"@type"      "ConfigurationValue"
                              "envVar"     "FLUREE_AES256_KEY"
                              "defaultVal" "default-key-for-testing"}}
               {"@id"              "connection"
                "@type"            "Connection"
                "parallelism"      4
                "cacheMaxMb"       1000
                "commitStorage"    {"@id" "fileStorage"}
                "indexStorage"     {"@id" "fileStorage"}
                "primaryPublisher" {"@type"   "Publisher"
                                    "storage" {"@id" "fileStorage"}}}]})
```

```bash
# Set environment variable
export FLUREE_AES256_KEY="my-secret-32-byte-encryption-key!"
```

## Security Best Practices

### Encryption Key Management

1. **Key Length**: Use exactly 32 bytes for optimal AES-256 security
2. **Key Generation**: Generate cryptographically secure random keys
3. **Key Storage**: Never store keys in source code or configuration files
4. **Key Rotation**: Implement a key rotation strategy for long-term security

### Example: Secure Key Generation

```clojure
;; Generate a secure 32-byte key (example only - use proper key management)
(defn generate-encryption-key []
  (let [secure-random (java.security.SecureRandom.)
        key-bytes (byte-array 32)]
    (.nextBytes secure-random key-bytes)
    (String. key-bytes "ISO-8859-1")))
```

### Production Deployment

```clojure
;; Production configuration example
(defn create-connection []
  (let [config {:storage-path (or (System/getenv "FLUREE_DATA_PATH") "./data")
                :parallelism (Integer/parseInt (or (System/getenv "FLUREE_PARALLELISM") "4"))
                :cache-max-mb (Integer/parseInt (or (System/getenv "FLUREE_CACHE_MB") "1000"))
                :aes256-key (System/getenv "FLUREE_ENCRYPTION_KEY")}]
    (fluree/connect-file config)))
```

## Performance Considerations

### Memory Usage

- **Cache Size**: Adjust `cache-max-mb` based on available memory
- **Parallelism**: Set `parallelism` to match your CPU cores for optimal performance

### Encryption Impact

- **CPU Usage**: Encryption adds ~10-20% CPU overhead
- **Storage**: Encrypted files are slightly larger due to padding
- **Performance**: Minimal impact on query performance due to efficient caching

## Migration and Compatibility

### Enabling Encryption on Existing Data

⚠️ **Important**: Encryption cannot be enabled on existing ledgers. To encrypt existing data:

1. Create a new encrypted connection
2. Export data from the unencrypted ledger
3. Import data into the new encrypted ledger

### Disabling Encryption

⚠️ **Important**: Once encryption is enabled, the data cannot be read without the key.

## Complete Working Example

Here's a full example showing how to set up encrypted storage and work with sensitive data:

```clojure
(require '[fluree.db.api :as fluree])

;; Connect with encryption
(def conn @(fluree/connect-file {:storage-path "data/encrypted"
                                 :aes256-key "my-secret-aes-256-key-32bytes!!"}))

;; Create a ledger and get initial database
(def ledger @(fluree/create conn "my-encrypted-ledger"))
(def db @(fluree/db ledger))

;; Insert some sensitive data
(def db-with-data 
  @(fluree/stage db 
    {"@context" {"ex" "http://example.org/"}
     "insert"   [{"@id"        "ex:user123"
                  "@type"      "ex:User"
                  "ex:name"    "John Doe"
                  "ex:email"   "john@example.com"
                  "ex:ssn"     "123-45-6789"}]}))

;; Commit the transaction
(def committed-db @(fluree/commit! ledger db-with-data))

;; Query the data - works transparently with encryption
(def results 
  @(fluree/query committed-db
    {"@context" {"ex" "http://example.org/"}
     "select"   {"?user" ["*"]}
     "where"    {"@id"   "?user"
                 "@type" "ex:User"}}))

;; The data files on disk are encrypted with AES-256
;; Without the correct key, the files cannot be read
(println "Query results from encrypted storage:")
(clojure.pprint/pprint results)
```

## Configuration Examples

### Development Environment

```clojure
;; Simple development setup
@(fluree/connect-file {:storage-path "./dev-data"})
```

### Testing Environment

```clojure
;; Testing with temporary encrypted storage
@(fluree/connect-file {:storage-path "./test-data"
                       :aes256-key "test-key-32-bytes-exactly!!!!!!!"})
```

### Production Environment

```clojure
;; Production with full security
@(fluree/connect-file {:storage-path "/var/lib/fluree/data"
                       :parallelism 8
                       :cache-max-mb 4000
                       :aes256-key (System/getenv "FLUREE_ENCRYPTION_KEY")})
```

## Troubleshooting

### Common Issues

1. **Invalid Key Length**: Ensure the encryption key is exactly 32 bytes
2. **Permission Errors**: Verify write permissions for the storage path
3. **Memory Issues**: Adjust `cache-max-mb` if experiencing out-of-memory errors
4. **Performance Issues**: Tune `parallelism` based on your hardware

### Error Messages

- `"Invalid key length"`: Encryption key must be exactly 32 bytes
- `"BadPaddingException"`: Incorrect encryption key or corrupted data
- `"FileNotFoundException"`: Check storage path permissions and disk space

## See Also

- [S3 Storage Guide](./S3_STORAGE_GUIDE.md) - For cloud storage options
- [API Documentation](../src/fluree/db/api.cljc) - Complete API reference
- [Examples](../examples/) - Code examples and use cases