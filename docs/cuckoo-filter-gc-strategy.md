# Cuckoo Filter for Cross-Branch Index Garbage Collection

## Overview

Fluree uses cuckoo filters to optimize index garbage collection across database branches. The implementation enables efficient checking of whether index nodes marked as garbage by one branch are still in use by other branches, preventing premature deletion while maintaining high performance with minimal memory overhead.

## Why Cuckoo Filters?

### The Cross-Branch Garbage Collection Problem
Fluree's content-addressed storage means multiple branches can share the same index segments. When garbage collection runs for branch A:

1. **Branch A** has a list of index nodes no longer needed after reindexing
2. **Other branches** (B, C, etc.) may still reference these same nodes
3. **Safety requirement**: Must not delete nodes still in use by other branches
4. **Performance requirement**: Need fast membership testing across potentially large sets
5. **Acceptable tradeoff**: False positives (keeping garbage longer) are acceptable; false negatives (deleting active nodes) are NOT acceptable

### Index Storage Architecture
- **Content-Addressed Storage**: All index files use SHA-256 hashes as filenames (base32 encoded)
- **Index Types**: 4 different index types (spot, post, opst, tspo)
- **Storage Structure**: `ledger-name/index/<type>/<sha256-hash>.json`
- **Leaf Nodes**: Average ~200KB; overflow threshold 500KB
- **Branch Nodes**: Can hold up to 500 children
- **Shared Segments**: Multiple branches frequently reference identical index segments

## How It Works

### Filter Storage and Lifecycle
Each branch maintains its own cuckoo filter stored at:
```
ledger-name/index/cuckoo/branch-name.cbor
```

**Filter Updates:**
1. **During indexing** (`novelty.cljc`): As new index segments are written, their addresses are automatically added to the branch's cuckoo filter
2. **During garbage collection** (`garbage.cljc`): Segments are removed from the current branch's filter before checking other branches
3. **Branch creation** (`branch.cljc`): New branches copy their parent's filter as a starting point

### Cross-Branch Checking Process
When garbage collection runs:
1. Load all other branch filters (excluding current branch)
2. Check each garbage segment against other branch filters
3. Retain segments that show up as "possibly in use" by other branches
4. Only delete segments that are confirmed not in use elsewhere

## Design Parameters

### Fingerprint Selection
- **Method**: Decodes base32 SHA-256 hash to raw bytes and takes the first 16 bits
- **Size**: 16 bits (first 2 bytes of the decoded SHA-256 hash)
- **Bucket Hashing**: FNV-1a 32-bit hash over first 8 bytes for primary bucket
- **Platform Stability**: FNV-1a ensures identical behavior across JVM and JavaScript
- **Rationale**: Platform-stable implementation using first 16 bits of SHA-256 for low false positive rates

### Filter Configuration
- **Bucket size**: 4 slots per bucket (standard for cuckoo filters)
- **Max relocations**: 500 attempts before considering filter full
- **Load factor target**: 90-95%
- **Automatic sizing**: Filter size is calculated based on expected item count

## Performance Characteristics

### Memory Usage
Actual measurements with realistic hash distribution:
- **16-bit fingerprints**: ~4.6 bytes per index segment (measured with 50K well-distributed segments)
- **Storage format**: CBOR binary encoding with bucket arrays
- **Load factor**: Typically 47-95% with good hash distribution
- **Note**: Slightly higher than theoretical 2.8 bytes due to CBOR structure overhead and empty slots

### False Positive Rates
- **16-bit fingerprints**: ~0.012% (1 in ~8,200)

### Database Size vs Filter Size

Using realistic index characteristics (200KB average leaf size, ~300 branch fanout):

| Database Size | Estimated Segments | Filter Size (16-bit) | Expected FP Rate |
|---------------|-------------------|---------------------|------------------|
| 100MB         | ~502              | ~2.3KB             | ~0.012%          |
| 1GB           | ~5,017            | ~23KB              | ~0.012%          |
| 10GB          | ~50,167           | ~224KB             | ~0.012%          |
| 100GB         | ~501,667          | ~2.2MB             | ~0.012%          |
| 1TB           | ~5,016,667        | ~22MB              | ~0.012%          |

**Calculations:**
- Segments = `(DB_size / 200KB) + (segments / 300)` (leaves + branches)
- Filter size ≈ `segments × 4.6 bytes` (measured with realistic SHA-256 hash distribution)

### Runtime Performance
- **Hash operations**: ~1 microsecond per segment
- **Filter lookups**: O(1) average, O(4) worst case
- **Negligible overhead** compared to actual disk operations
- **I/O impact**: Minimal - filters are small and cached during GC operations

## Implementation Details

### Key Components

The implementation spans three main namespaces:

#### 1. Core Filter (`fluree.db.indexer.cuckoo`)
```clojure
;; Primary operations
(create-filter expected-items)           ; Create new filter
(add-item filter segment-address)       ; Add segment to filter  
(contains-hash? filter segment-address) ; Check membership
(remove-item filter segment-address)    ; Remove segment
(serialize/deserialize filter)          ; Persistence
```

#### 2. Index Integration (`fluree.db.flake.index.novelty`)
During the index refresh process:
1. Collect all newly written segment addresses
2. Add segments (including root and garbage files) to current branch's filter
3. Write updated filter to storage

#### 3. Garbage Collection (`fluree.db.indexer.garbage`)
During garbage collection:
1. Remove garbage segments from current branch's filter (prevents self-checking)
2. Load filters from all other branches
3. Check each garbage segment against other branch filters
4. Only delete segments not found in any other branch

## Design Decisions

### 1. Safety-First Approach
- **False positives acceptable**: Better to keep garbage longer than delete active segments
- **Missing filter = retain all**: When a branch's filter can't be read, assume all segments are in use
- **Self-exclusion**: Branches never check their own filter during garbage collection

### 2. Fingerprint Strategy
- **Extract from base32 decoded bytes**: Takes first 16 bits from decoded SHA-256 hash
- **Platform consistent**: Works identically across JVM and JavaScript platforms
- **16-bit fixed size**: Provides good balance of memory efficiency and low false positive rate (~0.012%)

### 3. Storage Strategy
- **Per-branch filters**: Each branch maintains its own filter at `ledger/index/cuckoo/branch.cbor`
- **CBOR serialization**: Binary format with bucket array storage (~68 bytes per segment measured)
- **Filter chains**: Automatically creates new filters as needed to handle growth
- **Atomic updates**: Filters are written atomically during index completion
- **Platform support**: CBOR available on JVM and Node.js (gracefully degrades on browsers)

### 4. Concurrency and Consistency  
- **No locking required**: Filters are read-only during garbage collection
- **Sequential updates**: Filter updates happen synchronously during indexing
- **Immutable snapshots**: GC operates on filter snapshots, not live data

## Monitoring and Observability

### Log Messages
Garbage collection operations log retention statistics:
```
INFO: Checking 8 garbage segments from ledger "my-ledger:main" branch "main" t 1 
      - Retained 2 segments still in use by other branches 
      - Deleting 6 segments from disk
```

### Filter Statistics
The implementation provides metrics via `filter-stats`:
- **Count**: Number of items in the filter
- **Capacity**: Maximum items before resize needed
- **Load factor**: Current utilization percentage
- **Estimated FPR**: Theoretical false positive rate
- **Fingerprint bits**: Configuration setting

## Operational Characteristics

### Failure Handling
- **Corrupted filter**: Logged and treated as missing (retain all segments)
- **Storage errors**: GC conservatively retains segments when in doubt
- **Filter overflow**: Automatically creates larger filter as needed

### Branch Operations
- **Branch creation**: Copies parent's filter as starting point
- **Branch deletion**: Removes corresponding filter file
- **Branch rename**: Updates filter filename accordingly

## Implementation Reference

### Core Algorithm
The cuckoo filter uses two hash functions to determine bucket locations:

```clojure
;; Extract hash part and decode to bytes
(defn- compute-hashes [address num-buckets]
  (let [hash-bytes (address->bytes address)
        ;; Extract 16-bit fingerprint from first 2 bytes
        fp (bit-or (bit-shift-left (bit-and (first hash-bytes) 0xFF) 8)
                   (bit-and (second hash-bytes) 0xFF))
        ;; FNV-1a 32-bit hash for primary bucket
        ;; FNV-1a prime: 16777619, offset basis: 2166136261
        fnv-prime 16777619
        fnv-offset 2166136261
        b1-hash (reduce (fn [hash b]
                          ;; FNV-1a: hash = (hash XOR byte) * prime
                          (bit-and 0xFFFFFFFF 
                                   (* (bit-xor hash (bit-and b 0xFF))
                                      fnv-prime)))
                        fnv-offset
                        (take 8 hash-bytes))
        b1 (mod b1-hash num-buckets)
        ;; Compute alternate bucket using XOR with fingerprint
        b2 (mod (bit-xor b1 (hash fp)) num-buckets)]
    [fp b1 b2]))
```

### Key Operations
```clojure
;; Create filter chain (supports growth beyond initial capacity)
(create-filter-chain)

;; Add index segment to filter chain
(add-item-chain chain "fluree:file://ledger/index/spot/abc123.json")

;; Check if segment might be in filter chain (may have false positives)
(contains-hash-chain? chain "fluree:file://ledger/index/spot/abc123.json")

;; Remove segment from filter chain
(remove-item-chain chain "fluree:file://ledger/index/spot/abc123.json")

;; Batch operations for efficiency
(batch-add-chain chain segment-list)
(batch-remove-chain chain segment-list)
```

### Cross-Branch Checking
```clojure
;; Load all other branch filters
(defn load-other-branch-filters [index-catalog ledger current-branch]
  ;; Discovers branches by scanning storage, excludes current branch
  ;; Returns vector of loaded filters)

;; Check if any other branch uses this segment
(defn any-branch-uses? [other-filters segment-address]
  (some #(contains-hash? % segment-address) other-filters))
```

## References

- [Cuckoo Filter: Practically Better Than Bloom](https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf)
- [Implementation: `fluree.db.indexer.cuckoo`](../src/fluree/db/indexer/cuckoo.cljc)
- [Garbage Collection: `fluree.db.indexer.garbage`](../src/fluree/db/indexer/garbage.cljc)
- [Index Integration: `fluree.db.flake.index.novelty`](../src/fluree/db/flake/index/novelty.cljc)