# BM25 Virtual Graph to Nameservice Migration Plan

## Current Implementation Analysis

### How BM25 Currently Works
1. **Creation**: Users transact a special BM25 configuration object into their ledger data:
   ```json
   {
     "@id": "ex:articleSearch",
     "@type": ["f:VirtualGraph", "fidx:BM25"],
     "f:virtualGraph": "articleSearch",
     "fidx:stemmer": {"@id": "fidx:snowballStemmer-en"},
     "fidx:stopwords": {"@id": "fidx:stopwords-en"},
     "f:query": {
       "@type": "@json",
       "@value": {
         "@context": {"ex": "http://example.org/ns/"},
         "where": [{"@id": "?x", "ex:author": "?author"}],
         "select": {"?x": ["@id", "ex:author", "ex:title", "ex:summary"]}
       }
     }
   }
   ```

2. **Detection**: During transaction processing (`check-virtual-graph` in `index_graph.cljc`):
   - Scans flakes for objects with `@type` containing `f:VirtualGraph`
   - Extracts virtual graph configuration from flakes
   - Creates BM25 index instance

3. **Storage**: BM25 indexes are stored under:
   - Path: `{db-alias}/bm25/{vg-alias}/`
   - Files: `index.json`, `options.json`

4. **Updates**: When data changes:
   - Virtual graphs check if changed properties match their `property-deps`
   - If relevant, updates are applied asynchronously via `UpdatableVirtualGraph` protocol

5. **Query Integration**: Queries use special `graph` syntax:
   ```sparql
   graph ##articleSearch {
     fidx:target "search term"
     fidx:result { @id ?x, fidx:score ?score }
   }
   ```

## Proposed Nameservice Architecture

### Key Changes

1. **Move BM25 Configuration to Nameservice**
   - Create new nameservice record type: `f:VirtualGraphDatabase`
   - Store BM25 configuration in nameservice instead of ledger data
   - Example nameservice record:
   ```json
   {
     "@context": {"f": "https://ns.flur.ee/ledger#", "fidx": "https://ns.flur.ee/index#"},
     "@id": "mydb##articleSearch",
     "@type": ["f:VirtualGraphDatabase", "fidx:BM25"],
     "f:ledger": {"@id": "mydb"},
     "f:virtualGraph": "articleSearch",
     "f:status": "ready",
     "f:dependencies": [{"@id": "mydb@main"}],
     "fidx:config": {
       "@type": "@json",
       "@value": {
         "stemmer": "snowballStemmer-en",
         "stopwords": "stopwords-en",
         "query": {
           "@context": {"ex": "http://example.org/ns/"},
           "where": [{"@id": "?x", "ex:author": "?author"}],
           "select": {"?x": ["@id", "ex:author", "ex:title", "ex:summary"]}
         }
       }
     }
   }
   ```

2. **Virtual Graph Manager Service**
   - New component that monitors nameservice for virtual graph records
   - Watches dependent ledgers for updates
   - Manages virtual graph lifecycle (create, update, delete)
   - Handles asynchronous index updates

3. **API Changes**
   - New API endpoint to create/manage virtual graphs:
     ```clojure
     (fluree/create-virtual-graph conn 
       {:ledger "mydb"
        :alias "articleSearch"
        :type :bm25
        :config {...}})
     ```
   - Query syntax remains the same (using `graph ##name`)

### Implementation Steps

#### Phase 1: Core Infrastructure
1. Define new nameservice schema for virtual graphs
2. Create Virtual Graph Manager component
3. Implement nameservice monitoring for VG records
4. Add dependency tracking between VGs and ledgers

#### Phase 2: BM25 Migration
1. Refactor BM25 creation to use nameservice
2. Update storage paths to be nameservice-aware
3. Implement update triggers based on ledger commits
4. Add migration tool for existing BM25 indexes

#### Phase 3: API and Integration
1. Add `create-virtual-graph` API
2. Update query engine to lookup VGs from nameservice
3. Add virtual graph management to connection lifecycle
4. Update documentation and examples

### Benefits

1. **Separation of Concerns**: Virtual graphs are metadata, not user data
2. **Better Dependency Management**: Clear tracking of which ledgers affect which VGs
3. **Simplified Updates**: No need to scan transaction data for VG changes
4. **Multi-ledger Support**: Future ability to create VGs across multiple ledgers
5. **Cleaner Ledger Data**: User ledgers only contain their actual data

### Considerations

1. **Backward Compatibility**: Need migration path for existing BM25 indexes
2. **Performance**: Nameservice lookups for VG metadata on queries
3. **Consistency**: Ensuring VG updates when dependent ledgers change
4. **Security**: Access control for creating/managing virtual graphs

### Future Enhancements

1. Support for other virtual graph types (vector embeddings, graph algorithms)
2. Cross-ledger virtual graphs
3. Virtual graph versioning and history
4. Automatic virtual graph suggestions based on data patterns