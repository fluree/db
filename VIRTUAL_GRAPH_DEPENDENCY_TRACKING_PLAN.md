# Virtual Graph Dependency Tracking Implementation Plan

## Overview

This document outlines a plan to implement automatic updates for virtual graphs when their dependent ledgers change. The system will track dependencies in the nameservice and trigger index updates when relevant data changes.

**Key Design Decision**: The VG dependency tracking is integrated directly into the nameservice publisher rather than being a separate component. This keeps the architecture clean - the connection doesn't need to know about VG management, and the nameservice naturally handles all resource publishing and updates.

## Current Architecture Analysis

### What Exists Today

1. **Virtual Graph Infrastructure**:
   - `UpdatableVirtualGraph` protocol with `upsert` method
   - BM25 implementation that can handle incremental updates
   - Property dependency tracking (`property-deps`) to identify relevant changes
   - Virtual graphs stored in nameservice under `ns@v1/` directory

2. **Nameservice System**:
   - Publishes commits and virtual graph configurations
   - Subscription mechanism for ledgers (in `nameservice.sub`)
   - Notification system when commits are published
   - Primary and secondary publishers

3. **Missing Pieces**:
   - No dependency tracking between VGs and ledgers
   - No mechanism to trigger VG updates on ledger commits
   - No VG lifecycle management (loading/unloading)

## Proposed Solution

### 1. Dependency Tracking Architecture

Enhance the nameservice publisher to include VG dependency tracking:

```clojure
;; Enhancement to existing publisher record
(defrecord NameservicePublisher 
  [;; ... existing fields ...
   
   ;; VG dependency tracking state
   vg-state     ; {:dependencies {ledger-alias #{vg-name ...}}
                ;  :vg-channels {vg-name update-channel}
                ;  :loaded-vgs {vg-name vg-instance}}
   
   ;; Reference to catalogs for loading VG data
   commit-catalog
   index-catalog])
```

### 2. State Structure

```clojure
{:dependencies {
   "books" #{"book-search" "author-index"}
   "movies" #{"movie-search"}
 }
 :vg-channels {
   "book-search" <async-channel>
   "movie-search" <async-channel>
   "author-index" <async-channel>
 }
 :loaded-vgs {
   "book-search" <BM25-VirtualGraph-instance>
   ; others may be unloaded to save memory
 }}
```

### 3. Implementation Components

#### A. Nameservice Initialization

When the nameservice starts up, it must scan and parse all virtual graph records to build the dependency map:

```clojure
(defn initialize-vg-dependencies
  "Scans all virtual graph records at startup to build dependency map"
  [publisher]
  (go-try
    (let [vg-files (<? (list-virtual-graph-files publisher))
          state (atom {})]
      
      (doseq [vg-file vg-files]
        (let [vg-record (<? (read-vg-record publisher vg-file))
              vg-name (get vg-record "f:name")
              dependencies (mapv #(get % "@id") 
                                 (get vg-record "f:dependencies" []))]
          
          ;; Build dependency map: ledger -> #{vg-names}
          (doseq [dep dependencies]
            (swap! state update-in [:dependencies dep] 
                   (fnil conj #{}) vg-name))))
      
      ;; Set the initial state
      (reset! (:vg-state publisher) state))))

;; Call this during nameservice startup
(defn start-nameservice
  [config]
  (let [publisher (create-publisher config)]
    ;; Initialize VG dependencies from existing records
    (<? (initialize-vg-dependencies publisher))
    publisher))
```

#### B. VG Registration During Creation

When a virtual graph is created (`create-virtual-graph`):

1. Register dependencies with the publisher
2. Create an update channel for the VG
3. Initialize the VG and store it (with optional timeout for unloading)

Note: The beauty of this approach is that `publish-virtual-graph` in `nameservice/virtual-graph.cljc` 
already receives the publisher and has access to all the information needed to register dependencies.

```clojure
(defn register-vg-dependencies
  [publisher vg-name dependencies]
  (swap! (:vg-state publisher)
         (fn [state]
           (reduce (fn [s dep-ledger]
                     (update-in s [:dependencies dep-ledger] 
                                (fnil conj #{}) vg-name))
                   state
                   dependencies))))
```

#### B. Commit Notification Enhancement

Enhance the nameservice `publish` method to trigger VG updates:

```clojure
;; In nameservice implementation
(defn publish
  [publisher commit-data]
  (go-try
    ;; ... existing publish logic ...
    
    ;; After successful publish, notify dependent VGs
    (when-let [ledger-alias (get-first-value commit-data const/iri-alias)]
      (<? (notify-dependent-vgs publisher ledger-alias commit-data)))
    
    result))

(defn notify-dependent-vgs
  [publisher ledger-alias commit-data]
  (go-try
    (let [state @(:vg-state publisher)
          dependent-vgs (get-in state [:dependencies ledger-alias])]
      (doseq [vg-name dependent-vgs]
        (<? (update-virtual-graph publisher vg-name commit-data))))))
```

#### C. VG Update Handler

```clojure
(defn update-virtual-graph
  [publisher vg-name commit-data]
  (go-try
    (let [state @(:vg-state publisher)
          vg-instance (or (get-in state [:loaded-vgs vg-name])
                          (<? (load-virtual-graph publisher vg-name)))]
      
      ;; Extract flakes from commit
      (let [db-address (get-in commit-data ["f:data" "@id"])
            data (<? (read-data-jsonld (:commit-catalog publisher) db-address))
            new-flakes (:flakes data)
            remove-flakes (:retract data)
            ;; Need to get source-db - either from loaded ledger or reconstruct
            ledger-alias (get-first-value commit-data const/iri-alias)
            source-db (<? (get-source-db publisher ledger-alias))]
        
        ;; Update the VG
        (let [updated-vg (<? (vg/upsert vg-instance source-db 
                                        new-flakes remove-flakes))]
          ;; Update state with new VG instance
          (swap! (:vg-state publisher) 
                 assoc-in [:loaded-vgs vg-name] updated-vg))))))
```

#### D. VG Lifecycle Management

```clojure
(defn load-virtual-graph
  [publisher vg-name]
  (go-try
    ;; Load VG configuration from nameservice
    (let [vg-record (<? (load-vg-record publisher vg-name))
          vg-instance (<? (initialize-vg-from-record vg-record 
                                                      (:commit-catalog publisher)
                                                      (:index-catalog publisher)))]
      
      ;; Store in loaded VGs
      (swap! (:vg-state publisher) 
             assoc-in [:loaded-vgs vg-name] vg-instance)
      
      ;; Set up unload timer (30 min idle timeout)
      (schedule-unload publisher vg-name (* 30 60 1000))
      
      vg-instance)))

(defn unload-virtual-graph
  [publisher vg-name]
  (swap! (:vg-state publisher) update :loaded-vgs dissoc vg-name))
```

#### D. VG Deletion/Retraction

When a virtual graph is deleted, clean up dependencies:

```clojure
(defn retract-virtual-graph
  [publisher vg-name]
  (go-try
    ;; ... existing retraction logic ...
    
    ;; Clean up dependencies
    (swap! (:vg-state publisher)
           (fn [state]
             (reduce (fn [s [ledger vgs]]
                       (if (contains? vgs vg-name)
                         (update-in s [:dependencies ledger] disj vg-name)
                         s))
                     state
                     (:dependencies state))))
    
    ;; Unload if loaded
    (unload-virtual-graph publisher vg-name)))
```

#### E. Ledger Deletion Protection

Before allowing ledger deletion, check for dependent virtual graphs:

```clojure
(defn check-vg-dependencies
  "Returns set of VG names that depend on the ledger, or empty set if none"
  [publisher ledger-alias]
  (let [state @(:vg-state publisher)]
    (get-in state [:dependencies ledger-alias] #{})))

(defn can-delete-ledger?
  "Returns true if ledger can be safely deleted (no VG dependencies)"
  [publisher ledger-alias]
  (empty? (check-vg-dependencies publisher ledger-alias)))

;; Integration point - add to connection/drop-ledger
(defn drop-ledger
  [conn ledger-alias]
  (go-try
    (let [publisher (:primary-publisher conn)
          dependent-vgs (check-vg-dependencies publisher ledger-alias)]
      
      (when (seq dependent-vgs)
        (throw (ex-info (str "Cannot delete ledger '" ledger-alias 
                             "' - it has dependent virtual graphs: " 
                             (str/join ", " dependent-vgs)
                             ". Delete the virtual graphs first.")
                        {:status 400 
                         :error :db/ledger-has-dependencies
                         :ledger ledger-alias
                         :dependent-vgs dependent-vgs})))
      
      ;; ... existing drop logic ...)))
```

### 4. Integration Points

#### A. Publisher Enhancement

The nameservice publisher already has access to catalogs and handles publishing.
No changes needed to Connection - it remains clean and focused.

Key initialization point - the publisher must scan VG records at startup to build dependencies.

#### B. Ledger Deletion Integration

The existing `drop-ledger` function in `connection.cljc` needs to be enhanced to check for VG dependencies before allowing deletion. This prevents orphaned virtual graphs and maintains referential integrity.

#### C. Transaction Pipeline

No changes needed to `publish-commit` - the nameservice handles VG updates internally:

```clojure
;; In transact.cljc - unchanged
(defn publish-commit
  [{:keys [primary-publisher secondary-publishers] :as ledger} commit-jsonld]
  (go-try
    (let [result (<? (nameservice/publish primary-publisher commit-jsonld))]
      (nameservice/publish-to-all commit-jsonld secondary-publishers)
      result)))

;; VG updates happen inside nameservice/publish automatically
```

#### D. VG Creation Flow

Update `publish-virtual-graph` in nameservice to register dependencies:

```clojure
;; In nameservice/virtual-graph.cljc
(defn publish-virtual-graph
  [publisher vg-config]
  (go-try
    ;; ... existing publish logic ...
    
    ;; Register dependencies after successful publish
    (register-vg-dependencies publisher 
                              (:vg-name vg-config) 
                              (:dependencies vg-config))
    
    result))
```

### 5. Performance Considerations

1. **Selective Updates**: Only update VGs when their dependent properties change
2. **Async Processing**: Updates happen asynchronously to avoid blocking commits
3. **Memory Management**: Idle VGs are unloaded after 30 minutes
4. **Batch Updates**: Multiple rapid commits can be batched for efficiency

### 6. Error Handling

1. **Failed Updates**: Log errors but don't block commit pipeline
2. **Retry Logic**: Implement exponential backoff for failed updates
3. **Corrupt State**: Ability to rebuild VG from scratch if needed

### 7. Migration Strategy

1. **Backward Compatibility**: System works without VG manager (no auto-updates)
2. **Opt-in**: Existing VGs continue to work but won't auto-update
3. **Gradual Rollout**: Can be enabled per connection

## Testing Strategy

1. **Unit Tests**:
   - Dependency registration/unregistration
   - VG loading/unloading
   - Update filtering based on property-deps

2. **Integration Tests**:
   - End-to-end update flow from commit to VG update
   - Multiple VGs depending on same ledger
   - Rapid commit scenarios
   - **Startup initialization with existing VG records**
   - **Recovery after nameservice restart**
   - **Ledger deletion protection - should fail when VGs depend on it**
   - **VG deletion cleanup - dependencies should be removed**

3. **Performance Tests**:
   - Memory usage with many loaded VGs
   - Update latency measurements
   - Concurrent update handling

## Monitoring & Observability

Add metrics for:
- Number of loaded VGs
- Update queue depth
- Update latency
- Memory usage per VG
- Failed update attempts

## Implementation Phases

### Phase 1: Core Infrastructure (1-2 days)
- Enhance nameservice publisher with VG state
- Add dependency tracking methods
- **Implement startup initialization to scan existing VG records**
- Create basic registration flow
- **Add ledger deletion protection with dependency checks**

### Phase 2: Update Pipeline (2-3 days)
- Integrate with publish flow
- Implement VG update handler
- Add selective update logic based on property-deps

### Phase 3: Lifecycle Management (1-2 days)
- Implement VG loading/unloading
- Add idle timeout mechanism
- Memory management

### Phase 4: Testing & Optimization (2-3 days)
- Comprehensive test suite
- Performance benchmarking
- Error handling improvements

## Alternative Approaches Considered

### 1. Pull-based Updates
- VGs poll for changes periodically
- Pros: Simpler, no dependency tracking needed
- Cons: Inefficient, delayed updates, more resource usage

### 2. Embedded in Database
- Store VG state directly in database
- Pros: Transactional consistency
- Cons: Major architectural change, performance impact

### 3. External Service
- Separate microservice for VG management
- Pros: Scalability, isolation
- Cons: Complexity, deployment overhead

## Recommendation

Implement the push-based dependency tracking system as outlined above. It provides:
- Immediate updates when data changes
- Efficient resource usage
- Clean integration with existing architecture
- Flexibility for future enhancements

The system can start simple and evolve based on usage patterns and performance requirements.

## Critical Implementation Details

### Startup Sequence
1. Nameservice publisher is created
2. **Before accepting any commits**, scan all `ns@v1/*.json` files for VG records
3. Parse each VG record's `f:dependencies` to build the dependency map
4. Only then begin accepting commits and triggering updates

### Dependency Format
Virtual graph records store dependencies as:
```json
{
  "f:dependencies": [
    {"@id": "books@main"},
    {"@id": "authors@main"}
  ]
}
```

The ledger alias from commits is compared against these dependency IDs.

### Referential Integrity
When attempting to delete a ledger, the system must check if any virtual graphs depend on it:
- If dependencies exist, throw an error with the list of dependent VGs
- Force the user to delete VGs first, then the ledger
- This prevents orphaned VGs and maintains data consistency

## Advantages of Nameservice-Integrated Approach

Having the VG manager as part of the nameservice publisher rather than the connection provides several benefits:

1. **Cohesive Architecture**: The nameservice already handles publishing and notifications - VG updates are a natural extension
2. **Cleaner Separation**: The connection remains focused on ledger management without VG concerns
3. **Single Responsibility**: The nameservice becomes the single source of truth for all published resources (ledgers and VGs)
4. **Simplified Flow**: No need to pass VG manager references through multiple layers
5. **Natural Integration**: VG updates happen automatically as part of the publish flow without external coordination
6. **Startup Recovery**: The nameservice can rebuild its dependency state from existing VG records on restart