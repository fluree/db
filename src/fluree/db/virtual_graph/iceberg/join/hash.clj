(ns fluree.db.virtual-graph.iceberg.join.hash
  "Streaming hash join operator for multi-table Iceberg queries.

   Design Principles:
   - Use mutable Java containers (HashMap, ArrayList) in hot paths for performance
   - Batch-oriented interface: build! and probe accept sequences, return sequences
   - Null semantics: null keys never match (standard SQL equi-join behavior)
   - Composite key support: multiple join columns combined into vector key
   - SPARQL join semantics: overlapping variable bindings must be compatible

   Usage:
     (let [join (create-hash-join [:airline_id] [:id])]
       (build! join build-solutions)
       (probe join probe-solutions))

   The join operator works at the solution-map level (not Arrow batches).
   Solutions are Clojure maps with variable bindings from SPARQL execution.

   Join Column Storage:
   For Iceberg virtual graphs, join column values are stored under
   ::fluree.db.virtual-graph.iceberg.query/join-col-vals as a map of
   {keyword-col -> value}. This is separate from the SPARQL variable
   bindings to ensure correct key lookup."
  (:require [clojure.set]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util.log :as log])
  (:import [java.util ArrayList HashMap]))

(set! *warn-on-reflection* true)

;; Namespace-qualified key for join column values from query.clj
(def ^:private join-col-vals-key :fluree.db.virtual-graph.iceberg.query/join-col-vals)

;;; ---------------------------------------------------------------------------
;;; Key Extraction
;;; ---------------------------------------------------------------------------

(defn- extract-key
  "Extract join key value(s) from a solution map.

   Looks up keys in ::join-col-vals if present (Iceberg solutions),
   otherwise falls back to direct lookup (for unit tests with plain maps).

   For single-column keys, returns the value directly.
   For composite keys, returns a vector of values.
   Returns nil if any key column is nil (null never matches)."
  [solution key-columns]
  (let [;; Try ::join-col-vals first (Iceberg query solutions), then direct lookup
        join-vals (get solution join-col-vals-key)
        vals (mapv (fn [col]
                     (or (when join-vals (get join-vals col))
                         (get solution col)))
                   key-columns)]
    (when-not (some nil? vals)
      (if (= 1 (count vals))
        (first vals)
        vals))))

;;; ---------------------------------------------------------------------------
;;; SPARQL-Compatible Merge
;;; ---------------------------------------------------------------------------

(defn- get-binding-value
  "Extract the underlying value from a SPARQL binding (match object).
   Returns the value directly if it's not a match object."
  [binding]
  (if (map? binding)
    (or (where/get-value binding)
        (::where/iri binding)
        (::where/val binding)
        binding)
    binding))

(defn compatible-merge
  "Merge two solution maps with SPARQL join semantics.

   In SPARQL, if both solutions bind the same variable, the bindings must
   be equal for the join to produce a result. If bindings conflict, returns nil.

   Internal keys (namespaced keywords) are always merged without conflict check.
   ::join-col-vals maps are merged together.

   Returns merged solution or nil if bindings conflict."
  [sol-a sol-b]
  (let [;; Find overlapping variable keys (symbols, not namespaced keywords)
        keys-a (set (keys sol-a))
        keys-b (set (keys sol-b))
        ;; Only check symbol keys (SPARQL variables), not internal namespaced keys
        symbol-keys-a (set (filter symbol? keys-a))
        symbol-keys-b (set (filter symbol? keys-b))
        overlapping (clojure.set/intersection symbol-keys-a symbol-keys-b)]

    ;; Check all overlapping bindings for compatibility
    (if (every? (fn [k]
                  (let [val-a (get-binding-value (get sol-a k))
                        val-b (get-binding-value (get sol-b k))]
                    (= val-a val-b)))
                overlapping)
      ;; Compatible - merge the solutions
      (let [;; Merge ::join-col-vals specially
            join-vals-a (get sol-a join-col-vals-key)
            join-vals-b (get sol-b join-col-vals-key)
            merged-join-vals (when (or join-vals-a join-vals-b)
                               (merge join-vals-a join-vals-b))]
        (cond-> (merge sol-a sol-b)
          merged-join-vals (assoc join-col-vals-key merged-join-vals)))
      ;; Conflict - no result
      nil)))

;;; ---------------------------------------------------------------------------
;;; Hash Join Protocol
;;; ---------------------------------------------------------------------------

(defprotocol IHashJoin
  "Streaming hash join with solution-level interface.

   Build side is accumulated into a hash table, then probe side
   is streamed through to produce joined solutions."
  (build! [this solutions]
    "Add build-side solutions to the hash table.
     Can be called multiple times for streaming builds.")
  (probe [this solutions]
    "Probe the hash table with solutions, returning a lazy seq of joined results.
     Must be called after build phase is complete.

     IMPORTANT: Since probe returns a lazy seq, you must fully realize the
     results before calling close! on the join operator. Use doall if needed.")
  (build-count [this]
    "Return the number of build-side rows in the hash table.")
  (close! [this]
    "Release resources and clear the hash table."))

;;; ---------------------------------------------------------------------------
;;; Hash Join Implementation
;;; ---------------------------------------------------------------------------

(defn create-hash-join
  "Create a streaming hash join operator.

   Args:
     build-keys - Vector of keys to extract from build-side solutions
     probe-keys - Vector of keys to extract from probe-side solutions
                  (must correspond positionally to build-keys)

   Options:
     :memory-limit - Max estimated memory before warning (bytes, default 100MB)
     :on-memory-warning - Callback fn when memory limit approached

   Returns an IHashJoin implementation.

   Example:
     ;; Join routes.airline_id = airlines.id
     (let [join (create-hash-join [:airline_id] [:id])]
       (build! join airline-solutions)  ; Build side: airlines
       (probe join route-solutions))    ; Probe side: routes"
  ([build-keys probe-keys]
   (create-hash-join build-keys probe-keys {}))
  ([build-keys probe-keys {:keys [memory-limit on-memory-warning]
                           :or {memory-limit (* 100 1024 1024)}}]
   {:pre [(vector? build-keys)
          (vector? probe-keys)
          (= (count build-keys) (count probe-keys))]}
   (let [;; Use HashMap with ArrayList per key for handling duplicates
         ;; Much faster than persistent maps with conj in hot loop
         ^HashMap hash-table (HashMap.)
         build-count-atom (atom 0)
         estimated-memory (atom 0)]

     (reify IHashJoin
       (build! [_ solutions]
         (doseq [solution solutions]
           (when-let [key (extract-key solution build-keys)]
             (let [^ArrayList rows (or (.get hash-table key)
                                       (let [al (ArrayList.)]
                                         (.put hash-table key al)
                                         al))]
               (.add rows solution)
               (swap! build-count-atom inc)
               ;; Rough memory estimate: ~500 bytes per solution
               (swap! estimated-memory + 500))))
         ;; Check memory and warn if needed
         (when (and on-memory-warning
                    (> @estimated-memory memory-limit))
           (on-memory-warning {:estimated-memory @estimated-memory
                               :build-count @build-count-atom
                               :unique-keys (.size hash-table)})))

       (probe [_ solutions]
         ;; Returns a lazy seq of joined solutions.
         ;; This enables streaming: results are yielded on demand rather than
         ;; materializing the entire result set upfront.
         (letfn [(probe-solution [probe-sol]
                   ;; Returns a lazy seq of matches for one probe solution
                   (when-let [key (extract-key probe-sol probe-keys)]
                     (when-let [^ArrayList matches (.get hash-table key)]
                       ;; Generate lazy seq of compatible merges
                       (keep (fn [i]
                               (let [build-solution (.get matches (int i))]
                                 (compatible-merge build-solution probe-sol)))
                             (range (.size matches))))))]
           ;; Lazily process each probe solution and concatenate results
           (mapcat probe-solution solutions)))

       (build-count [_]
         @build-count-atom)

       (close! [_]
         (.clear hash-table)
         (reset! build-count-atom 0)
         (reset! estimated-memory 0))))))

;;; ---------------------------------------------------------------------------
;;; Convenience Functions
;;; ---------------------------------------------------------------------------

(defn hash-join
  "Perform a complete hash join in one call.

   Builds the hash table from build-solutions, then probes with probe-solutions.
   For large datasets, prefer the streaming interface (create-hash-join).

   Note: This convenience function realizes all results before returning.
   For streaming behavior, use create-hash-join directly and manage the
   lifecycle yourself.

   Args:
     build-solutions - Sequence of solutions for build side (smaller table preferred)
     probe-solutions - Sequence of solutions for probe side
     build-keys      - Vector of keys to extract from build solutions
     probe-keys      - Vector of keys to extract from probe solutions

   Returns sequence of joined solutions.

   Example:
     (hash-join airlines routes [:id] [:airline_id])"
  [build-solutions probe-solutions build-keys probe-keys]
  (let [join (create-hash-join build-keys probe-keys)]
    (try
      (build! join build-solutions)
      (log/debug "Hash join built:" {:build-count (build-count join)})
      ;; Force realization since close! will clear the hash table
      (doall (probe join probe-solutions))
      (finally
        (close! join)))))

(defn left-outer-hash-join
  "Perform a left outer hash join.

   Like hash-join, but preserves ALL probe-side rows. For probe rows that
   have no matching build row, the result includes the probe row with nil
   values for where the build-side would have contributed.

   This implements SPARQL OPTIONAL semantics where the probe side is the
   required pattern and the build side is the optional pattern.

   Note: Unlike hash-join, this does NOT add nil placeholders for build-side
   variable bindings since SPARQL solutions don't include unbound variables.
   The result simply omits bindings from the optional (build) side.

   Args:
     build-solutions - Sequence of solutions for build side (optional pattern)
     probe-solutions - Sequence of solutions for probe side (required pattern)
     build-keys      - Vector of keys to extract from build solutions
     probe-keys      - Vector of keys to extract from probe solutions

   Returns sequence of joined solutions (probe rows always included)."
  [build-solutions probe-solutions build-keys probe-keys]
  (let [join (create-hash-join build-keys probe-keys)]
    (try
      (build! join build-solutions)
      (log/debug "Left outer hash join built:" {:build-count (build-count join)})
      ;; Custom probe that preserves unmatched probe rows
      (doall
       (mapcat
        (fn [probe-sol]
          ;; Try to find matches via probe
          (let [matches (probe join [probe-sol])]
            (if (seq matches)
              matches
              ;; No matches - return probe row alone (left outer semantics)
              [probe-sol])))
        probe-solutions))
      (finally
        (close! join)))))

(defn hash-join-with-edge
  "Perform a hash join using a JoinEdge specification.

   The JoinEdge determines which table is build vs probe based on
   child/parent relationship. Parent table is typically build side
   (dimension table), child table is probe side (fact table).

   Args:
     parent-solutions - Solutions from parent table (build side)
     child-solutions  - Solutions from child table (probe side)
     edge            - JoinEdge from join graph

   Returns sequence of joined solutions."
  [parent-solutions child-solutions edge]
  (let [;; Extract parent/child columns from join-conditions in edge
        parent-cols (mapv (comp keyword :parent) (:columns edge))
        child-cols (mapv (comp keyword :child) (:columns edge))]
    (log/debug "Hash join with edge:" {:parent (:parent-table edge)
                                       :child (:child-table edge)
                                       :parent-cols parent-cols
                                       :child-cols child-cols})
    (hash-join parent-solutions child-solutions parent-cols child-cols)))

;;; ---------------------------------------------------------------------------
;;; Multi-Way Join
;;; ---------------------------------------------------------------------------

(defn pipeline-hash-joins
  "Execute a sequence of hash joins in pipeline fashion.

   Takes an initial set of solutions and a sequence of join specs,
   executing each join in order with the result of the previous.

   Args:
     initial-solutions - Starting solutions (first table scan result)
     join-specs        - Vector of {:solutions [...] :build-keys [...] :probe-keys [...]}
                         Each spec joins against the accumulated result.

   Returns final joined solutions.

   Example:
     (pipeline-hash-joins
       route-solutions
       [{:solutions airline-solutions
         :build-keys [:id]
         :probe-keys [:airline_id]}
        {:solutions airport-solutions
         :build-keys [:id]
         :probe-keys [:src_airport_id]}])"
  [initial-solutions join-specs]
  (reduce
   (fn [accumulated {:keys [solutions build-keys probe-keys]}]
     (if (empty? accumulated)
       [] ; Short-circuit if no solutions
       (hash-join solutions accumulated build-keys probe-keys)))
   initial-solutions
   join-specs))
