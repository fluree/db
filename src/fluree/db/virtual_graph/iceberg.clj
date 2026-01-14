(ns fluree.db.virtual-graph.iceberg
  "Iceberg implementation of virtual graph using ITabularSource.

   Supports R2RML mappings over Iceberg tables with predicate pushdown.

   Naming Convention:
     Iceberg virtual graphs use the same naming as ledgers:
       <name>:<branch>@iso:<time-travel-iso-8601>
       <name>:<branch>@t:<snapshot-id>

     Examples:
       \"sales-vg\"              - defaults to :main branch, latest snapshot
       \"sales-vg:main\"         - explicit main branch
       \"sales-vg@iso:2024-01-15T00:00:00Z\"  - time travel to specific time
       \"sales-vg@t:12345\"      - specific snapshot ID

   Configuration:
     {:type :iceberg
      :name \"my-vg\"
      :config {:warehouse-path \"/path/to/warehouse\"    ; for HadoopTables
               :store my-fluree-store                    ; for FlureeIcebergSource
               :metadata-location \"s3://...\"            ; direct metadata location
               :mapping \"path/to/mapping.ttl\"
               :table \"namespace/tablename\"}}"
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.optimize :as optimize]
            [fluree.db.util.async :refer [empty-channel]]
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph :as vg]
            [fluree.db.virtual-graph.iceberg.antijoin :as antijoin]
            [fluree.db.virtual-graph.iceberg.exec :as exec]
            [fluree.db.virtual-graph.iceberg.expr :as expr]
            [fluree.db.virtual-graph.iceberg.factory :as factory]
            [fluree.db.virtual-graph.iceberg.modifiers :as modifiers]
            [fluree.db.virtual-graph.iceberg.pushdown :as pushdown]
            [fluree.db.virtual-graph.iceberg.query :as query]
            [fluree.db.virtual-graph.iceberg.transitive :as transitive]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; Re-exported Dynamic Vars (for backwards compatibility)
;;; ---------------------------------------------------------------------------

(def ^:dynamic *max-cartesian-product-size*
  "Maximum allowed Cartesian product size before throwing an error.
   Set to nil to disable the check (not recommended for production).
   Default: 100,000 rows.

   Can be overridden via binding:
     (binding [*max-cartesian-product-size* 1000000]
       (execute-query ...))

   Or set to nil to allow unbounded Cartesian products:
     (binding [*max-cartesian-product-size* nil]
       (execute-query ...))"
  exec/*max-cartesian-product-size*)

(def ^:dynamic *columnar-execution*
  "Enable Phase 3 columnar execution path.

   When true, uses the plan compiler and Arrow-batch operators for query
   execution, keeping data in columnar format through joins.

   When false (default), uses the row-based solution approach from Phase 2.

   This flag enables A/B testing between execution strategies:
     (binding [*columnar-execution* true]
       (execute-query ...))"
  exec/*columnar-execution*)

;;; ---------------------------------------------------------------------------
;;; IcebergDatabase Record (Multi-Table Support)
;;; ---------------------------------------------------------------------------

(defrecord IcebergDatabase [alias config sources mappings routing-indexes join-graph time-travel
                            query-pushdown aggregation-spec anti-join-spec expression-evaluators
                            transitive-spec]
  ;; sources: {table-name -> IcebergSource}
  ;; mappings: {table-key -> {:table, :class, :predicates, ...}}
  ;; routing-indexes: {:class->mappings {rdf-class -> [mappings...]}, :predicate->mappings {pred -> [mappings...]}}
  ;; join-graph: {:edges [JoinEdge...], :by-table {table -> [edges]}, :tm->table {iri -> table}}
  ;; query-pushdown: atom holding query-time pushdown predicates (set in -reorder, used in -finalize)
  ;; aggregation-spec: atom holding aggregation spec {:group-keys [...] :aggregates [...] :order-by [...] :limit n}
  ;; anti-join-spec: atom holding anti-join patterns [{:type :exists/:not-exists/:minus :patterns [...]} ...]
  ;; expression-evaluators: atom holding residual FILTER/BIND evaluators {:filters [...] :binds [...]} (set in -reorder, used in -finalize)
  ;; transitive-spec: atom holding transitive path specs [{:subject s :predicate p :object o :tag :one+/:zero+} ...] (set in -reorder, used in -finalize)
  ;; NOTE: Subqueries are handled by standard Fluree execution via match-pattern :query, not here.

  vg/UpdatableVirtualGraph
  (upsert [this _source-db _new-flakes _remove-flakes]
    (go this))
  (initialize [this _source-db]
    (go this))

  where/Matcher
  (-match-id [_ _tracker _solution _s-mch _error-ch]
    empty-channel)

  (-match-triple [_this _tracker solution triple _error-ch]
    (go
      (let [iceberg-patterns (get solution ::iceberg-patterns [])
            updated (conj iceberg-patterns triple)
            ;; Extract any pushdown filters from pattern metadata
            triple-meta (meta triple)
            pushdown-filters (::pushdown/pushdown-filters triple-meta)
            ;; Accumulate pushdown filters in solution
            existing-pushdown (get solution ::solution-pushdown-filters [])
            new-pushdown (if pushdown-filters
                           (into existing-pushdown pushdown-filters)
                           existing-pushdown)]
        (when pushdown-filters
          (log/debug "Iceberg -match-triple received pattern with pushdown filters:"
                     pushdown-filters))
        (cond-> (assoc solution ::iceberg-patterns updated)
          (seq new-pushdown) (assoc ::solution-pushdown-filters new-pushdown)))))

  (-match-class [_this _tracker solution class-triple _error-ch]
    (go
      (let [iceberg-patterns (get solution ::iceberg-patterns [])
            updated (conj iceberg-patterns class-triple)]
        (assoc solution ::iceberg-patterns updated))))

  (-match-properties [_this _tracker solution triples _error-ch]
    (go
      (let [iceberg-patterns (get solution ::iceberg-patterns [])
            updated (into iceberg-patterns triples)]
        (assoc solution ::iceberg-patterns updated))))

  (-activate-alias [this _alias]
    (go this))

  (-aliases [_]
    [alias])

  (-finalize [_this _tracker error-ch solution-ch]
    (let [;; Pushdown predicates from atom (includes both FILTER and VALUES predicates)
          ;; This is the primary path since pattern metadata doesn't survive through
          ;; the WHERE executor (known limitation)
          atom-pushdown (when query-pushdown @query-pushdown)
          ;; Capture aggregation spec from atom (set in -reorder)
          agg-info (when aggregation-spec @aggregation-spec)
          ;; Capture anti-join spec from atom (set in -reorder)
          anti-joins (when anti-join-spec @anti-join-spec)
          ;; Capture expression evaluators from atom (set in -reorder)
          ;; These are non-pushable FILTER and BIND expressions
          expr-evals (when expression-evaluators @expression-evaluators)
          ;; Capture transitive path spec from atom (set in -reorder)
          ;; These are property path patterns like ex:knows+ or ex:broader*
          trans-paths (when transitive-spec @transitive-spec)
          ;; NOTE: Subqueries are handled by standard Fluree execution via
          ;; match-pattern :query, not here. This avoids shared-state issues.
          ;; Capture columnar execution flag at query start (binding may change)
          use-columnar? exec/*columnar-execution*
          ;; If aggregation, anti-joins, expression evaluators, or transitive paths are needed,
          ;; we must collect all solutions before emitting.
          needs-collection? (or agg-info (seq anti-joins) (seq trans-paths)
                                (seq (:filters expr-evals)) (seq (:binds expr-evals)))
          out-ch (async/chan 1 (map #(dissoc % ::iceberg-patterns)))

          ;; Create the execute-inner-fn for anti-joins
          execute-inner-fn (fn [inner-patterns outer-solution]
                             (let [pattern-groups (query/group-patterns-by-table inner-patterns mappings routing-indexes)]
                               (cond
                                 (empty? pattern-groups)
                                 []

                                 (= 1 (count pattern-groups))
                                 (let [{:keys [mapping patterns]} (first pattern-groups)
                                       table-name (:table mapping)
                                       source (get sources table-name)]
                                   (if-not source
                                     []
                                     (if use-columnar?
                                       (exec/execute-columnar-single-table
                                        source mapping patterns outer-solution time-travel nil)
                                       (query/execute-iceberg-query
                                        source mapping patterns outer-solution time-travel nil nil nil mappings))))

                                 :else
                                 (if use-columnar?
                                   (exec/execute-columnar-multi-table
                                    sources pattern-groups outer-solution time-travel nil join-graph)
                                   (exec/execute-multi-table-hash-join
                                    sources pattern-groups outer-solution time-travel nil join-graph mappings)))))]

      (when (seq atom-pushdown)
        (log/debug "Iceberg -finalize using VALUES pushdown from atom:" atom-pushdown))
      (when agg-info
        (log/debug "Iceberg -finalize will apply aggregation:" agg-info))
      (when (seq anti-joins)
        (log/debug "Iceberg -finalize will apply anti-joins:" {:count (count anti-joins)
                                                               :types (mapv :type anti-joins)}))
      (when (or (seq (:filters expr-evals)) (seq (:binds expr-evals)))
        (log/debug "Iceberg -finalize will apply expression evaluators:"
                   {:filters (count (:filters expr-evals))
                    :binds (count (:binds expr-evals))}))
      (when (seq trans-paths)
        (log/debug "Iceberg -finalize will apply transitive paths:"
                   {:count (count trans-paths)
                    :tags (mapv :tag trans-paths)}))
      (when use-columnar?
        (log/debug "Iceberg -finalize using Phase 3 columnar execution"))

      ;; Use pipeline-async with thread (not go) for blocking I/O operations
      ;; Iceberg queries involve lazy seq realization with actual I/O, which would
      ;; block the limited go thread pool and cause contention under load
      (if needs-collection?
        ;; Collection path: collect all solutions, apply anti-joins/aggregation, then emit
        (async/thread
          (try
            (let [all-solutions (atom [])]
              ;; Process each incoming solution
              (loop []
                (when-let [solution (async/<!! solution-ch)]
                  (let [patterns (get solution ::iceberg-patterns)
                        ;; If we have transitive patterns, expand the solution first
                        ;; by executing transitive patterns to get additional variable bindings
                        base-solutions (if (seq trans-paths)
                                         (let [trans-results (transitive/apply-transitive-patterns
                                                              sources mappings routing-indexes
                                                              trans-paths solution time-travel)]
                                           (if (seq trans-results)
                                             trans-results
                                             ;; No transitive results - don't continue
                                             []))
                                         ;; No transitive patterns - use incoming solution
                                         [solution])]
                    ;; Execute regular patterns for each base solution
                    (doseq [base-sol base-solutions]
                      (if (seq patterns)
                        (let [solution-pushdown (into (or (get solution ::solution-pushdown-filters) [])
                                                      (or atom-pushdown []))]
                          ;; Execute query and collect results
                          (if (query/has-union-patterns? patterns)
                            ;; UNION path
                            (let [{:keys [union-patterns regular-patterns]} (query/extract-union-patterns patterns)
                                  results (exec/execute-union-patterns
                                           sources mappings routing-indexes join-graph
                                           union-patterns regular-patterns base-sol
                                           time-travel solution-pushdown use-columnar?)]
                              (swap! all-solutions into results))
                            ;; Standard path
                            (let [pattern-groups (query/group-patterns-by-table patterns mappings routing-indexes)]
                              (if (= 1 (count pattern-groups))
                                ;; Single table
                                (let [{:keys [mapping patterns]} (first pattern-groups)
                                      table-name (:table mapping)
                                      source (get sources table-name)]
                                  (when source
                                    (let [results (if use-columnar?
                                                    (exec/execute-columnar-single-table
                                                     source mapping patterns base-sol
                                                     time-travel solution-pushdown)
                                                    (query/execute-iceberg-query
                                                     source mapping patterns base-sol
                                                     time-travel nil solution-pushdown nil mappings))]
                                      (swap! all-solutions into results))))
                                ;; Multiple tables
                                (let [results (if use-columnar?
                                                (exec/execute-columnar-multi-table
                                                 sources pattern-groups base-sol
                                                 time-travel solution-pushdown join-graph)
                                                (exec/execute-multi-table-hash-join
                                                 sources pattern-groups base-sol
                                                 time-travel solution-pushdown join-graph mappings))]
                                  (swap! all-solutions into results))))))
                        ;; No regular patterns - just add transitive results
                        (swap! all-solutions conj base-sol))))
                  (recur)))
              ;; Apply expression evaluators first (BIND then FILTER)
              ;; This happens before anti-joins so bound vars are available
              (let [after-expressions (if expr-evals
                                        (vec (expr/apply-expression-evaluators @all-solutions expr-evals))
                                        @all-solutions)
                    ;; NOTE: Subqueries are handled by standard Fluree execution via
                    ;; match-pattern :query, not here.
                    ;; Apply anti-joins (before query modifiers)
                    after-anti-joins (if (seq anti-joins)
                                       (antijoin/apply-anti-joins after-expressions anti-joins execute-inner-fn)
                                       after-expressions)
                    ;; Apply query modifiers (aggregation, DISTINCT, ORDER BY, LIMIT)
                    modified (if agg-info
                               (modifiers/finalize-query-modifiers after-anti-joins agg-info mappings)
                               after-anti-joins)]
                (log/debug "Query modifiers applied:" {:input (count @all-solutions)
                                                       :after-expressions (count after-expressions)
                                                       :after-anti-joins (count after-anti-joins)
                                                       :output (count modified)})
                (doseq [sol modified]
                  (async/>!! out-ch sol))))
            (catch Exception e
              (log/error e "Error in Iceberg aggregation")
              (async/>!! error-ch e))
            (finally
              (async/close! out-ch))))

        ;; Non-aggregation path: stream solutions directly
        (async/pipeline-async
         2
         out-ch
         (fn [solution ch]
           (async/thread
             (try
               (let [patterns (get solution ::iceberg-patterns)]
                 (if (seq patterns)
                   ;; Combine: pattern metadata pushdown (FILTER) + atom pushdown (VALUES)
                   ;; Pattern metadata may not survive WHERE executor, but atom path is reliable
                   (let [solution-pushdown (into (or (get solution ::solution-pushdown-filters) [])
                                                 (or atom-pushdown []))]
                     (when (seq solution-pushdown)
                       (log/debug "Iceberg -finalize combined solution pushdown:" solution-pushdown))

                     ;; Check for UNION patterns - handle them specially
                     (if (query/has-union-patterns? patterns)
                       ;; UNION path - extract and execute UNION branches
                       (let [{:keys [union-patterns regular-patterns]} (query/extract-union-patterns patterns)
                             final-solutions (exec/execute-union-patterns
                                              sources mappings routing-indexes join-graph
                                              union-patterns regular-patterns solution
                                              time-travel solution-pushdown use-columnar?)]
                         (log/debug "UNION execution complete:" {:union-count (count union-patterns)
                                                                 :result-count (count final-solutions)})
                         (doseq [sol final-solutions]
                           (async/>!! ch sol))
                         (async/close! ch))

                       ;; Standard path - no UNION patterns
                       (let [pattern-groups (query/group-patterns-by-table patterns mappings routing-indexes)]
                         (if (= 1 (count pattern-groups))
                           ;; Single table - simple case
                           (let [{:keys [mapping patterns]} (first pattern-groups)
                                 table-name (:table mapping)
                                 source (get sources table-name)]
                             (when-not source
                               (throw (ex-info (str "No source found for table: " table-name)
                                               {:error :db/missing-source
                                                :table table-name
                                                :available-sources (keys sources)})))
                             ;; Use columnar path when enabled
                             (let [solutions (if use-columnar?
                                               (exec/execute-columnar-single-table
                                                source mapping patterns solution
                                                time-travel solution-pushdown)
                                               (query/execute-iceberg-query source mapping patterns solution
                                                                            time-travel nil solution-pushdown nil mappings))]
                               (doseq [sol solutions]
                                 (async/>!! ch sol))
                               (async/close! ch)))
                           ;; Multiple tables - use hash join when join graph available
                           (let [final-solutions (if use-columnar?
                                                   (exec/execute-columnar-multi-table
                                                    sources pattern-groups solution
                                                    time-travel solution-pushdown join-graph)
                                                   (exec/execute-multi-table-hash-join
                                                    sources pattern-groups solution
                                                    time-travel solution-pushdown join-graph mappings))]
                             (doseq [sol final-solutions]
                               (async/>!! ch sol))
                             (async/close! ch))))))
                   (do (async/>!! ch solution)
                       (async/close! ch))))
               (catch Exception e
                 (log/error e "Error in Iceberg query execution")
                 (async/>!! error-ch e)
                 (async/close! ch)))))
         solution-ch))
      out-ch))

  optimize/Optimizable
  (-reorder [_ parsed-query]
    (go
      ;; Clear any stale specs from previous queries
      (when query-pushdown
        (reset! query-pushdown nil))
      (when aggregation-spec
        (reset! aggregation-spec nil))
      (when anti-join-spec
        (reset! anti-join-spec nil))
      (when expression-evaluators
        (reset! expression-evaluators nil))
      (when transitive-spec
        (reset! transitive-spec nil))
      (let [where-patterns (:where parsed-query)
            ;; Helper to extract pattern type from both MapEntry and vector formats
            ;; SPARQL translator produces vectors like ["not-exists" [...]]
            ;; FQL parser produces MapEntry like [:not-exists [...]]
            get-pattern-type (fn [pattern]
                               (cond
                                 (map-entry? pattern) (key pattern)
                                 (vector? pattern) (let [first-elem (first pattern)]
                                                     (cond
                                                       (keyword? first-elem) first-elem
                                                       (string? first-elem) (keyword first-elem)
                                                       :else :tuple))
                                 :else :tuple))
            ;; Helper to extract pattern data
            get-pattern-data (fn [pattern]
                               (cond
                                 (map-entry? pattern) (val pattern)
                                 (vector? pattern) (second pattern)
                                 :else pattern))]
        (if (seq where-patterns)
          ;; Separate different pattern types
          ;; Handles both MapEntry and vector pattern formats
          (let [{filters true, non-filters false}
                (group-by #(= :filter (get-pattern-type %)) where-patterns)

                {values-patterns true, other-patterns false}
                (group-by #(= :values (get-pattern-type %)) non-filters)

                ;; Separate BIND patterns - they'll be evaluated in -finalize
                {bind-patterns true, non-bind-patterns false}
                (group-by #(= :bind (get-pattern-type %)) other-patterns)

                ;; Extract anti-join patterns (EXISTS, NOT EXISTS, MINUS)
                ;; These are evaluated after the main query in -finalize
                anti-join-types #{:exists :not-exists :minus}
                {anti-join-patterns true, regular-patterns false}
                (group-by #(contains? anti-join-types (get-pattern-type %)) non-bind-patterns)

                ;; Store anti-join patterns for -finalize if present
                _ (when (and anti-join-spec (seq anti-join-patterns))
                    (let [parsed-anti-joins
                          (mapv (fn [pattern]
                                  ;; Extract type and data, normalizing to keywords
                                  {:type (get-pattern-type pattern)
                                   :patterns (get-pattern-data pattern)})
                                anti-join-patterns)]
                      (log/debug "Iceberg -reorder storing anti-join patterns:"
                                 {:count (count parsed-anti-joins)
                                  :types (mapv :type parsed-anti-joins)})
                      (reset! anti-join-spec parsed-anti-joins)))

                ;; NOTE: Subquery patterns (:query) are NOT handled specially here.
                ;; They stay in the WHERE clause and are processed by:
                ;; 1. exec/prep-subqueries (compiles raw subquery maps into executor functions)
                ;; 2. match-pattern :query (calls the executor functions during WHERE processing)
                ;; This ensures proper isolation - each subquery gets its own execution context.

                ;; Detect transitive property path patterns (pred+ or pred*)
                ;; These are tuple patterns where the predicate has ::recur tag (:one+ or :zero+)
                ;; They need special execution in -finalize (iterative BFS traversal)
                ;; IMPORTANT: Must return boolean (not nil) for proper group-by grouping
                is-transitive-pattern?
                (fn [pattern]
                  (boolean
                   (when (= :tuple (get-pattern-type pattern))
                     (let [triple (if (map-entry? pattern) (val pattern) pattern)
                           [_s p _o] triple]
                       (where/get-transitive-property p)))))

                {transitive-patterns true, non-transitive-patterns false}
                (group-by is-transitive-pattern? regular-patterns)

                ;; Store transitive patterns for execution in -finalize
                _ (when (and transitive-spec (seq transitive-patterns))
                    (let [parsed-trans
                          (mapv (fn [pattern]
                                  (let [triple (if (map-entry? pattern) (val pattern) pattern)
                                        [s p o] triple
                                        tag (where/get-transitive-property p)
                                        ;; Remove transitivity from predicate for regular lookup
                                        p* (where/remove-transitivity p)]
                                    {:subject s
                                     :predicate p*
                                     :object o
                                     :tag tag ;; :one+ or :zero+
                                     :original-pattern pattern}))
                                transitive-patterns)]
                      (log/debug "Iceberg -reorder storing transitive patterns:"
                                 {:count (count parsed-trans)
                                  :tags (mapv :tag parsed-trans)})
                      (reset! transitive-spec parsed-trans)))

                ;; Use non-transitive patterns for normal WHERE processing
                other-patterns non-transitive-patterns

                ;; Analyze each filter for pushability
                analyzed (map pushdown/analyze-filter-pattern filters)
                {pushable true, non-pushable false}
                (group-by :pushable? analyzed)

                ;; Store non-pushable filters and BIND patterns in expression-evaluators
                ;; These will be evaluated in -finalize after Iceberg scan
                ;; FILTER patterns already have compiled functions in pattern-data
                ;; BIND patterns have {var {::where/var v, ::where/fn f}} in pattern-data
                _ (when (and expression-evaluators (or (seq non-pushable) (seq bind-patterns)))
                    (let [;; Extract compiled filter functions from non-pushable filters
                          filter-fns (mapv (fn [{:keys [pattern]}]
                                             {:fn (where/pattern-data pattern)
                                              :meta (meta pattern)})
                                           non-pushable)
                          ;; Extract BIND specs (already compiled)
                          bind-specs (mapv (fn [bp]
                                             (where/pattern-data bp))
                                           bind-patterns)]
                      (log/debug "Iceberg -reorder storing expression evaluators:"
                                 {:non-pushable-filters (count filter-fns)
                                  :binds (count bind-specs)})
                      (reset! expression-evaluators
                              {:filters filter-fns
                               :binds bind-specs})))

                ;; Extract pushable VALUES patterns (single-var with literals)
                values-predicates (keep pushdown/extract-values-in-predicate values-patterns)

                ;; Build direct pushdown map {column -> [predicates]}
                ;; This survives the query optimization pipeline
                ;; Values are coerced based on column datatype from mapping
                direct-pushdown-map
                (reduce
                 (fn [m {:keys [var values]}]
                   (let [binding-idx (pushdown/find-first-binding-pattern other-patterns var)]
                     (if binding-idx
                       (let [pred-iri (pushdown/var->predicate-iri other-patterns var)
                             pred->mappings (:predicate->mappings routing-indexes)
                             ;; Takes first when multiple mappings exist
                             routed-mapping (first (get pred->mappings pred-iri))
                             obj-map (get-in routed-mapping [:predicates pred-iri])
                             column (when (and obj-map (= :column (:type obj-map)))
                                      (:value obj-map))
                            ;; Coerce values consistently (datatype if present; schema fallback if available)
                             ctx (when routed-mapping (pushdown/build-coercion-ctx routed-mapping))
                             pred (when (and ctx column)
                                    (pushdown/coerce-predicate ctx {:op :in :column column :value values}))]
                         (if pred
                           (update m column (fnil conj []) {:op :in :value (:value pred)})
                           (do
                             (log/debug "Skipping VALUES pushdown - coercion failed or no column mapping for var:"
                                        {:var var :pred-iri pred-iri
                                         :routed-mapping (boolean routed-mapping)})
                             m)))
                       (do
                         (log/debug "Skipping VALUES pushdown - no binding pattern for var:" var)
                         m))))
                 {}
                 values-predicates)

                ;; Annotate patterns with FILTER pushdown metadata
                ;; Returns {:patterns [...] :failed [...]} where failed contains analyses
                ;; that couldn't be pushed (e.g., BIND-created vars with no column mapping)
                {:keys [patterns failed-pushable]}
                (if (seq pushable)
                  (let [{:keys [patterns failed]} (pushdown/annotate-patterns-with-pushdown
                                                   other-patterns pushable mappings routing-indexes)]
                    {:patterns patterns :failed-pushable failed})
                  {:patterns (vec other-patterns) :failed-pushable []})

                ;; Build explicit FILTER pushdown predicates (survives executor path)
                ;; This duplicates the annotation logic but stores predicates in an atom
                ;; to avoid metadata loss through the WHERE executor
                filter-pushdown-predicates
                (when (seq pushable)
                  (let [pred->mappings (:predicate->mappings routing-indexes)]
                    (->> pushable
                         (keep (fn [{:keys [comparisons vars]}]
                                 (let [var (first vars)
                                       pred-iri (pushdown/var->predicate-iri other-patterns var)
                                       routed-mapping (first (get pred->mappings pred-iri))
                                       obj-map (get-in routed-mapping [:predicates pred-iri])
                                       column (when (and obj-map (= :column (:type obj-map)))
                                                (:value obj-map))
                                       ctx (when routed-mapping (pushdown/build-coercion-ctx routed-mapping))]
                                   (when column
                                     ;; Coerce and build predicates for this filter
                                     (keep (fn [comp]
                                             (when ctx
                                               (pushdown/coerce-predicate ctx {:op (:op comp)
                                                                               :column column
                                                                               :value (:value comp)})))
                                           comparisons)))))
                         (apply concat)
                         vec)))

                ;; Annotate patterns with VALUES/IN pushdown metadata
                final-patterns (if (seq values-predicates)
                                 (pushdown/annotate-values-pushdown
                                  patterns values-predicates mappings routing-indexes)
                                 patterns)

                ;; Track which vars were successfully pushed to Iceberg
                ;; These VALUES patterns should be REMOVED from WHERE to avoid double-application
                pushed-vars (set (keep (fn [{:keys [var]}]
                                         (let [binding-idx (pushdown/find-first-binding-pattern other-patterns var)]
                                           (when binding-idx
                                             (let [pred-iri (pushdown/var->predicate-iri other-patterns var)
                                                   pred->mappings (:predicate->mappings routing-indexes)
                                                   ;; Takes first when multiple mappings exist
                                                   routed-mapping (first (get pred->mappings pred-iri))
                                                   column (when routed-mapping
                                                            (when-let [obj-map (get-in routed-mapping [:predicates pred-iri])]
                                                              (when (= :column (:type obj-map))
                                                                (:value obj-map))))]
                                               (when column var)))))
                                       values-predicates))

                ;; Filter out VALUES patterns that were fully pushed to avoid double-application
                ;; Keep VALUES patterns for vars that couldn't be pushed (no column mapping, etc.)
                unpushed-values-patterns
                (remove (fn [vp]
                          (when-let [{:keys [var]} (pushdown/extract-values-in-predicate vp)]
                            (contains? pushed-vars var)))
                        values-patterns)

                _ (when (and (seq values-patterns) (seq pushed-vars))
                    (log/debug "VALUES pushdown - removing pushed patterns from WHERE:"
                               {:pushed-vars pushed-vars
                                :original-count (count values-patterns)
                                :remaining-count (count unpushed-values-patterns)}))

                ;; Reconstruct where: annotated patterns + only UNPUSHED VALUES patterns
                ;; - Pushable filters are handled via metadata annotation on patterns
                ;; - Non-pushable filters are stored in expression-evaluators for -finalize
                ;; - BIND patterns are stored in expression-evaluators for -finalize
                ;; - Pushed VALUES are handled via pattern metadata
                new-where (-> final-patterns
                              (into unpushed-values-patterns))

                ;; Flatten direct-pushdown-map to a vector of predicates
                ;; Format: [{:op :in :column "country" :value ["US" "Canada"]} ...]
                atom-pushdown-predicates
                (->> direct-pushdown-map
                     (mapcat (fn [[column preds]]
                               (map #(assoc % :column column) preds)))
                     vec)

                _ (log/debug "Iceberg filter pushdown:"
                             {:total-filters (count filters)
                              :pushable-filters (count pushable)
                              :failed-pushable (count failed-pushable)
                              :filter-pushdown-predicates (count filter-pushdown-predicates)
                              :values-patterns (count values-patterns)
                              :values-in-predicates (count values-predicates)
                              :atom-pushdown-predicates (count atom-pushdown-predicates)
                              :patterns-annotated (count (filter #(::pushdown/pushdown-filters (meta %))
                                                                 final-patterns))})

                ;; Add failed-pushable filters to expression-evaluators
                ;; These are filters that were structurally pushable but couldn't be pushed
                ;; (e.g., BIND-created vars with no column mapping)
                _ (when (and expression-evaluators (seq failed-pushable))
                    (let [failed-filter-fns (mapv (fn [{:keys [pattern]}]
                                                    {:fn (where/pattern-data pattern)
                                                     :meta (meta pattern)})
                                                  failed-pushable)]
                      (log/debug "Adding failed-pushable filters to expression evaluators:"
                                 {:count (count failed-filter-fns)})
                      (swap! expression-evaluators
                             update :filters into failed-filter-fns)))

                ;; Combine FILTER and VALUES pushdown predicates for storage in atom
                ;; This explicit storage survives the executor path (unlike pattern metadata)
                all-pushdown-predicates (into (vec atom-pushdown-predicates)
                                              filter-pushdown-predicates)

                ;; Store combined pushdown predicates in the atom for retrieval in -finalize
                _ (when (and query-pushdown (seq all-pushdown-predicates))
                    (log/debug "Storing pushdown predicates in atom:"
                               {:filter-count (count filter-pushdown-predicates)
                                :values-count (count atom-pushdown-predicates)
                                :total (count all-pushdown-predicates)})
                    (reset! query-pushdown all-pushdown-predicates))

                ;; Extract query modifiers for use in -finalize
                ;; Includes aggregation, DISTINCT, HAVING, ORDER BY, LIMIT/OFFSET
                ;; Handle both :selectDistinct (SPARQL) and :select-distinct (FQL)
                distinct? (or (some? (:selectDistinct parsed-query))
                              (some? (:select-distinct parsed-query)))
                has-modifiers? (or (query/has-aggregations? parsed-query)
                                   distinct?
                                   (:having parsed-query)
                                   (:orderBy parsed-query)
                                   (:order-by parsed-query)
                                   (:limit parsed-query)
                                   (:offset parsed-query))
                query-info (when has-modifiers?
                             {:select (or (:select parsed-query)
                                          (:selectDistinct parsed-query)
                                          (:select-distinct parsed-query))
                              :group-by (:group-by parsed-query)
                              ;; HAVING is a pre-compiled filter function (compiled in parse.cljc)
                              :having (:having parsed-query)
                              ;; Handle both :orderBy (SPARQL translator) and :order-by (JSON-LD)
                              :order-by (or (:orderBy parsed-query) (:order-by parsed-query))
                              :distinct? distinct?
                              :limit (:limit parsed-query)
                              :offset (:offset parsed-query)})
                _ (when (and aggregation-spec query-info)
                    (log/debug "Iceberg -reorder storing query modifiers:" query-info)
                    (reset! aggregation-spec query-info))

                ;; Check if VG is handling aggregation
                vg-handles-aggregation? (query/has-aggregations? parsed-query)

                ;; Get output format for creating new selectors
                ;; Use :output from opts, which defaults to :fql
                ;; (:format controls input format, :output controls output format - they are independent)
                output-format (or (get-in parsed-query [:opts :output]) :fql)

                ;; Get the current select clause
                current-select (or (:select parsed-query)
                                   (:selectDistinct parsed-query)
                                   (:select-distinct parsed-query))

                _ (when vg-handles-aggregation?
                    (log/debug "Iceberg -reorder transforming aggregation:"
                               {:vg-handles-aggregation? vg-handles-aggregation?
                                :output-format output-format
                                :opts-keys (keys (:opts parsed-query))
                                :opts-output (get-in parsed-query [:opts :output])
                                :opts-format (get-in parsed-query [:opts :format])
                                :current-select-types (mapv type current-select)}))]

            ;; Store direct pushdown map in query opts for retrieval in -finalize
            ;; When VG handles aggregation, also:
            ;; - Remove :group-by so group/combine doesn't run again
            ;; - Transform aggregate selectors to simple variable selectors
            (cond-> parsed-query
              true (assoc :where new-where)
              true (assoc-in [:opts ::iceberg-direct-pushdown] direct-pushdown-map)
              ;; When VG handles aggregation, modify query to skip executor's aggregation
              vg-handles-aggregation?
              (-> (dissoc :group-by)
                  (assoc :select (modifiers/transform-aggregates-to-variables current-select output-format))
                  ;; Remove selectDistinct/select-distinct if present (we'll apply DISTINCT in VG)
                  (dissoc :selectDistinct :select-distinct))))
          parsed-query))))

  (-explain [_ parsed-query]
    (go
      (let [where-patterns (:where parsed-query)
            {filters true, non-filters false}
            (group-by #(= :filter (first %)) where-patterns)
            {values-patterns true, _other-patterns false}
            (group-by #(= :values (first %)) non-filters)
            analyzed (map pushdown/analyze-filter-pattern filters)
            {pushable true, _not-pushable false}
            (group-by :pushable? analyzed)
            values-predicates (keep pushdown/extract-values-in-predicate values-patterns)]
        {:original parsed-query
         :optimized parsed-query
         :segments []
         :changed? (or (boolean (seq pushable)) (boolean (seq values-predicates)))
         :iceberg-pushdown {:total-filters (count filters)
                            :pushable-filters (count pushable)
                            :pushed-ops (mapv #(-> % :comparisons first :op) pushable)
                            :values-patterns (count values-patterns)
                            :values-in-predicates (count values-predicates)
                            :values-vars (mapv :var values-predicates)}}))))

;;; ---------------------------------------------------------------------------
;;; Public API
;;; ---------------------------------------------------------------------------

;; Re-export factory functions for backwards compatibility

(def parse-time-travel
  "Convert time-travel value from parse-ledger-alias to Iceberg format.
   See fluree.db.virtual-graph.iceberg.factory/parse-time-travel for details."
  factory/parse-time-travel)

(def with-time-travel
  "Create a view of this IcebergDatabase pinned to a specific snapshot.
   See fluree.db.virtual-graph.iceberg.factory/with-time-travel for details."
  factory/with-time-travel)

(defn create
  "Create an IcebergDatabase virtual graph with multi-table support.

   Registration-time alias format:
     <name>           - defaults to :main branch
     <name>:<branch>  - explicit branch

   Time-travel is a QUERY-TIME concern, not registration-time.
   At query time, use FROM <alias@t:snapshot-id> or FROM <alias@iso:timestamp>
   to specify which snapshot to query.

   Multi-Table Support:
     The R2RML mapping can define multiple TriplesMap entries, each mapping
     a different table to a different RDF class. This VG will automatically:
     - Create an IcebergSource for each unique table in the mappings
     - Route query patterns to the appropriate table based on class/predicate
     - Execute cross-table joins using nested loop join strategy

   Examples:
     Registration: 'openflights-vg' (with R2RML mapping airlines, airports, routes)
     Query: SELECT ?airline ?airport WHERE { ?airline a :Airline . ?airport a :Airport }

   Config:
     :alias          - Virtual graph alias with optional branch (required)
     :config         - Configuration map containing:
       :warehouse-path  - Path to Iceberg warehouse (for HadoopTables)
       :store           - Fluree storage store (for FlureeIcebergSource)
       :metadata-location - Direct path to metadata JSON (optional)
       :mapping         - Path to R2RML mapping file
       :mappingInline   - Inline R2RML mapping (Turtle or JSON-LD)
       :catalog         - Optional catalog config (inline or catalog-name reference)
     :iceberg-config - Optional publisher-level Iceberg config (catalogs, cache, etc.)
     :cache-instance - Optional shared cache instance from publisher

   Either :warehouse-path or :store must be provided."
  [{:keys [alias config iceberg-config cache-instance]}]
  (map->IcebergDatabase
   (factory/build-database-map alias config iceberg-config cache-instance)))
