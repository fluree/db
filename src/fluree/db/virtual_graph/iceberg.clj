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
            [clojure.set]
            [clojure.string :as str]
            [fluree.db.constants :as const]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.optimize :as optimize]
            [fluree.db.tabular.iceberg :as iceberg]
            [fluree.db.tabular.protocol :as tabular]
            [fluree.db.util.async :refer [empty-channel]]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph :as vg]
            [fluree.db.virtual-graph.iceberg.join :as join]
            [fluree.db.virtual-graph.iceberg.join.hash :as hash-join]
            [fluree.db.virtual-graph.iceberg.pushdown :as pushdown]
            [fluree.db.virtual-graph.iceberg.query :as query]
            [fluree.db.virtual-graph.iceberg.r2rml :as r2rml])
  (:import [java.time Instant]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; Multi-Table Join Execution
;;; ---------------------------------------------------------------------------

(defn- collect-join-columns-for-table
  "Collect all join column names for a table from the join graph.

   Returns a set of column names (strings) that this table uses in joins,
   both as child and parent columns."
  [join-graph table-name]
  (when join-graph
    (let [edges (join/edges-for-table join-graph table-name)]
      (into #{}
            (for [edge edges
                  col (if (= table-name (:child-table edge))
                        (join/child-columns edge)
                        (join/parent-columns edge))]
              col)))))

(defn- extract-pattern-predicate
  "Extract the predicate IRI from a pattern item."
  [item]
  (let [triple (if (and (vector? item) (= :class (first item)))
                 (second item)
                 item)
        [_s p _o] triple]
    (when (map? p)
      (::where/iri p))))

(defn- extract-pattern-subject-var
  "Extract the subject variable from a pattern item."
  [item]
  (let [triple (if (and (vector? item) (= :class (first item)))
                 (second item)
                 item)
        [s _p _o] triple]
    (when (and (map? s) (::where/var s))
      (::where/var s))))

(defn- extract-pattern-object-var
  "Extract the object variable from a pattern item."
  [item]
  (let [triple (if (and (vector? item) (= :class (first item)))
                 (second item)
                 item)
        [_s _p o] triple]
    (when (and (map? o) (::where/var o))
      (::where/var o))))

(defn- patterns-traverse-join-edge?
  "Check if patterns actually traverse a join edge via shared variables.

   A join edge is traversed when:
   1. The child patterns use the RefObjectMap predicate (:predicate on edge)
   2. The object variable of that pattern matches the subject of parent patterns

   This ensures joins are only applied when the SPARQL query explicitly
   traverses the FK relationship, not just because tables happen to be related.

   Arguments:
     child-patterns  - Patterns for the child table (with FK)
     parent-patterns - Patterns for the parent table (with PK)
     edge            - Join edge containing :predicate for the FK relationship

   Returns true if the join should be applied."
  [child-patterns parent-patterns edge]
  (let [fk-predicate (:predicate edge)]
    (when fk-predicate
      ;; Find patterns in child that use the FK predicate
      (let [fk-patterns (filter #(= fk-predicate (extract-pattern-predicate %)) child-patterns)]
        (when (seq fk-patterns)
          ;; Get object variables from FK patterns
          (let [fk-object-vars (set (keep extract-pattern-object-var fk-patterns))
                ;; Get subject variables from parent patterns
                parent-subject-vars (set (keep extract-pattern-subject-var parent-patterns))]
            ;; Join is traversed if any FK object var matches a parent subject var
            (boolean (seq (clojure.set/intersection fk-object-vars parent-subject-vars)))))))))

(defn- find-traversed-edge
  "Find a join edge that is actually traversed by the query patterns.

   Checks both directions (child->parent and parent->child) to find an
   edge where the patterns explicitly use the FK predicate with matching variables.

   Returns {:edge edge :child-table :parent-table} or nil if no traversed edge found."
  [join-graph accumulated-patterns current-patterns accumulated-tables current-table]
  (first
   (for [acc-table accumulated-tables
         edge (join/edges-between join-graph acc-table current-table)
         :let [child-table (:child-table edge)
               parent-table (:parent-table edge)
               ;; Determine which patterns belong to child vs parent
               ;; When current-table is child: child-patterns = current, parent = acc-table
               ;; When current-table is parent: child-patterns = acc-table, parent = current
               [child-patterns parent-patterns]
               (if (= current-table child-table)
                 [current-patterns (get accumulated-patterns acc-table)]
                 [(get accumulated-patterns acc-table) current-patterns])]
         :when (patterns-traverse-join-edge? child-patterns parent-patterns edge)]
     {:edge edge
      :child-table child-table
      :parent-table parent-table
      :acc-table acc-table})))

(defn- execute-pattern-group
  "Execute a single pattern group against its Iceberg source.

   When join-columns is provided, those columns are included in the scan
   and their raw values are stored in the solution for hash join operations.

   Returns a lazy seq of solutions."
  [sources mapping patterns base-solution time-travel solution-pushdown join-columns]
  (let [table-name (:table mapping)
        source (get sources table-name)]
    (when-not source
      (throw (ex-info (str "No source found for table: " table-name)
                      {:error :db/missing-source
                       :table table-name
                       :available-sources (keys sources)})))
    (query/execute-iceberg-query source mapping patterns base-solution
                                 time-travel nil solution-pushdown join-columns)))

(defn- execute-multi-table-hash-join
  "Execute a multi-table query using hash joins.

   Strategy:
   1. Collect join columns for each table from join graph
   2. Execute each table query independently (with join columns projected)
   3. Find join edges that are actually traversed by the query patterns
   4. Apply hash join only when patterns traverse the FK relationship
   5. Use SPARQL-compatible merge for overlapping variable bindings

   IMPORTANT: Join edges are only applied when the SPARQL query explicitly
   traverses the FK relationship via the RefObjectMap predicate. This prevents
   implicit joins from changing query semantics. If two tables appear in a query
   but the patterns don't traverse the FK, a Cartesian product is used.

   Falls back to Cartesian product with compatible-merge if no traversed edges exist."
  [sources pattern-groups solution time-travel solution-pushdown join-graph]
  (let [;; Collect join columns for each table so they're included in results
        table->join-cols (into {}
                               (for [{:keys [mapping]} pattern-groups
                                     :let [table (:table mapping)
                                           cols (collect-join-columns-for-table join-graph table)]
                                     :when (seq cols)]
                                 [table cols]))

        ;; Build table->patterns map for traversal checking
        table->patterns (into {}
                              (for [{:keys [mapping patterns]} pattern-groups]
                                [(:table mapping) patterns]))

        _ (log/debug "Join columns by table:" table->join-cols)

        ;; Execute all table queries with join columns projected
        group-results (mapv (fn [{:keys [mapping patterns]}]
                              (let [table (:table mapping)
                                    join-cols (get table->join-cols table)]
                                {:mapping mapping
                                 :patterns patterns
                                 :solutions (vec (execute-pattern-group
                                                  sources mapping patterns solution
                                                  time-travel solution-pushdown join-cols))}))
                            pattern-groups)

        _ (log/debug "Multi-table query executed:"
                     {:groups (count group-results)
                      :solution-counts (mapv #(count (:solutions %)) group-results)})]

    ;; Short-circuit if any group returns empty
    (if (some #(empty? (:solutions %)) group-results)
      []

      ;; Check if we have join edges to potentially use
      (if (and join-graph (join/has-join-edges? join-graph))
        ;; Use hash join strategy - but only for traversed edges
        (:accumulated-solutions
         (reduce
          (fn [{:keys [accumulated-solutions accumulated-tables accumulated-patterns]}
               {:keys [mapping patterns] :as current-group}]
            (if (empty? accumulated-solutions)
              {:accumulated-solutions []
               :accumulated-tables accumulated-tables
               :accumulated-patterns accumulated-patterns}

              ;; Find join relationship that is actually traversed by patterns
              (let [current-table (:table mapping)
                    current-solutions (:solutions current-group)

                    ;; Find a traversed edge (checks if patterns use the FK predicate)
                    traversed-edge (find-traversed-edge
                                    join-graph
                                    accumulated-patterns
                                    patterns
                                    accumulated-tables
                                    current-table)

                    _ (when traversed-edge
                        (log/debug "Found traversed join edge:" traversed-edge))

                    new-solutions
                    (if traversed-edge
                      ;; Hash join path - edge is actually traversed by patterns
                      (let [edge (:edge traversed-edge)
                            ;; Determine build vs probe based on child/parent relationship
                            current-is-child? (= current-table (:child-table edge))
                            [build-solutions probe-solutions build-cols probe-cols]
                            (if current-is-child?
                              ;; Current is child (fact table) -> accumulated is parent
                              [accumulated-solutions current-solutions
                               (mapv keyword (join/parent-columns edge))
                               (mapv keyword (join/child-columns edge))]
                              ;; Current is parent (dimension table) -> build with current
                              [current-solutions accumulated-solutions
                               (mapv keyword (join/parent-columns edge))
                               (mapv keyword (join/child-columns edge))])

                            _ (log/debug "Hash join execution:"
                                         {:build-count (count build-solutions)
                                          :probe-count (count probe-solutions)
                                          :build-cols build-cols
                                          :probe-cols probe-cols})

                            joined (hash-join/hash-join build-solutions probe-solutions
                                                        build-cols probe-cols)]
                        (log/debug "Hash join result count:" (count joined))
                        joined)

                      ;; No traversed edge - patterns don't use FK relationship
                      ;; Use Cartesian product with compatible-merge (SPARQL semantics)
                      (do
                        (log/debug "No traversed join edge, using Cartesian product:"
                                   {:accumulated-tables accumulated-tables
                                    :current-table current-table})
                        (vec (keep (fn [[acc curr]]
                                     (hash-join/compatible-merge acc curr))
                                   (for [acc accumulated-solutions
                                         curr current-solutions]
                                     [acc curr])))))]

                {:accumulated-solutions new-solutions
                 :accumulated-tables (conj accumulated-tables current-table)
                 :accumulated-patterns (assoc accumulated-patterns current-table patterns)})))

          ;; Start with first group's solutions and its table/patterns
          (let [first-group (first group-results)]
            {:accumulated-solutions (:solutions first-group)
             :accumulated-tables #{(get-in first-group [:mapping :table])}
             :accumulated-patterns {(get-in first-group [:mapping :table])
                                    (:patterns first-group)}})
          (rest group-results)))

        ;; No join graph - fall back to Cartesian with compatible-merge
        (do
          (log/debug "No join graph available, using Cartesian product")
          (reduce
           (fn [accumulated {:keys [solutions]}]
             (if (empty? accumulated)
               []
               ;; Use compatible-merge for SPARQL semantics
               (vec (keep (fn [[acc curr]]
                            (hash-join/compatible-merge acc curr))
                          (for [acc accumulated
                                curr solutions]
                            [acc curr])))))
           (:solutions (first group-results))
           (rest group-results)))))))

;;; ---------------------------------------------------------------------------
;;; IcebergDatabase Record (Multi-Table Support)
;;; ---------------------------------------------------------------------------

(defrecord IcebergDatabase [alias config sources mappings routing-indexes join-graph time-travel query-pushdown]
  ;; sources: {table-name -> IcebergSource}
  ;; mappings: {table-key -> {:table, :class, :predicates, ...}}
  ;; routing-indexes: {:class->mappings {rdf-class -> [mappings...]}, :predicate->mappings {pred -> [mappings...]}}
  ;; join-graph: {:edges [JoinEdge...], :by-table {table -> [edges]}, :tm->table {iri -> table}}
  ;; query-pushdown: atom holding query-time pushdown predicates (set in -reorder, used in -finalize)

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

  (-activate-alias [this _alias]
    (go this))

  (-aliases [_]
    [alias])

  (-finalize [_ _tracker error-ch solution-ch]
    (let [out-ch (async/chan 1 (map #(dissoc % ::iceberg-patterns)))
          ;; VALUES pushdown from atom - this is the primary path since pattern metadata
          ;; doesn't survive through the WHERE executor (known limitation)
          values-pushdown (when query-pushdown @query-pushdown)]
      (when (seq values-pushdown)
        (log/debug "Iceberg -finalize using VALUES pushdown from atom:" values-pushdown))
      ;; Use pipeline-async with thread (not go) for blocking I/O operations
      ;; Iceberg queries involve lazy seq realization with actual I/O, which would
      ;; block the limited go thread pool and cause contention under load
      (async/pipeline-async
       2
       out-ch
       (fn [solution ch]
         (async/thread
           (try
             (let [patterns (get solution ::iceberg-patterns)]
               (if (seq patterns)
                 ;; Group patterns by table and execute each group
                 ;; Combine: pattern metadata pushdown (FILTER) + atom pushdown (VALUES)
                 ;; Pattern metadata may not survive WHERE executor, but atom path is reliable
                 (let [pattern-groups (query/group-patterns-by-table patterns mappings routing-indexes)
                       solution-pushdown (into (or (get solution ::solution-pushdown-filters) [])
                                               (or values-pushdown []))]
                   (when (seq solution-pushdown)
                     (log/debug "Iceberg -finalize combined solution pushdown:" solution-pushdown))
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
                       (let [solutions (query/execute-iceberg-query source mapping patterns solution
                                                                    time-travel nil solution-pushdown)]
                         (doseq [sol solutions]
                           (async/>!! ch sol))
                         (async/close! ch)))
                       ;; Multiple tables - use hash join when join graph available
                     (let [final-solutions (execute-multi-table-hash-join
                                            sources pattern-groups solution
                                            time-travel solution-pushdown join-graph)]
                       (doseq [sol final-solutions]
                         (async/>!! ch sol))
                       (async/close! ch))))
                 (do (async/>!! ch solution)
                     (async/close! ch))))
             (catch Exception e
               (log/error e "Error in Iceberg query execution")
               (async/>!! error-ch e)
               (async/close! ch)))))
       solution-ch)
      out-ch))

  optimize/Optimizable
  (-reorder [_ parsed-query]
    (go
      ;; Clear any stale VALUES pushdown from previous queries
      (when query-pushdown
        (reset! query-pushdown nil))
      (let [where-patterns (:where parsed-query)]
        (if (seq where-patterns)
          ;; Separate different pattern types
          (let [{filters true, non-filters false}
                (group-by #(= :filter (first %)) where-patterns)

                {values-patterns true, other-patterns false}
                (group-by #(= :values (first %)) non-filters)

                ;; Analyze each filter for pushability
                analyzed (map pushdown/analyze-filter-pattern filters)
                {pushable true, _not-pushable false}
                (group-by :pushable? analyzed)

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
                             datatype (:datatype obj-map)
                             ;; Coerce values based on column datatype
                             coerced-values (mapv #(pushdown/coerce-value % datatype nil) values)]
                         (if column
                           (update m column (fnil conj []) {:op :in :value coerced-values})
                           (do
                             (log/debug "Skipping VALUES pushdown - no column mapping for var:"
                                        {:var var :pred-iri pred-iri
                                         :routed-mapping (boolean routed-mapping)})
                             m)))
                       (do
                         (log/debug "Skipping VALUES pushdown - no binding pattern for var:" var)
                         m))))
                 {}
                 values-predicates)

                ;; Annotate patterns with FILTER pushdown metadata
                annotated-patterns (if (seq pushable)
                                     (pushdown/annotate-patterns-with-pushdown
                                      other-patterns pushable mappings routing-indexes)
                                     (vec other-patterns))

                ;; Annotate patterns with VALUES/IN pushdown metadata
                final-patterns (if (seq values-predicates)
                                 (pushdown/annotate-values-pushdown
                                  annotated-patterns values-predicates mappings routing-indexes)
                                 annotated-patterns)

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

                ;; Reconstruct where: annotated patterns + filters + only UNPUSHED VALUES patterns
                ;; Pushed VALUES are handled via pattern metadata, not VALUES decomposition
                new-where (-> final-patterns
                              (into filters)
                              (into unpushed-values-patterns))

                ;; Flatten direct-pushdown-map to a vector of predicates
                ;; Format: [{:op :in :column "country" :value ["US" "Canada"]} ...]
                values-pushdown-predicates
                (->> direct-pushdown-map
                     (mapcat (fn [[column preds]]
                               (map #(assoc % :column column) preds)))
                     vec)

                _ (log/debug "Iceberg filter pushdown:"
                             {:total-filters (count filters)
                              :pushable-filters (count pushable)
                              :values-patterns (count values-patterns)
                              :values-in-predicates (count values-predicates)
                              :values-pushdown-predicates values-pushdown-predicates
                              :patterns-annotated (count (filter #(::pushdown/pushdown-filters (meta %))
                                                                 final-patterns))})

                ;; Store VALUES predicates in the atom for retrieval in -finalize
                _ (when (and query-pushdown (seq values-pushdown-predicates))
                    (reset! query-pushdown values-pushdown-predicates))]

            ;; Store direct pushdown map in query opts for retrieval in -finalize
            (-> parsed-query
                (assoc :where new-where)
                (assoc-in [:opts ::iceberg-direct-pushdown] direct-pushdown-map)))
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
;;; Factory
;;; ---------------------------------------------------------------------------

(defn parse-time-travel
  "Convert time-travel value from parse-ledger-alias to Iceberg format.

   Used at query-time to parse time-travel from FROM clause aliases.

   Input (from parse-ledger-alias :t value):
   - nil -> nil (latest snapshot)
   - Long -> {:snapshot-id Long} (t: syntax)
   - String -> {:as-of-time Instant} (iso: syntax)
   - {:sha ...} -> not supported for Iceberg, throws

   Output:
   - nil
   - {:snapshot-id Long}
   - {:as-of-time Instant}

   Example:
     (parse-time-travel 12345)
     ;; => {:snapshot-id 12345}

     (parse-time-travel \"2024-01-15T00:00:00Z\")
     ;; => {:as-of-time #inst \"2024-01-15T00:00:00Z\"}"
  [t-val]
  (cond
    (nil? t-val)
    nil

    (integer? t-val)
    {:snapshot-id t-val}

    (string? t-val)
    {:as-of-time (Instant/parse t-val)}

    (and (map? t-val) (:sha t-val))
    (throw (ex-info "SHA-based time travel not supported for Iceberg virtual graphs"
                    {:error :db/invalid-config :t t-val}))

    :else
    (throw (ex-info "Invalid time travel value"
                    {:error :db/invalid-config :t t-val}))))

(defn- validate-snapshot-exists
  "Validate that a snapshot exists in the Iceberg table.
   Returns the snapshot info if valid, throws if not found."
  [source table-name time-travel]
  (let [opts (cond-> {}
               (:snapshot-id time-travel)
               (assoc :snapshot-id (:snapshot-id time-travel))

               (:as-of-time time-travel)
               (assoc :as-of-time (:as-of-time time-travel)))
        stats (tabular/get-statistics source table-name opts)]
    (when-not stats
      (throw (ex-info "Snapshot not found for time-travel specification"
                      {:error :db/invalid-time-travel
                       :time-travel time-travel
                       :table table-name})))
    stats))

(defn with-time-travel
  "Create a view of this IcebergDatabase pinned to a specific snapshot.

   Validates that the snapshot/time exists before returning.
   Returns a new IcebergDatabase with time-travel set.

   Usage (from query resolver when parsing FROM <airlines@t:12345>):
     (let [{:keys [t]} (parse-ledger-alias \"airlines@t:12345\")
           time-travel (parse-time-travel t)]
       (with-time-travel registered-db time-travel))

   The returned database will use the specified snapshot for all queries.
   If time-travel is nil, returns the database unchanged (latest snapshot)."
  [iceberg-db time-travel]
  (if time-travel
    (let [{:keys [sources mappings]} iceberg-db
          ;; Validate against the first table (all tables should have same snapshot time for consistency)
          table-name (some-> mappings vals first :table)
          source (when table-name (get sources table-name))]
      (when (and table-name source)
        (validate-snapshot-exists source table-name time-travel))
      (assoc iceberg-db :time-travel time-travel))
    iceberg-db))

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

   Either :warehouse-path or :store must be provided."
  [{:keys [alias config]}]
  (let [;; Reject @ in alias - reserved character
        _ (when (str/includes? alias "@")
            (throw (ex-info (str "Virtual graph name cannot contain '@' character. Provided: " alias)
                            {:error :db/invalid-config :alias alias})))

        ;; Parse alias for name and branch only
        {:keys [ledger branch]} (util.ledger/parse-ledger-alias alias)
        base-alias (if branch (str ledger ":" branch) ledger)

        ;; Get warehouse/store config
        warehouse-path (or (:warehouse-path config)
                           (get config "warehouse-path")
                           (get config "warehousePath"))
        store (or (:store config) (get config "store"))
        metadata-location (or (:metadata-location config)
                              (get config "metadata-location")
                              (get config "metadataLocation"))

        ;; Catalog config (REST)
        catalog (or (:catalog config) (get config "catalog"))
        catalog-type (keyword (or (:type catalog) (get catalog "type")))
        rest-catalog? (= catalog-type :rest)

        _ (when-not (or warehouse-path store rest-catalog?)
            (throw (ex-info "Iceberg virtual graph requires :warehouse-path, :store, or REST :catalog"
                            {:error :db/invalid-config :config config})))

        ;; Get mapping
        mapping-source (or (:mappingInline config)
                           (get config "mappingInline")
                           (:mapping config)
                           (get config "mapping"))
        _ (when-not mapping-source
            (throw (ex-info "Iceberg virtual graph requires :mapping or :mappingInline"
                            {:error :db/invalid-config :config config})))

        ;; Parse R2RML mappings first to discover all tables
        mappings (r2rml/parse-r2rml mapping-source)

        ;; Extract unique table names from all mappings
        table-names (->> mappings
                         vals
                         (map :table)
                         (remove nil?)
                         distinct)

        ;; Create source factory function
        create-source-fn (cond
                           store
                           #(iceberg/create-fluree-iceberg-source
                             {:store store
                              :warehouse-path (or warehouse-path "")})

                           (= catalog-type :rest)
                           #(iceberg/create-rest-iceberg-source
                             {:uri (or (:uri catalog) (get catalog "uri"))
                              :warehouse (or (:warehouse catalog) (get catalog "warehouse"))
                              :auth-token (or (:auth-token catalog) (get catalog "auth-token"))
                              :headers (or (:headers catalog) (get catalog "headers"))
                              :properties (or (:properties catalog) (get catalog "properties"))})

                           :else
                           #(iceberg/create-iceberg-source
                             {:warehouse-path warehouse-path}))

        backend-desc (cond
                       store "store-backed"
                       rest-catalog? (str "rest:" (or (:uri catalog) (get catalog "uri")))
                       :else (str "warehouse:" warehouse-path))

        ;; Create an IcebergSource for each unique table
        ;; Note: Currently we use the same source for all tables in the same warehouse
        ;; In the future, we could optimize by sharing the source instance
        sources (into {}
                      (for [table-name table-names]
                        [table-name (create-source-fn)]))

        ;; Build routing indexes for efficient pattern-to-table mapping
        routing-indexes (query/build-routing-indexes mappings)

        ;; Build join graph from RefObjectMap declarations
        join-graph (join/build-join-graph mappings)]

    (log/info "Created Iceberg virtual graph:" base-alias backend-desc
              "tables:" (vec table-names)
              "mappings:" (count mappings)
              "join-edges:" (count (:edges join-graph)))

    (map->IcebergDatabase {:alias base-alias
                           :config (cond-> config
                                     metadata-location
                                     (assoc :metadata-location metadata-location))
                           :sources sources
                           :mappings mappings
                           :routing-indexes routing-indexes
                           :join-graph join-graph
                           :time-travel nil
                           :query-pushdown (atom nil)})))
