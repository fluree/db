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
            [fluree.db.datatype :as datatype]
            [fluree.db.query.exec.select :as select]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.optimize :as optimize]
            [fluree.db.tabular.iceberg :as iceberg]
            [fluree.db.tabular.iceberg.core :as iceberg-core]
            [fluree.db.tabular.protocol :as tabular]
            [fluree.db.util.async :refer [empty-channel]]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph :as vg]
            [fluree.db.virtual-graph.iceberg.join :as join]
            [fluree.db.virtual-graph.iceberg.join.hash :as hash-join]
            [fluree.db.virtual-graph.iceberg.plan :as plan]
            [fluree.db.virtual-graph.iceberg.pushdown :as pushdown]
            [fluree.db.virtual-graph.iceberg.query :as query]
            [fluree.db.virtual-graph.iceberg.r2rml :as r2rml])
  (:import [fluree.db.query.exec.select AsSelector]
           [java.time Instant]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; Cartesian Product Safety
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
  100000)

(def ^:dynamic *columnar-execution*
  "Enable Phase 3 columnar execution path.

   When true, uses the plan compiler and Arrow-batch operators for query
   execution, keeping data in columnar format through joins.

   When false (default), uses the row-based solution approach from Phase 2.

   This flag enables A/B testing between execution strategies:
     (binding [*columnar-execution* true]
       (execute-query ...))"
  false)

(defn- check-cartesian-product-size!
  "Check if a Cartesian product would exceed the safety threshold.
   Throws ex-info with helpful error message if threshold exceeded.

   Args:
     left-count  - Number of rows in left table
     right-count - Number of rows in right table
     left-table  - Name of left table (for error message)
     right-table - Name of right table (for error message)"
  [left-count right-count left-table right-table]
  (when *max-cartesian-product-size*
    (let [estimated-size (* left-count right-count)]
      (when (> estimated-size *max-cartesian-product-size*)
        (throw (ex-info
                (str "Cartesian product would produce " estimated-size " rows, "
                     "exceeding safety limit of " *max-cartesian-product-size* ". "
                     "This typically means the query is missing a join condition. "
                     "Ensure your SPARQL/FQL query uses the foreign key predicate "
                     "(e.g., ex:operatedBy) to link tables, not just column mappings. "
                     "Tables: " left-table " (" left-count " rows) × "
                     right-table " (" right-count " rows)")
                {:error :db/cartesian-product-too-large
                 :left-table left-table
                 :left-count left-count
                 :right-table right-table
                 :right-count right-count
                 :estimated-size estimated-size
                 :max-allowed *max-cartesian-product-size*}))))))

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

(defn- collect-all-join-columns
  "Collect all join key columns from the join graph.

   Returns a set of all column names used as join keys across all tables."
  [join-graph]
  (when join-graph
    (into #{}
          (for [edge (:edges join-graph)
                col (concat (join/parent-columns edge)
                            (join/child-columns edge))]
            col))))

(defn- extract-columns-from-pattern-groups
  "Extract all column names needed by the query from pattern groups.

   Looks at each pattern's predicate IRI and maps it to a column name
   via the R2RML mapping. Also includes columns from pushdown predicates.

   Returns a set of column names (strings) needed for the query."
  [pattern-groups predicates]
  (into #{}
        (concat
         ;; Columns from pushdown predicates
         (keep :column predicates)
         ;; Columns from pattern predicates mapped via R2RML
         (for [{:keys [mapping patterns]} pattern-groups
               pattern patterns
               :let [;; Extract predicate IRI from pattern
                     triple (if (and (vector? pattern) (= :class (first pattern)))
                              (second pattern)
                              pattern)
                     [_s p _o] triple
                     pred-iri (when (map? p) (::where/iri p))
                     ;; Map predicate IRI to column via R2RML mapping
                     object-map (when pred-iri
                                  (get-in mapping [:predicates pred-iri]))
                     column (when (and (map? object-map)
                                       (= :column (:type object-map)))
                              (:value object-map))]
               :when column]
           column))))

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
               ;; Determine patterns for child vs parent tables.
               ;; One side is current (provided as current-patterns), and the other
               ;; must already exist in accumulated-patterns.
               child-patterns (cond
                                (= current-table child-table) current-patterns
                                (contains? accumulated-patterns child-table) (get accumulated-patterns child-table)
                                :else nil)
               parent-patterns (cond
                                 (= current-table parent-table) current-patterns
                                 (contains? accumulated-patterns parent-table) (get accumulated-patterns parent-table)
                                 :else nil)]
         :when (and (seq child-patterns)
                    (seq parent-patterns)
                    (patterns-traverse-join-edge? child-patterns parent-patterns edge))]
     {:edge edge
      :child-table child-table
      :parent-table parent-table
      :acc-table acc-table})))

(defn- execute-pattern-group
  "Execute a single pattern group against its Iceberg source.

   When join-columns is provided, those columns are included in the scan
   and their raw values are stored in the solution for hash join operations.

   When all-mappings is provided, it's passed through for RefObjectMap resolution.

   Returns a lazy seq of solutions."
  [sources mapping patterns base-solution time-travel solution-pushdown join-columns all-mappings]
  (let [table-name (:table mapping)
        source (get sources table-name)]
    (when-not source
      (throw (ex-info (str "No source found for table: " table-name)
                      {:error :db/missing-source
                       :table table-name
                       :available-sources (keys sources)})))
    (query/execute-iceberg-query source mapping patterns base-solution
                                 time-travel nil solution-pushdown join-columns all-mappings)))

(defn- execute-multi-table-hash-join
  "Execute a multi-table query using hash joins.

   Strategy:
   1. Collect join columns for each table from join graph
   2. Execute each table query independently (with join columns projected)
   3. Find join edges that are actually traversed by the query patterns
   4. Apply hash join only when patterns traverse the FK relationship
   5. Use SPARQL-compatible merge for overlapping variable bindings
   6. Use left outer join for OPTIONAL pattern groups

   IMPORTANT: Join edges are only applied when the SPARQL query explicitly
   traverses the FK relationship via the RefObjectMap predicate. This prevents
   implicit joins from changing query semantics. If two tables appear in a query
   but the patterns don't traverse the FK, a Cartesian product is used.

   Falls back to Cartesian product with compatible-merge if no traversed edges exist."
  [sources pattern-groups solution time-travel solution-pushdown join-graph all-mappings]
  (let [;; Collect join columns for each table so they're included in results
        table->join-cols (into {}
                               (for [{:keys [mapping]} pattern-groups
                                     :let [table (:table mapping)
                                           cols (collect-join-columns-for-table join-graph table)]
                                     :when (seq cols)]
                                 [table cols]))

        ;; Track which tables are from optional pattern groups
        table->optional? (into {}
                               (for [{:keys [mapping optional?]} pattern-groups]
                                 [(:table mapping) (boolean optional?)]))

        _ (log/debug "Join columns by table:" table->join-cols)
        _ (log/debug "Optional tables:" table->optional?)

        ;; Execute all table queries with join columns projected
        group-results (mapv (fn [{:keys [mapping patterns optional?]}]
                              (let [table (:table mapping)
                                    join-cols (get table->join-cols table)]
                                {:mapping mapping
                                 :patterns patterns
                                 :optional? (boolean optional?)
                                 :solutions (vec (execute-pattern-group
                                                  sources mapping patterns solution
                                                  time-travel solution-pushdown join-cols all-mappings))}))
                            pattern-groups)

        _ (log/debug "Multi-table query executed:"
                     {:groups (count group-results)
                      :solution-counts (mapv #(count (:solutions %)) group-results)})]

    ;; Short-circuit if any NON-OPTIONAL group returns empty
    ;; Optional groups can be empty - that's the point of OPTIONAL
    (if (some #(and (empty? (:solutions %)) (not (:optional? %))) group-results)
      []

      ;; Check if we have join edges to potentially use
      (if (and join-graph (join/has-join-edges? join-graph))
        ;; Use hash join strategy - but only for traversed edges
        (:accumulated-solutions
         (reduce
          (fn [{:keys [accumulated-solutions accumulated-tables accumulated-patterns]}
               {:keys [mapping patterns optional?] :as current-group}]
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
                        (log/debug "Found traversed join edge:" traversed-edge
                                   "optional?" optional?))

                    new-solutions
                    (if traversed-edge
                      ;; Hash join path - edge is actually traversed by patterns
                      (let [edge (:edge traversed-edge)
                            ;; For OPTIONAL (left outer join), we must ensure:
                            ;; - probe side = accumulated (required) - gets preserved
                            ;; - build side = current (optional) - allows nulls
                            ;;
                            ;; For inner join, use FK-based heuristic:
                            ;; - child table (fact) usually probes into parent (dimension)
                            current-is-child? (= current-table (:child-table edge))

                            ;; CRITICAL: For OPTIONAL, force correct orientation
                            ;; Left outer join preserves ALL probe rows, so probe must be required
                            [build-solutions probe-solutions build-cols probe-cols]
                            (if optional?
                              ;; OPTIONAL: accumulated is required (probe), current is optional (build)
                              ;; This ensures all required rows are preserved with nulls for optional
                              (if current-is-child?
                                ;; Current (optional) is child, accumulated (required) is parent
                                ;; probe=accumulated uses parent cols, build=current uses child cols
                                [current-solutions accumulated-solutions
                                 (mapv keyword (join/child-columns edge))
                                 (mapv keyword (join/parent-columns edge))]
                                ;; Current (optional) is parent, accumulated (required) is child
                                ;; probe=accumulated uses child cols, build=current uses parent cols
                                [current-solutions accumulated-solutions
                                 (mapv keyword (join/parent-columns edge))
                                 (mapv keyword (join/child-columns edge))])
                              ;; Inner join: use FK-based heuristic for efficiency
                              (if current-is-child?
                                ;; Current is child (fact table) -> accumulated is parent
                                [accumulated-solutions current-solutions
                                 (mapv keyword (join/parent-columns edge))
                                 (mapv keyword (join/child-columns edge))]
                                ;; Current is parent (dimension table) -> build with current
                                [current-solutions accumulated-solutions
                                 (mapv keyword (join/parent-columns edge))
                                 (mapv keyword (join/child-columns edge))]))

                            _ (log/debug "Hash join execution:"
                                         {:build-count (count build-solutions)
                                          :probe-count (count probe-solutions)
                                          :build-cols build-cols
                                          :probe-cols probe-cols
                                          :left-outer? optional?
                                          :optional-orientation (when optional? "probe=required, build=optional")})

                            ;; Use left outer join for optional pattern groups
                            joined (if optional?
                                     (hash-join/left-outer-hash-join
                                      build-solutions probe-solutions
                                      build-cols probe-cols)
                                     (hash-join/hash-join
                                      build-solutions probe-solutions
                                      build-cols probe-cols))]
                        (log/debug "Hash join result count:" (count joined))
                        joined)

                      ;; No traversed edge - patterns don't use FK relationship
                      ;; Use Cartesian product with compatible-merge (SPARQL semantics)
                      (let [acc-count (count accumulated-solutions)
                            curr-count (count current-solutions)
                            ;; Get a representative table name from accumulated-tables
                            acc-table-str (str/join ", " accumulated-tables)]
                        (log/warn "No traversed join edge, using Cartesian product:"
                                  {:accumulated-tables accumulated-tables
                                   :accumulated-count acc-count
                                   :current-table current-table
                                   :current-count curr-count
                                   :estimated-product (* acc-count curr-count)})
                        ;; Safety check - prevent memory explosion
                        (check-cartesian-product-size! acc-count curr-count
                                                       acc-table-str current-table)
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
          (log/warn "No join graph available, using Cartesian product for"
                    (count group-results) "table groups")
          (:solutions
           (reduce
            (fn [{:keys [solutions table-names]} group]
              (let [curr-solutions (:solutions group)
                    curr-table (or (get-in group [:mapping :table]) "unknown")
                    acc-count (count solutions)
                    curr-count (count curr-solutions)]
                (if (empty? solutions)
                  {:solutions [] :table-names (conj table-names curr-table)}
                  (do
                    ;; Safety check - prevent memory explosion
                    (check-cartesian-product-size! acc-count curr-count
                                                   (str/join ", " table-names) curr-table)
                    ;; Use compatible-merge for SPARQL semantics
                    {:solutions (vec (keep (fn [[acc curr]]
                                             (hash-join/compatible-merge acc curr))
                                           (for [acc solutions
                                                 curr curr-solutions]
                                             [acc curr])))
                     :table-names (conj table-names curr-table)}))))
            {:solutions (:solutions (first group-results))
             :table-names #{(get-in (first group-results) [:mapping :table] "first-table")}}
            (rest group-results))))))))

;;; ---------------------------------------------------------------------------
;;; Columnar Plan Execution (Phase 3)
;;; ---------------------------------------------------------------------------

(defn- get-table-statistics
  "Get statistics for tables in pattern groups."
  [sources pattern-groups time-travel]
  (into {}
        (for [{:keys [mapping]} pattern-groups
              :let [table-name (:table mapping)
                    source (get sources table-name)]
              :when source]
          [table-name
           (tabular/get-statistics source table-name
                                   (cond-> {}
                                     (:snapshot-id time-travel)
                                     (assoc :snapshot-id (:snapshot-id time-travel))
                                     (:as-of-time time-travel)
                                     (assoc :as-of-time (:as-of-time time-travel))))])))

(defn- columnar-batch->solutions
  "Convert a batch of columnar data to SPARQL solutions.

   This is the boundary conversion from Arrow batches to solution maps.
   Handles both Arrow VectorSchemaRoot batches (columnar mode) and
   individual row maps (row-maps mode for backward compatibility).

   When predicates are provided, applies row-level filtering after converting
   from Arrow batches to row maps. This is necessary because Arrow vectorized
   reads only perform file/row-group pruning based on statistics, not row-level
   filtering.

   Uses R2RML mapping to transform column values to proper RDF terms."
  ([batch mapping patterns base-solution]
   (columnar-batch->solutions batch mapping patterns base-solution nil))
  ([batch mapping patterns base-solution predicates]
   (let [pred->var (query/extract-predicate-bindings patterns)
         subject-var (some query/extract-subject-variable patterns)
         ;; Helper to filter row maps based on predicates
         filter-rows (fn [rows]
                       (if (seq predicates)
                         (filter #(iceberg-core/row-matches-predicates? predicates %) rows)
                         rows))]
     (cond
       ;; Arrow VectorSchemaRoot - convert to row maps, filter, then to solutions
       (instance? org.apache.arrow.vector.VectorSchemaRoot batch)
       (let [row-maps (plan/batch->row-maps batch)
             filtered-rows (filter-rows row-maps)]
         (map (fn [row-map]
                (query/row->solution row-map mapping pred->var subject-var base-solution))
              filtered-rows))

       ;; Already a row map (from row-maps mode or legacy path)
       (map? batch)
       (let [rows (filter-rows [batch])]
         (map #(query/row->solution % mapping pred->var subject-var base-solution) rows))

       :else
       (do
         (log/warn "Unexpected batch type in columnar-batch->solutions:" (type batch))
         [])))))

(defn- execute-columnar-single-table
  "Execute a single-table query using columnar plan execution.

   Uses ScanOp from the plan compiler to read batches, then converts to
   solutions at the boundary using R2RML mapping.

   Phase 3b: Uses true columnar execution with filtered Arrow batches:
   1. Vectorized row-level filtering on Arrow vectors
   2. Data copied to avoid buffer reuse issues
   3. Arrow batches converted to solutions at boundary"
  [source mapping patterns base-solution time-travel predicates]
  (let [table-name (:table mapping)
        ;; Get all columns needed for this query
        columns (distinct
                 (concat
                  ;; Columns from predicate filters
                  (keep :column predicates)
                  ;; Columns from mapping predicates
                  (keep (fn [[_pred obj-map]]
                          (when (= :column (:type obj-map))
                            (:value obj-map)))
                        (:predicates mapping))))
        ;; Create scan plan - use Arrow batches for columnar execution
        ;; scan-arrow-batches now returns filtered, copied batches (safe to hold)
        scan-plan (plan/compile-single-table-plan
                   source table-name
                   (when (seq columns) (vec columns))
                   predicates time-travel
                   {:use-arrow-batches? true})]  ;; Phase 3b: true columnar execution
    (log/debug "Columnar single-table execution:" {:table table-name
                                                   :columns columns
                                                   :predicates (count predicates)
                                                   :use-arrow-batches? true})
    ;; Execute plan and convert batches to solutions
    (try
      (plan/open! scan-plan)
      (loop [solutions []]
        (if-let [batch (plan/next-batch! scan-plan)]
          ;; batch is VectorSchemaRoot when use-arrow-batches? is true
          (recur (into solutions (columnar-batch->solutions batch mapping patterns base-solution)))
          solutions))
      (finally
        (plan/close! scan-plan)))))

(defn- execute-columnar-multi-table
  "Execute a multi-table query using columnar plan execution.

   Uses the plan compiler to create an operator tree with ScanOps
   and HashJoinOps.

   Phase 3c: True vectorized execution with automatic projection pushdown:
   1. ScanOps use filtered Arrow batches (vectorized filtering, copied data)
   2. HashJoinOp uses vectorized mode (batch storage + gather output)
   3. Automatic projection pushdown - only copy columns needed by query
   4. HashJoinOp outputs Arrow batches converted to row maps at boundary"
  [sources pattern-groups base-solution time-travel predicates join-graph]
  (let [stats-by-table (get-table-statistics sources pattern-groups time-travel)
        ;; Add predicates to pattern groups
        groups-with-predicates
        (mapv (fn [{:keys [mapping] :as group}]
                (let [table-name (:table mapping)
                      table-predicates (filter #(= table-name (:table %)) predicates)]
                  (assoc group :predicates table-predicates)))
              pattern-groups)

        ;; Calculate columns needed by the query for projection pushdown
        ;; Include: pattern columns + join keys + predicate columns
        query-columns (extract-columns-from-pattern-groups groups-with-predicates predicates)
        join-columns (collect-all-join-columns join-graph)
        output-columns (into query-columns join-columns)]

    (log/debug "Columnar multi-table execution:" {:tables (count pattern-groups)
                                                  :stats stats-by-table
                                                  :vectorized? true
                                                  :query-columns (count query-columns)
                                                  :join-columns (count join-columns)
                                                  :output-columns (count output-columns)})

    ;; Compile the plan with vectorized mode and projection pushdown
    ;; Phase 3c: Full columnar pipeline with automatic optimization
    (if-let [root-plan (plan/compile-plan sources groups-with-predicates
                                          join-graph stats-by-table time-travel
                                          {:use-arrow-batches? true
                                           :copy-batches? true  ;; Required for vectorized build
                                           :vectorized? true    ;; True vectorized hash join
                                           :output-columns output-columns})]
      (try
        (plan/open! root-plan)
        (loop [solutions []]
          (if-let [batch (plan/next-batch! root-plan)]
            ;; In vectorized mode, batch is VectorSchemaRoot from gather
            ;; Convert to row maps at the boundary
            (let [row-maps (cond
                             (instance? org.apache.arrow.vector.VectorSchemaRoot batch)
                             (let [rows (plan/batch->row-maps batch)]
                               ;; Close the gathered batch to free Arrow memory
                               (.close ^org.apache.arrow.vector.VectorSchemaRoot batch)
                               rows)
                             (map? batch) [batch]
                             (vector? batch) batch
                             (sequential? batch) (vec batch)
                             :else [])]
              (recur (into solutions
                           (map #(merge base-solution %) row-maps))))
            solutions))
        (finally
          (plan/close! root-plan)))
      ;; No plan compiled - return empty
      [])))

;;; ---------------------------------------------------------------------------
;;; UNION Pattern Execution
;;; ---------------------------------------------------------------------------

(declare execute-union-patterns)

(defn- execute-union-branch
  "Execute a single UNION branch and return solutions.

   A branch is a vector of patterns (like a WHERE clause).
   Routes patterns to tables and executes them."
  [sources mappings routing-indexes join-graph
   branch-patterns base-solution time-travel solution-pushdown use-columnar?]
  ;; Support nested UNION by recursively expanding and executing.
  ;; Without this, group-patterns-by-table would silently drop UNION patterns.
  (if (query/has-union-patterns? branch-patterns)
    (let [{:keys [union-patterns regular-patterns]} (query/extract-union-patterns branch-patterns)]
      (execute-union-patterns sources mappings routing-indexes join-graph
                              union-patterns regular-patterns base-solution
                              time-travel solution-pushdown use-columnar?))
    (let [pattern-groups (query/group-patterns-by-table branch-patterns mappings routing-indexes)]
      (log/debug "UNION branch execution:" {:patterns (count branch-patterns)
                                            :groups (count pattern-groups)})
      (cond
        ;; Empty branch - no results
        (empty? pattern-groups)
        []

        ;; Single table
        (= 1 (count pattern-groups))
        (let [{:keys [mapping patterns]} (first pattern-groups)
              table-name (:table mapping)
              source (get sources table-name)]
          (if-not source
            (do
              (log/warn "No source found for table in UNION branch:" table-name)
              [])
            (if use-columnar?
              (execute-columnar-single-table source mapping patterns base-solution
                                             time-travel solution-pushdown)
              (vec (query/execute-iceberg-query source mapping patterns base-solution
                                                time-travel nil solution-pushdown nil mappings)))))

        ;; Multiple tables - use hash join
        :else
        (if use-columnar?
          (execute-columnar-multi-table sources pattern-groups base-solution
                                        time-travel solution-pushdown join-graph)
          (execute-multi-table-hash-join sources pattern-groups base-solution
                                         time-travel solution-pushdown join-graph mappings))))))

(defn- execute-union-patterns
  "Execute UNION patterns and return combined solutions.

   UNION returns all results from all branches concatenated.
   Each branch is executed independently and results are combined.

   Args:
     union-patterns   - Vector of UNION patterns (MapEntry with :union key)
     regular-patterns - Vector of non-UNION patterns (executed normally)
     ... other args passed through to branch execution

   Returns vector of solutions from all UNION branches."
  [sources mappings routing-indexes join-graph
   union-patterns regular-patterns base-solution time-travel solution-pushdown use-columnar?]
  (log/debug "Executing UNION patterns:" {:union-count (count union-patterns)
                                          :regular-count (count regular-patterns)})
  ;; SPARQL semantics:
  ;; - UNION is evaluated as a union of graph pattern alternatives.
  ;; - If UNION appears alongside regular patterns, the regular patterns must be
  ;;   included in (joined with) EACH UNION branch (distribution), not cross-producted
  ;;   after the fact.
  ;;
  ;; Additionally, if multiple UNION patterns appear at the same level, semantics
  ;; are equivalent to a UNION over the cartesian product of branch choices.
  (let [expanded-branch-patterns
        (reduce
         (fn [acc union-pattern]
           (let [branches (val union-pattern)]
             (log/debug "UNION pattern has branches:" (count branches))
             (vec (for [prefix acc
                        branch branches]
                    (into (vec prefix) branch)))))
         [[]]
         union-patterns)

        combined-branches
        (mapv (fn [branch]
                (if (seq regular-patterns)
                  (into (vec regular-patterns) branch)
                  (vec branch)))
              expanded-branch-patterns)]

    (vec
     (mapcat
      (fn [branch-patterns]
        (execute-union-branch sources mappings routing-indexes join-graph
                              branch-patterns base-solution time-travel
                              solution-pushdown use-columnar?))
      combined-branches))))

;;; ---------------------------------------------------------------------------
;;; Aggregation Execution
;;; ---------------------------------------------------------------------------

(defn- create-aggregator
  "Create initial accumulator state for an aggregate function."
  [agg-type]
  (case agg-type
    :count {:type :count :count 0}
    :count-distinct {:type :count-distinct :values #{}}
    :sum {:type :sum :sum 0}
    :avg {:type :avg :sum 0 :count 0}
    :min {:type :min :value nil}
    :max {:type :max :value nil}
    {:type :count :count 0}))

(defn- update-aggregator
  "Update aggregator state with a new value."
  [state value]
  (case (:type state)
    :count
    (if (some? value)
      (update state :count inc)
      state)

    :count-distinct
    (if (some? value)
      (update state :values conj value)
      state)

    :sum
    (if (and (some? value) (number? value))
      (update state :sum + value)
      state)

    :avg
    (if (and (some? value) (number? value))
      (-> state
          (update :sum + value)
          (update :count inc))
      state)

    :min
    (if (some? value)
      (let [current (:value state)]
        (if (or (nil? current)
                (and (number? value) (number? current) (< value current))
                (and (string? value) (string? current) (neg? (compare value current))))
          (assoc state :value value)
          state))
      state)

    :max
    (if (some? value)
      (let [current (:value state)]
        (if (or (nil? current)
                (and (number? value) (number? current) (> value current))
                (and (string? value) (string? current) (pos? (compare value current))))
          (assoc state :value value)
          state))
      state)

    state))

(defn- finalize-aggregator
  "Compute final aggregate value from accumulator state."
  [state]
  (case (:type state)
    :count (:count state)
    :count-distinct (count (:values state))
    :sum (:sum state)
    :avg (let [{:keys [sum count]} state]
           (if (pos? count)
             (/ sum count)
             0))
    :min (:value state)
    :max (:value state)
    nil))

(defn- unwrap-match-value
  "Extract scalar value from a Fluree match object or return raw value.

   SPARQL solutions in Fluree contain match objects with metadata.
   This function extracts the raw value for aggregation purposes.
   Handles both literal values (::val) and IRI values (::iri)."
  [v]
  (cond
    ;; Already a scalar (from row-based execution)
    (or (number? v) (string? v) (boolean? v) (nil? v))
    v

    ;; Fluree match object - try get-value first (for literals), then get-iri (for IRIs)
    (map? v)
    (or (where/get-value v) (where/get-iri v))

    ;; Other (keywords, etc.)
    :else v))

(defn- solution-get-column-value
  "Extract a column value from a solution map.

   Solutions have SPARQL variable bindings as symbols, but columns are strings.
   This function handles the translation, looking for both the column name
   directly and as a SPARQL variable (with ? prefix).

   Also unwraps Fluree match objects to get scalar values for aggregation."
  [solution column]
  (-> (or
       ;; Direct column name lookup (from row maps)
       (get solution column)
       ;; SPARQL variable lookup (symbol with ?)
       (get solution (symbol (str "?" column)))
       ;; Symbol without ?
       (get solution (symbol column))
       ;; Keyword lookup
       (get solution (keyword column)))
      unwrap-match-value))

(defn apply-aggregation
  "Apply GROUP BY and aggregation to a vector of solutions.

   This function implements SPARQL aggregation semantics, grouping solutions
   by the specified keys and computing aggregate functions over each group.

   Args:
     solutions  - Vector of solution maps (from Iceberg query execution)
     group-keys - Vector of column names to GROUP BY (empty for implicit grouping)
     aggregates - Vector of aggregate specifications:
                  [{:fn :count/:sum/:avg/:min/:max/:count-distinct
                    :column column-name (nil for COUNT(*))
                    :alias output-column-name}]

   Returns vector of aggregated solution maps, one per group.

   Examples:
     ;; COUNT(*) with no grouping
     (apply-aggregation solutions [] [{:fn :count :column nil :alias \"total\"}])
     ;; => [{\"total\" 42}]

     ;; GROUP BY country with COUNT
     (apply-aggregation solutions [\"country\"]
                        [{:fn :count :column nil :alias \"cnt\"}])
     ;; => [{\"country\" \"US\" \"cnt\" 10} {\"country\" \"UK\" \"cnt\" 5}]"
  [solutions group-keys aggregates]
  (when (seq aggregates)
    (let [^java.util.HashMap groups (java.util.HashMap.)]
      ;; Process each solution
      (doseq [solution solutions]
        (let [;; Extract group key
              group-key (if (seq group-keys)
                          (mapv #(solution-get-column-value solution %) group-keys)
                          [::all-rows])
              ;; Get or create group state
              group-state (or (.get groups group-key)
                              (let [initial {:aggs (mapv #(create-aggregator (:fn %)) aggregates)
                                             :group-values (when (seq group-keys)
                                                             (zipmap group-keys group-key))}]
                                (.put groups group-key initial)
                                initial))
              ;; Update aggregators
              updated-aggs
              (mapv (fn [agg-state agg-spec]
                      (let [col (:column agg-spec)
                            ;; For COUNT(*), always pass a non-nil sentinel
                            value (if (nil? col)
                                    ::count-star
                                    (solution-get-column-value solution col))]
                        (update-aggregator agg-state value)))
                    (:aggs group-state)
                    aggregates)]
          (.put groups group-key (assoc group-state :aggs updated-aggs))))

      ;; SPARQL semantics: implicit grouping (no GROUP BY) with 0 input rows
      ;; must still return 1 row with COUNT()=0, SUM()=0, AVG()=null, etc.
      (when (and (empty? group-keys) (.isEmpty groups))
        (let [implicit-key [::all-rows]
              initial-aggs (mapv #(create-aggregator (:fn %)) aggregates)]
          (.put groups implicit-key {:aggs initial-aggs :group-values nil})))

      ;; Build result rows
      (vec
       (for [group-key (keys groups)
             :let [group-state (.get groups group-key)
                   group-vals (or (:group-values group-state) {})
                   agg-vals (into {}
                                  (map (fn [agg-state agg-spec]
                                         [(:alias agg-spec) (finalize-aggregator agg-state)])
                                       (:aggs group-state)
                                       aggregates))]]
         (merge group-vals agg-vals))))))

(defn- apply-order-by
  "Apply ORDER BY to a sequence of aggregated solutions.

   Supports both ASC (default) and DESC ordering on aggregate result columns.

   Handles multiple ORDER BY formats:
     - SPARQL translator: vector of lists like [(\"desc\" ?count) (\"asc\" ?name)]
     - JSON-LD/map: vector of maps like [{:var ?count :order :desc}]
     - Simple: vector of symbols like [?count ?name]"
  [solutions order-by-clause]
  (if (seq order-by-clause)
    (let [;; Parse a single order-by spec into {:key string :desc? bool}
          parse-spec (fn [spec]
                       (cond
                         ;; SPARQL translator format: ("desc" ?count) or ("asc" ?name)
                         (seq? spec)
                         (let [[direction var] spec
                               var-name (cond
                                          (symbol? var) (name var)
                                          (string? var) var
                                          :else (str var))]
                           {:key var-name
                            :desc? (= "desc" (str/lower-case (str direction)))})

                         ;; Already a map with :var and :order
                         (map? spec)
                         {:key (if-let [v (:var spec)]
                                 (if (symbol? v) (name v) (str v))
                                 (str spec))
                          :desc? (= :desc (:order spec))}

                         ;; Symbol like ?count
                         (symbol? spec)
                         {:key (name spec) :desc? false}

                         ;; String expression like "(desc ?count)"
                         (string? spec)
                         (let [desc? (str/starts-with? (str/lower-case spec) "(desc")
                               ;; Extract variable name
                               var-match (re-find #"\?(\w+)" spec)]
                           {:key (or (second var-match) spec)
                            :desc? desc?})

                         :else {:key (str spec) :desc? false}))
          ;; Parse order-by specs - handle various formats
          order-specs (cond
                        ;; Vector of specs
                        (vector? order-by-clause)
                        (mapv parse-spec order-by-clause)
                        ;; Single spec
                        :else [(parse-spec order-by-clause)])
          comparator (fn [a b]
                       (reduce (fn [result {:keys [key desc?]}]
                                 (if (zero? result)
                                   (let [va (or (get a key) (get a (str "?" key)))
                                         vb (or (get b key) (get b (str "?" key)))
                                         cmp (compare va vb)]
                                     (if desc? (- cmp) cmp))
                                   result))
                               0
                               order-specs))]
      (sort comparator solutions))
    solutions))

(defn- apply-limit-offset
  "Apply LIMIT and OFFSET to a sequence of solutions."
  [solutions limit offset]
  (cond->> solutions
    offset (drop offset)
    limit (take limit)))

(defn- apply-distinct
  "Apply DISTINCT to a sequence of solutions, deduplicating by all keys.

   Uses a Set to track seen solutions for O(1) lookup per solution.
   Solutions are compared by their complete map structure.

   Args:
     solutions - Sequence of solution maps

   Returns deduplicated sequence preserving first occurrence order."
  [solutions]
  (let [seen (java.util.HashSet.)]
    (filter (fn [sol]
              (let [added? (.add seen sol)]
                added?))
            solutions)))

;;; ---------------------------------------------------------------------------
;;; Anti-Join Execution (EXISTS, NOT EXISTS, MINUS)
;;; ---------------------------------------------------------------------------

(defn- execute-anti-join-inner
  "Execute inner patterns of an anti-join against Iceberg tables.

   This is used by EXISTS, NOT EXISTS, and MINUS to evaluate the inner pattern.

   For EXISTS/NOT EXISTS (correlated), the outer solution's bindings are
   available to the inner pattern execution.

   For MINUS (uncorrelated), the inner pattern is executed independently.

   Args:
     sources         - Map of table-name -> IcebergSource
     mappings        - R2RML mappings
     routing-indexes - Pattern routing indexes
     join-graph      - Join graph for multi-table queries
     inner-patterns  - The inner WHERE patterns to execute
     outer-solution  - Solution from outer query (for correlated subqueries)
     time-travel     - Time travel spec (or nil)
     use-columnar?   - Whether to use columnar execution

   Returns sequence of solutions from inner pattern execution."
  [sources mappings routing-indexes join-graph inner-patterns outer-solution time-travel use-columnar?]
  ;; Route inner patterns to tables
  (let [pattern-groups (query/group-patterns-by-table inner-patterns mappings routing-indexes)]
    (cond
      ;; Empty patterns - no results
      (empty? pattern-groups)
      []

      ;; Single table
      (= 1 (count pattern-groups))
      (let [{:keys [mapping patterns]} (first pattern-groups)
            table-name (:table mapping)
            source (get sources table-name)]
        (if-not source
          []
          (if use-columnar?
            (execute-columnar-single-table
             source mapping patterns outer-solution time-travel nil)
            (query/execute-iceberg-query
             source mapping patterns outer-solution time-travel nil nil nil mappings))))

      ;; Multiple tables - use join execution
      :else
      (if use-columnar?
        (execute-columnar-multi-table
         sources pattern-groups outer-solution time-travel nil join-graph)
        (execute-multi-table-hash-join
         sources pattern-groups outer-solution time-travel nil join-graph mappings)))))

(defn- extract-pattern-vars
  "Extract all variable symbols from a sequence of WHERE patterns.

   Used to determine which outer solution bindings are relevant for
   correlated subquery memoization.

   Handles:
   - MapEntry patterns (from where/->pattern): [:tuple {:s {:var ?x} ...}]
   - Raw map patterns (SPARQL): {:s ?x :p \"pred\" :o \"val\"}
   - Vector patterns (SPARQL nested): [\"exists\" [{:s ?x ...}]]"
  [patterns]
  (if-not (sequential? patterns)
    #{}  ;; Return empty set for non-sequential inputs
    (into #{}
          (mapcat (fn [pattern]
                    (cond
                  ;; MapEntry pattern - use where accessors
                      (map-entry? pattern)
                      (let [ptype (where/pattern-type pattern)
                            pdata (where/pattern-data pattern)]
                        (case ptype
                          :tuple
                      ;; Extract vars from tuple pattern slots
                          (->> pdata
                               (keep (fn [[_slot m]]
                                       (when (and (map? m) (:var m))
                                         (:var m)))))
                      ;; Nested patterns - recurse
                          (:exists :not-exists :minus)
                          (extract-pattern-vars pdata)
                      ;; Other pattern types - no vars extracted
                          nil))

                  ;; Vector pattern - could be:
                  ;; 1. SPARQL nested like ["exists" [...]]
                  ;; 2. Tuple as [s-match p-match o-match] where matches have ::where/var
                      (vector? pattern)
                      (let [first-elem (first pattern)]
                        (cond
                      ;; Nested anti-join pattern
                          (or (keyword? first-elem) (string? first-elem))
                          (let [ptype (if (keyword? first-elem) first-elem (keyword first-elem))]
                            (when (#{:exists :not-exists :minus} ptype)
                              (extract-pattern-vars (second pattern))))

                      ;; Tuple as vector of match objects [s p o]
                          (map? first-elem)
                          (->> pattern
                               (keep (fn [match-obj]
                                       (when (map? match-obj)
                                     ;; Check for ::where/var in the match object
                                         (or (::where/var match-obj)
                                             (:var match-obj)
                                         ;; Handle namespaced key as keyword
                                             (get match-obj :fluree.db.query.exec.where/var))))))))

                  ;; Raw map pattern (tuple) - extract vars directly
                      (map? pattern)
                      (->> pattern
                           (keep (fn [[_slot m]]
                                   (cond
                                 ;; Match object with :var
                                     (and (map? m) (:var m))
                                     (:var m)
                                 ;; Direct symbol (SPARQL raw pattern)
                                     (symbol? m)
                                     m
                                     :else nil))))

                      :else nil))
                  patterns))))

(defn- apply-exists
  "Apply EXISTS filter: keep solutions where inner pattern matches.

   EXISTS is a correlated subquery - the inner pattern uses bindings from
   the outer solution. A solution is kept if the inner pattern produces
   at least one result.

   SPARQL semantics per spec section 8.2.

   Performance optimization: Instead of executing the inner query per outer
   solution (expensive), we execute it ONCE with no correlations to get all
   possible matches, then use set membership tests. This converts EXISTS to
   a semi-join operation which is much more efficient.

   Args:
     solutions       - Sequence of outer solutions
     inner-patterns  - Patterns from the EXISTS clause
     sources         - Map of table-name -> IcebergSource
     mappings        - R2RML mappings
     routing-indexes - Pattern routing indexes
     join-graph      - Join graph
     time-travel     - Time travel spec
     use-columnar?   - Whether to use columnar execution

   Returns filtered sequence of solutions."
  [solutions inner-patterns sources mappings routing-indexes join-graph time-travel use-columnar?]
  (let [solutions-vec (vec solutions)]
    (if (empty? solutions-vec)
      solutions-vec
      ;; Find variables used in inner patterns
      (let [inner-vars (extract-pattern-vars inner-patterns)
            outer-keys (set (keys (first solutions-vec)))
            ;; Correlated vars are those in both outer solution and inner patterns
            correlated-vars (vec (clojure.set/intersection outer-keys inner-vars))]
        (log/debug "EXISTS semi-join:" {:inner-var-count (count inner-vars)
                                        :correlated-var-count (count correlated-vars)})
        (if (empty? correlated-vars)
          ;; No correlation - EXISTS evaluates to same result for all outer solutions
          ;; Execute once and keep all or none
          (let [inner-results (execute-anti-join-inner
                               sources mappings routing-indexes join-graph
                               inner-patterns {} time-travel use-columnar?)]
            (if (seq inner-results)
              solutions-vec  ;; Inner has results - keep all outer
              []))           ;; Inner empty - remove all outer
          ;; Has correlated vars - execute inner once, build index, do semi-join
          (let [;; Execute inner query once without outer bindings
                inner-results (vec (execute-anti-join-inner
                                    sources mappings routing-indexes join-graph
                                    inner-patterns {} time-travel use-columnar?))
                ;; Build index: {[correlated-var-values] -> true}
                inner-index (into #{}
                                  (keep (fn [inner-sol]
                                          (let [vals (mapv #(get inner-sol %) correlated-vars)]
                                            (when (every? some? vals)
                                              vals))))
                                  inner-results)]
            (log/debug "EXISTS index built:" {:inner-count (count inner-results)
                                              :index-size (count inner-index)})
            ;; Filter outer solutions using index - O(1) lookup
            (filterv
             (fn [outer-sol]
               (let [outer-vals (mapv #(get outer-sol %) correlated-vars)]
                 (and (every? some? outer-vals)
                      (contains? inner-index outer-vals))))
             solutions-vec)))))))

(defn- apply-not-exists
  "Apply NOT EXISTS filter: keep solutions where inner pattern does NOT match.

   NOT EXISTS is a correlated subquery - the inner pattern uses bindings from
   the outer solution. A solution is kept if the inner pattern produces
   NO results.

   SPARQL semantics per spec section 8.2.

   Performance optimization: Instead of executing the inner query per outer
   solution (expensive), we execute it ONCE with no correlations to get all
   possible matches, then use set membership tests. This converts NOT EXISTS
   to an anti-semi-join operation which is much more efficient.

   Args:
     solutions       - Sequence of outer solutions
     inner-patterns  - Patterns from the NOT EXISTS clause
     sources         - Map of table-name -> IcebergSource
     mappings        - R2RML mappings
     routing-indexes - Pattern routing indexes
     join-graph      - Join graph
     time-travel     - Time travel spec
     use-columnar?   - Whether to use columnar execution

   Returns filtered sequence of solutions."
  [solutions inner-patterns sources mappings routing-indexes join-graph time-travel use-columnar?]
  (let [solutions-vec (vec solutions)]
    (if (empty? solutions-vec)
      solutions-vec
      ;; Find variables used in inner patterns
      (let [inner-vars (extract-pattern-vars inner-patterns)
            outer-keys (set (keys (first solutions-vec)))
            ;; Correlated vars are those in both outer solution and inner patterns
            correlated-vars (vec (clojure.set/intersection outer-keys inner-vars))]
        (log/debug "NOT EXISTS anti-semi-join:" {:inner-var-count (count inner-vars)
                                                 :correlated-var-count (count correlated-vars)})
        (if (empty? correlated-vars)
          ;; No correlation - NOT EXISTS evaluates to same result for all outer solutions
          ;; Execute once and keep all or none
          (let [inner-results (execute-anti-join-inner
                               sources mappings routing-indexes join-graph
                               inner-patterns {} time-travel use-columnar?)]
            (if (seq inner-results)
              []              ;; Inner has results - remove all outer
              solutions-vec)) ;; Inner empty - keep all outer
          ;; Has correlated vars - execute inner once, build index, do anti-semi-join
          (let [;; Execute inner query once without outer bindings
                inner-results (vec (execute-anti-join-inner
                                    sources mappings routing-indexes join-graph
                                    inner-patterns {} time-travel use-columnar?))
                ;; Build index: {[correlated-var-values] -> true}
                inner-index (into #{}
                                  (keep (fn [inner-sol]
                                          (let [vals (mapv #(get inner-sol %) correlated-vars)]
                                            (when (every? some? vals)
                                              vals))))
                                  inner-results)]
            (log/debug "NOT EXISTS index built:" {:inner-count (count inner-results)
                                                  :index-size (count inner-index)})
            ;; Filter outer solutions using index - O(1) lookup
            ;; Keep solutions NOT in the inner index
            (filterv
             (fn [outer-sol]
               (let [outer-vals (mapv #(get outer-sol %) correlated-vars)]
                 (or (some nil? outer-vals)  ;; Unbound var - not a match, keep
                     (not (contains? inner-index outer-vals)))))
             solutions-vec)))))))

(defn- apply-minus
  "Apply MINUS set difference: remove solutions that match inner pattern.

   MINUS is NOT a correlated subquery - the inner pattern is executed
   independently. Then, for each outer solution, if there exists an inner
   solution with the same values for all shared variables, the outer
   solution is removed.

   SPARQL semantics per spec section 8.3:
   - Only shared variables are compared
   - Unbound variables in either solution are treated as non-matching

   Performance: Uses O(1) hash index lookup instead of O(inner) scan per outer.
   Shared variables are determined once from solution structure, then inner
   solutions are indexed by their shared-var values.

   Args:
     solutions       - Sequence of outer solutions
     inner-patterns  - Patterns from the MINUS clause
     sources         - Map of table-name -> IcebergSource
     mappings        - R2RML mappings
     routing-indexes - Pattern routing indexes
     join-graph      - Join graph
     time-travel     - Time travel spec
     use-columnar?   - Whether to use columnar execution

   Returns filtered sequence of solutions."
  [solutions inner-patterns sources mappings routing-indexes join-graph time-travel use-columnar?]
  ;; Execute inner pattern once (uncorrelated - no outer bindings)
  (let [inner-solutions (vec (execute-anti-join-inner
                              sources mappings routing-indexes join-graph
                              inner-patterns {} time-travel use-columnar?))
        outer-solutions (vec solutions)]
    (cond
      ;; No inner solutions - keep all outer solutions
      (empty? inner-solutions)
      outer-solutions

      ;; No outer solutions - nothing to filter
      (empty? outer-solutions)
      outer-solutions

      :else
      ;; Determine shared vars from solution structure (consistent within each result set)
      (let [inner-keys (set (keys (first inner-solutions)))
            outer-keys (set (keys (first outer-solutions)))
            shared-vars (vec (clojure.set/intersection outer-keys inner-keys))]
        (if (empty? shared-vars)
          ;; No shared variables - nothing can match, keep all
          outer-solutions
          ;; Build hash index: {[shared-var-values] -> true}
          (let [inner-index (into #{}
                                  (keep (fn [inner-sol]
                                          (let [vals (mapv #(get inner-sol %) shared-vars)]
                                        ;; Only index if all shared vars are bound
                                            (when (every? some? vals)
                                              vals))))
                                  inner-solutions)]
            (log/debug "MINUS index built:" {:shared-vars shared-vars
                                             :inner-count (count inner-solutions)
                                             :index-size (count inner-index)})
            ;; Filter outer solutions - O(1) lookup per solution
            (filterv
             (fn [outer-sol]
               (let [outer-vals (mapv #(get outer-sol %) shared-vars)]
                  ;; Keep if: any shared var is unbound, OR values not in inner index
                 (or (some nil? outer-vals)
                     (not (contains? inner-index outer-vals)))))
             outer-solutions)))))))

(defn- apply-anti-joins
  "Apply all anti-join patterns to solutions in sequence.

   Anti-joins are applied after the main query execution and before
   query modifiers (DISTINCT, ORDER BY, LIMIT).

   Args:
     solutions       - Sequence of solutions from main query
     anti-joins      - Vector of {:type :exists/:not-exists/:minus :patterns [...]}
     sources         - Map of table-name -> IcebergSource
     mappings        - R2RML mappings
     routing-indexes - Pattern routing indexes
     join-graph      - Join graph
     time-travel     - Time travel spec
     use-columnar?   - Whether to use columnar execution

   Returns solutions after applying all anti-joins."
  [solutions anti-joins sources mappings routing-indexes join-graph time-travel use-columnar?]
  (reduce
   (fn [sols {:keys [type patterns]}]
     (log/debug "Applying anti-join:" {:type type :pattern-count (count patterns)
                                       :input-solutions (count sols)})
     (let [result (case type
                    :exists (apply-exists sols patterns sources mappings
                                          routing-indexes join-graph time-travel use-columnar?)
                    :not-exists (apply-not-exists sols patterns sources mappings
                                                  routing-indexes join-graph time-travel use-columnar?)
                    :minus (apply-minus sols patterns sources mappings
                                        routing-indexes join-graph time-travel use-columnar?)
                    ;; Unknown type - pass through
                    (do (log/warn "Unknown anti-join type:" type)
                        sols))
           ;; Force realization to get accurate count for logging
           result-vec (vec result)]
       (log/debug "Anti-join result:" {:type type :output-solutions (count result-vec)})
       result-vec))
   solutions
   anti-joins))

(defn- transform-aggregates-to-variables
  "Transform aggregate selectors to simple variable selectors.

   When VG handles aggregation, we need to modify the parsed query so the
   query executor's group/combine doesn't try to aggregate again.

   Replaces AsSelector (aggregate) with VariableSelector using the bind-var.
   For example: (COUNT ?airline AS ?count) -> ?count"
  [selectors output-format]
  (mapv (fn [sel]
          (if (instance? AsSelector sel)
            ;; Replace aggregate with simple variable selector using bind-var
            (let [bind-var (:bind-var sel)
                  new-sel (select/variable-selector bind-var output-format)]
              (log/debug "transform-aggregates-to-variables: replacing AsSelector"
                         {:bind-var bind-var
                          :output-format output-format
                          :new-sel-type (type new-sel)
                          :new-sel-meta-keys (keys (meta new-sel))})
              new-sel)
            ;; Keep non-aggregates as-is
            sel))
        selectors))

(defn- convert-aggregated-to-solutions
  "Convert aggregated results to SPARQL solutions with symbol keys.

   Aggregated results have keys like {'country' 'US', 'count' 10}
   SPARQL solutions need symbol keys like {?country match-obj, ?count match-obj}
   where match-obj wraps the value for proper SPARQL result formatting.

   Uses the group-by clause and aggregate specs to build the key mapping."
  [aggregated-rows group-by-clause group-keys aggregates]
  (when (seq aggregated-rows)
    ;; Build mapping from string column keys to SPARQL variable symbols
    ;; 1. Group-by: map column name to original variable (group-by has [?country], group-keys has ['country'])
    (let [group-key-map (when (and (seq group-by-clause) (seq group-keys))
                          (zipmap group-keys group-by-clause))
          ;; 2. Aggregates: map alias to bind-var (or derive symbol from alias)
          agg-key-map (into {}
                            (keep (fn [{:keys [alias bind-var]}]
                                    (when alias
                                      ;; Use bind-var if available, else create symbol from alias
                                      (let [sym (or bind-var
                                                    (symbol (str "?" alias)))]
                                        [alias sym])))
                                  aggregates))
          key-map (merge group-key-map agg-key-map)]
      (log/debug "convert-aggregated-to-solutions key-map:" {:group-key-map group-key-map
                                                             :agg-key-map agg-key-map
                                                             :key-map key-map})
      ;; Convert each row - use symbol keys with wrapped values for SPARQL select formatters
      (mapv (fn [row]
              (reduce-kv (fn [acc str-key value]
                           ;; Get the SPARQL variable symbol (like ?country)
                           (let [var-sym (or (get key-map str-key)
                                             ;; Fallback: create symbol from string
                                             (symbol (str "?" str-key)))]
                             ;; Wrap value in a match object for SPARQL select formatters
                             ;; Use empty map {} as base (var-sym is the key, not inside match)
                             ;; and infer datatype from value
                             (if (nil? value)
                               (assoc acc var-sym (where/unmatched-var var-sym))
                               (assoc acc var-sym (where/match-value {} value (datatype/infer-iri value))))))
                         {}
                         row))
            aggregated-rows))))

(defn- apply-having
  "Apply HAVING filter to aggregated solutions.

   HAVING is a pre-compiled filter function that works on aggregated results.
   It expects solutions with symbol keys and match objects, same as FILTER.
   Returns solutions where the HAVING condition evaluates to truthy.

   Note: HAVING functions are compiled by eval/compile and return typed values
   with a :value key (e.g., {:value true}). We extract :value to match the
   standard having.cljc behavior.

   Current limitation: Iceberg VG should use aggregate alias variables in HAVING
   (e.g., HAVING ?count > 50) rather than re-computing aggregates
   (e.g., HAVING COUNT(?x) > 50). This is because aggregates are computed at
   the database level and raw values aren't available for re-computation.

   Args:
     solutions - Sequence of aggregated solution maps (already realized)
     having-fn - Pre-compiled HAVING filter function (from eval/compile)"
  [solutions having-fn]
  (if having-fn
    (let [input-count (count solutions)
          _ (log/debug "Applying HAVING filter:" {:input-count input-count})
          filtered (filterv (fn [solution]
                              (try
                                (let [result (having-fn solution)]
                                  ;; HAVING function returns {:value true/false}
                                  ;; per standard having.cljc behavior
                                  (:value result))
                                (catch Exception e
                                  (log/debug "HAVING evaluation error:" (ex-message e))
                                  false)))
                            solutions)]
      (log/debug "HAVING filter complete:" {:output-count (count filtered)})
      filtered)
    solutions))

(defn- finalize-query-modifiers
  "Apply query modifiers (aggregation, HAVING, DISTINCT, ORDER BY, LIMIT) to solutions.

   This function is called when the aggregation-spec atom contains
   query modifier info from the parsed query.

   SPARQL modifier order (per spec section 15):
   1. GROUP BY + aggregates
   2. HAVING
   3. DISTINCT
   4. ORDER BY
   5. LIMIT/OFFSET

   Args:
     solutions  - Sequence of solution maps from VG execution
     query-info - Map with :select, :group-by, :having, :order-by, :distinct?, :limit, :offset
     mappings   - R2RML mappings for variable->column resolution

   Returns modified solutions."
  [solutions query-info mappings]
  (log/debug "finalize-query-modifiers input:" {:query-info (dissoc query-info :having)
                                                :has-having? (some? (:having query-info))
                                                :mapping-count (count mappings)
                                                :solution-count (count solutions)})
  (let [{:keys [select group-by having order-by distinct? limit offset]} query-info
        ;; Build a combined mapping from all available mappings
        ;; This is needed to resolve variables to columns
        combined-mapping (reduce
                          (fn [acc [_ m]]
                            (update acc :predicates merge (:predicates m)))
                          {:predicates {}}
                          mappings)
        _ (log/debug "finalize-query-modifiers combined-mapping predicates:"
                     {:predicate-keys (keys (:predicates combined-mapping))})
        ;; Build aggregation spec using the existing function
        parsed-query {:select select :group-by group-by}
        agg-spec (query/build-aggregation-spec parsed-query combined-mapping)
        _ (log/debug "finalize-query-modifiers agg-spec:" {:agg-spec agg-spec})]

    (if agg-spec
      (let [{:keys [group-keys aggregates]} agg-spec
            _ (log/debug "Applying VG-level aggregation:" {:group-keys group-keys
                                                           :aggregates aggregates
                                                           :distinct? distinct?
                                                           :has-having? (some? having)
                                                           :input-solutions (count solutions)
                                                           :first-solution (first solutions)})
            ;; Force realization of solutions for aggregation
            solutions-vec (vec solutions)
            ;; Apply aggregation (returns string-keyed result maps)
            aggregated-raw (apply-aggregation solutions-vec group-keys aggregates)
            _ (log/debug "Aggregation raw result:" {:output-count (count aggregated-raw)
                                                    :first-result (first aggregated-raw)
                                                    :first-result-keys (when (first aggregated-raw) (keys (first aggregated-raw)))})
            ;; Convert aggregated results back to SPARQL solutions with symbol keys
            aggregated (convert-aggregated-to-solutions aggregated-raw group-by group-keys aggregates)
            _ (log/debug "Aggregation converted result:" {:output-count (count aggregated)
                                                          :first-result (first aggregated)
                                                          :first-result-keys (when (first aggregated) (keys (first aggregated)))})
            ;; Apply HAVING (after aggregation, before DISTINCT per SPARQL spec)
            after-having (apply-having aggregated having)
            ;; Apply DISTINCT (after HAVING, before ORDER BY per SPARQL spec)
            deduped (if distinct?
                      (apply-distinct after-having)
                      after-having)
            ;; Apply ORDER BY
            ordered (apply-order-by deduped order-by)
            ;; Apply LIMIT/OFFSET
            limited (apply-limit-offset ordered limit offset)]
        (log/debug "Query modifiers complete:" {:output-rows (count limited)
                                                :distinct? distinct?
                                                :had-having? (some? having)})
        limited)
      ;; No aggregation - apply DISTINCT, ORDER BY, and LIMIT if present
      ;; Note: HAVING without aggregation is unusual but technically valid
      (let [after-having (apply-having solutions having)
            deduped (if distinct?
                      (do
                        (log/debug "Applying VG-level DISTINCT:" {:input-solutions (count after-having)})
                        (apply-distinct after-having))
                      after-having)
            ordered (apply-order-by deduped order-by)
            limited (apply-limit-offset ordered limit offset)]
        (when distinct?
          (log/debug "DISTINCT complete:" {:output-rows (count limited)}))
        limited))))

;;; ---------------------------------------------------------------------------
;;; Expression Evaluation (Residual FILTER + BIND)
;;; ---------------------------------------------------------------------------

(defn- apply-filter-fn
  "Apply a pre-compiled filter function to a solution.
   Returns the solution if filter passes, nil otherwise.

   Filter functions from eval.cljc expect solutions with match objects
   (symbol keys to {::where/val, ::where/datatype-iri, ...}).
   Iceberg solutions already have this format via row->solution."
  [solution filter-fn]
  (try
    (when (filter-fn solution)
      solution)
    (catch Exception e
      (log/debug "Filter evaluation error:" (ex-message e))
      nil)))

(defn- apply-filters
  "Apply all compiled filter functions to solutions.
   Works with both eager (vec) and lazy (seq) inputs.

   Args:
     solutions    - Sequence of solution maps
     filter-specs - Vector of {:fn compiled-filter-fn, :meta pattern-metadata}"
  [solutions filter-specs]
  (if (seq filter-specs)
    (let [filter-fns (map :fn filter-specs)]
      (filter (fn [sol]
                (every? #(apply-filter-fn sol %) filter-fns))
              solutions))
    solutions))

(defn- apply-bind-spec
  "Apply a BIND spec to a solution, adding new variable bindings.

   Spec is a map {var-sym {::where/var v, ::where/fn f}} from the BIND pattern.
   For each binding:
   - If ::where/fn is present, evaluate the function and bind result
   - Otherwise, it's a static binding

   Args:
     solution  - Current solution map
     bind-spec - Map of {var-sym -> bind-info}"
  [solution bind-spec]
  (reduce-kv
   (fn [sol var-sym bind-info]
     (let [f (::where/fn bind-info)]
       (if f
         (try
           (let [result (f sol)
                 result-mch (where/typed-val->mch (where/unmatched-var var-sym) result)]
             (or (where/update-solution-binding sol var-sym result-mch)
                 (assoc sol ::invalidated true)))
           (catch Exception e
             (log/debug "BIND evaluation error for" var-sym ":" (ex-message e))
             (assoc sol ::invalidated true)))
         ;; Static binding - bind-info is already a match object
         (or (where/update-solution-binding sol var-sym bind-info)
             (assoc sol ::invalidated true)))))
   solution
   bind-spec))

(defn- apply-binds
  "Apply all BIND specs to solutions.
   Solutions marked ::invalidated are removed.

   Args:
     solutions  - Sequence of solution maps
     bind-specs - Vector of bind specs (each a map {var-sym -> bind-info})"
  [solutions bind-specs]
  (if (seq bind-specs)
    (->> solutions
         (map (fn [sol] (reduce apply-bind-spec sol bind-specs)))
         (remove ::invalidated))
    solutions))

(defn- apply-expression-evaluators
  "Apply residual BIND and FILTER evaluators to solutions.

   This is called in -finalize after Iceberg scan but before anti-joins
   and aggregation. Order: BIND first (to introduce variables that may
   be needed for correlated EXISTS/NOT EXISTS), then FILTER.

   Args:
     solutions   - Sequence of solution maps from Iceberg scan
     evaluators  - Map {:filters [...] :binds [...]}"
  [solutions evaluators]
  (if (or (seq (:filters evaluators)) (seq (:binds evaluators)))
    (do
      (log/debug "Applying expression evaluators:"
                 {:filters (count (:filters evaluators))
                  :binds (count (:binds evaluators))
                  :input-count (if (counted? solutions) (count solutions) "lazy")})
      (let [;; Apply BINDs first to introduce new variables
            with-binds (apply-binds solutions (:binds evaluators))
            ;; Then apply FILTERs
            filtered (apply-filters with-binds (:filters evaluators))]
        (log/debug "Expression evaluation complete")
        filtered))
    solutions))

;;; ---------------------------------------------------------------------------
;;; IcebergDatabase Record (Multi-Table Support)
;;; ---------------------------------------------------------------------------

(defrecord IcebergDatabase [alias config sources mappings routing-indexes join-graph time-travel
                            query-pushdown aggregation-spec anti-join-spec expression-evaluators]
  ;; sources: {table-name -> IcebergSource}
  ;; mappings: {table-key -> {:table, :class, :predicates, ...}}
  ;; routing-indexes: {:class->mappings {rdf-class -> [mappings...]}, :predicate->mappings {pred -> [mappings...]}}
  ;; join-graph: {:edges [JoinEdge...], :by-table {table -> [edges]}, :tm->table {iri -> table}}
  ;; query-pushdown: atom holding query-time pushdown predicates (set in -reorder, used in -finalize)
  ;; aggregation-spec: atom holding aggregation spec {:group-keys [...] :aggregates [...] :order-by [...] :limit n}
  ;; anti-join-spec: atom holding anti-join patterns [{:type :exists/:not-exists/:minus :patterns [...]} ...]
  ;; expression-evaluators: atom holding residual FILTER/BIND evaluators {:filters [...] :binds [...]} (set in -reorder, used in -finalize)
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

  (-activate-alias [this _alias]
    (go this))

  (-aliases [_]
    [alias])

  (-finalize [_this _tracker error-ch solution-ch]
    (let [;; VALUES pushdown from atom - this is the primary path since pattern metadata
          ;; doesn't survive through the WHERE executor (known limitation)
          values-pushdown (when query-pushdown @query-pushdown)
          ;; Capture aggregation spec from atom (set in -reorder)
          agg-info (when aggregation-spec @aggregation-spec)
          ;; Capture anti-join spec from atom (set in -reorder)
          anti-joins (when anti-join-spec @anti-join-spec)
          ;; Capture expression evaluators from atom (set in -reorder)
          ;; These are non-pushable FILTER and BIND expressions
          expr-evals (when expression-evaluators @expression-evaluators)
          ;; NOTE: Subqueries are handled by standard Fluree execution via
          ;; match-pattern :query, not here. This avoids shared-state issues.
          ;; Capture columnar execution flag at query start (binding may change)
          use-columnar? *columnar-execution*
          ;; If aggregation, anti-joins, or expression evaluators are needed,
          ;; we must collect all solutions before emitting.
          needs-collection? (or agg-info (seq anti-joins)
                                (seq (:filters expr-evals)) (seq (:binds expr-evals)))
          out-ch (async/chan 1 (map #(dissoc % ::iceberg-patterns)))]
      (when (seq values-pushdown)
        (log/debug "Iceberg -finalize using VALUES pushdown from atom:" values-pushdown))
      (when agg-info
        (log/debug "Iceberg -finalize will apply aggregation:" agg-info))
      (when (seq anti-joins)
        (log/debug "Iceberg -finalize will apply anti-joins:" {:count (count anti-joins)
                                                               :types (mapv :type anti-joins)}))
      (when (or (seq (:filters expr-evals)) (seq (:binds expr-evals)))
        (log/debug "Iceberg -finalize will apply expression evaluators:"
                   {:filters (count (:filters expr-evals))
                    :binds (count (:binds expr-evals))}))
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
                  (let [patterns (get solution ::iceberg-patterns)]
                    (if (seq patterns)
                      (let [solution-pushdown (into (or (get solution ::solution-pushdown-filters) [])
                                                    (or values-pushdown []))]
                        ;; Execute query and collect results
                        (if (query/has-union-patterns? patterns)
                          ;; UNION path
                          (let [{:keys [union-patterns regular-patterns]} (query/extract-union-patterns patterns)
                                results (execute-union-patterns
                                         sources mappings routing-indexes join-graph
                                         union-patterns regular-patterns solution
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
                                                  (execute-columnar-single-table
                                                   source mapping patterns solution
                                                   time-travel solution-pushdown)
                                                  (query/execute-iceberg-query
                                                   source mapping patterns solution
                                                   time-travel nil solution-pushdown nil mappings))]
                                    (swap! all-solutions into results))))
                              ;; Multiple tables
                              (let [results (if use-columnar?
                                              (execute-columnar-multi-table
                                               sources pattern-groups solution
                                               time-travel solution-pushdown join-graph)
                                              (execute-multi-table-hash-join
                                               sources pattern-groups solution
                                               time-travel solution-pushdown join-graph mappings))]
                                (swap! all-solutions into results))))))
                      ;; No patterns - pass through
                      (swap! all-solutions conj solution)))
                  (recur)))
              ;; Apply expression evaluators first (BIND then FILTER)
              ;; This happens before anti-joins so bound vars are available
              (let [after-expressions (if expr-evals
                                        (vec (apply-expression-evaluators @all-solutions expr-evals))
                                        @all-solutions)
                    ;; NOTE: Subqueries are handled by standard Fluree execution via
                    ;; match-pattern :query, not here.
                    ;; Apply anti-joins (before query modifiers)
                    after-anti-joins (if (seq anti-joins)
                                       (apply-anti-joins after-expressions anti-joins
                                                         sources mappings routing-indexes
                                                         join-graph time-travel use-columnar?)
                                       after-expressions)
                    ;; Apply query modifiers (aggregation, DISTINCT, ORDER BY, LIMIT)
                    modified (if agg-info
                               (finalize-query-modifiers after-anti-joins agg-info mappings)
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
                                                 (or values-pushdown []))]
                     (when (seq solution-pushdown)
                       (log/debug "Iceberg -finalize combined solution pushdown:" solution-pushdown))

                     ;; Check for UNION patterns - handle them specially
                     (if (query/has-union-patterns? patterns)
                       ;; UNION path - extract and execute UNION branches
                       (let [{:keys [union-patterns regular-patterns]} (query/extract-union-patterns patterns)
                             final-solutions (execute-union-patterns
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
                                               (execute-columnar-single-table
                                                source mapping patterns solution
                                                time-travel solution-pushdown)
                                               (query/execute-iceberg-query source mapping patterns solution
                                                                            time-travel nil solution-pushdown nil mappings))]
                               (doseq [sol solutions]
                                 (async/>!! ch sol))
                               (async/close! ch)))
                           ;; Multiple tables - use hash join when join graph available
                           (let [final-solutions (if use-columnar?
                                                   (execute-columnar-multi-table
                                                    sources pattern-groups solution
                                                    time-travel solution-pushdown join-graph)
                                                   (execute-multi-table-hash-join
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
                other-patterns regular-patterns

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
                ;; Returns {:patterns [...] :failed [...]} where failed contains analyses
                ;; that couldn't be pushed (e.g., BIND-created vars with no column mapping)
                {:keys [patterns failed-pushable]}
                (if (seq pushable)
                  (let [{:keys [patterns failed]} (pushdown/annotate-patterns-with-pushdown
                                                   other-patterns pushable mappings routing-indexes)]
                    {:patterns patterns :failed-pushable failed})
                  {:patterns (vec other-patterns) :failed-pushable []})

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
                values-pushdown-predicates
                (->> direct-pushdown-map
                     (mapcat (fn [[column preds]]
                               (map #(assoc % :column column) preds)))
                     vec)

                _ (log/debug "Iceberg filter pushdown:"
                             {:total-filters (count filters)
                              :pushable-filters (count pushable)
                              :failed-pushable (count failed-pushable)
                              :values-patterns (count values-patterns)
                              :values-in-predicates (count values-predicates)
                              :values-pushdown-predicates values-pushdown-predicates
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

                ;; Store VALUES predicates in the atom for retrieval in -finalize
                _ (when (and query-pushdown (seq values-pushdown-predicates))
                    (reset! query-pushdown values-pushdown-predicates))

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
                  (assoc :select (transform-aggregates-to-variables current-select output-format))
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
            (throw (ex-info "Iceberg virtual graph requires :warehouse-path or :store (REST catalog mode also requires :store)"
                            {:error :db/invalid-config :config config})))
        _ (when (and rest-catalog? (nil? store))
            (throw (ex-info "Iceberg virtual graph REST :catalog requires :store (S3Store, FileStore, etc.)"
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
                              :store store
                              :auth-token (or (:auth-token catalog) (get catalog "auth-token"))})

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
                           :query-pushdown (atom nil)
                           :aggregation-spec (atom nil)
                           :anti-join-spec (atom nil)
                           :expression-evaluators (atom nil)})))
