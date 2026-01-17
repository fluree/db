(ns fluree.db.virtual-graph.iceberg.exec.arrow
  "Arrow-based columnar execution for Iceberg VG queries.

   This namespace provides the columnar execution entrypoint for db-iceberg.
   It requires the db-iceberg-arrow module with Arrow dependencies.

   Called from db-iceberg exec.clj via requiring-resolve when
   *columnar-execution* is true."
  (:require [fluree.db.tabular.protocol :as tabular]
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph.iceberg.plan :as plan]
            [fluree.db.virtual-graph.iceberg.query :as query])
  (:import [org.apache.arrow.vector VectorSchemaRoot]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; Utility Functions (from plan.clj)
;;; ---------------------------------------------------------------------------

(defn batch->row-maps
  "Convert Arrow VectorSchemaRoot batch to seq of row maps.
   Re-exports from plan for use by exec.clj."
  [batch]
  (plan/batch->row-maps batch))

;;; ---------------------------------------------------------------------------
;;; Statistics Collection
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

;;; ---------------------------------------------------------------------------
;;; Batch to Solutions Conversion
;;; ---------------------------------------------------------------------------

(defn- columnar-batch->solutions
  "Convert a batch of columnar data to SPARQL solutions.

   This is the boundary conversion from Arrow batches to solution maps.
   Handles both Arrow VectorSchemaRoot batches and individual row maps.

   Uses R2RML mapping to transform column values to proper RDF terms."
  ([batch mapping patterns base-solution]
   (columnar-batch->solutions batch mapping patterns base-solution nil))
  ([batch mapping patterns base-solution predicates]
   (let [pred->var (query/extract-predicate-bindings patterns)
         subject-var (some query/extract-subject-variable patterns)
         ;; Use core's row filtering (core is in db-iceberg, available via deps)
         row-matches? (fn [row]
                        (if (seq predicates)
                          (every? (fn [{:keys [column op value]}]
                                    (let [v (get row column)]
                                      (case op
                                        :eq (= v value)
                                        :ne (not= v value)
                                        :gt (when v (> (compare v value) 0))
                                        :gte (when v (>= (compare v value) 0))
                                        :lt (when v (< (compare v value) 0))
                                        :lte (when v (<= (compare v value) 0))
                                        :in (contains? (set value) v)
                                        :is-null (nil? v)
                                        :not-null (some? v)
                                        true)))
                                  predicates)
                          true))
         filter-rows (fn [rows] (filter row-matches? rows))]
     (cond
       ;; Arrow VectorSchemaRoot - convert to row maps, filter, then to solutions
       (instance? VectorSchemaRoot batch)
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

;;; ---------------------------------------------------------------------------
;;; Column Extraction Utilities
;;; ---------------------------------------------------------------------------

(defn- extract-columns-from-pattern-groups
  "Extract all column names needed by the query from pattern groups.

   Returns a set of column names (strings) needed for the query."
  [pattern-groups predicates]
  (into #{}
        (concat
         ;; Columns from pushdown predicates
         (keep :column predicates)
         ;; Columns from pattern predicates mapped via R2RML
         (for [{:keys [mapping patterns]} pattern-groups
               pattern patterns
               :let [triple (if (and (vector? pattern) (= :class (first pattern)))
                              (second pattern)
                              pattern)
                     [_s p _o] triple
                     pred-iri (when (map? p) (:fluree.db.query.exec.where/iri p))
                     object-map (when pred-iri
                                  (get-in mapping [:predicates pred-iri]))
                     column (when (and (map? object-map)
                                       (= :column (:type object-map)))
                              (:value object-map))]
               :when column]
           column))))

(defn- collect-all-join-columns
  "Collect all join key columns from the join graph."
  [join-graph]
  (when join-graph
    (into #{}
          (for [edge (:edges join-graph)
                col (concat (when-let [pc (:parent-columns edge)] pc)
                            (when-let [cc (:child-columns edge)] cc))]
            col))))

;;; ---------------------------------------------------------------------------
;;; Columnar Single-Table Execution
;;; ---------------------------------------------------------------------------

(defn execute-single-table
  "Execute a single-table query using columnar plan execution.

   Uses ScanOp from the plan compiler to read batches, then converts to
   solutions at the boundary using R2RML mapping.

   Phase 3b: Uses true columnar execution with filtered Arrow batches."
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
        scan-plan (plan/compile-single-table-plan
                   source table-name
                   (when (seq columns) (vec columns))
                   predicates time-travel
                   {:use-arrow-batches? true})]
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

;;; ---------------------------------------------------------------------------
;;; Columnar Multi-Table Execution
;;; ---------------------------------------------------------------------------

(defn execute-multi-table
  "Execute a multi-table query using columnar plan execution.

   Uses the plan compiler to create an operator tree with ScanOps
   and HashJoinOps.

   Phase 3c: True vectorized execution with automatic projection pushdown."
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
    (if-let [root-plan (plan/compile-plan sources groups-with-predicates
                                          join-graph stats-by-table time-travel
                                          {:use-arrow-batches? true
                                           :copy-batches? true
                                           :vectorized? true
                                           :output-columns output-columns})]
      (try
        (plan/open! root-plan)
        (loop [solutions []]
          (if-let [batch (plan/next-batch! root-plan)]
            ;; In vectorized mode, batch is VectorSchemaRoot from gather
            ;; Convert to row maps at the boundary
            (let [row-maps (cond
                             (instance? VectorSchemaRoot batch)
                             (let [rows (plan/batch->row-maps batch)]
                               ;; Close the gathered batch to free Arrow memory
                               (.close ^VectorSchemaRoot batch)
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
;;; Unified Entrypoint
;;; ---------------------------------------------------------------------------

(defn execute
  "Unified columnar executor entrypoint.

   Called from db-iceberg exec.clj via requiring-resolve when
   *columnar-execution* is true.

   Mode:
     :single-table - Execute single table columnar scan
     :multi-table  - Execute multi-table columnar join

   Args are passed as a map containing the parameters for each mode."
  [mode args]
  (case mode
    :single-table
    (let [{:keys [source mapping patterns base-solution time-travel predicates]} args]
      (execute-single-table source mapping patterns base-solution time-travel predicates))

    :multi-table
    (let [{:keys [sources pattern-groups base-solution time-travel predicates join-graph]} args]
      (execute-multi-table sources pattern-groups base-solution time-travel predicates join-graph))

    (throw (ex-info (str "Unknown columnar execution mode: " mode)
                    {:mode mode :error :db/invalid-execution-mode}))))
