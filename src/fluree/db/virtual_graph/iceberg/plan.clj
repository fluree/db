(ns fluree.db.virtual-graph.iceberg.plan
  "Tabular plan execution for columnar Iceberg queries.

   This namespace provides the ITabularPlan protocol and physical operators
   for executing multi-table queries in a fully columnar fashion. Plans are
   compiled from SPARQL pattern groups and executed on Arrow batches, with
   solution maps materialized only at the boundary.

   Key Components:
   - ITabularPlan protocol: Lifecycle interface (open!/next-batch!/close!)
   - ScanOp: Leaf operator that reads from ITabularSource
   - HashJoinOp: Columnar hash join on Arrow batches
   - FilterOp: Vectorized predicate evaluation
   - ProjectOp: Column projection/renaming

   Execution Model:
   - Pull-based: Operators pull batches from children via next-batch!
   - Batched: Data flows as Arrow VectorSchemaRoot batches
   - Lazy: Batches are produced on-demand
   - Resource-managed: close! releases all resources

   Example:
     (let [plan (compile-plan sources pattern-groups join-graph stats)
           _    (open! plan)]
       (try
         (loop [solutions []]
           (if-let [batch (next-batch! plan)]
             (recur (into solutions (batch->solutions batch mapping)))
             solutions))
         (finally
           (close! plan))))"
  (:require [fluree.db.tabular.protocol :as tabular]
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph.iceberg.join :as join])
  (:import [java.util HashMap ArrayList]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; ITabularPlan Protocol
;;; ---------------------------------------------------------------------------

(defprotocol ITabularPlan
  "Executable tabular plan node.

   Lifecycle:
   1. Create plan via factory function
   2. Call open! to initialize (returns self)
   3. Call next-batch! repeatedly until nil
   4. Call close! to release resources

   Thread safety: Not thread-safe. Use from a single thread."

  (open! [this]
    "Initialize the plan operator.
     - Opens child operators (if any)
     - Allocates resources (Arrow memory, hash tables, etc.)
     Returns self for chaining.")

  (next-batch! [this]
    "Produce the next batch of results.
     Returns an Arrow VectorSchemaRoot or nil when exhausted.

     IMPORTANT: The returned batch is owned by the operator and may be
     reused on the next call to next-batch!. Callers must consume the
     batch before calling next-batch! again.")

  (close! [this]
    "Release all resources held by this operator.
     - Closes child operators
     - Releases Arrow memory
     - Clears hash tables
     Safe to call multiple times.")

  (estimated-rows [this]
    "Return estimated output row count for planning.
     Used by the optimizer for join ordering and memory allocation."))

(defprotocol IColumnarHashJoin
  "Internal protocol for columnar hash join operations."
  (build-from-batch! [this batch]
    "Add a batch to the build side hash table.")
  (probe-batch [this batch]
    "Probe the hash table with a batch, returning joined batch."))

;;; ---------------------------------------------------------------------------
;;; Arrow Batch Utilities
;;; ---------------------------------------------------------------------------

(defn- get-vector-value
  "Extract a value from an Arrow vector at the given index.
   Returns nil for null values."
  [^org.apache.arrow.vector.FieldVector vector ^long idx]
  (when-not (.isNull vector (int idx))
    (let [type-id (.getMinorType vector)]
      (case (str type-id)
        "INT" (.get ^org.apache.arrow.vector.IntVector vector (int idx))
        "BIGINT" (.get ^org.apache.arrow.vector.BigIntVector vector (int idx))
        "FLOAT4" (.get ^org.apache.arrow.vector.Float4Vector vector (int idx))
        "FLOAT8" (.get ^org.apache.arrow.vector.Float8Vector vector (int idx))
        "VARCHAR" (str (.getObject ^org.apache.arrow.vector.VarCharVector vector (int idx)))
        "BIT" (= 1 (.get ^org.apache.arrow.vector.BitVector vector (int idx)))
        ;; Default: try getObject
        (.getObject vector (int idx))))))

(defn- extract-key-from-batch
  "Extract join key values from a batch at the given row index.
   For single-column keys, returns the value directly.
   For composite keys, returns a vector of values.
   Returns nil if any key column is null (null never matches)."
  [^org.apache.arrow.vector.VectorSchemaRoot batch key-columns ^long row-idx]
  (let [vals (mapv (fn [col-name]
                     (when-let [vector (.getVector batch ^String col-name)]
                       (get-vector-value vector row-idx)))
                   key-columns)]
    (when-not (some nil? vals)
      (if (= 1 (count vals))
        (first vals)
        vals))))

(defn- batch-row-count
  "Get the number of rows in a batch."
  [^org.apache.arrow.vector.VectorSchemaRoot batch]
  (.getRowCount batch))

;;; ---------------------------------------------------------------------------
;;; ScanOp - Leaf Operator
;;; ---------------------------------------------------------------------------

(defrecord ScanOp [source table-name columns predicates time-travel
                   batch-size use-arrow-batches? state]
  ;; state is an atom containing:
  ;; {:batch-iter nil :opened? false :row-count-estimate nil :mode :row-maps|:arrow}
  ITabularPlan
  (open! [this]
    (when-not (:opened? @state)
      (log/debug "ScanOp opening:" {:table table-name
                                    :columns columns
                                    :use-arrow-batches? use-arrow-batches?})
      ;; Get statistics for row count estimate
      (let [stats (tabular/get-statistics source table-name
                                          (cond-> {}
                                            (:snapshot-id time-travel)
                                            (assoc :snapshot-id (:snapshot-id time-travel))
                                            (:as-of-time time-travel)
                                            (assoc :as-of-time (:as-of-time time-travel))))
            row-count-estimate (or (:row-count stats) 1000)
            ;; Choose scan method based on use-arrow-batches? flag
            scan-opts (cond-> {:columns columns
                               :predicates predicates
                               :batch-size (or batch-size 4096)}
                        (:snapshot-id time-travel)
                        (assoc :snapshot-id (:snapshot-id time-travel))
                        (:as-of-time time-travel)
                        (assoc :as-of-time (:as-of-time time-travel)))
            batches (if use-arrow-batches?
                      ;; Use raw Arrow batches for columnar execution
                      ;; scan-arrow-batches returns VectorSchemaRoot directly
                      (tabular/scan-arrow-batches source table-name scan-opts)
                      ;; Use row maps (legacy behavior)
                      (tabular/scan-batches source table-name scan-opts))]
        (reset! state {:batch-iter (seq batches)
                       :opened? true
                       :row-count-estimate row-count-estimate
                       :mode (if use-arrow-batches? :arrow :row-maps)})))
    this)

  (next-batch! [_this]
    (let [{:keys [opened? batch-iter]} @state]
      (when (and opened? batch-iter)
        (let [batch (first batch-iter)]
          (swap! state assoc :batch-iter (next batch-iter))
          ;; Batch is either a row map or VectorSchemaRoot depending on mode
          batch))))

  (close! [this]
    (when (:opened? @state)
      (log/debug "ScanOp closing:" {:table table-name})
      (reset! state {:batch-iter nil :opened? false :row-count-estimate nil :mode nil}))
    this)

  (estimated-rows [_this]
    (or (:row-count-estimate @state) 1000)))

(defn create-scan-op
  "Create a scan operator for reading from an Iceberg table.

   Args:
     source      - ITabularSource implementation
     table-name  - Fully qualified table name
     columns     - Seq of column names to project (nil = all)
     predicates  - Seq of pushdown predicate maps
     time-travel - Time travel spec {:snapshot-id or :as-of-time}

   Options:
     :batch-size        - Rows per batch (default 4096)
     :use-arrow-batches? - If true, return raw Arrow VectorSchemaRoot batches.
                           If false (default), return row maps for backward
                           compatibility. Set to true for columnar execution."
  ([source table-name columns predicates]
   (create-scan-op source table-name columns predicates nil {}))
  ([source table-name columns predicates time-travel]
   (create-scan-op source table-name columns predicates time-travel {}))
  ([source table-name columns predicates time-travel opts]
   (map->ScanOp {:source source
                 :table-name table-name
                 :columns (when columns (vec columns))
                 :predicates (when predicates (vec predicates))
                 :time-travel time-travel
                 :batch-size (get opts :batch-size 4096)
                 :use-arrow-batches? (get opts :use-arrow-batches? false)
                 :state (atom {:batch-iter nil
                               :opened? false
                               :row-count-estimate nil
                               :mode nil})})))

;;; ---------------------------------------------------------------------------
;;; HashJoinOp - Columnar Hash Join
;;; ---------------------------------------------------------------------------

(defrecord HashJoinOp [build-child probe-child build-keys probe-keys state]
  ;; state is an atom containing:
  ;; {:hash-table HashMap, :build-complete? bool, :opened? bool,
  ;;  :build-row-count int, :estimated-output-rows int}
  ITabularPlan
  (open! [this]
    (when-not (:opened? @state)
      (log/debug "HashJoinOp opening:" {:build-keys build-keys :probe-keys probe-keys})
      ;; Open children
      (open! build-child)
      (open! probe-child)
      ;; Estimate output rows using join cardinality estimation
      (let [build-rows (estimated-rows build-child)
            probe-rows (estimated-rows probe-child)
            est-output (min (* build-rows probe-rows) (max build-rows probe-rows))]
        (reset! state {:hash-table (HashMap.)
                       :build-complete? false
                       :opened? true
                       :build-row-count 0
                       :estimated-output-rows est-output})))
    this)

  (next-batch! [this]
    (let [{:keys [opened? build-complete?]} @state]
      (when opened?
        ;; Phase 1: Build hash table from build side (if not done)
        (when-not build-complete?
          (log/debug "HashJoinOp building hash table...")
          (loop []
            (when-let [batch (next-batch! build-child)]
              (build-from-batch! this batch)
              (recur)))
          (swap! state assoc :build-complete? true)
          (let [{:keys [hash-table build-row-count]} @state]
            (log/debug "HashJoinOp build complete:" {:build-rows build-row-count
                                                     :unique-keys (.size ^HashMap hash-table)})))
        ;; Phase 2: Probe with batches from probe side
        (when-let [probe-b (next-batch! probe-child)]
          (probe-batch this probe-b)))))

  (close! [this]
    (when (:opened? @state)
      (log/debug "HashJoinOp closing")
      (close! build-child)
      (close! probe-child)
      (when-let [^HashMap ht (:hash-table @state)]
        (.clear ht))
      (reset! state {:hash-table nil :build-complete? false :opened? false
                     :build-row-count 0 :estimated-output-rows nil}))
    this)

  (estimated-rows [_this]
    (or (:estimated-output-rows @state) 1000))

  IColumnarHashJoin
  (build-from-batch! [_this batch]
    (let [^HashMap hash-table (:hash-table @state)
          ^org.apache.arrow.vector.VectorSchemaRoot vsr batch
          row-count (batch-row-count vsr)]
      (dotimes [i row-count]
        (when-let [key (extract-key-from-batch vsr build-keys i)]
          ;; Store row index + batch reference (simplified - real impl would copy values)
          (let [^ArrayList rows (or (.get hash-table key)
                                    (let [al (ArrayList.)]
                                      (.put hash-table key al)
                                      al))
                ;; Extract row data from batch columns
                row-data (into {}
                               (for [^org.apache.arrow.vector.FieldVector fv (.getFieldVectors vsr)
                                     :let [col-name (.getName (.getField fv))]]
                                 [col-name (get-vector-value fv i)]))]
            (.add rows row-data)
            (swap! state update :build-row-count inc))))))

  (probe-batch [_this batch]
    ;; For now, return the probe batch with matches
    ;; A full implementation would produce a new batch with joined columns
    ;; This is a placeholder that will be enhanced
    (let [^HashMap hash-table (:hash-table @state)
          row-count (batch-row-count batch)
          matches (atom 0)]
      (dotimes [i row-count]
        (when-let [key (extract-key-from-batch batch probe-keys i)]
          (when-let [^ArrayList build-rows (.get hash-table key)]
            (swap! matches + (.size build-rows)))))
      (log/debug "HashJoinOp probe batch:" {:probe-rows row-count :matches @matches})
      ;; TODO: Build proper output batch with joined columns
      ;; For now, return probe batch as placeholder
      batch)))

(defn create-hash-join-op
  "Create a hash join operator for joining two tabular plans.

   The build child's output is loaded into a hash table, then the probe
   child's batches are streamed through to find matches.

   Args:
     build-child - ITabularPlan for build side (smaller table preferred)
     probe-child - ITabularPlan for probe side
     build-keys  - Vector of column names for build-side key
     probe-keys  - Vector of column names for probe-side key

   The join produces output containing columns from both sides."
  [build-child probe-child build-keys probe-keys]
  {:pre [(satisfies? ITabularPlan build-child)
         (satisfies? ITabularPlan probe-child)
         (vector? build-keys)
         (vector? probe-keys)
         (= (count build-keys) (count probe-keys))]}
  (map->HashJoinOp {:build-child build-child
                    :probe-child probe-child
                    :build-keys build-keys
                    :probe-keys probe-keys
                    :state (atom {:hash-table nil
                                  :build-complete? false
                                  :opened? false
                                  :build-row-count 0
                                  :estimated-output-rows nil})}))

;;; ---------------------------------------------------------------------------
;;; FilterOp - Vectorized Filtering
;;; ---------------------------------------------------------------------------

(defrecord FilterOp [child predicates state]
  ;; state is an atom containing {:opened? bool}
  ITabularPlan
  (open! [this]
    (when-not (:opened? @state)
      (log/debug "FilterOp opening:" {:predicates (count predicates)})
      (open! child)
      (reset! state {:opened? true}))
    this)

  (next-batch! [_this]
    (when (:opened? @state)
      ;; Get next batch from child and apply filters
      ;; For now, pass through (filtering done at scan level via pushdown)
      ;; A full implementation would apply vectorized predicates here
      (when-let [batch (next-batch! child)]
        ;; TODO: Apply predicates that couldn't be pushed down
        ;; For Iceberg, most predicates are pushed to scan level
        batch)))

  (close! [this]
    (when (:opened? @state)
      (log/debug "FilterOp closing")
      (close! child)
      (reset! state {:opened? false}))
    this)

  (estimated-rows [_this]
    ;; Apply selectivity estimate
    (let [child-rows (estimated-rows child)
          selectivity (join/estimate-selectivity {} predicates)]
      (long (* child-rows selectivity)))))

(defn create-filter-op
  "Create a filter operator for applying predicates.

   Note: For Iceberg sources, predicates are typically pushed down to
   the scan level. This operator handles residual predicates that
   couldn't be pushed down.

   Args:
     child      - Child ITabularPlan
     predicates - Seq of predicate maps {:column :op :value}"
  [child predicates]
  {:pre [(satisfies? ITabularPlan child)]}
  (map->FilterOp {:child child
                  :predicates (vec predicates)
                  :state (atom {:opened? false})}))

;;; ---------------------------------------------------------------------------
;;; ProjectOp - Column Projection
;;; ---------------------------------------------------------------------------

(defrecord ProjectOp [child columns column-aliases state]
  ;; state is an atom containing {:opened? bool}
  ITabularPlan
  (open! [this]
    (when-not (:opened? @state)
      (log/debug "ProjectOp opening:" {:columns columns})
      (open! child)
      (reset! state {:opened? true}))
    this)

  (next-batch! [_this]
    (when (:opened? @state)
      (when-let [batch (next-batch! child)]
        ;; TODO: Create new batch with only projected columns
        ;; For now, return original batch
        ;; A full implementation would slice the batch to requested columns
        batch)))

  (close! [this]
    (when (:opened? @state)
      (log/debug "ProjectOp closing")
      (close! child)
      (reset! state {:opened? false}))
    this)

  (estimated-rows [_this]
    (estimated-rows child)))

(defn create-project-op
  "Create a projection operator for selecting/renaming columns.

   Note: For Iceberg sources, column projection is typically pushed
   down to the scan level. This operator handles post-join projections
   and column aliasing for SPARQL variables.

   Args:
     child          - Child ITabularPlan
     columns        - Seq of column names to keep
     column-aliases - Map of {old-name -> new-name} for renaming (optional)"
  ([child columns]
   (create-project-op child columns nil))
  ([child columns column-aliases]
   {:pre [(satisfies? ITabularPlan child)]}
   (map->ProjectOp {:child child
                    :columns (vec columns)
                    :column-aliases column-aliases
                    :state (atom {:opened? false})})))

;;; ---------------------------------------------------------------------------
;;; Batch to Solution Conversion
;;; ---------------------------------------------------------------------------

(defn batch->row-maps
  "Convert an Arrow batch to a lazy seq of row maps.

   Args:
     batch - Arrow VectorSchemaRoot

   Returns lazy seq of {column-name -> value} maps."
  [^org.apache.arrow.vector.VectorSchemaRoot batch]
  (let [row-count (.getRowCount batch)
        field-vectors (.getFieldVectors batch)]
    (for [i (range row-count)]
      (into {}
            (for [^org.apache.arrow.vector.FieldVector fv field-vectors
                  :let [col-name (.getName (.getField fv))
                        value (get-vector-value fv i)]
                  :when (some? value)]
              [col-name value])))))

;;; ---------------------------------------------------------------------------
;;; Plan Execution Helper
;;; ---------------------------------------------------------------------------

(defn execute-plan
  "Execute a tabular plan, returning all row maps.

   Opens the plan, drains all batches, converts to row maps, and closes.
   Use for testing or when full materialization is acceptable.

   Args:
     plan - ITabularPlan to execute

   Returns vector of row maps."
  [plan]
  (open! plan)
  (try
    (loop [rows []]
      (if-let [batch (next-batch! plan)]
        (recur (into rows (batch->row-maps batch)))
        rows))
    (finally
      (close! plan))))

;;; ---------------------------------------------------------------------------
;;; Plan Compiler
;;; ---------------------------------------------------------------------------

(defn- collect-columns-for-table
  "Collect all columns needed for a table: predicates + join columns."
  [mapping predicates join-graph]
  (let [table-name (:table mapping)
        ;; Columns from predicates (for WHERE conditions)
        predicate-cols (keep :column predicates)
        ;; Columns from subject template
        subject-cols (when-let [template (:subject-template mapping)]
                       (re-seq #"\{([^}]+)\}" template))
        ;; Join columns from edges
        join-cols (when join-graph
                    (let [edges (join/edges-for-table join-graph table-name)]
                      (for [edge edges
                            col (if (= table-name (:child-table edge))
                                  (join/child-columns edge)
                                  (join/parent-columns edge))]
                        col)))]
    (-> (concat predicate-cols
                (map second subject-cols)
                join-cols)
        distinct
        vec)))

(defn- build-scan-op-for-group
  "Build a ScanOp for a single pattern group.

   Args:
     sources           - Map of {table-name -> ITabularSource}
     mapping           - R2RML mapping for this table
     predicates        - Pushdown predicates for this table
     join-graph        - Join graph (for join column inclusion)
     time-travel       - Time travel spec
     use-arrow-batches? - If true, use raw Arrow batches for columnar execution"
  [sources mapping predicates join-graph time-travel use-arrow-batches?]
  (let [table-name (:table mapping)
        source (get sources table-name)
        columns (collect-columns-for-table mapping predicates join-graph)]
    (when-not source
      (throw (ex-info (str "No source for table: " table-name)
                      {:table table-name
                       :available (keys sources)})))
    (create-scan-op source table-name columns predicates time-travel
                    {:use-arrow-batches? use-arrow-batches?})))

(defn- find-join-edge
  "Find the join edge connecting two tables, or nil if not connected."
  [join-graph table-a table-b]
  (first (join/edges-between join-graph table-a table-b)))

(defn compile-plan
  "Compile a tabular plan from pattern groups.

   This is the main entry point for creating an executable plan from
   SPARQL pattern groups routed to Iceberg tables.

   The compiler:
   1. Creates a ScanOp for each table
   2. Uses greedy join ordering based on cardinality estimates
   3. Chains ScanOps together with HashJoinOps
   4. Returns the root operator

   Args:
     sources         - Map of {table-name -> ITabularSource}
     pattern-groups  - [{:mapping m :patterns [...] :predicates [...]}]
     join-graph      - Join graph from build-join-graph
     stats-by-table  - Map of {table-name -> statistics}
     time-travel     - Optional time travel spec
     opts            - Options map:
                       :use-arrow-batches? - If true, use raw Arrow batches
                                             for columnar execution (default false)

   Returns:
     ITabularPlan root operator, or nil if no pattern groups."
  ([sources pattern-groups join-graph stats-by-table time-travel]
   (compile-plan sources pattern-groups join-graph stats-by-table time-travel {}))
  ([sources pattern-groups join-graph stats-by-table time-travel opts]
   (when (seq pattern-groups)
     (let [use-arrow-batches? (get opts :use-arrow-batches? false)
           ;; Build scan ops for each table
           scans-by-table
           (into {}
                 (for [{:keys [mapping predicates]} pattern-groups
                       :let [table-name (:table mapping)]]
                   [table-name
                    {:scan (build-scan-op-for-group sources mapping
                                                    (or predicates [])
                                                    join-graph time-travel
                                                    use-arrow-batches?)
                     :mapping mapping}]))

           table-names (keys scans-by-table)

          ;; Use greedy join ordering if multiple tables
           join-order (if (> (count table-names) 1)
                        (join/greedy-join-order (set table-names)
                                                join-graph
                                                stats-by-table
                                                {})
                        (vec table-names))]

       (log/debug "Plan compiler:" {:tables table-names
                                    :join-order join-order})

       (if (= 1 (count join-order))
        ;; Single table - just return the scan
         (:scan (get scans-by-table (first join-order)))

        ;; Multiple tables - chain with hash joins
         (reduce
          (fn [accumulated-plan current-table]
            (if (nil? accumulated-plan)
             ;; First table - start with its scan
              (:scan (get scans-by-table current-table))
             ;; Subsequent tables - join to accumulated plan
              (let [current-scan (:scan (get scans-by-table current-table))
                   ;; Find join edge between current and any accumulated table
                   ;; This is a simplification - real impl would track accumulated tables
                    edge (some (fn [t]
                                 (find-join-edge join-graph t current-table))
                               (take-while #(not= % current-table) join-order))]
                (if edge
                 ;; Create hash join
                  (let [;; Determine build vs probe based on which is accumulated
                        current-is-child? (= current-table (:child-table edge))
                        [build-plan probe-plan build-keys probe-keys]
                        (if current-is-child?
                         ;; Accumulated is parent (dimension), current is child (fact)
                          [accumulated-plan current-scan
                           (vec (join/parent-columns edge))
                           (vec (join/child-columns edge))]
                         ;; Current is parent, accumulated is child
                          [current-scan accumulated-plan
                           (vec (join/parent-columns edge))
                           (vec (join/child-columns edge))])]
                    (log/debug "Creating hash join:" {:build-keys build-keys
                                                      :probe-keys probe-keys
                                                      :edge edge})
                    (create-hash-join-op build-plan probe-plan build-keys probe-keys))
                 ;; No edge found - would be Cartesian product
                 ;; For now, just return accumulated (caller should handle)
                  (do
                    (log/warn "No join edge found, skipping table:" current-table)
                    accumulated-plan)))))
          nil
          join-order))))))

(defn compile-single-table-plan
  "Compile a plan for a single table query (no joins).

   Simpler entry point when only one table is involved.

   Args:
     source      - ITabularSource
     table-name  - Table name
     columns     - Columns to project
     predicates  - Pushdown predicates
     time-travel - Optional time travel spec
     opts        - Options map:
                   :use-arrow-batches? - If true, use raw Arrow batches
                                         for columnar execution (default false)

   Returns:
     ScanOp for the table."
  ([source table-name columns predicates time-travel]
   (compile-single-table-plan source table-name columns predicates time-travel {}))
  ([source table-name columns predicates time-travel opts]
   (create-scan-op source table-name columns predicates time-travel opts)))
