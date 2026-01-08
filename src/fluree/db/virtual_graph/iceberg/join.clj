(ns fluree.db.virtual-graph.iceberg.join
  "Join graph construction and operations for multi-table Iceberg virtual graphs.

   This namespace provides:
   - JoinEdge data structure for representing table relationships
   - Join graph construction from R2RML RefObjectMap declarations
   - Query-time join planning utilities

   A join edge represents a foreign key relationship between two tables:
   {:child-table   \"routes\"        ; Table containing the FK
    :parent-table  \"airlines\"      ; Table containing the PK
    :columns       [{:child \"airline_id\" :parent \"id\"}]  ; Join columns (supports composite keys)
    :predicate     \"http://example.org/operatedBy\"       ; RDF predicate from RefObjectMap
    :estimated-selectivity nil}     ; Optional: for cardinality estimation

   Join Graph Structure:
   {:edges    [JoinEdge...]                    ; All join edges
    :by-table {\"table\" -> [JoinEdge...]}     ; Edges indexed by participating table
    :tm->table {\"<#TriplesMap>\" -> \"table\"}  ; TriplesMap IRI to table name lookup}"
  (:require [fluree.db.util.log :as log]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; JoinEdge Construction
;;; ---------------------------------------------------------------------------

(defn make-join-edge
  "Create a join edge from a RefObjectMap.

   Args:
     child-table   - Table name containing the foreign key
     parent-table  - Table name containing the primary key
     join-conditions - Vector of {:child \"col\" :parent \"col\"} from R2RML
     predicate     - RDF predicate IRI from the predicateObjectMap

   Returns a join edge map."
  [child-table parent-table join-conditions predicate]
  {:child-table child-table
   :parent-table parent-table
   :columns join-conditions
   :predicate predicate
   :estimated-selectivity nil})

;;; ---------------------------------------------------------------------------
;;; Join Graph Construction
;;; ---------------------------------------------------------------------------

(defn- build-triples-map-index
  "Build an index from TriplesMap IRI to table name.

   This is needed to resolve parentTriplesMap references."
  [mappings]
  (into {}
        (for [[_table-key mapping] mappings
              :let [tm-iri (:triples-map-iri mapping)
                    table (:table mapping)]
              :when (and tm-iri table)]
          [tm-iri table])))

(defn- extract-ref-predicates
  "Extract all RefObjectMap predicates from a mapping.

   Returns a sequence of {:predicate iri :ref ref-object-map} for each
   predicate with type :ref."
  [mapping]
  (for [[pred-iri obj-map] (:predicates mapping)
        :when (= :ref (:type obj-map))]
    {:predicate pred-iri
     :ref obj-map}))

(defn build-join-graph
  "Build a join graph from R2RML mappings.

   Extracts join edges from RefObjectMap declarations in the mappings.
   Each RefObjectMap with parentTriplesMap creates a directed edge from
   the child table (containing the FK) to the parent table (containing the PK).

   Args:
     mappings - Map of {table-key -> mapping} from parse-r2rml

   Returns:
     {:edges     [JoinEdge...]
      :by-table  {\"table\" -> [JoinEdge...]}  ; All edges where table participates
      :tm->table {\"<#TriplesMap>\" -> \"table\"}}

   Example:
     Given R2RML with RouteMapping referencing AirlineMapping:
       rr:objectMap [ rr:parentTriplesMap <#AirlineMapping> ;
                      rr:joinCondition [ rr:child \"airline_id\" ; rr:parent \"id\" ] ]

     Returns edge:
       {:child-table \"routes\" :parent-table \"airlines\"
        :columns [{:child \"airline_id\" :parent \"id\"}]
        :predicate \"http://example.org/operatedBy\"}"
  [mappings]
  (let [tm->table (build-triples-map-index mappings)
        edges (vec
               (for [[_table-key mapping] mappings
                     :let [child-table (:table mapping)]
                     {:keys [predicate ref]} (extract-ref-predicates mapping)
                     :let [parent-tm (:parent-triples-map ref)
                           parent-table (get tm->table parent-tm)
                           join-conditions (:join-conditions ref)]
                     :when (and parent-table (seq join-conditions))]
                 (do
                   (log/debug "Found join edge:" {:child child-table
                                                  :parent parent-table
                                                  :predicate predicate
                                                  :columns join-conditions})
                   (make-join-edge child-table parent-table join-conditions predicate))))
        ;; Index edges by participating table (both child and parent)
        by-table (reduce (fn [idx edge]
                           (-> idx
                               (update (:child-table edge) (fnil conj []) edge)
                               (update (:parent-table edge) (fnil conj []) edge)))
                         {}
                         edges)]
    (when (seq edges)
      (log/info "Built join graph:" {:edge-count (count edges)
                                     :tables (keys by-table)}))
    {:edges edges
     :by-table by-table
     :tm->table tm->table}))

;;; ---------------------------------------------------------------------------
;;; Join Graph Query Operations
;;; ---------------------------------------------------------------------------

(defn edges-for-table
  "Get all join edges where a table participates (as child or parent)."
  [join-graph table-name]
  (get-in join-graph [:by-table table-name] []))

(defn edges-between
  "Get join edges connecting two specific tables."
  [join-graph table-a table-b]
  (let [edges-a (edges-for-table join-graph table-a)]
    (filter (fn [edge]
              (or (and (= (:child-table edge) table-a)
                       (= (:parent-table edge) table-b))
                  (and (= (:child-table edge) table-b)
                       (= (:parent-table edge) table-a))))
            edges-a)))

(defn connected-tables
  "Get all tables directly connected to a table via join edges."
  [join-graph table-name]
  (let [edges (edges-for-table join-graph table-name)]
    (set (for [edge edges]
           (if (= (:child-table edge) table-name)
             (:parent-table edge)
             (:child-table edge))))))

(defn edge-for-predicate
  "Find the join edge associated with a specific RDF predicate.

   Useful for resolving RefObjectMap predicates during query execution."
  [join-graph predicate-iri]
  (first (filter #(= (:predicate %) predicate-iri)
                 (:edges join-graph))))

(defn has-join-edges?
  "Check if the join graph has any edges."
  [join-graph]
  (boolean (seq (:edges join-graph))))

;;; ---------------------------------------------------------------------------
;;; Join Column Extraction
;;; ---------------------------------------------------------------------------

(defn child-columns
  "Extract the child column names from a join edge.

   For composite keys, returns a vector of column names."
  [edge]
  (mapv :child (:columns edge)))

(defn parent-columns
  "Extract the parent column names from a join edge.

   For composite keys, returns a vector of column names."
  [edge]
  (mapv :parent (:columns edge)))

(defn join-column-pairs
  "Get pairs of [child-col parent-col] for a join edge.

   For hash join key extraction."
  [edge]
  (mapv (juxt :child :parent) (:columns edge)))

;;; ---------------------------------------------------------------------------
;;; Cardinality Estimation
;;; ---------------------------------------------------------------------------

(defn- get-ndv
  "Extract NDV (Number of Distinct Values) for a column from statistics.

   Fallback strategy when distinct-count is not available:
   1. If value-count < row-count, use value-count as conservative estimate
   2. Otherwise assume all values are unique (worst case for join estimation)

   Args:
     stats   - Table statistics from extract-statistics with :include-column-stats? true
     col-key - Column name (keyword or string)

   Returns estimated NDV (always >= 1)"
  [stats col-key]
  (let [col-name (name col-key)
        col-stats (get-in stats [:column-stats col-name])
        row-count (or (:row-count stats) 1)
        ;; Prefer distinct-count (from Theta Sketch / HLL if available)
        ndv (:distinct-count col-stats)
        value-count (:value-count col-stats)]
    (or ndv
        ;; Fallback: if value-count is less than row-count, it's a conservative estimate
        ;; (value-count includes nulls typically, so this is usually close to row count)
        (when (and value-count (< value-count row-count))
          value-count)
        ;; Last resort: assume all unique
        row-count
        1)))

(defn estimate-join-cardinality
  "Estimate the result cardinality of a join between two tables.

   Uses the formula: |R ⋈ S| = |R| * |S| / max(NDV(R.k), NDV(S.k))

   This assumes a uniform distribution of join key values. For skewed data,
   this may underestimate cardinality. For foreign key joins where every
   child row has a matching parent, this is typically accurate.

   Args:
     left-stats  - Statistics for left table (from extract-statistics)
     right-stats - Statistics for right table
     left-key    - Join column name in left table
     right-key   - Join column name in right table

   Returns estimated row count for the join result."
  [left-stats right-stats left-key right-key]
  (let [left-rows (or (:row-count left-stats) 1)
        right-rows (or (:row-count right-stats) 1)
        left-ndv (get-ndv left-stats left-key)
        right-ndv (get-ndv right-stats right-key)
        max-ndv (max left-ndv right-ndv 1)]
    (log/debug "Join cardinality estimation:"
               {:left-rows left-rows
                :right-rows right-rows
                :left-ndv left-ndv
                :right-ndv right-ndv
                :max-ndv max-ndv})
    (long (/ (* left-rows right-rows) max-ndv))))

(defn estimate-selectivity
  "Estimate selectivity of predicates on a table.

   For now, uses simple heuristics:
   - Equality on primary/unique key: 1/row-count
   - Equality on other column: 1/NDV or 10% if unknown
   - Range predicate: 30% (conservative)
   - IN list: n/NDV where n is list size

   Args:
     stats      - Table statistics
     predicates - Seq of predicate maps with :op, :column, :value

   Returns selectivity as a decimal (0.0 to 1.0)"
  [stats predicates]
  (if (empty? predicates)
    1.0
    (reduce
     (fn [sel {:keys [op column value]}]
       (let [ndv (get-ndv stats column)
             row-count (or (:row-count stats) 1)
             pred-sel (case op
                        :eq (/ 1.0 (max ndv 1))
                        :ne (- 1.0 (/ 1.0 (max ndv 1)))
                        :in (/ (count value) (max ndv 1))
                        (:gt :gte :lt :lte) 0.3
                        :between 0.1
                        :is-null (let [null-count (get-in stats [:column-stats (name column) :null-count] 0)]
                                   (/ null-count (max row-count 1)))
                        :not-null (let [null-count (get-in stats [:column-stats (name column) :null-count] 0)]
                                    (- 1.0 (/ null-count (max row-count 1))))
                        ;; Default: assume 50% selectivity
                        0.5)]
         ;; Combine selectivities (assumes independence)
         (* sel (min 1.0 (max 0.001 pred-sel)))))
     1.0
     predicates)))

;;; ---------------------------------------------------------------------------
;;; Greedy Join Ordering
;;; ---------------------------------------------------------------------------

(defn- find-connecting-edge
  "Find a join edge connecting joined-tables to candidate-table.

   Returns the edge or nil if tables are not connected."
  [join-graph joined-tables candidate-table]
  (first
   (for [edge (:edges join-graph)
         :when (or (and (contains? joined-tables (:child-table edge))
                        (= candidate-table (:parent-table edge)))
                   (and (contains? joined-tables (:parent-table edge))
                        (= candidate-table (:child-table edge))))]
     edge)))

(defn- estimate-join-cost
  "Estimate the cost of joining candidate-table to already-joined tables.

   Returns the estimated intermediate result cardinality."
  [join-graph joined-tables candidate-table stats-by-table current-cardinality]
  (if-let [edge (find-connecting-edge join-graph joined-tables candidate-table)]
    (let [candidate-stats (get stats-by-table candidate-table)
          candidate-rows (or (:row-count candidate-stats) 1)
          ;; For the join, we need the join column from the candidate side
          candidate-key (if (contains? joined-tables (:child-table edge))
                          ;; Joined side is child, candidate is parent
                          (first (parent-columns edge))
                          ;; Joined side is parent, candidate is child
                          (first (child-columns edge)))
          ;; Get NDV from candidate side (the new table being joined)
          candidate-ndv (get-ndv candidate-stats candidate-key)]
      ;; Estimate: current_rows * candidate_rows / max(current_ndv, candidate_ndv)
      ;; Simplified: use candidate NDV as the divisor
      (long (/ (* current-cardinality candidate-rows)
               (max candidate-ndv 1))))
    ;; Not connected - would be cartesian product (very expensive!)
    Long/MAX_VALUE))

(defn greedy-join-order
  "Determine join order using a greedy algorithm that minimizes intermediate result sizes.

   Strategy:
   1. Start with the most selective table (smallest estimated row count after predicates)
   2. Greedily add the table that produces the smallest intermediate result
   3. Only consider tables connected to the current joined set (no cartesian products)

   Args:
     tables            - Set of table names to join
     join-graph        - Join graph from build-join-graph
     stats-by-table    - Map of {table-name -> statistics}
     predicates-by-table - Map of {table-name -> [predicates]} for selectivity estimation

   Returns:
     Vector of table names in recommended join order, or nil if tables are disconnected."
  [tables join-graph stats-by-table predicates-by-table]
  (when (seq tables)
    (let [tables-set (set tables)
          ;; Estimate post-predicate row counts for each table
          estimated-rows (into {}
                               (for [t tables-set
                                     :let [stats (get stats-by-table t)
                                           predicates (get predicates-by-table t [])
                                           row-count (or (:row-count stats) 1)
                                           selectivity (estimate-selectivity stats predicates)]]
                                 [t (long (* row-count selectivity))]))
          ;; Start with the most selective table (smallest estimated rows)
          start-table (key (apply min-key val estimated-rows))
          start-rows (get estimated-rows start-table)]

      (log/debug "Join ordering - starting table:" start-table
                 "with estimated" start-rows "rows")

      (loop [joined #{start-table}
             order [start-table]
             current-cardinality start-rows
             remaining (disj tables-set start-table)]
        (if (empty? remaining)
          (do
            (log/debug "Join order determined:" order)
            order)
          ;; Find connected candidates
          (let [connected (filter #(find-connecting-edge join-graph joined %)
                                  remaining)]
            (if (empty? connected)
              ;; No connected tables - remaining tables would require cartesian product
              (do
                (log/warn "Join ordering: disconnected tables require cartesian product:"
                          {:joined joined :remaining remaining})
                ;; Return partial order - caller must handle disconnected tables
                (into order remaining))
              ;; Pick candidate with minimum estimated join cost
              (let [costs (for [t connected]
                            [t (estimate-join-cost join-graph joined t stats-by-table current-cardinality)])
                    [best-table best-cost] (apply min-key second costs)]
                (log/debug "Adding table" best-table "to join order, estimated intermediate:" best-cost)
                (recur (conj joined best-table)
                       (conj order best-table)
                       best-cost
                       (disj remaining best-table))))))))))

(defn plan-join-sequence
  "Plan the sequence of join operations for a multi-table query.

   Returns a vector of join steps, each describing which table to join
   and on which columns.

   Args:
     join-order      - Vector of table names in order (from greedy-join-order)
     join-graph      - Join graph with edges
     stats-by-table  - Statistics for cardinality estimates

   Returns:
     [{:table \"first-table\" :type :scan}
      {:table \"second-table\" :type :hash-join :edge JoinEdge :estimated-rows N}
      ...]"
  [join-order join-graph stats-by-table]
  (when (seq join-order)
    (loop [remaining (rest join-order)
           joined #{(first join-order)}
           current-rows (or (get-in stats-by-table [(first join-order) :row-count]) 1)
           plan [{:table (first join-order)
                  :type :scan
                  :estimated-rows current-rows}]]
      (if (empty? remaining)
        plan
        (let [next-table (first remaining)
              edge (find-connecting-edge join-graph joined next-table)
              next-stats (get stats-by-table next-table)
              next-rows (or (:row-count next-stats) 1)
              ;; Estimate join result size
              estimated-rows (if edge
                               (estimate-join-cost join-graph joined next-table
                                                   stats-by-table current-rows)
                               (* current-rows next-rows))] ;; Cartesian if no edge
          (recur (rest remaining)
                 (conj joined next-table)
                 estimated-rows
                 (conj plan {:table next-table
                             :type (if edge :hash-join :cartesian)
                             :edge edge
                             :estimated-rows estimated-rows})))))))
