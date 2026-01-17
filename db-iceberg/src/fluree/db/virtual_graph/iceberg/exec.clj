(ns fluree.db.virtual-graph.iceberg.exec
  "Execution strategy and orchestration for Iceberg VG queries.

   Handles row-based / columnar / multi-table join / UNION execution
   and Cartesian product safety guards.

   Columnar execution (Arrow) requires db-iceberg-arrow module.
   When *columnar-execution* is true, delegates to exec.arrow via requiring-resolve."
  (:require [clojure.set]
            [clojure.string :as str]
            [fluree.db.tabular.iceberg.core :as iceberg-core]
            [fluree.db.tabular.protocol :as tabular]
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph.iceberg.join :as join]
            [fluree.db.virtual-graph.iceberg.join.hash :as hash-join]
            [fluree.db.virtual-graph.iceberg.query :as query]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; Dynamic Configuration
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
  "Enable columnar execution path (requires db-iceberg-arrow module).

   When true, uses the plan compiler and Arrow-batch operators for query
   execution, keeping data in columnar format through joins.

   When false (default), uses the row-based solution approach.

   This flag enables A/B testing between execution strategies:
     (binding [*columnar-execution* true]
       (execute-query ...))

   NOTE: Columnar execution requires db-iceberg-arrow dependency.
   If the module is not available, an error will be thrown."
  false)

;;; ---------------------------------------------------------------------------
;;; Columnar Execution Support (requires db-iceberg-arrow)
;;; ---------------------------------------------------------------------------

(defn- resolve-arrow-executor
  "Resolve the Arrow executor from db-iceberg-arrow module.
   Returns nil if module not available."
  []
  (try
    (requiring-resolve 'fluree.db.virtual-graph.iceberg.exec.arrow/execute)
    (catch Exception _
      nil)))

(defn- execute-columnar*
  "Execute a columnar operation via db-iceberg-arrow module.
   Throws if module not available."
  [mode args]
  (if-let [executor (resolve-arrow-executor)]
    (executor mode args)
    (throw (ex-info "Arrow module not available. Add com.fluree/db-iceberg-arrow dependency for columnar execution."
                    {:status 501
                     :error :db/missing-arrow-module
                     :mode mode}))))

(defn execute-columnar-single-table
  "Execute a single-table query using columnar plan execution.
   Requires db-iceberg-arrow module."
  [source mapping patterns base-solution time-travel predicates]
  (execute-columnar* :single-table
                     {:source source
                      :mapping mapping
                      :patterns patterns
                      :base-solution base-solution
                      :time-travel time-travel
                      :predicates predicates}))

(defn execute-columnar-multi-table
  "Execute a multi-table query using columnar plan execution.
   Requires db-iceberg-arrow module."
  [sources pattern-groups base-solution time-travel predicates join-graph]
  (execute-columnar* :multi-table
                     {:sources sources
                      :pattern-groups pattern-groups
                      :base-solution base-solution
                      :time-travel time-travel
                      :predicates predicates
                      :join-graph join-graph}))

;;; ---------------------------------------------------------------------------
;;; Cartesian Product Safety
;;; ---------------------------------------------------------------------------

(defn check-cartesian-product-size!
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
;;; Multi-Table Join Execution Utilities
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
      (:fluree.db.query.exec.where/iri p))))

(defn- extract-pattern-subject-var
  "Extract the subject variable from a pattern item."
  [item]
  (let [triple (if (and (vector? item) (= :class (first item)))
                 (second item)
                 item)
        [s _p _o] triple]
    (when (and (map? s) (:fluree.db.query.exec.where/var s))
      (:fluree.db.query.exec.where/var s))))

(defn- extract-pattern-object-var
  "Extract the object variable from a pattern item."
  [item]
  (let [triple (if (and (vector? item) (= :class (first item)))
                 (second item)
                 item)
        [_s _p o] triple]
    (when (and (map? o) (:fluree.db.query.exec.where/var o))
      (:fluree.db.query.exec.where/var o))))

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

(defn execute-multi-table-hash-join
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
                            current-is-child? (= current-table (:child-table edge))

                            ;; CRITICAL: For OPTIONAL, force correct orientation
                            [build-solutions probe-solutions build-cols probe-cols]
                            (if optional?
                              ;; OPTIONAL: accumulated is required (probe), current is optional (build)
                              (if current-is-child?
                                [current-solutions accumulated-solutions
                                 (mapv keyword (join/child-columns edge))
                                 (mapv keyword (join/parent-columns edge))]
                                [current-solutions accumulated-solutions
                                 (mapv keyword (join/parent-columns edge))
                                 (mapv keyword (join/child-columns edge))])
                              ;; Inner join: use FK-based heuristic for efficiency
                              (if current-is-child?
                                [accumulated-solutions current-solutions
                                 (mapv keyword (join/parent-columns edge))
                                 (mapv keyword (join/child-columns edge))]
                                [current-solutions accumulated-solutions
                                 (mapv keyword (join/parent-columns edge))
                                 (mapv keyword (join/child-columns edge))]))

                            _ (log/debug "Hash join execution:"
                                         {:build-count (count build-solutions)
                                          :probe-count (count probe-solutions)
                                          :build-cols build-cols
                                          :probe-cols probe-cols
                                          :left-outer? optional?})

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
                      (let [acc-count (count accumulated-solutions)
                            curr-count (count current-solutions)
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

(defn execute-union-patterns
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
