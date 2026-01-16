(ns fluree.db.virtual-graph.iceberg.modifiers
  "Query modifiers for Iceberg VG: aggregation, ordering, distinct, limit/offset, HAVING.

   These are post-scan operations applied to solutions after Iceberg table scans."
  (:require [clojure.string :as str]
            [fluree.db.datatype :as datatype]
            [fluree.db.query.exec.select :as select]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph.iceberg.query :as query])
  (:import [fluree.db.query.exec.select AsSelector]))

(set! *warn-on-reflection* true)

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

;;; ---------------------------------------------------------------------------
;;; Ordering / Limiting / Distinct
;;; ---------------------------------------------------------------------------

(defn apply-order-by
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

(defn apply-limit-offset
  "Apply LIMIT and OFFSET to a sequence of solutions."
  [solutions limit offset]
  (cond->> solutions
    offset (drop offset)
    limit (take limit)))

(defn apply-distinct
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
;;; Query Modifier Finalization Pipeline
;;; ---------------------------------------------------------------------------

(defn transform-aggregates-to-variables
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

(defn apply-having
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

(defn finalize-query-modifiers
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
