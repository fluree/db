(ns fluree.db.virtual-graph.iceberg.query
  "Query execution and result transformation for Iceberg virtual graphs.

   This namespace handles:
   - Pattern routing to appropriate tables
   - Query execution against Iceberg sources
   - Result transformation from Iceberg rows to SPARQL solutions

   The query executor combines predicate pushdown from multiple sources
   (triple patterns, FILTER clauses, VALUES clauses) and executes
   optimized scans against the underlying Iceberg tables."
  (:require [clojure.string :as str]
            [fluree.db.constants :as const]
            [fluree.db.query.exec.where :as where]
            [fluree.db.tabular.protocol :as tabular]
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph.iceberg.pushdown :as pushdown]
            [fluree.db.virtual-graph.iceberg.r2rml :as r2rml]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; Pattern Routing (Multi-Table Support)
;;; ---------------------------------------------------------------------------

(defn build-routing-indexes
  "Build indexes for routing patterns to the correct table.

   Uses multi-maps to support multiple tables mapping the same class/predicate.
   This is common in RDF where the same predicate may appear in multiple tables.

   Returns:
     {:class->mappings {rdf-class -> [mapping...]}
      :predicate->mappings {predicate-iri -> [mapping...]}}"
  [mappings]
  (let [;; Build class -> [mappings] multi-map
        class->mappings (->> mappings
                             vals
                             (filter :class)
                             (reduce (fn [acc m]
                                       (update acc (:class m) (fnil conj []) m))
                                     {}))
        ;; Build predicate -> [mappings] multi-map
        predicate->mappings (->> mappings
                                 vals
                                 (reduce (fn [acc m]
                                           (reduce (fn [a pred]
                                                     (update a pred (fnil conj []) m))
                                                   acc
                                                   (keys (:predicates m))))
                                         {}))]
    {:class->mappings class->mappings
     :predicate->mappings predicate->mappings}))

(defn- extract-pattern-info
  "Extract type and predicates from a pattern item."
  [item]
  (let [triple (if (= :class (first item)) (second item) item)
        [s p o] triple
        subject-var (when (and (map? s) (get s ::where/var))
                      (get s ::where/var))
        pred-iri (when (map? p) (get p ::where/iri))
        is-type? (= const/iri-rdf-type pred-iri)
        rdf-type (when (and is-type? (or (string? o) (map? o)))
                   (if (string? o) o (get o ::where/iri)))]
    {:subject-var subject-var
     :predicate pred-iri
     :is-type? is-type?
     :rdf-type rdf-type
     :item item}))

(defn group-patterns-by-table
  "Group patterns by which table they should be routed to.

   Uses the routing indexes to determine which table handles each pattern.
   Patterns are grouped by subject variable to keep related patterns together.

   Note: When multiple tables map the same class/predicate, the first mapping
   is used. For multi-table joins, use find-all-mappings instead.

   Returns: [{:mapping mapping :patterns [...]} ...]"
  [patterns mappings routing-indexes]
  (let [{:keys [class->mappings predicate->mappings]} routing-indexes
        pattern-infos (map extract-pattern-info patterns)

        ;; Find mapping for each pattern (takes first when multiple exist)
        find-mapping (fn [{:keys [rdf-type predicate]}]
                       (or (when rdf-type (first (get class->mappings rdf-type)))
                           (when predicate (first (get predicate->mappings predicate)))
                           (first (vals mappings))))

        ;; Group by subject variable first, then by mapping
        by-subject (group-by :subject-var pattern-infos)

        ;; For each subject group, determine the primary mapping
        groups (for [[_subj-var infos] by-subject
                     :let [;; Find mappings for patterns with type info first
                           type-patterns (filter :rdf-type infos)
                           mapping (if (seq type-patterns)
                                     (find-mapping (first type-patterns))
                                     (find-mapping (first infos)))]]
                 {:mapping mapping
                  :patterns (mapv :item infos)})]
    (vec groups)))

(defn analyze-clause-for-mapping
  "Find the mapping that matches the query patterns."
  [clause mappings]
  (when (seq mappings)
    (let [type-triple (first (filter (fn [item]
                                       (let [triple (if (= :class (first item))
                                                      (second item)
                                                      item)
                                             [_ p o] triple]
                                         (and (map? p)
                                              (= const/iri-rdf-type (get p ::where/iri))
                                              (or (string? o)
                                                  (and (map? o) (get o ::where/iri))))))
                                     clause))
          rdf-type (when type-triple
                     (let [triple (if (= :class (first type-triple))
                                    (second type-triple)
                                    type-triple)
                           o (nth triple 2)]
                       (if (string? o) o (get o ::where/iri))))
          predicates (->> clause
                          (map second)
                          (filter map?)
                          (map ::where/iri)
                          set)
          relevant (if rdf-type
                     (->> mappings
                          (filter (fn [[_ m]] (= (:class m) rdf-type)))
                          (map second))
                     (->> mappings
                          (filter (fn [[_ m]]
                                    (some #(get-in m [:predicates %]) predicates)))
                          (map second)))]
      (or (first relevant) (first (vals mappings))))))

;;; ---------------------------------------------------------------------------
;;; Pattern Analysis
;;; ---------------------------------------------------------------------------

(defn extract-predicate-bindings
  "Extract predicate IRI -> variable name mappings from patterns."
  [clause]
  (->> clause
       (map (fn [item]
              (let [[_ p o] (if (= :class (first item)) (second item) item)]
                (when (and (map? p) (map? o) (get o ::where/var))
                  [(get p ::where/iri) (get o ::where/var)]))))
       (remove nil?)
       (into {})))

(defn extract-literal-filters
  "Extract predicate IRI -> literal value for WHERE conditions."
  [clause]
  (->> clause
       (map (fn [item]
              (let [[_ p o] (if (= :class (first item)) (second item) item)]
                (when (and (map? p) (get p ::where/iri)
                           (map? o) (get o ::where/val))
                  [(get p ::where/iri) (get o ::where/val)]))))
       (remove nil?)
       (into {})))

(defn extract-solution-predicates
  "Extract pushdown predicates from solution bindings.

   When a pattern has a variable in object position, and that variable is
   already bound in the solution (e.g., from VALUES decomposition), we can
   push that binding as an equality predicate to Iceberg.

   Returns a seq of {:column :op :value} predicate maps with coerced values."
  [patterns solution mapping]
  (let [pred->var (extract-predicate-bindings patterns)]
    (for [[pred-iri var-name] pred->var
          :let [match (get solution var-name)
                ;; Get the literal value from the match
                literal-val (when match (where/get-value match))
                ;; Map predicate IRI to column name and get datatype
                object-map (get-in mapping [:predicates pred-iri])
                column (when (and (map? object-map) (= :column (:type object-map)))
                         (:value object-map))
                datatype (:datatype object-map)
                ;; Coerce the value based on R2RML datatype
                coerced-val (pushdown/coerce-value literal-val datatype nil)]
          :when (and column coerced-val)]
      {:column column :op :eq :value coerced-val})))

(defn extract-subject-variable
  "Extract the subject variable from a pattern item."
  [item]
  (cond
    (and (vector? item) (= :class (first item)) (vector? (second item)))
    (let [[subject] (second item)]
      (when (and (map? subject) (get subject ::where/var))
        (get subject ::where/var)))
    (vector? item)
    (let [[subject] item]
      (when (and (map? subject) (get subject ::where/var))
        (get subject ::where/var)))))

;;; ---------------------------------------------------------------------------
;;; Predicate Translation
;;; ---------------------------------------------------------------------------

(defn literal-filters->predicates
  "Convert literal filters to ITabularSource predicates with coerced values."
  [pred->literal mapping]
  (for [[pred-iri literal-val] pred->literal
        :let [object-map (get-in mapping [:predicates pred-iri])
              column (when (and (map? object-map) (= :column (:type object-map)))
                       (:value object-map))
              datatype (:datatype object-map)
              coerced-val (pushdown/coerce-value literal-val datatype nil)]
        :when (and column coerced-val)]
    {:column column :op :eq :value coerced-val}))

;;; ---------------------------------------------------------------------------
;;; Result Transformation
;;; ---------------------------------------------------------------------------

(defn- process-template-subject
  "Process subject template by substituting column values."
  [template row]
  (when template
    (reduce (fn [tmpl col]
              (let [col-val (or (get row col)
                                (get row (str/lower-case col))
                                (get row (str/upper-case col)))]
                (if col-val
                  (str/replace tmpl (str "{" col "}") (str col-val))
                  tmpl)))
            template
            (r2rml/extract-template-cols template))))

(defn- value->rdf-match
  "Convert an Iceberg value to an RDF match object."
  [value var-sym]
  (cond
    (nil? value)
    (where/unmatched-var var-sym)

    (integer? value)
    (where/match-value {} value const/iri-xsd-integer)

    (float? value)
    (where/match-value {} value const/iri-xsd-double)

    (instance? Double value)
    (where/match-value {} value const/iri-xsd-double)

    :else
    (where/match-value {} value const/iri-string)))

(defn row->solution
  "Transform an Iceberg row to a SPARQL solution map.

   When join-columns are provided, stores raw column values under
   ::join-col-vals for use by hash join operators."
  ([row mapping var-mappings subject-var base-solution]
   (row->solution row mapping var-mappings subject-var base-solution nil))
  ([row mapping var-mappings subject-var base-solution join-columns]
   (let [subject-id (process-template-subject (:subject-template mapping) row)
         subject-binding (when subject-var
                           (let [subj-sym (if (symbol? subject-var) subject-var (symbol subject-var))]
                             [[subj-sym (where/match-iri {} (or subject-id "urn:unknown"))]]))
         pred-bindings (for [[pred-iri var-name] var-mappings
                             :when (and var-name
                                        (not= pred-iri const/iri-rdf-type))
                             :let [object-map (get-in mapping [:predicates pred-iri])
                                   column (when (and (map? object-map) (= :column (:type object-map)))
                                            (:value object-map))
                                   value (when column
                                           (or (get row column)
                                               (get row (str/lower-case column))))
                                   var-sym (if (symbol? var-name) var-name (symbol var-name))]
                             :when value]
                         [var-sym (value->rdf-match value var-sym)])
         ;; Store raw join column values for hash join operators
         ;; These are stored under keywords (not symbols) for efficient lookup
         join-col-vals (when (seq join-columns)
                         (into {}
                               (for [col join-columns
                                     :let [value (or (get row col)
                                                     (get row (str/lower-case col))
                                                     (get row (str/upper-case col)))]
                                     :when (some? value)]
                                 [(keyword col) value])))]
     (cond-> (into (or base-solution {})
                   (concat subject-binding pred-bindings))
       (seq join-col-vals) (assoc ::join-col-vals join-col-vals)))))

;;; ---------------------------------------------------------------------------
;;; Query Execution
;;; ---------------------------------------------------------------------------

(defn execute-iceberg-query
  "Execute query against Iceberg source with predicate pushdown.

   time-travel can be:
   - nil (latest snapshot)
   - {:snapshot-id Long} (specific Iceberg snapshot)
   - {:as-of-time Instant} (time-travel to specific time)

   limit is an optional hint to limit the number of rows scanned.
   solution-pushdown is an optional vector of pushdown filters from the solution map.
   join-columns is an optional set of column names to include for join operations.
   Returns a lazy seq of solutions - limit is enforced at the scan level
   for early termination."
  ([source mapping patterns base-solution time-travel]
   (execute-iceberg-query source mapping patterns base-solution time-travel nil nil nil))
  ([source mapping patterns base-solution time-travel limit]
   (execute-iceberg-query source mapping patterns base-solution time-travel limit nil nil))
  ([source mapping patterns base-solution time-travel limit solution-pushdown]
   (execute-iceberg-query source mapping patterns base-solution time-travel limit solution-pushdown nil))
  ([source mapping patterns base-solution time-travel limit solution-pushdown join-columns]
   (let [table-name (:table mapping)
         pred->var (extract-predicate-bindings patterns)
         pred->literal (extract-literal-filters patterns)
         subject-var (some extract-subject-variable patterns)

         ;; Build columns to select (include join columns for hash join support)
         query-columns (->> pred->var
                            keys
                            (keep (fn [pred-iri]
                                    (let [om (get-in mapping [:predicates pred-iri])]
                                      (when (= :column (:type om))
                                        (:value om)))))
                            (concat (r2rml/extract-template-cols (:subject-template mapping))))
         columns (-> (concat query-columns (or join-columns []))
                     distinct
                     vec)

         ;; Build predicates for pushdown from triple patterns (equality)
         literal-predicates (vec (literal-filters->predicates pred->literal mapping))

         ;; Extract pushed-down FILTER predicates from pattern metadata
         pushed-predicates (pushdown/extract-pushdown-filters patterns)

         ;; Extract predicates from solution bindings (from VALUES decomposition)
         ;; When a variable is already bound in the solution, we can push it as equality
         solution-bound-predicates (vec (extract-solution-predicates patterns base-solution mapping))

         ;; Include explicit solution-level pushdown filters
         all-solution-pushdown (or solution-pushdown [])

         ;; Combine all predicates and coalesce eq predicates on same column into IN
         all-predicates (-> literal-predicates
                            (into pushed-predicates)
                            (into solution-bound-predicates)
                            (into all-solution-pushdown)
                            pushdown/coalesce-predicates)

         _ (log/debug "Iceberg query:" {:table table-name
                                        :columns columns
                                        :join-columns join-columns
                                        :literal-predicates (count literal-predicates)
                                        :pushed-predicates (count pushed-predicates)
                                        :solution-bound-predicates (count solution-bound-predicates)
                                        :solution-pushdown (count all-solution-pushdown)
                                        :total-predicates (count all-predicates)
                                        :coalesced-predicates all-predicates
                                        :time-travel time-travel
                                        :limit limit})

         ;; Execute scan with time-travel and limit options
         ;; Returns a lazy seq - limit is enforced at iterator level for early termination
         rows (tabular/scan-rows source table-name
                                 (cond-> {:columns (when (seq columns) columns)
                                          :predicates (when (seq all-predicates) all-predicates)}
                                   (:snapshot-id time-travel)
                                   (assoc :snapshot-id (:snapshot-id time-travel))

                                   (:as-of-time time-travel)
                                   (assoc :as-of-time (:as-of-time time-travel))

                                   limit
                                   (assoc :limit limit)))]

     ;; Transform to solutions - this is also lazy
     ;; Pass join-columns so raw values are stored for hash join
     (map #(row->solution % mapping pred->var subject-var base-solution join-columns) rows))))
