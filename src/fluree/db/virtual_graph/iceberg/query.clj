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
            [fluree.db.query.exec.select :as select]
            [fluree.db.query.exec.where :as where]
            [fluree.db.tabular.protocol :as tabular]
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph.iceberg.pushdown :as pushdown]
            [fluree.db.virtual-graph.iceberg.r2rml :as r2rml])
  (:import [fluree.db.query.exec.select AsSelector]))

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

(defn- union-pattern?
  "Check if a pattern is a UNION pattern.
   UNION patterns are MapEntry with :union as the key."
  [item]
  (and (map-entry? item)
       (= :union (key item))))

(defn- extract-pattern-info
  "Extract type and predicates from a pattern item.

   Also detects :optional patterns and extracts the inner patterns,
   marking them as optional.

   Returns nil for UNION patterns (they must be handled separately at a higher level)."
  [item]
  ;; UNION patterns are handled separately - return special marker
  (cond
    ;; UNION pattern - return special marker to be filtered out
    (union-pattern? item)
    {:union-pattern? true
     :item item}

    ;; Optional pattern container
    (and (vector? item) (= :optional (first item)))
    ;; Extract inner patterns and mark as optional
    (let [inner-patterns (second item)]
      (mapv #(assoc (extract-pattern-info %) :optional? true) inner-patterns))

    ;; Regular pattern (triple or :class)
    :else
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
       :item item
       :optional? false})))

(defn extract-union-patterns
  "Extract UNION patterns from a list of patterns.

   Returns a map with:
     :union-patterns - vector of UNION patterns (each is a MapEntry with :union key)
     :regular-patterns - vector of non-UNION patterns

   UNION patterns are MapEntry with :union as the key and a vector of
   where-clauses as the value. Each where-clause is itself a vector of patterns."
  [patterns]
  (let [grouped (group-by union-pattern? patterns)]
    {:union-patterns (vec (get grouped true []))
     :regular-patterns (vec (get grouped false []))}))

(defn has-union-patterns?
  "Check if pattern list contains any UNION patterns."
  [patterns]
  (some union-pattern? patterns))

(defn group-patterns-by-table
  "Group patterns by which table they should be routed to.

   Uses the routing indexes to determine which table handles each pattern.
   Patterns are grouped by subject variable to keep related patterns together.

   Also handles OPTIONAL patterns, marking the resulting pattern groups
   with :optional? true so joins can use left outer join semantics.

   NOTE: UNION patterns are filtered out and should be handled separately
   using extract-union-patterns before calling this function.

   LIMITATION: OPTIONAL block structure is not preserved for multi-table cases.
   Currently, each pattern is individually marked as optional, then grouped by
   subject. This works correctly for simple two-table OPTIONAL cases like:

     ?airline ex:name ?name .
     OPTIONAL { ?route ex:operatedBy ?airline . }

   But for complex multi-table OPTIONAL blocks:

     ?airline ex:name ?name .
     OPTIONAL {
       ?route ex:operatedBy ?airline .
       ?airport ex:city ?city .
       ?route ex:sourceAirportRef ?airport .
     }

   The patterns within the OPTIONAL block should inner-join with each other
   before left-outer-joining with required patterns. The current implementation
   treats each optional group independently, which may produce incorrect results
   for complex multi-table OPTIONAL scenarios.

   Note: When multiple tables map the same class/predicate, the first mapping
   is used. For multi-table joins, use find-all-mappings instead.

   Returns: [{:mapping mapping :patterns [...] :optional? bool} ...]"
  [patterns mappings routing-indexes]
  (let [{:keys [class->mappings predicate->mappings]} routing-indexes
        ;; Extract pattern infos - this may return vectors for :optional patterns
        raw-pattern-infos (map extract-pattern-info patterns)
        ;; Flatten any nested vectors from :optional expansion
        ;; NOTE: :optional patterns return a vector of info maps, regular patterns return a single map
        ;; We check if the element is a vector (from :optional) to flatten it
        pattern-infos (mapcat #(if (vector? %) % [%]) raw-pattern-infos)
        ;; Filter out UNION patterns (they're handled separately)
        non-union-infos (remove :union-pattern? pattern-infos)

        ;; Find mapping for each pattern (takes first when multiple exist)
        find-mapping (fn [{:keys [rdf-type predicate]}]
                       (or (when rdf-type (first (get class->mappings rdf-type)))
                           (when predicate (first (get predicate->mappings predicate)))
                           (first (vals mappings))))

        ;; Group by subject variable first, then by mapping
        by-subject (group-by :subject-var non-union-infos)

        ;; For each subject group, determine the primary mapping
        ;; and whether it's optional (all patterns in group must be optional)
        groups (for [[_subj-var infos] by-subject
                     :when (seq infos)  ;; Skip empty groups
                     :let [;; Find mappings for patterns with type info first
                           type-patterns (filter :rdf-type infos)
                           mapping (if (seq type-patterns)
                                     (find-mapping (first type-patterns))
                                     (find-mapping (first infos)))
                           ;; Group is optional if ALL patterns in it are optional
                           optional? (every? :optional? infos)]]
                 {:mapping mapping
                  :patterns (mapv :item infos)
                  :optional? optional?})]
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
   ::join-col-vals for use by hash join operators.

   When all-mappings is provided, also handles RefObjectMap predicates
   by building parent IRIs from foreign key values."
  ([row mapping var-mappings subject-var base-solution]
   (row->solution row mapping var-mappings subject-var base-solution nil nil))
  ([row mapping var-mappings subject-var base-solution join-columns]
   (row->solution row mapping var-mappings subject-var base-solution join-columns nil))
  ([row mapping var-mappings subject-var base-solution join-columns all-mappings]
   (let [subject-id (process-template-subject (:subject-template mapping) row)
         subject-binding (when subject-var
                           (let [subj-sym (if (symbol? subject-var) subject-var (symbol subject-var))]
                             [[subj-sym (where/match-iri {} (or subject-id "urn:unknown"))]]))
         ;; Handle :column type predicates (simple column mappings)
         column-bindings (for [[pred-iri var-name] var-mappings
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
         ;; Handle :ref type predicates (RefObjectMap - foreign key relationships)
         ;; Build parent IRI from FK value using parent mapping's subject template
         ref-bindings (when all-mappings
                        (for [[pred-iri var-name] var-mappings
                              :when (and var-name
                                         (not= pred-iri const/iri-rdf-type))
                              :let [object-map (get-in mapping [:predicates pred-iri])]
                              :when (= :ref (:type object-map))
                              :let [;; Get the parent TriplesMap IRI and look up its mapping
                                    parent-tm-iri (:parent-triples-map object-map)
                                    ;; Find parent mapping by TriplesMap IRI
                                    parent-mapping (some (fn [[_ m]]
                                                           (when (= parent-tm-iri (:triples-map-iri m))
                                                             m))
                                                         all-mappings)
                                    parent-template (when parent-mapping
                                                      (:subject-template parent-mapping))
                                    ;; Get FK value from child column
                                    join-cond (first (:join-conditions object-map))
                                    fk-col (:child join-cond)
                                    pk-col (:parent join-cond)
                                    fk-value (when fk-col
                                               (or (get row fk-col)
                                                   (get row (str/lower-case fk-col))
                                                   (get row (str/upper-case fk-col))))
                                    ;; Build parent IRI by substituting FK value for PK column
                                    parent-iri (when (and parent-template fk-value pk-col)
                                                 (str/replace parent-template
                                                              (str "{" pk-col "}")
                                                              (str fk-value)))
                                    var-sym (if (symbol? var-name) var-name (symbol var-name))]
                              :when parent-iri]
                          [var-sym (where/match-iri {} parent-iri)]))
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
                   (concat subject-binding column-bindings ref-bindings))
       (seq join-col-vals) (assoc ::join-col-vals join-col-vals)))))

;;; ---------------------------------------------------------------------------
;;; Query Execution
;;; ---------------------------------------------------------------------------

;;; ---------------------------------------------------------------------------
;;; Aggregation Detection
;;; ---------------------------------------------------------------------------

(def ^:private aggregate-fn-names
  "Set of aggregate function names (as symbols or strings)."
  #{'count 'count-distinct 'sum 'avg 'min 'max 'sample 'sample1 'groupconcat
    "count" "count-distinct" "sum" "avg" "min" "max" "sample" "sample1" "groupconcat"})

(defn- parse-aggregate-expr
  "Parse an aggregate expression from SELECT clause.

   Handles formats:
   - \"(count ?var)\" - direct aggregate
   - \"(as (count ?var) ?alias)\" - aggregate with alias
   - (count ?var) - list form
   - AsSelector records with :aggregate? field

   Returns nil if not an aggregate, or a map:
   {:fn :count/:sum/:avg/:min/:max/:count-distinct
    :column column-name (nil for COUNT(*))
    :alias output-column-name (string)
    :var original-variable-symbol}"
  [expr mapping]
  (log/debug "parse-aggregate-expr input:" {:expr-type (type expr)
                                            :is-as-selector? (instance? AsSelector expr)
                                            :aggregate? (when (instance? AsSelector expr) (:aggregate? expr))})
  (cond
    ;; Handle AsSelector records from parsed SPARQL queries
    (and (instance? AsSelector expr)
         (:aggregate? expr))
    (let [agg-fn-name (:aggregate? expr)
          bind-var (:bind-var expr)
          ;; Get aggregate-info from metadata if available
          agg-info (::select/aggregate-info (meta expr))
          _ (log/debug "parse-aggregate-expr AsSelector:" {:agg-fn-name agg-fn-name
                                                           :bind-var bind-var
                                                           :agg-info agg-info
                                                           :vars (:vars agg-info)})
          fn-name-sym (or (:fn-name agg-info) agg-fn-name)
          all-vars (:vars agg-info)
          ;; Filter out the bind-var from vars to get the actual aggregated variable
          ;; The vars set includes both the output alias and the variable being aggregated
          agg-vars (when all-vars (disj all-vars bind-var))
          ;; Convert function name to keyword
          fn-keyword (case (if (symbol? fn-name-sym) fn-name-sym (symbol (str fn-name-sym)))
                       count :count
                       count-distinct :count-distinct
                       sum :sum
                       avg :avg
                       min :min
                       max :max
                       sample :sample
                       sample1 :sample
                       groupconcat :groupconcat
                       :count)
          ;; For COUNT(*), the actual aggregate vars (excluding bind-var) will be empty
          is-count-star? (or (empty? agg-vars) (nil? agg-vars))
          ;; Get the first variable from the filtered set (aggregates typically operate on one var)
          var-sym (first agg-vars)
          ;; Extract variable name, stripping ? prefix
          raw-var-name (when var-sym (name var-sym))
          var-name (when raw-var-name
                     (if (str/starts-with? raw-var-name "?")
                       (subs raw-var-name 1)
                       raw-var-name))
          ;; Try to find column from predicate mapping
          mapped-column (when (and var-name mapping)
                          (some (fn [[pred obj-map]]
                                  (when (= :column (:type obj-map))
                                    (let [obj-var (:var obj-map)
                                          obj-value (:value obj-map)
                                          pred-suffix (when (string? pred)
                                                        (last (str/split pred #"[/#]")))]
                                      (when (or (= var-name obj-var)
                                                (= var-name obj-value)
                                                (= var-name pred-suffix))
                                        obj-value))))
                                (:predicates mapping)))
          column (if is-count-star?
                   nil
                   (or mapped-column var-name))
          alias-name (if (symbol? bind-var)
                       (let [n (name bind-var)]
                         (if (str/starts-with? n "?")
                           (subs n 1)
                           n))
                       (str bind-var))
          result {:fn fn-keyword
                  :column column
                  :alias alias-name
                  :var var-sym
                  :var-name var-name
                  ;; Include bind-var as the output SPARQL variable for result key conversion
                  :bind-var bind-var}]
      (log/debug "parse-aggregate-expr AsSelector result:" {:is-count-star? is-count-star?
                                                            :column column
                                                            :alias alias-name
                                                            :bind-var bind-var
                                                            :result result})
      result)

    ;; Handle string and list forms
    :else
    (let [;; Parse string expressions into list form
          parsed (cond
                   (string? expr)
                   (try
                     (read-string expr)
                     (catch Exception _ nil))

                   (list? expr)
                   expr

                   (seq? expr)
                   expr

                   :else nil)]
      (when (and parsed (seq? parsed))
        (let [[fn-name & args] parsed]
          (cond
            ;; (as (aggregate-fn ...) ?alias)
            (= fn-name 'as)
            (let [[inner-expr alias-var] args
                  inner-parsed (parse-aggregate-expr (if (seq? inner-expr)
                                                       inner-expr
                                                       (str inner-expr))
                                                     mapping)]
              (when inner-parsed
                (assoc inner-parsed
                       :alias (if (symbol? alias-var)
                                (name alias-var)
                                (str alias-var))
                       :var alias-var)))

            ;; Direct aggregate: (count ?var), (sum ?var), etc.
            (contains? aggregate-fn-names fn-name)
            (let [fn-keyword (case (if (symbol? fn-name) fn-name (symbol fn-name))
                               count :count
                               count-distinct :count-distinct
                               sum :sum
                               avg :avg
                               min :min
                               max :max
                               sample :sample
                               sample1 :sample
                               groupconcat :groupconcat
                               :count)
                  ;; For count(*), args is typically empty or contains *
                  ;; For other aggs, first arg is the variable
                  var-arg (first args)
                  is-count-star? (or (nil? var-arg)
                                     (= var-arg '*)
                                     (= var-arg "*"))
                  ;; Extract variable name, stripping ? prefix if present
                  ;; (name '?country) returns "?country", so we strip the leading ?
                  raw-var-name (when (and (not is-count-star?) (symbol? var-arg))
                                 (name var-arg))
                  var-name (when raw-var-name
                             (if (str/starts-with? raw-var-name "?")
                               (subs raw-var-name 1)
                               raw-var-name))
                  ;; Try to find column from predicate mapping
                  ;; Compare both with and without ? prefix for robustness
                  mapped-column (when (and var-name mapping)
                                  (some (fn [[_pred obj-map]]
                                          (when (= :column (:type obj-map))
                                            (let [obj-var (:var obj-map)]
                                              (when (or (= var-name obj-var)
                                                        (= raw-var-name obj-var)
                                                        (= var-name (str "?" obj-var)))
                                                (:value obj-map)))))
                                        (:predicates mapping)))
                  ;; Use mapped column if found, else use var-name directly
                  ;; solution-get-column-value handles symbol/string lookup
                  column (if is-count-star?
                           nil  ;; Only nil for COUNT(*)
                           (or mapped-column var-name))
                  ;; Build a descriptive default alias for bare aggregates without (as ...)
                  ;; SPARQL spec requires aliases for aggregates in SELECT, so bare aggregates
                  ;; indicate the translator didn't wrap properly. Use descriptive default.
                  default-alias (if is-count-star?
                                  (str fn-name)  ;; "count" for COUNT(*)
                                  (str fn-name "_" (or var-name "val")))]  ;; "count_country" for COUNT(?country)
              ;; Note: Bare aggregates without (as ...) are technically invalid SPARQL.
              ;; The translator should always produce (as (count ?x) ?alias) forms.
              {:fn fn-keyword
               :column column
               :alias default-alias
               :var var-arg
               :var-name var-name})

            :else nil))))))

(defn extract-aggregates-from-select
  "Extract aggregate specifications from a query SELECT clause.

   Args:
     select-clause - The :select value from parsed query (vector of selectors)
     mapping       - R2RML mapping for variable->column resolution

   Returns vector of aggregate specs:
   [{:fn :count/:sum/:avg/:min/:max/:count-distinct
     :column column-name (nil for COUNT(*))
     :alias output-column-name
     :var original-variable}]"
  [select-clause mapping]
  (when (and select-clause (or (vector? select-clause) (sequential? select-clause)))
    (vec (keep #(parse-aggregate-expr % mapping) select-clause))))

(defn extract-group-by-columns
  "Extract GROUP BY column names from a query.

   Args:
     group-by-clause - The :group-by value from parsed query
     mapping         - R2RML mapping for variable->column resolution

   Returns vector of column names (strings)."
  [group-by-clause mapping]
  (when group-by-clause
    (let [vars (if (vector? group-by-clause)
                 group-by-clause
                 [group-by-clause])]
      (vec (keep (fn [var]
                   ;; Extract var name without ? prefix
                   (let [var-str (cond
                                   (symbol? var) (name var)
                                   (string? var) var
                                   :else nil)
                         ;; Strip leading ? if present
                         var-name (when var-str
                                    (if (str/starts-with? var-str "?")
                                      (subs var-str 1)
                                      var-str))]
                     ;; Try to find column from predicate mapping
                     ;; Match against: :var in obj-map, :value in obj-map, or predicate IRI suffix
                     (when (and var-name mapping)
                       (some (fn [[pred obj-map]]
                               (when (= :column (:type obj-map))
                                 (let [obj-var (:var obj-map)
                                       obj-value (:value obj-map)
                                       ;; Extract predicate suffix (last path segment)
                                       pred-suffix (when (string? pred)
                                                     (last (str/split pred #"[/#]")))]
                                   (when (or (= var-name obj-var)
                                             (= var-name obj-value)
                                             (= var-name pred-suffix))
                                     obj-value))))
                             (:predicates mapping)))))
                 vars)))))

(defn has-aggregations?
  "Check if a query has any aggregate functions or GROUP BY.

   Args:
     parsed-query - The parsed query map with :select and :group-by

   Returns true if the query requires aggregation."
  [parsed-query]
  (or (some? (:group-by parsed-query))
      (and (:select parsed-query)
           (some (fn [sel]
                   (when (string? sel)
                     (or (str/includes? sel "(count")
                         (str/includes? sel "(sum")
                         (str/includes? sel "(avg")
                         (str/includes? sel "(min")
                         (str/includes? sel "(max")
                         (str/includes? sel "(count-distinct"))))
                 (:select parsed-query)))))

(defn build-aggregation-spec
  "Build a complete aggregation specification from a parsed query.

   This function analyzes the parsed query to extract:
   - GROUP BY column names
   - Aggregate function specifications

   Args:
     parsed-query - The parsed query with :select, :group-by, :where
     mapping      - R2RML mapping for this query's tables

   Returns nil if no aggregation needed, or:
   {:group-keys [column-names...]
    :aggregates [{:fn :count/:sum/... :column col :alias alias}...]}"
  [parsed-query mapping]
  (when (has-aggregations? parsed-query)
    (let [group-keys (or (extract-group-by-columns (:group-by parsed-query) mapping) [])
          aggregates (extract-aggregates-from-select (:select parsed-query) mapping)]
      (when (or (seq group-keys) (seq aggregates))
        {:group-keys group-keys
         :aggregates (or aggregates [])}))))

(defn execute-iceberg-query
  "Execute query against Iceberg source with predicate pushdown.

   time-travel can be:
   - nil (latest snapshot)
   - {:snapshot-id Long} (specific Iceberg snapshot)
   - {:as-of-time Instant} (time-travel to specific time)

   limit is an optional hint to limit the number of rows scanned.
   solution-pushdown is an optional vector of pushdown filters from the solution map.
   join-columns is an optional set of column names to include for join operations.
   all-mappings is optional map of all R2RML mappings (needed for RefObjectMap resolution).
   Returns a lazy seq of solutions - limit is enforced at the scan level
   for early termination."
  ([source mapping patterns base-solution time-travel]
   (execute-iceberg-query source mapping patterns base-solution time-travel nil nil nil nil))
  ([source mapping patterns base-solution time-travel limit]
   (execute-iceberg-query source mapping patterns base-solution time-travel limit nil nil nil))
  ([source mapping patterns base-solution time-travel limit solution-pushdown]
   (execute-iceberg-query source mapping patterns base-solution time-travel limit solution-pushdown nil nil))
  ([source mapping patterns base-solution time-travel limit solution-pushdown join-columns]
   (execute-iceberg-query source mapping patterns base-solution time-travel limit solution-pushdown join-columns nil))
  ([source mapping patterns base-solution time-travel limit solution-pushdown join-columns all-mappings]
   (let [table-name (:table mapping)
         pred->var (extract-predicate-bindings patterns)
         pred->literal (extract-literal-filters patterns)
         subject-var (some extract-subject-variable patterns)

         ;; Build columns to select (include join columns for hash join support)
         ;; Also include FK columns from :ref type predicates (RefObjectMap)
         column-type-cols (->> pred->var
                               keys
                               (keep (fn [pred-iri]
                                       (let [om (get-in mapping [:predicates pred-iri])]
                                         (when (= :column (:type om))
                                           (:value om))))))
         ;; FK columns from RefObjectMap predicates (child column from join condition)
         ref-type-cols (->> pred->var
                            keys
                            (keep (fn [pred-iri]
                                    (let [om (get-in mapping [:predicates pred-iri])]
                                      (when (= :ref (:type om))
                                        ;; Get the child column from join condition
                                        (:child (first (:join-conditions om))))))))
         query-columns (concat column-type-cols
                               ref-type-cols
                               (r2rml/extract-template-cols (:subject-template mapping)))
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
                                        :ref-cols (vec ref-type-cols)
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
     ;; Pass join-columns and all-mappings for hash join and RefObjectMap support
     (map #(row->solution % mapping pred->var subject-var base-solution join-columns all-mappings) rows))))
