(ns fluree.db.virtual-graph.iceberg.pushdown
  "Predicate pushdown analysis for Iceberg virtual graphs.

   This namespace handles:
   - Type coercion for predicate values (XSD and Iceberg types)
   - FILTER clause analysis and pushdown
   - VALUES clause -> IN predicate conversion
   - Pattern annotation with pushdown metadata

   Predicate pushdown allows SPARQL filters to be executed directly by
   Iceberg rather than post-filtering in Clojure, significantly improving
   performance for selective queries."
  (:require [fluree.db.query.exec.where :as where]
            [fluree.db.util.log :as log])
  (:import [java.time Instant]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; Type Coercion for Predicates
;;; ---------------------------------------------------------------------------
;;
;; SPARQL values come as strings but Iceberg columns may be numeric/date types.
;; Without coercion, predicates like `{:column "id" :op :eq :value "123"}` fail
;; to match because "123" != 123.
;;
;; Coercion sources (in priority order):
;; 1. Explicit rr:datatype in R2RML mapping (e.g., xsd:integer)
;; 2. Iceberg schema column type (from ITabularSource.get-schema)

(def ^:const xsd-ns "http://www.w3.org/2001/XMLSchema#")

(defn- coercion-failed?
  "Check if a value represents a failed coercion attempt.
   The sentinel ::coercion-failed indicates coercion was required but failed,
   signaling that the predicate should not be pushed down."
  [v]
  (= v ::coercion-failed))

(def ^:private xsd-type->coercer
  "Map of XSD datatype IRIs to coercion functions.
   Each coercer takes a value and returns the coerced type or nil on failure."
  {(str xsd-ns "integer")      #(when % (parse-long (str %)))
   (str xsd-ns "long")         #(when % (parse-long (str %)))
   (str xsd-ns "int")          #(when % (parse-long (str %)))
   (str xsd-ns "short")        #(when % (parse-long (str %)))
   (str xsd-ns "byte")         #(when % (parse-long (str %)))
   (str xsd-ns "decimal")      #(when % (parse-double (str %)))
   (str xsd-ns "double")       #(when % (parse-double (str %)))
   (str xsd-ns "float")        #(when % (parse-double (str %)))
   ;; Boolean coercer is strict: only accepts actual booleans or valid boolean strings
   (str xsd-ns "boolean")      #(cond
                                  (boolean? %) %
                                  (string? %) (parse-boolean %)  ; returns nil for invalid
                                  :else nil)  ; non-string, non-boolean -> fail
   (str xsd-ns "dateTime")     #(when %
                                  (try
                                    (if (instance? Instant %)
                                      %
                                      (Instant/parse (str %)))
                                    (catch Exception _ nil)))
   (str xsd-ns "date")         #(when %
                                  (try
                                    (java.time.LocalDate/parse (str %))
                                    (catch Exception _ nil)))
   (str xsd-ns "string")       str})

(def ^:private iceberg-type->coercer
  "Map of Iceberg column type keywords to coercion functions.
   Used when R2RML doesn't specify rr:datatype."
  {:long    #(when % (if (number? %) (long %) (parse-long (str %))))
   :int     #(when % (if (number? %) (int %) (parse-long (str %))))
   :double  #(when % (if (number? %) (double %) (parse-double (str %))))
   :float   #(when % (if (number? %) (float %) (parse-double (str %))))
   ;; Boolean coercer is strict: only accepts actual booleans or valid boolean strings
   :boolean #(cond
               (boolean? %) %
               (string? %) (parse-boolean %)  ; returns nil for invalid
               :else nil)  ; non-string, non-boolean -> fail
   :timestamp #(when %
                 (try
                   (if (instance? Instant %)
                     %
                     (Instant/parse (str %)))
                   (catch Exception _ nil)))
   :date    #(when %
               (try
                 (java.time.LocalDate/parse (str %))
                 (catch Exception _ nil)))
   :string  str})

(defn coerce-value
  "Coerce a predicate value to match the column's expected type.

   Args:
     value    - The value to coerce (may be string, number, etc.)
     datatype - XSD datatype IRI from R2RML mapping (optional)
     col-type - Iceberg column type keyword from schema (optional)

   Returns the coerced value, or ::coercion-failed if coercion was required but failed.
   When a datatype is specified (via R2RML rr:datatype), coercion failure returns
   ::coercion-failed to signal the predicate should not be pushed down.
   This prevents silent type mismatches from masking bad metadata or data bugs."
  [value datatype col-type]
  (cond
    ;; nil stays nil
    (nil? value) nil

    ;; Try XSD datatype coercion first (R2RML specified)
    ;; When datatype is explicit, coercion failure is an error - return sentinel
    (and datatype (contains? xsd-type->coercer datatype))
    (let [coercer (get xsd-type->coercer datatype)
          coerced (try (coercer value) (catch Exception _ nil))]
      (if (some? coerced)
        coerced
        (do
          (log/warn "Coercion failed for value" value "with datatype" datatype
                    "- predicate will not be pushed down")
          ::coercion-failed)))

    ;; Fall back to Iceberg schema type
    ;; Schema-based coercion failure also returns sentinel
    (and col-type (contains? iceberg-type->coercer col-type))
    (let [coercer (get iceberg-type->coercer col-type)
          coerced (try (coercer value) (catch Exception _ nil))]
      (if (some? coerced)
        coerced
        (do
          (log/warn "Coercion failed for value" value "with column type" col-type
                    "- predicate will not be pushed down")
          ::coercion-failed)))

    ;; No coercion needed - return as-is
    :else value))

(defn coerce-predicate-value
  "Coerce a predicate's value(s) based on column mapping and schema.

   Handles both single values (:eq, :gt, etc.) and collections (:in, :between).

   Returns the predicate with coerced value(s), or nil if any coercion failed.
   A nil return signals that this predicate should not be pushed down."
  [pred object-map col-schema]
  (let [datatype (:datatype object-map)
        col-type (when col-schema
                   (let [col-name (:value object-map)]
                     (->> col-schema
                          (filter #(= col-name (:name %)))
                          first
                          :type)))
        value (:value pred)]
    (if (or (vector? value) (set? value) (sequential? value))
      ;; Collection value (IN, BETWEEN) - coerce each element
      (let [coerced (mapv #(coerce-value % datatype col-type) value)]
        (if (some coercion-failed? coerced)
          nil  ; Any failure -> predicate not pushable
          (assoc pred :value coerced)))
      ;; Single value
      (let [coerced (coerce-value value datatype col-type)]
        (if (coercion-failed? coerced)
          nil  ; Failure -> predicate not pushable
          (assoc pred :value coerced))))))

;;; ---------------------------------------------------------------------------
;;; FILTER Pushdown Analysis
;;; ---------------------------------------------------------------------------

(def ^:private pushable-ops
  "Comparison operators that can be pushed down to Iceberg.
   Maps from parsed form symbols to Iceberg predicate ops."
  {'> :gt
   '>= :gte
   '< :lt
   '<= :lte
   '= :eq
   'equal :eq
   'not= :ne
   'not-equal :ne
   'in :in
   'nil? :is-null
   'bound :not-null})

(defn- extract-comparison
  "Extract comparison details from a parsed filter form.
   Returns {:op :var :value} or nil if not a simple pushable comparison.

   Handles forms like:
     (> ?x 100)     -> {:op :gt, :var ?x, :value 100}
     (= ?x \"foo\") -> {:op :eq, :var ?x, :value \"foo\"}
     (in ?x [1 2])  -> {:op :in, :var ?x, :value [1 2]}
     (nil? ?x)      -> {:op :is-null, :var ?x, :value nil}
     (bound ?x)     -> {:op :not-null, :var ?x, :value nil}"
  [form]
  (when (sequential? form)
    (let [[op-sym & args] form
          iceberg-op (get pushable-ops op-sym)]
      (when iceberg-op
        (cond
          ;; Unary: (nil? ?x) or (bound ?x)
          (#{:is-null :not-null} iceberg-op)
          (let [[arg] args]
            (when (where/variable? arg)
              {:op iceberg-op :var arg :value nil}))

          ;; IN: (in ?x [values...])
          (= :in iceberg-op)
          (let [[arg values] args]
            (when (and (where/variable? arg) (vector? values))
              {:op iceberg-op :var arg :value values}))

          ;; Binary comparison: (> ?x 100) or (> 100 ?x)
          :else
          (let [[arg1 arg2] args
                var1? (where/variable? arg1)
                var2? (where/variable? arg2)]
            (cond
              ;; (?x op literal) - normal order
              (and var1? (not var2?))
              {:op iceberg-op :var arg1 :value arg2}

              ;; (literal op ?x) - reversed, flip comparison
              (and var2? (not var1?))
              (let [flipped-op (case iceberg-op
                                 :gt :lt
                                 :gte :lte
                                 :lt :gt
                                 :lte :gte
                                 iceberg-op)] ; eq, ne don't need flipping
                {:op flipped-op :var arg2 :value arg1})

              ;; Both vars or both literals - not pushable
              :else nil)))))))

(defn analyze-filter-pattern
  "Analyze a :filter pattern for pushability.
   Returns {:pushable? true :comparisons [...]} or {:pushable? false}."
  [pattern]
  (let [filter-fn (second pattern)
        {:keys [forms vars]} (meta filter-fn)]
    (if (and (= 1 (count vars))  ; Single variable only
             (seq forms))
      (let [comparisons (keep extract-comparison forms)]
        (if (= (count comparisons) (count forms))
          ;; All forms are pushable comparisons
          {:pushable? true
           :comparisons comparisons
           :vars vars
           :pattern pattern}
          ;; Some forms not pushable - keep whole filter
          {:pushable? false :pattern pattern}))
      ;; Multi-var or no forms - not pushable
      {:pushable? false :pattern pattern})))

;;; ---------------------------------------------------------------------------
;;; Pattern Analysis Helpers
;;; ---------------------------------------------------------------------------

(defn raw-triple?
  "Check if pattern is a raw triple [s p o] (not a tagged pattern like [:filter ...])."
  [pattern]
  (and (vector? pattern)
       (= 3 (count pattern))
       (map? (first pattern))))

(defn find-first-binding-pattern
  "Find the index of the first pattern that binds the given variable.
   Skips :optional, :union, :filter, :bind, and :values patterns.

   Handles both raw triples [s p o] and tagged patterns [:class [s p o]]."
  [patterns var]
  (first
   (keep-indexed
    (fn [idx pattern]
      (let [pattern-type (first pattern)
            ;; Check for tagged pattern types to skip
            skip-pattern? (#{:optional :union :filter :bind :values} pattern-type)]
        (when-not skip-pattern?
          (let [;; Determine the actual triple data
                triple-data (cond
                              ;; Raw triple [s p o] - pattern itself is the triple
                              (raw-triple? pattern)
                              pattern

                              ;; Tagged pattern [:class [s p o]] or similar
                              (vector? (second pattern))
                              (second pattern)

                              :else nil)
                ;; Extract variables from the triple
                pattern-vars (when triple-data
                               (keep #(cond
                                        (where/variable? %) %
                                        (and (map? %) (::where/var %)) (::where/var %))
                                     triple-data))]
            (when (some #{var} pattern-vars)
              idx)))))
    patterns)))

(defn var->predicate-iri
  "Find the predicate IRI that binds a variable in the given patterns.

   Handles both raw triples [s p o] and tagged patterns [:class [s p o]]."
  [patterns var]
  (some
   (fn [pattern]
     (let [;; Determine the actual triple data
           triple (cond
                    ;; Raw triple [s p o]
                    (raw-triple? pattern)
                    pattern

                    ;; Tagged pattern [:class [s p o]] or similar
                    (vector? (second pattern))
                    (second pattern)

                    :else nil)]
       (when triple
         (let [[_s p o] triple]
           (when (and (map? p) (::where/iri p)
                      (or (= var o)
                          (and (map? o) (= var (::where/var o)))))
             (::where/iri p))))))
   patterns))

;;; ---------------------------------------------------------------------------
;;; Pattern Annotation
;;; ---------------------------------------------------------------------------

(defn annotate-pattern-with-filters
  "Attach pushdown filters to a pattern, handling both raw triples and MapEntry patterns.
   For MapEntry patterns like [:tuple [s p o]], attaches metadata to the inner tuple
   so it survives pattern-data extraction in the WHERE executor."
  [pattern pushdown-filters]
  (let [add-meta #(vary-meta % update ::pushdown-filters
                             (fnil into []) pushdown-filters)]
    (cond
      ;; Raw triple [s p o] - just add metadata
      (raw-triple? pattern)
      (add-meta pattern)

      ;; MapEntry pattern - extract inner data, add metadata, rebuild MapEntry
      (instance? clojure.lang.MapEntry pattern)
      (let [pattern-type (key pattern)
            pattern-data (val pattern)
            ;; Add metadata to the inner data (which becomes the 'triple' in -match-triple)
            annotated-data (if (vector? pattern-data)
                             (add-meta pattern-data)
                             pattern-data)]
        ;; Return a new MapEntry with annotated data
        (clojure.lang.MapEntry/create pattern-type annotated-data))

      ;; Vector pattern like [:class [s p o]] - also handle as pseudo-MapEntry
      (and (vector? pattern)
           (= 2 (count pattern))
           (keyword? (first pattern)))
      (let [pattern-type (first pattern)
            pattern-data (second pattern)
            annotated-data (if (vector? pattern-data)
                             (add-meta pattern-data)
                             pattern-data)]
        ;; Convert to MapEntry for proper handling by WHERE executor
        (clojure.lang.MapEntry/create pattern-type annotated-data))

      ;; Unknown pattern type - return unchanged
      :else pattern)))

(defn annotate-patterns-with-pushdown
  "Attach :pushdown-filters metadata to patterns that first bind pushed-down vars.
   Returns {:patterns [...] :failed [...]} where :failed contains analyses that
   couldn't be pushed down (e.g., BIND-created variables with no column mapping,
   or coercion failures).

   Uses routing-indexes to find the correct mapping for each predicate,
   ensuring filters are only pushed down to the table that owns that predicate.
   Values are coerced based on column datatype from mapping."
  [patterns pushable-analyses _mappings routing-indexes]
  (let [pred->mappings (:predicate->mappings routing-indexes)]
    (reduce
     (fn [{:keys [patterns failed]} {:keys [comparisons vars] :as analysis}]
       (let [var (first vars)
             binding-idx (find-first-binding-pattern patterns var)]
         (if binding-idx
           ;; Find the predicate IRI that binds this var
           (let [pred-iri (var->predicate-iri patterns var)
                 ;; Use routing to find the correct mapping for this predicate
                 ;; Takes first when multiple mappings exist
                 routed-mapping (first (get pred->mappings pred-iri))
                 ;; Get the full object-map for column and datatype
                 obj-map (get-in routed-mapping [:predicates pred-iri])
                 column (when (and obj-map (= :column (:type obj-map)))
                          (:value obj-map))
                 datatype (:datatype obj-map)]
             (if column
               ;; Attempt to coerce values, checking for failures
               (let [coerced-comparisons (mapv (fn [comp]
                                                 (let [coerced (coerce-value (:value comp) datatype nil)]
                                                   (-> comp
                                                       (assoc :column column)
                                                       (assoc :value coerced))))
                                               comparisons)
                     any-failed? (some #(coercion-failed? (:value %)) coerced-comparisons)]
                 (if any-failed?
                   ;; Coercion failed - add to failed list, don't push down
                   (do
                     (log/debug "Skipping FILTER pushdown - coercion failed:"
                                {:var var :column column :datatype datatype})
                     {:patterns patterns
                      :failed (conj failed analysis)})
                   ;; All coercions succeeded - annotate the pattern
                   (do
                     (log/debug "Annotating pattern with FILTER pushdown:"
                                {:var var :column column :ops (mapv :op comparisons)
                                 :datatype datatype})
                     {:patterns (update patterns binding-idx
                                        #(annotate-pattern-with-filters % coerced-comparisons))
                      :failed failed})))
               ;; No routed mapping or column found - add to failed list
               (do
                 (log/debug "Skipping FILTER pushdown - no column mapping:"
                            {:var var :pred-iri pred-iri
                             :has-routed-mapping? (boolean routed-mapping)})
                 {:patterns patterns
                  :failed (conj failed analysis)})))
           (do
             (log/debug "Skipping FILTER pushdown - no binding pattern for var:" var)
             {:patterns patterns
              :failed (conj failed analysis)}))))
     {:patterns (vec patterns) :failed []}
     pushable-analyses)))

;;; ---------------------------------------------------------------------------
;;; VALUES Clause -> IN Predicate Pushdown
;;; ---------------------------------------------------------------------------

(defn- extract-value
  "Extract literal value from various formats.
   Returns the value or nil if not a pushable literal."
  [v]
  (cond
    ;; Wrapped match object {::where/val value}
    (and (map? v) (contains? v ::where/val))
    (::where/val v)

    ;; Raw string/number literal (from SPARQL translator)
    (or (string? v) (number? v))
    v

    ;; IRI or other non-pushable format
    :else nil))

(defn extract-values-in-predicate
  "Extract IN predicate from a VALUES pattern.

   VALUES patterns that bind a single variable to multiple literal values
   can be pushed down as IN predicates.

   VALUES pattern structure can be:
   1. After FQL parsing: [:values [{var match-obj} {var match-obj} ...]]
      - Vector of solution maps, each binding the same var to a value
   2. From SPARQL translator: [:values [var [values...]]]
      - var is symbol or string, values is vector of match objects or raw values

   Returns {:var symbol :values [v1 v2 ...]} or nil if not pushable.

   Only single-variable VALUES with all literal values are pushable.
   Multi-variable VALUES or VALUES with IRIs are not currently supported."
  [pattern]
  (when (= :values (first pattern))
    (let [pattern-data (second pattern)]
      (cond
        ;; Format 1: [:values [{?var match-obj} ...]] - parsed FQL format
        ;; Each solution map should have exactly one key (the variable)
        (and (sequential? pattern-data)
             (seq pattern-data)
             (every? map? pattern-data))
        (let [;; All solutions should bind the same single variable
              vars-per-solution (map keys pattern-data)
              all-single-var? (every? #(= 1 (count %)) vars-per-solution)
              var-sets (map (comp set keys) pattern-data)
              same-var? (apply = var-sets)]
          (when (and all-single-var? same-var?)
            (let [;; Get the variable key from the first solution map
                  var-key (first (keys (first pattern-data)))
                  var-sym (cond
                            (symbol? var-key) var-key
                            (string? var-key) (symbol var-key)
                            :else nil)
                  ;; Extract values from each solution map
                  extracted (keep (fn [sol]
                                    (let [match-obj (first (vals sol))]
                                      (extract-value match-obj)))
                                  pattern-data)]
              (when (and var-sym
                         (seq extracted)
                         (= (count extracted) (count pattern-data)))
                {:var var-sym
                 :values (vec extracted)}))))

        ;; Format 2: [:values [var solutions]] - SPARQL translator format
        (and (vector? pattern-data)
             (= 2 (count pattern-data))
             (let [var-elem (first pattern-data)]
               (or (symbol? var-elem)
                   (string? var-elem))))
        (let [[var-elem solutions] pattern-data
              var-sym (if (symbol? var-elem)
                        var-elem
                        (symbol var-elem))
              ;; Extract values from various formats
              extracted (when (sequential? solutions)
                          (keep extract-value solutions))]
          ;; Only pushable if all values were extracted successfully
          (when (and (seq extracted)
                     (= (count extracted) (count solutions)))
            {:var var-sym
             :values (vec extracted)}))

        ;; Format 3: [:values {?var [values...]}] - map format
        (and (map? pattern-data)
             (= 1 (count pattern-data)))
        (let [[var-key solutions] (first pattern-data)
              var-name (cond
                         (symbol? var-key) var-key
                         (string? var-key) (symbol var-key)
                         :else nil)
              extracted (when (and var-name (sequential? solutions))
                          (keep extract-value solutions))]
          (when (and (seq extracted)
                     (= (count extracted) (count solutions)))
            {:var var-name
             :values (vec extracted)}))

        ;; Other formats - not pushable
        :else nil))))

(defn annotate-values-pushdown
  "Annotate patterns with IN predicates from VALUES clauses.

   For each VALUES clause with a single variable and multiple literal values,
   find the triple pattern that binds that variable and attach an :in predicate.

   This allows VALUES clauses like:
     VALUES ?country { 'US' 'Canada' 'Mexico' }
   to be pushed down to Iceberg as:
     column IN ('US', 'Canada', 'Mexico')

   Uses routing-indexes to ensure the IN predicate is only pushed to the
   table that owns the column. Values are coerced based on column datatype.
   If any value fails coercion, the VALUES clause is not pushed down."
  [patterns values-predicates _mappings routing-indexes]
  (let [pred->mappings (:predicate->mappings routing-indexes)]
    (reduce
     (fn [patterns {:keys [var values]}]
       (let [binding-idx (find-first-binding-pattern patterns var)]
         (if binding-idx
           (let [pred-iri (var->predicate-iri patterns var)
                 ;; Takes first when multiple mappings exist
                 routed-mapping (first (get pred->mappings pred-iri))
                 obj-map (get-in routed-mapping [:predicates pred-iri])
                 column (when (and obj-map (= :column (:type obj-map)))
                          (:value obj-map))
                 datatype (:datatype obj-map)]
             (if column
               ;; Coerce all values based on column datatype, checking for failures
               (let [coerced-values (mapv #(coerce-value % datatype nil) values)]
                 (if (some coercion-failed? coerced-values)
                   ;; Coercion failed for at least one value - skip pushdown
                   (do
                     (log/debug "Skipping VALUES annotation - coercion failed:"
                                {:var var :column column :datatype datatype})
                     patterns)
                   ;; All coercions succeeded - annotate the pattern
                   (let [pushdown-filter [{:op :in :column column :value coerced-values}]]
                     (log/debug "Annotating pattern with VALUES IN pushdown:"
                                {:var var :column column :values-count (count values)
                                 :datatype datatype})
                     (update patterns binding-idx
                             #(annotate-pattern-with-filters % pushdown-filter)))))
               (do
                 (log/debug "Skipping VALUES annotation - no column mapping:"
                            {:var var :pred-iri pred-iri
                             :has-routed-mapping? (boolean routed-mapping)})
                 patterns)))
           (do
             (log/debug "Skipping VALUES annotation - no binding pattern for var:" var)
             patterns))))
     (vec patterns)
     values-predicates)))

;;; ---------------------------------------------------------------------------
;;; Predicate Coalescing
;;; ---------------------------------------------------------------------------

(defn coalesce-predicates
  "Coalesce multiple equality predicates on the same column into a single IN predicate.

   This normalizes the predicate representation so that:
   - Single :eq predicates remain as :eq
   - Multiple :eq predicates on the same column become :in
   - Existing :in predicates are merged with :eq predicates on the same column

   Example:
     [{:op :eq :column \"country\" :value \"US\"}
      {:op :eq :column \"country\" :value \"Canada\"}
      {:op :eq :column \"name\" :value \"Delta\"}]
     =>
     [{:op :in :column \"country\" :value [\"US\" \"Canada\"]}
      {:op :eq :column \"name\" :value \"Delta\"}]"
  [predicates]
  (if (empty? predicates)
    predicates
    (let [;; Group predicates by column
          by-column (group-by :column predicates)
          ;; For each column, coalesce eq predicates
          coalesced (mapcat
                     (fn [[column preds]]
                       (let [{eq-preds :eq
                              in-preds :in
                              ;; group-by returns nil key for missing ops; we'll treat "other"
                              ;; as anything not :eq/:in explicitly.
                              :as by-op} (group-by :op preds)
                             other-preds (->> by-op
                                              (remove (fn [[op _]] (#{:eq :in} op)))
                                              (mapcat val))
                             ;; Collect all values from :eq predicates
                             eq-values (mapv :value eq-preds)
                             ;; Collect all values from :in predicates (each :value should be sequential)
                             in-values (mapcat :value in-preds)
                             ;; Combine all values
                             all-values (into (vec eq-values) in-values)]
                         (concat
                          ;; Non-eq/in predicates pass through unchanged
                          other-preds
                          ;; Coalesce eq/in predicates
                          (cond
                            ;; No equality-type predicates
                            (empty? all-values) nil
                            ;; Single value - use :eq
                            (= 1 (count all-values))
                            [{:op :eq :column column :value (first all-values)}]
                            ;; Multiple values - use :in
                            :else
                            [{:op :in :column column :value (vec all-values)}]))))
                     by-column)]
      (vec coalesced))))

;;; ---------------------------------------------------------------------------
;;; Extract Pushdown Filters from Metadata
;;; ---------------------------------------------------------------------------

(defn extract-pushdown-filters
  "Extract pushed-down filters from pattern metadata.
   These were attached by the Optimizable -reorder pass."
  [patterns]
  (->> patterns
       (mapcat #(::pushdown-filters (meta %)))
       (remove nil?)
       vec))
