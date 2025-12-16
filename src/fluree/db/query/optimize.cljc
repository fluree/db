(ns fluree.db.query.optimize
  (:require [fluree.db.query.exec.eval :as eval]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util.async :refer [go-try <?]]))

(defn compare-component
  [cmp-a cmp-b]
  (if (where/matched-value? cmp-a)
    (if (where/matched-value? cmp-b)
      0
      -1)
    (if (where/matched-value? cmp-b)
      1
      0)))

(def triple-pattern-types
  #{:tuple :class})

(defn triple-pattern?
  [x]
  (contains? triple-pattern-types (where/pattern-type x)))

(defn coerce-triple
  [x]
  (if (triple-pattern? x)
    (where/pattern-data x)
    (throw (ex-info "Optimization failed on non triple pattern type"
                    {:status   500
                     :error    :db/optimization-failure
                     ::pattern x}))))

(defn compare-triples
  [a b]
  (let [a' (coerce-triple a)
        b' (coerce-triple b)]
    (reduce (fn [_ nxt]
              (if (zero? nxt)
                nxt
                (reduced nxt)))
            (map compare-component a' b'))))

(defprotocol Optimizable
  "Protocol for query optimization based on database statistics."

  (-reorder [db parsed-query]
    "Reorder query patterns based on database statistics.

    Returns a channel that will contain the optimized query with patterns
    reordered for optimal execution. If the database has no statistics
    available, returns the query unchanged.

    Parameters:
      db - The database (FlakeDB, AsyncDB, etc.)
      parsed-query - The parsed query from fql/parse-query

    Returns:
      Channel containing optimized query")

  (-explain [db parsed-query]
    "Generate an execution plan for the query showing optimization details.

    Returns a channel that will contain a query plan map

    Parameters:
      db - The database (FlakeDB, AsyncDB, etc.)
      parsed-query - The parsed query from fql/parse-query

    Returns:
      Channel containing query plan map"))

;; Inline filter optimization

(defn get-filtered-variable
  "Get the variable from a filter pattern that references exactly one variable.
  Returns the variable symbol if it is a single-variable filter, nil otherwise."
  [pattern]
  (when (= :filter (where/pattern-type pattern))
    (let [f    (where/pattern-data pattern)
          vars (-> f meta :vars)]
      (when (= 1 (count vars))
        (first vars)))))

(defn matches-var?
  "Check if a match object references the given variable."
  [match variable]
  (= variable (where/get-variable match)))

(defn binds-var?
  "Check if a match object binds the given variable.
  A match object binds a variable if it references that variable and is unmatched."
  [match variable]
  (and (where/unmatched? match)
       (matches-var? match variable)))

(defn tuple-binds-var?
  "Check if a tuple pattern binds the given variable."
  [tuple variable]
  (some #(binds-var? % variable) tuple))

;; --- Range extraction helpers for filter pushdown ---

(def ^:private comparison-ops
  "Set of comparison operators that can be used to derive range bounds."
  #{'> '>= '< '<=})

(defn- scalar-literal?
  "Returns true if x is a scalar literal (number or string) that can be used
  as a range bound."
  [x]
  (or (number? x) (string? x)))

(defn- extract-comparison-forms
  "Extract comparison forms from potentially nested (and ...) expressions.
  Returns a sequence of comparison forms."
  [form]
  (cond
    (and (seq? form) (= 'and (first form)))
    (mapcat extract-comparison-forms (rest form))

    (and (seq? form) (contains? comparison-ops (first form)))
    [form]

    :else
    []))

(defn- comparison-form->range
  "Convert a single comparison form to a range bound map.
  Returns nil if the form doesn't match the expected pattern.

  Examples:
    (> ?v 10)  with variable=?v -> {:lower {:value 10 :strict? true}}
    (< 5 ?v)  with variable=?v -> {:lower {:value 5 :strict? true}}"
  [form variable]
  (when (and (seq? form) (= 3 (count form)))
    (let [[op a b] form]
      (when (contains? comparison-ops op)
        (cond
          ;; (< ?v 10) means ?v < 10, so upper bound
          (and (= a variable) (scalar-literal? b))
          (case op
            >  {:lower {:value b :strict? true}}
            >= {:lower {:value b :strict? false}}
            <  {:upper {:value b :strict? true}}
            <= {:upper {:value b :strict? false}})

          ;; (< 10 ?v) means 10 < ?v, so lower bound
          (and (scalar-literal? a) (= b variable))
          (case op
            >  {:upper {:value a :strict? true}}
            >= {:upper {:value a :strict? false}}
            <  {:lower {:value a :strict? true}}
            <= {:lower {:value a :strict? false}})

          :else
          nil)))))

(defn- tighter-bound
  "Pick the tighter of two bounds using pick-fn to decide based on comparison.
  pick-fn receives [bound-a bound-b comparison-result] and returns the tighter bound."
  [a b pick-fn]
  (cond
    (nil? a) b
    (nil? b) a
    :else
    (let [va (:value a)
          vb (:value b)
          c  (compare va vb)]
      (pick-fn a b c))))

(defn- merge-range-bounds
  "Merge two range maps, keeping tighter bounds.
  For lower bounds, picks the larger value. For upper bounds, picks the smaller value.
  When values are equal, prefers strict bounds (> or <) over non-strict (>= or <=)."
  [r1 r2]
  (when (or r1 r2)
    (let [l1 (:lower r1) l2 (:lower r2)
          u1 (:upper r1) u2 (:upper r2)
          ;; For lower bound, pick the larger value (tighter constraint)
          lower (tighter-bound l1 l2 (fn [a b c]
                                       (cond
                                         (neg? c) b
                                         (pos? c) a
                                         ;; equal values - prefer strict bounds
                                         :else (if (:strict? a) a b))))
          ;; For upper bound, pick the smaller value (tighter constraint)
          upper (tighter-bound u1 u2 (fn [a b c]
                                       (cond
                                         (pos? c) b
                                         (neg? c) a
                                         ;; equal values - prefer strict bounds
                                         :else (if (:strict? a) a b))))]
      (cond-> {}
        lower (assoc :lower lower)
        upper (assoc :upper upper)))))

(defn- bound->scan-value
  "Convert a bound to a scan value for index range queries.
  For strict bounds on doubles (CLJ only), uses nextUp/nextDown to adjust the value
  so the range scan excludes the boundary value.
  In CLJS, returns the value as-is and relies on the filter fn to enforce strictness."
  [{:keys [value strict?]} direction]
  (if (and strict?
           #?(:clj (instance? Double value) :cljs false))
    (case direction
      :lower #?(:clj (Math/nextUp (double value)) :cljs value)
      :upper #?(:clj (Math/nextDown (double value)) :cljs value))
    value))

(defn- extract-range-from-codes
  "Extract a range map from filter code forms for a given variable.
  Returns a map with :start-o and/or :end-o keys for index scanning,
  or nil if no range bounds could be extracted."
  [codes variable]
  (let [ranges (->> codes
                    (mapcat extract-comparison-forms)
                    (keep #(comparison-form->range % variable)))]
    (when (seq ranges)
      (let [r (reduce merge-range-bounds nil ranges)]
        (when (seq r)
          (cond-> r
            (:lower r) (assoc :start-o (bound->scan-value (:lower r) :lower))
            (:upper r) (assoc :end-o (bound->scan-value (:upper r) :upper))))))))

(defn with-filter-code
  "Attach filter code to a match object for later compilation.
  Stores the code and variable in metadata for later compilation.

  Also extracts range bounds from simple comparison filters like:
    (< ?v n), (<= ?v n), (> ?v n), (>= ?v n)
    (< ?v \"str\"), (<= ?v \"str\"), (> ?v \"str\"), (>= ?v \"str\")
  and nested (and ...) combinations of those.

  Stores derived range on the match object as ::where/range with :start-o / :end-o."
  [mch variable codes]
  (let [range-from-codes (extract-range-from-codes codes variable)]
    (cond-> (assoc mch ::filter-code {:variable variable, :forms codes})
      range-from-codes (assoc ::where/range range-from-codes))))

(defn with-var-filter
  "Add filter code to the match object for the variable in tuple."
  [tuple variable codes]
  (mapv (fn [mch]
          (if (matches-var? mch variable)
            (with-filter-code mch variable codes)
            mch))
        tuple))

(defn get-filter-codes
  "Extract filter codes from a filter function's metadata."
  [filter-fn]
  (-> filter-fn meta :forms vec))

(defn compile-filter
  "Compile filter code into an executable filter function.
  The returned function takes [solution var-value] and applies the filter.
  Codes are expected to already be parsed (not strings)."
  [variable codes context]
  (let [compiled-filters (mapv #(eval/compile-filter % variable context)
                               codes)]
    (if (= 1 (count compiled-filters))
      (nth compiled-filters 0)
      (fn [solution var-value]
        (every? (fn [f]
                  (f solution var-value))
                compiled-filters)))))

(defn compile-inline-filters
  "Compile any filter codes in a match object."
  [mch context]
  (if-let [{:keys [variable forms]} (::filter-code mch)]
    (-> mch
        (dissoc ::filter-code)
        (where/with-filter (compile-filter variable forms context)))
    mch))

(declare compile-filter-codes)

(defn compile-pattern-filters
  "Recursively compile filter codes in a pattern."
  [pattern context]
  (let [pattern-type (where/pattern-type pattern)]
    (case (where/pattern-type pattern)

      :tuple
      ;; Tuple patterns are vectors of match objects
      (mapv #(compile-inline-filters % context) pattern)

      (:class :id)
      ;; Class and ID patterns have a single match object as data
      (let [mch (-> pattern
                    where/pattern-data
                    (compile-inline-filters context))]
        (where/->pattern pattern-type mch))

      :union
      ;; Union patterns contain a vector of where clauses
      (let [clauses (->> (where/pattern-data pattern)
                         (mapv (partial compile-filter-codes context)))]
        (where/->pattern pattern-type clauses))

      (:optional :exists :not-exists :minus)
      ;; Optional, exists, not-exists, and minus patterns contain a single where
      ;; clause
      (let [where-clause (->> (where/pattern-data pattern)
                              (compile-filter-codes context))]
        (where/->pattern pattern-type where-clause))

      :graph
      ;; Graph patterns contain [graph* where-clause]
      (let [[graph where-clause] (where/pattern-data pattern)
            where-clause* (compile-filter-codes context where-clause)]
        (where/->pattern pattern-type [graph where-clause*]))

      ;; All other pattern types pass through unchanged
      pattern)))

(defn compile-filter-codes
  "Walk through where clause and compile all filter codes."
  [context where-clause]
  (if (seq where-clause)
    (mapv #(compile-pattern-filters % context) where-clause)
    where-clause))

(defn extract-var-filters
  "Extract a map of variable -> filter function for single-variable filters."
  [where-clause]
  (reduce (fn [acc pattern]
            (if-let [variable (get-filtered-variable pattern)]
              (assoc acc variable (where/pattern-data pattern))
              acc))
          {} where-clause))

(defn find-filtered-vars
  "Find variables that the tuple binds and have filters."
  [tuple var-filters]
  (filter (partial tuple-binds-var? tuple)
          (keys var-filters)))

(defn attach-filters
  "Attach filter codes to a tuple for the given filtered variables."
  [tuple filtered-vars var-filters]
  (reduce (fn [tuple* variable]
            (let [codes (-> var-filters (get variable) get-filter-codes)]
              (with-var-filter tuple* variable codes)))
          tuple filtered-vars))

(defn process-binding-pattern
  "Process a binding pattern (tuple/class/id) and attach inline filters.
  Returns [processed-pattern filtered-vars]."
  [pattern pattern-type var-filters]
  (let [tuple (if (= :tuple pattern-type)
                pattern
                (where/pattern-data pattern))
        filtered-vars (find-filtered-vars tuple var-filters)
        tuple* (attach-filters tuple filtered-vars var-filters)
        pattern* (if (and (seq filtered-vars) (not= :tuple pattern-type))
                   (where/->pattern pattern-type tuple*)
                   (if (seq filtered-vars) tuple* pattern))]
    [pattern* filtered-vars]))

(defn keep-filter?
  "Determine if a filter pattern should be kept.
  Multi-variable filters are always kept. Single-variable filters are kept
  only if they weren't inlined."
  [pattern inlined]
  (if-some [variable (get-filtered-variable pattern)]
    (not (contains? inlined variable))
    true))

(defn optimize-inline-filters
  "Rewrite single-variable filters as inline filters attached to the pattern
  that binds the variable. Returns the optimized where clause."
  [where-clause]
  (if (seq where-clause)
    (let [var-filters (extract-var-filters where-clause)]
      (loop [patterns where-clause
             result []
             inlined #{}]
        (if (empty? patterns)
          result
          (let [pattern (first patterns)
                pattern-type (where/pattern-type pattern)]
            (case pattern-type
              (:tuple :class :id)
              (let [[pattern* filtered-vars] (process-binding-pattern pattern pattern-type var-filters)
                    inlined* (into inlined filtered-vars)]
                (recur (rest patterns) (conj result pattern*) inlined*))

              :filter
              (if (keep-filter? pattern inlined)
                (recur (rest patterns) (conj result pattern) inlined)
                (recur (rest patterns) result inlined))

              (recur (rest patterns) (conj result pattern) inlined))))))
    where-clause))

(defn optimize
  "Optimize a parsed query by first reordering patterns based on statistics,
  then applying inline filter optimizations.

  Returns a channel that will contain the fully optimized query.

  Parameters:
    db - The database (FlakeDB, AsyncDB, etc.)
    parsed-query - The parsed query from fql/parse-query

  Returns:
    Channel containing optimized query with inlined filters compiled"
  [db parsed-query]
  (go-try
    (let [;; First apply statistical optimization (reordering patterns)
          reordered-query (<? (-reorder db parsed-query))
          context         (:context reordered-query)
          ;; Then apply inline filter optimization
          reordered-query (if-let [where (:where reordered-query)]
                            (let [where-optimized (->> where
                                                       optimize-inline-filters
                                                       (compile-filter-codes context))]
                              (assoc reordered-query :where where-optimized))
                            reordered-query)]
      reordered-query)))
