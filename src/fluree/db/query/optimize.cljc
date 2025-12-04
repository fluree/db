(ns fluree.db.query.optimize
  (:require [clojure.set :as set]
            [fluree.db.query.exec.eval :as eval]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util :as util]
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

(defn filter-info
  "Describe a `:filter` pattern by returning a map of useful details, or nil if the
  pattern is not a filter.

  {:pattern <original pattern entry>
   :fn      <compiled filter fn>
   :vars    #{sym ...} ; symbols referenced by the filter
   :forms   [form ...] ; parsed forms used to compile the filter
   :order   [sym ...]} ; vars in a deterministic dependency order

  The values mirror what the parser stored in the filter function's metadata, but
  packaged in a plain map so downstream optimizations can reason about them without
  digging through metadata directly."
  [pattern]
  (when (= :filter (where/pattern-type pattern))
    (let [f     (where/pattern-data pattern)
          forms (some-> f meta :forms vec)
          vars  (-> f meta :vars)
          order (or (some-> f meta :dependency-order vec)
                    (some-> vars sort vec))]
      {:pattern pattern
       :fn      f
       :vars    vars
       :forms   forms
       :order   order})))

(defn get-filtered-variable
  "Get the variable from a filter pattern that references exactly one variable.
  Returns the variable symbol if it is a single-variable filter, nil otherwise."
  [pattern]
  (let [vars (some-> pattern filter-info :vars)]
    (when (= 1 (count vars))
      (first vars))))

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

(defn with-filter-code
  "Attach filter code to a match object for later compilation.
  Stores the code and variable in metadata for later compilation."
  [mch variable codes]
  (assoc mch ::filter-code {:variable variable, :forms codes}))

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

(def binding-pattern-types
  #{:tuple :class :id})

(defn binding-pattern?
  [pattern-type]
  (contains? binding-pattern-types pattern-type))

(defn tuple-bound-vars
  [tuple]
  (->> tuple
       (keep (fn [m]
               (let [var (where/get-variable m)]
                 (when (and var (where/unmatched? m))
                   var))))
       set))

(defn pattern-bound-vars
  [pattern pattern-type]
  (when (binding-pattern? pattern-type)
    (let [tuple (if (= :tuple pattern-type)
                  (util/ensure-vector pattern)
                  (util/ensure-vector (where/pattern-data pattern)))]
      (tuple-bound-vars tuple))))

(defn choose-target-var
  "Return the last variable from `ordered-vars` that is present in the set
  `likely-vars`. `likely-vars` must be a set of symbols and `ordered-vars` a
  vector of symbols."
  [likely-vars ordered-vars]
  (some likely-vars (rseq ordered-vars)))

(defn update-pending-for-pattern
  [pending pattern-vars]
  (reduce-kv
   (fn [[pending* inline] id {:keys [info remaining inlined?] :as entry}]
     (if inlined?
       [(assoc pending* id entry) inline]
       (let [remaining (or remaining #{})
             newly-bound (set/intersection remaining pattern-vars)
             remaining*  (set/difference remaining pattern-vars)]
         (if (and (seq newly-bound) (empty? remaining*))
           (let [target (choose-target-var newly-bound (:order info))]
             [(assoc pending* id (assoc entry
                                        :remaining remaining*
                                        :inlined? true
                                        :target target))
              (conj inline {:id id
                            :target target
                            :forms  (:forms info)})])
           [(assoc pending* id (assoc entry :remaining remaining*))
            inline]))))
   [pending []]
   pending))

(defn attach-inline-filters
  [pattern pattern-type pending pattern-vars]
  (let [tuple (if (= :tuple pattern-type)
                (util/ensure-vector pattern)
                (util/ensure-vector (where/pattern-data pattern)))
        [pending* inline] (update-pending-for-pattern pending pattern-vars)
        tuple* (reduce (fn [tuple {:keys [target forms]}]
                         (with-var-filter tuple target forms))
                       tuple inline)
        pattern* (if (seq inline)
                   (if (= :tuple pattern-type)
                     tuple*
                     (where/->pattern pattern-type tuple*))
                   pattern)]
    {:pattern pattern*
     :pending pending*
     :inlined? (seq inline)}))

(def ^:private pending-filter-key ::pending-filter)

(declare inline-where-clause)

(defn process-higher-order-pattern
  [pattern pattern-type bound]
  (case pattern-type
    (:optional :exists :not-exists :minus)
    (let [clause  (where/pattern-data pattern)
          clause* (inline-where-clause clause bound)]
      (where/->pattern pattern-type clause*))

    :union
    (let [clauses  (where/pattern-data pattern)
          clauses* (mapv #(inline-where-clause % bound) clauses)]
      (where/->pattern pattern-type clauses*))

    :graph
    (let [[graph-clause where-clause] (where/pattern-data pattern)
          where-clause* (inline-where-clause where-clause bound)]
      (where/->pattern pattern-type [graph-clause where-clause*]))

    pattern))

(defn finalize-inline-result
  [result pending]
  (into []
        (mapcat (fn [entry]
                  (if (and (map? entry)
                           (contains? entry pending-filter-key))
                    (let [id (get entry pending-filter-key)
                          {:keys [info inlined?]} (get pending id)]
                      (when-not inlined?
                        [(:pattern info)]))
                    [entry])))
        result))

(defn inline-where-clause*
  [patterns bound]
  (loop [remaining patterns
         result []
         bound bound
         pending {}]
    (if-let [pattern (first remaining)]
      (let [pattern-type (where/pattern-type pattern)]
        (case pattern-type
          :filter
          (if-let [{:keys [vars] :as info} (filter-info pattern)]
            (if (seq vars)
              (let [id (gensym "filter")
                    pending-entry {:info info
                                   :remaining (set vars)
                                   :inlined? false}]
                (recur (rest remaining)
                       (conj result {pending-filter-key id})
                       bound
                       (assoc pending id pending-entry)))
              (recur (rest remaining)
                     (conj result pattern)
                     bound
                     pending))
            (recur (rest remaining)
                   (conj result pattern)
                   bound
                   pending))

          (:tuple :class :id)
          (let [pattern-vars (or (pattern-bound-vars pattern pattern-type) #{})
                {:keys [pattern pending]} (attach-inline-filters pattern pattern-type pending pattern-vars)
                bound* (into bound pattern-vars)]
            (recur (rest remaining)
                   (conj result pattern)
                   bound*
                   pending))

          (:optional :exists :not-exists :minus :union :graph)
          (let [pattern* (process-higher-order-pattern pattern pattern-type bound)]
            (recur (rest remaining)
                   (conj result pattern*)
                   bound
                   pending))

          (recur (rest remaining)
                 (conj result pattern)
                 bound
                 pending)))
      {:result result
       :pending pending
       :bound bound})))

(defn inline-where-clause
  [patterns bound]
  (if (seq patterns)
    (let [{:keys [result pending]} (inline-where-clause* patterns bound)]
      (finalize-inline-result result pending))
    patterns))

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

(defn optimize-inline-filters
  "Rewrite single-variable filters as inline filters attached to the pattern
  that binds the variable. Returns the optimized where clause."
  [where-clause]
  (inline-where-clause where-clause #{}))

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
          context         (:context reordered-query)]
      ;; Then apply inline filter optimization
      (if-let [where (:where reordered-query)]
        (let [where-optimized  (->> where
                                    optimize-inline-filters
                                    (compile-filter-codes context))]
          (assoc reordered-query :where where-optimized))
        reordered-query))))
