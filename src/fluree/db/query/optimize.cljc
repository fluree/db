(ns fluree.db.query.optimize
  (:require [clojure.core.async :as async]
            [clojure.set :as set]
            [fluree.db.flake.optimize :as flake-optimize]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util :as util]
            [fluree.db.util.async :refer [go-try <?]]))

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

(defn collect-filters
  "Split a where clause into binding patterns and top-level filter descriptors.

  Returns a map with:
  - :binding-patterns — patterns excluding top-level :filter entries
  - :filters — descriptor maps from `filter-info` for each top-level filter

  Filters nested in higher‑order patterns are left in place."
  [patterns]
  (loop [remaining patterns
         binding-patterns []
         filters []]
    (if-let [pattern (first remaining)]
      (let [next-remaining (rest remaining)
            pattern-type   (where/pattern-type pattern)]
        (if (= :filter pattern-type)
          (if-let [info (filter-info pattern)]
            (recur next-remaining binding-patterns (conj filters info))
            (recur next-remaining (conj binding-patterns pattern) filters))
          (recur next-remaining (conj binding-patterns pattern) filters)))
      {:binding-patterns binding-patterns
       :filters          filters})))

(defn get-filtered-variable
  "If a :filter pattern references exactly one variable, return it; else nil."
  [pattern]
  (let [vars (some-> pattern filter-info :vars)]
    (when (= 1 (count vars))
      (first vars))))

(defn matches-var?
  "Return true if the match object references `variable`."
  [match variable]
  (= variable (where/get-variable match)))

(defn binds-var?
  "Return true if match both references `variable` and is unmatched."
  [match variable]
  (and (where/unmatched? match)
       (matches-var? match variable)))

(defn tuple-binds-var?
  "Return true if any element of `tuple` binds `variable`."
  [tuple variable]
  (some #(binds-var? % variable) tuple))

(defn with-filter-code
  "Attach parsed filter forms to a match object under ::filter-code."
  [mch variable codes]
  (assoc mch ::filter-code {:variable variable, :forms codes}))

(defn with-var-filter
  "Attach filter code to the match in `tuple` that binds `variable`."
  [tuple variable codes]
  (mapv (fn [mch]
          (if (matches-var? mch variable)
            (with-filter-code mch variable codes)
            mch))
        tuple))

(defn get-filter-codes
  "Return parsed filter forms from a filter fn’s metadata."
  [filter-fn]
  (-> filter-fn meta :forms vec))

(def binding-pattern-types
  #{:tuple :class :id})

(defn binding-pattern?
  [pattern-type]
  (contains? binding-pattern-types pattern-type))

(defn tuple-bindings
  [tuple]
  (->> tuple
       (keep (fn [m]
               (let [var (where/get-variable m)]
                 (when (and var (where/unmatched? m))
                   var))))
       set))

(defn bound-vars
  "Return vars guaranteed to be bound by a binding pattern, or nil otherwise."
  [pattern]
  (let [pattern-type (where/pattern-type pattern)]
    (when (binding-pattern? pattern-type)
      (->> pattern where/pattern-data util/ensure-vector tuple-bindings))))

(declare clause-bindings)

(defn- union-bindings
  "Intersection of variables guaranteed to be bound across all union branches."
  [branches]
  (let [branch-vars (map clause-bindings branches)]
    (if (seq branch-vars)
      (reduce set/intersection branch-vars)
      #{})))

(def ^:private pattern-hierarchy
  (-> (make-hierarchy)
      (derive :tuple :simple/binding)
      (derive :class :simple/binding)
      (derive :id :simple/binding)
      (derive :exists :nested/single)
      (derive :not-exists :nested/single)
      (derive :minus :nested/single)))

(defmulti pattern-bindings
  "Return the set of variables guaranteed to be bound by a single pattern."
  where/pattern-type :hierarchy #'pattern-hierarchy)

(defmethod pattern-bindings :simple/binding
  [pattern]
  (or (bound-vars pattern) #{}))

(defmethod pattern-bindings :graph
  [pattern]
  (->> pattern where/pattern-data second clause-bindings))

(defmethod pattern-bindings :union
  [pattern]
  (->> pattern where/pattern-data union-bindings))

(defmethod pattern-bindings :nested/single
  [pattern]
  (->> pattern where/pattern-data clause-bindings))

(defmethod pattern-bindings :default
  [_]
  #{})

(defn clause-bindings
  "Return the set of variables guaranteed to be bound by a where clause.
  Aggregates over patterns using `pattern-bindings`."
  [clause]
  (reduce set/union #{} (map pattern-bindings clause)))

;; -----------------------------------------------------------------------------
;; Database-provided optimization scoring
;; -----------------------------------------------------------------------------

(defprotocol Optimizer
  "Dispatch on the database to compute a baseline ordering score for a pattern
  used by the optimizer. Returns a core.async channel that yields a non-negative
  number (lower is better), or nil when unsupported."
  (ordering-score [db pattern]))

(def ^:const default-selectivity 1000)

;; -----------------------------------------------------------------------------
;; Pattern cost (tuple/class/id) — static facts + stats-based base cost
;; -----------------------------------------------------------------------------

(defn- pattern-refs
  "Return the set of variables syntactically referenced by a tuple/class/id
  pattern. Extracts variables from match components regardless of whether they
  are matched or unmatched."
  [pattern]
  (->> (where/pattern-data pattern)
       util/ensure-vector
       (keep where/get-variable)
       set))

(defmulti pattern-cost
  "Return static pattern facts and a stats-based base cost for scheduling.

  Output map keys:
  - :refs        — variables referenced by the pattern (set)
  - :guarantees  — variables the pattern guarantees to bind (set)
  - :base        — baseline cost/selectivity estimate from stats (number)

  The base is computed using existing selectivity estimation for triple-like
  patterns and class/id patterns, independent of the dynamic bound set."
  (fn [_db pattern] (where/pattern-type pattern))
  :hierarchy #'pattern-hierarchy)

(defmethod pattern-cost :simple/binding
  [_db pattern]
  ;; Base is a static fallback for scheduling; dynamic ordering-score is fetched
  ;; asynchronously during scheduling.
  {:refs       (pattern-refs pattern)
   :guarantees (or (bound-vars pattern) #{})
   :base       default-selectivity})

(defn partition-appendable
  "Partition filter descriptors into [appendable remaining] given `bound`."
  [filters bound]
  [(filter #(set/subset? (:vars %) bound) filters)
   (remove #(set/subset? (:vars %) bound) filters)])

(defn append-clause-filters
  "Append `appendable` filter patterns to inner clause and return it."
  [inner appendable]
  (into (vec inner) (map :pattern) appendable))

(defn append-union-filters
  "Append filters to every union branch, returning updated branches."
  [branches appendable]
  (mapv (fn [cl] (append-clause-filters cl appendable)) branches))

(defn append-pattern-filters
  "Append eligible filters into higher‑order patterns when safe (no recursion).
  Returns {:pattern p' :remaining-filters f'}."
  [pattern filter-descriptors bound-vars]
  (let [t (where/pattern-type pattern)]
    (case t
      :optional
      {:pattern pattern :remaining-filters filter-descriptors}

      :graph
      (let [[graph inner] (where/pattern-data pattern)]
        (if (where/virtual-graph? graph)
          {:pattern pattern :remaining-filters filter-descriptors}
          (let [[appendable remaining] (->> inner
                                            clause-bindings
                                            (into bound-vars)
                                            (partition-appendable filter-descriptors))
                inner*               (append-clause-filters inner appendable)]
            {:pattern (where/->pattern t [graph inner*])
             :remaining-filters remaining})))

      (:exists :not-exists :minus)
      (let [inner                (where/pattern-data pattern)
            [appendable remaining]  (->> inner
                                         clause-bindings
                                         (into bound-vars)
                                         (partition-appendable filter-descriptors))
            inner*               (append-clause-filters inner appendable)]
        {:pattern (where/->pattern t inner*)
         :remaining-filters remaining})

      :union
      (let [branches            (where/pattern-data pattern)
            [appendable remaining] (->> branches
                                        (union-bindings)
                                        (into bound-vars)
                                        (partition-appendable filter-descriptors))
            branches*           (append-union-filters branches appendable)]
        {:pattern (where/->pattern t branches*)
         :remaining-filters remaining})

      {:pattern pattern :remaining-filters filter-descriptors})))

(declare nest-filters)

(defn nest-pattern-filters
  "Recursively nest filters into higher-order patterns beyond one level.
  Optionals remain opaque. For unions, append then recurse per branch.
  Returns {:pattern p' :filters f'}."
  [pattern filters bound]
  (let [t (where/pattern-type pattern)]
    (case t
      :optional
      {:pattern pattern :filters filters}

      :graph
      (let [[graph _]  (where/pattern-data pattern)
            {:keys [pattern remaining-filters]}
            (append-pattern-filters pattern filters bound)
            ;; Extract inner* back out for recursion when filters remain
            [_ inner*] (where/pattern-data pattern)
            filters'   remaining-filters]
        (if (seq filters')
          (let [{:keys [patterns filters]} (nest-filters inner* filters' bound)]
            {:pattern (where/->pattern t [graph patterns])
             :filters filters})
          {:pattern (where/->pattern t [graph inner*])
           :filters filters'}))

      (:exists :not-exists :minus)
      (let [{:keys [pattern remaining-filters]}
            (append-pattern-filters pattern filters bound)

            inner*   (where/pattern-data pattern)
            filters* remaining-filters]
        (if (seq filters*)
          (let [{:keys [patterns filters]}
                (nest-filters inner* filters* bound)]
            {:pattern (where/->pattern t patterns)
             :filters filters})
          {:pattern (where/->pattern t inner*)
           :filters filters*}))

      :union
      (let [branches (where/pattern-data pattern)

            ;; Push eligible filters into union branches using intersection
            ;; of branch bindings. This yields updated branches (patterns)
            ;; and remaining filters to attempt recursively.
            {:keys [pattern remaining-filters]}
            (append-pattern-filters (where/->pattern :union branches)
                                    filters
                                    bound)

            clauses       (where/pattern-data pattern)
            ;; Recurse into each branch with remaining filters and
            ;; accumulate any that still cannot be pushed.
            branch-results (mapv #(nest-filters % remaining-filters bound)
                                 clauses)
            branches* (mapv :patterns branch-results)
            leftovers (into [] (mapcat :filters) branch-results)]
        {:pattern (where/->pattern :union branches*) :filters leftovers})

      {:pattern pattern :filters filters})))

(defn nest-filters
  "Walk binding patterns and nest eligible top‑level filters into deeper clauses.
  Recurse into graph/exists/not-exists/minus when pending remain; optionals are
  opaque; unions append then recurse. Returns {:patterns [...], :filters [...]}."
  [binding-patterns filter-descriptors bound-vars]
  (loop [remaining binding-patterns
         acc []
         filters filter-descriptors
         bound bound-vars]
    (if-let [p (first remaining)]
      (let [t            (where/pattern-type p)
            guaranteed   (pattern-bindings p)
            bound-next   (into bound guaranteed)]
        (if (contains? #{:optional :exists :not-exists :minus :union :graph} t)
          (let [{:keys [pattern filters]} (nest-pattern-filters p filters bound)]
            (recur (rest remaining) (conj acc pattern) filters bound-next))
          (recur (rest remaining) (conj acc p) filters bound-next)))
      {:patterns acc :filters filters})))

(defn select-target-var
  "Return the last symbol in `ordered-vars` that is contained in `likely-vars`."
  [likely-vars ordered-vars]
  (some likely-vars (rseq ordered-vars)))

(defn advance-pending
  [pending pattern-vars]
  (reduce-kv
   (fn [[pending* inline] id {:keys [info remaining inlined?] :as entry}]
     (if inlined?
       [(assoc pending* id entry) inline]
       (let [remaining (or remaining #{})
             newly-bound (set/intersection remaining pattern-vars)
             remaining*  (set/difference remaining pattern-vars)]
         (if (and (seq newly-bound) (empty? remaining*))
           (let [target (select-target-var newly-bound (:order info))]
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
        [pending* inline] (advance-pending pending pattern-vars)
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

(declare inline-clause)

(defn inline-nested
  [pattern pattern-type bound]
  (case pattern-type
    (:optional :exists :not-exists :minus)
    (let [clause  (where/pattern-data pattern)
          clause* (inline-clause bound clause)]
      (where/->pattern pattern-type clause*))

    :union
    (let [clauses  (where/pattern-data pattern)
          clauses* (mapv #(inline-clause bound %) clauses)]
      (where/->pattern pattern-type clauses*))

    :graph
    (let [[graph-clause where-clause] (where/pattern-data pattern)]
      (if (where/virtual-graph? graph-clause)
        pattern ; Do not inline within virtual graph clauses
        (let [where-clause* (inline-clause bound where-clause)]
          (where/->pattern pattern-type [graph-clause where-clause*]))))

    pattern))

(defn emit-pending
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

(defn inline-clause*
  [bound patterns]
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
          (let [pattern-vars (or (bound-vars pattern) #{})
                {:keys [pattern pending]} (attach-inline-filters pattern pattern-type pending pattern-vars)
                bound* (into bound pattern-vars)]
            (recur (rest remaining)
                   (conj result pattern)
                   bound*
                   pending))

          (:optional :exists :not-exists :minus :union :graph)
          (let [pattern* (inline-nested pattern pattern-type bound)]
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

(defn inline-clause
  [bound clause]
  (if (seq clause)
    (let [{:keys [result pending]} (inline-clause* bound clause)]
      (emit-pending result pending))
    clause))

(defn strip-filter-code
  "Remove temporary `::filter-code` metadata from a match object, if present."
  [mch]
  (if (::filter-code mch)
    (dissoc mch ::filter-code)
    mch))

(declare strip-clause-filters)

(defn strip-pattern-filters
  "Recursively remove temporary filter-code metadata within a pattern."
  [pattern context]
  (let [pattern-type (where/pattern-type pattern)]
    (case pattern-type

      :tuple
      ;; Tuple patterns are vectors of match objects
      (mapv strip-filter-code pattern)

      (:class :id)
      ;; Class and ID patterns have a single match object as data
      (let [mch (-> pattern where/pattern-data strip-filter-code)]
        (where/->pattern pattern-type mch))

      :union
      ;; Union patterns contain a vector of where clauses
      (let [clauses (->> (where/pattern-data pattern)
                         (mapv (partial strip-clause-filters context)))]
        (where/->pattern pattern-type clauses))

      (:optional :exists :not-exists :minus)
      ;; Optional, exists, not-exists, and minus patterns contain a single where
      ;; clause
      (let [where-clause (->> (where/pattern-data pattern)
                              (strip-clause-filters context))]
        (where/->pattern pattern-type where-clause))

      :graph
      ;; Graph patterns contain [graph* where-clause]
      (let [[graph where-clause] (where/pattern-data pattern)
            where-clause* (strip-clause-filters context where-clause)]
        (where/->pattern pattern-type [graph where-clause*]))

      ;; All other pattern types pass through unchanged
      pattern)))

(defn strip-clause-filters
  "Walk a where clause and remove temporary filter-code metadata."
  [context where-clause]
  (if (seq where-clause)
    (mapv #(strip-pattern-filters % context) where-clause)
    where-clause))

(defn optimize-inline-filters
  "Rewrite single-variable filters as inline filters attached to the pattern
  that binds the variable. Returns the optimized where clause.

  Uses Optimizer/ordering-score indirectly via the reordering phase; this phase
  assumes patterns have already been scheduled using Optimizer."
  [binding-patterns filter-descriptors]
  (let [clause (into (vec binding-patterns)
                     (map :pattern)
                     filter-descriptors)]
    (inline-clause #{} clause)))

(declare reorder-where-clause reorder-union-pattern)

(defn reorder-union-pattern
  "Reorder each branch of a union pattern. Returns a channel that yields a map
  with keys `:clauses` and `:changed?` to indicate whether any branch changed."
  [db union-pattern]
  (go-try
    (loop [remaining (where/pattern-data union-pattern)
           clauses   []
           changed?  false]
      (if-let [clause (first remaining)]
        (let [clause* (<? (reorder-where-clause db clause))]
          (recur (rest remaining)
                 (conj clauses clause*)
                 (or changed? (not (identical? clause clause*)))))
        (if changed?
          (where/->pattern :union clauses)
          union-pattern)))))

(defn reorder-nested-clause
  "Reorder nested patterns that contain where clauses.

  Returns a channel yielding the updated pattern (or the original pattern when
  no changes are necessary)."
  [db pattern]
  (go-try
    (let [ptype (where/pattern-type pattern)]
      (case ptype
        (:optional :exists :not-exists :minus)
        (let [clause  (where/pattern-data pattern)
              clause* (<? (reorder-where-clause db clause))]
          (if (identical? clause clause*)
            pattern
            (where/->pattern ptype clause*)))

        :union
        (<? (reorder-union-pattern db pattern))

        :graph
        (let [[graph-clause where-clause] (where/pattern-data pattern)
              where-clause*               (<? (reorder-where-clause db where-clause))]
          (if (identical? where-clause where-clause*)
            pattern
            (where/->pattern ptype [graph-clause where-clause*])))

        pattern))))

(declare score-clause clause-rows)

(defn- score-pattern
  "Return a channel yielding a numeric score for `pattern`.
  Binding patterns delegate to Optimizer/ordering-score.
  Higher-order patterns score as the minimum score among their nested patterns."
  [db pattern]
  (go-try
    (let [ptype (where/pattern-type pattern)]
      (case ptype
        (:tuple :class :id)
        (let [s (<? (ordering-score db pattern))]
          (or s default-selectivity))

        :graph
        (let [[_graph where-clause] (where/pattern-data pattern)]
          (<? (score-clause db where-clause)))

        :union
        (let [branches    (where/pattern-data pattern)
              avg-chs     (map #(score-clause db %) branches)
              rows-chs    (map #(clause-rows db %) branches)
              avgs        (<? (async/map (fn [& xs] (vec xs)) (vec avg-chs)))
              rows        (<? (async/map (fn [& xs] (vec xs)) (vec rows-chs)))
              pairs       (map vector avgs rows)
              pairs*      (remove (fn [[a r]] (or (nil? a) (nil? r))) pairs)
              total-rows  (reduce + 0 (map second pairs*))
              weighted    (when (pos? total-rows)
                            (/ (reduce + 0 (map (fn [[a r]] (* a r)) pairs*))
                               total-rows))]
          (or weighted default-selectivity))

        (:optional :exists :not-exists :minus)
        (let [clause (where/pattern-data pattern)
              s      (<? (score-clause db clause))]
          (or s default-selectivity))

        ;; Default: no nested content; use conservative default
        default-selectivity))))

(defn- score-clause
  "Return a channel yielding the average score among patterns in `clause`.
  Returns default-selectivity for empty clauses."
  [db clause]
  (go-try
    (if (seq clause)
      (let [chs    (map #(score-pattern db %) clause)
            result (<? (async/map (fn [& xs]
                                    (let [ss (remove nil? xs)]
                                      (when (seq ss)
                                        (/ (reduce + ss) (count ss)))))
                                  (vec chs)))]
        (or result default-selectivity))
      default-selectivity)))

(defn- clause-rows
  "Return a channel yielding an estimated row count for a clause as the sum of
  per-pattern scores (using score-pattern recursively)."
  [db clause]
  (go-try
    (if (seq clause)
      (let [chs (map #(score-pattern db %) clause)
            sum (<? (async/map (fn [& xs]
                                 (let [ss (remove nil? xs)]
                                   (when (seq ss)
                                     (reduce + ss))))
                               (vec chs)))]
        (or sum default-selectivity))
      default-selectivity)))

;; Former run-min-score now unused after per-pattern ranking.

(defn- order-top-level-clause
  "Return a channel yielding an ordered top-level clause.
  Strategy: rank each pattern individually by score, regardless of type.
  - Binding patterns: Optimizer/ordering-score
  - Higher-order patterns: score-pattern (nested scoring)
  Stable tie-breaker by original index."
  [db clause]
  (go-try
    (if (seq clause)
      (let [scored-chs (map-indexed (fn [i pattern]
                                      (go-try
                                        (let [ptype  (where/pattern-type pattern)
                                              score  (if (binding-pattern? ptype)
                                                       (or (<? (ordering-score db pattern)) default-selectivity)
                                                       (<? (score-pattern db pattern)))]
                                          {:index i :pattern pattern :score score})))
                                    clause)
            scored     (<? (async/map (fn [& xs] (vec xs)) (vec scored-chs)))
            ordered    (->> scored
                            (sort-by (fn [{:keys [score index]}]
                                       [(or score default-selectivity) index]))
                            (mapv :pattern))]
        ordered)
      clause)))

(defn- reorder-nested-patterns
  "Return a channel yielding `top-level` with nested patterns reordered
  recursively."
  [db top-level]
  (go-try
    (loop [remaining top-level
           reordered []]
      (if-let [pattern (first remaining)]
        (let [pattern* (<? (reorder-nested-clause db pattern))]
          (recur (rest remaining) (conj reordered pattern*)))
        reordered))))

(defn reorder-where-clause
  "Recursively reorder a parsed where clause using Optimizer/ordering-score.

  Strategy: treat contiguous runs of binding patterns (:tuple/:class/:id) as
  reorderable segments. For each run, fetch scores via Optimizer and order by
  ascending score (stable by original index for ties). Non-binding, higher-order
  patterns act as boundaries and are left in place at top level. Nested clauses
  are then reordered recursively.

  Returns a channel that yields the reordered clause or the original clause when
  reordering is not possible."
  [db clause]
  (go-try
    (if (seq clause)
      (let [top-level (<? (order-top-level-clause db clause))]
        (<? (reorder-nested-patterns db top-level)))
      clause)))

(defn optimize-where-clause
  "Optimize a parsed where clause by reordering binding patterns, applying inline
  filter optimizations, and compiling filter code. Returns a channel yielding the
  optimized clause or the original clause when optimization is unnecessary."
  [db context where-clause]
  (go-try
    (if (seq where-clause)
      (let [{:keys [binding-patterns filters]} (collect-filters where-clause)
            reordered   (<? (reorder-where-clause db binding-patterns))
            ;; Phase A: opportunistically nest eligible filters into deeper clauses
            {:keys [patterns filters]} (nest-filters reordered filters #{})
            ;; Phase B: inline filters against binding points (existing pass)
            inlined     (optimize-inline-filters patterns filters)]
        (strip-clause-filters context inlined))
      where-clause)))

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
    (if-let [where-clause (-> parsed-query :where not-empty)]
      (let [context        (:context parsed-query)
            where-optimized (<? (optimize-where-clause db context where-clause))]
        (assoc parsed-query :where where-optimized))
      parsed-query)))

(defn explain
  "Return a query plan. For FlakeDB instances (i.e., DBs with statistics),
  delegate to flake-optimize/explain-query to preserve the rich explain output
  expected by tests. For other DB types, return a minimal plan."
  [db parsed-query]
  (go-try
    (let [stats (:stats db)]
      (if (and stats (or (seq (:properties stats)) (seq (:classes stats))))
        (flake-optimize/explain-query db parsed-query)
        {:query parsed-query
         :plan  {:optimization :none
                 :reason       "No statistics available"
                 :where-clause (:where parsed-query)}}))))
