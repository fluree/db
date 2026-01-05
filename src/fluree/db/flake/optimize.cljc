(ns fluree.db.flake.optimize
  (:require [fluree.db.constants :as const]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.optimize :refer [compare-triples]]
            [fluree.json-ld :as json-ld]))

;; Selectivity score constants
;; Lower score = more selective = execute first
(def ^:const highly-selective 0)
(def ^:const moderately-selective 10)
(def ^:const default-selectivity 1000)
(def ^:const full-scan 1.0e12)

(defn optimizable-pattern?
  "Returns true for patterns we know how to optimize.
   Any pattern not recognized here is treated as an optimization boundary."
  [pattern]
  (let [ptype (where/pattern-type pattern)]
    (#{:tuple :class :id} ptype)))

(defn get-sid
  "Get SID from component efficiently. Tries where/get-sid first (hot path),
   then falls back to encoding IRI. Avoids double-encoding."
  [db component]
  (or (where/get-sid component db)
      (when-let [component-iri (where/get-iri component)]
        (iri/encode-iri db component-iri))))

(defn get-property-count
  "Get count for a property from stats"
  [stats sid]
  (get-in stats [:properties sid :count]))

(defn get-class-count
  "Get count for a class from stats"
  [stats sid]
  (get-in stats [:classes sid :count]))

(defn get-ndv-values
  "Get raw NDV(values|p) for a property. Used by explain API to show inputs."
  [stats sid]
  (get-in stats [:properties sid :ndv-values]))

(defn get-ndv-subjects
  "Get raw NDV(subjects|p) for a property. Used by explain API to show inputs."
  [stats sid]
  (get-in stats [:properties sid :ndv-subjects]))

(defn get-selectivity-value
  "Get pre-computed selectivity estimate for bound value patterns (?s p o).
   Returns count/ndv-values, computed during indexing."
  [stats sid]
  (get-in stats [:properties sid :selectivity-value]))

(defn get-selectivity-subject
  "Get pre-computed selectivity estimate for bound subject patterns (s p ?o).
   Returns count/ndv-subjects, computed during indexing."
  [stats sid]
  (get-in stats [:properties sid :selectivity-subject]))

(defn estimate-bound-value
  "Estimate selectivity for bound value pattern: (?s p o) where o is bound.
   Uses pre-computed selectivity estimate (already ceiled and clamped during indexing).
   Falls back to conservative estimate if not available."
  [stats sid]
  (if-let [selectivity (get-selectivity-value stats sid)]
    selectivity  ; Already an integer >= 1
    ;; Conservative fallback if selectivity not available
    (if-let [count (get-property-count stats sid)]
      (min count 1000)
      moderately-selective)))

(defn estimate-bound-subject
  "Estimate selectivity for bound subject pattern: (s p ?o) where s is bound.
   Uses pre-computed selectivity estimate (already ceiled and clamped during indexing).
   Falls back to conservative estimate if not available."
  [stats sid]
  (if-let [selectivity (get-selectivity-subject stats sid)]
    selectivity  ; Already an integer >= 1
    ;; Conservative fallback if selectivity not available
    (if-let [count (get-property-count stats sid)]
      (min count 10)
      moderately-selective)))

(defn calculate-selectivity-with-details
  "Calculate selectivity score with detailed inputs for explain API.
   Returns map with :score and :inputs showing the exact values used.

   Per QUERY_STATS_AND_HLL.md lines 277-296, inputs include:
   - :count, :ndv-values, :ndv-subjects (when applicable)
   - Flags: :used-exact?, :used-values-ndv?, :used-subjects-ndv?, :fallback?, :clamped-to-one?"
  [db stats pattern]
  (let [pattern-type (where/pattern-type pattern)
        pattern-data (where/pattern-data pattern)]

    (cond
      (or (nil? stats) (empty? stats))
      {:score nil :inputs {:fallback? true :reason "No statistics available"}}

      (= :id pattern-type)
      (if (where/matched? pattern-data)
        {:score highly-selective
         :inputs {:type :id :matched? true}}
        {:score moderately-selective
         :inputs {:type :id :matched? false}})

      :else
      (let [[s p o] pattern-data]
        (cond
          (= :class pattern-type)
          (let [class-sid (get-sid db o)
                class-count (get-class-count stats class-sid)
                score (or class-count default-selectivity)]
            {:score score
             :inputs (cond-> {:type :class
                              :class-sid class-sid
                              :class-count class-count}
                       (nil? class-count)
                       (assoc :fallback? true :reason "Class count not available"))})

          (where/all-matched? [s p o])
          {:score highly-selective
           :inputs {:type :tuple-exact :all-matched? true}}

          (and (where/matched? s) (where/matched? p) (where/unmatched? o))
          (let [pred-sid (get-sid db p)
                count (get-property-count stats pred-sid)
                ndv-subjects (get-ndv-subjects stats pred-sid)
                selectivity (get-selectivity-subject stats pred-sid)
                score (estimate-bound-subject stats pred-sid)]
            {:score score
             :inputs (cond-> {:type :triple
                              :pattern :bound-subject
                              :property-sid pred-sid
                              :count count
                              :ndv-subjects ndv-subjects
                              :selectivity selectivity
                              :used-subjects-ndv? (some? selectivity)}
                       (nil? selectivity)
                       (assoc :fallback? true
                              :reason (if count "NDV not available" "Count not available")))})

          (and (where/unmatched? s) (where/matched? p) (where/unmatched? o))
          (let [pred-sid (get-sid db p)
                count (get-property-count stats pred-sid)
                score (or count default-selectivity)]
            {:score score
             :inputs (cond-> {:type :triple
                              :pattern :property-scan
                              :property-sid pred-sid
                              :count count}
                       (nil? count)
                       (assoc :fallback? true :reason "Count not available"))})

          (and (where/unmatched? s) (where/matched? p) (where/matched? o))
          (let [pred-sid (get-sid db p)
                count (get-property-count stats pred-sid)
                ndv-values (get-ndv-values stats pred-sid)
                selectivity (get-selectivity-value stats pred-sid)
                clamped? (and selectivity (< selectivity 1.0))
                score (estimate-bound-value stats pred-sid)]
            {:score score
             :inputs (cond-> {:type :triple
                              :pattern :bound-object
                              :property-sid pred-sid
                              :count count
                              :ndv-values ndv-values
                              :selectivity selectivity
                              :used-values-ndv? (some? selectivity)
                              :clamped-to-one? clamped?}
                       (nil? selectivity)
                       (assoc :fallback? true
                              :reason (if count "NDV not available" "Count not available")))})

          (and (where/unmatched? s) (where/unmatched? p) (where/unmatched? o))
          {:score full-scan
           :inputs {:type :full-scan :full-scan? true}}

          :else
          {:score default-selectivity
           :inputs {:type :unknown :fallback? true :reason "Pattern not recognized"}})))))

(defn split-by-optimization-boundaries
  "Split where clause into segments separated by optimization boundaries.
   Optimizable patterns (:tuple, :class, :id) are grouped together.
   All other patterns (boundaries) separate the segments."
  [where-clause]
  (reduce
   (fn [segments pattern]
     (if (optimizable-pattern? pattern)
       (if (and (seq segments)
                (= :optimizable (:type (peek segments))))
         (update segments (dec (count segments))
                 update :data conj pattern)
         (conj segments {:type :optimizable :data [pattern]}))
       ;; Boundary - add as separate segment
       (conj segments {:type :boundary :data pattern})))
   []
   where-clause))

(defn- pattern-vars
  "Returns a set of variables referenced by `pattern`."
  [pattern]
  (where/clause-variables (where/pattern-data pattern)))

(defn- pattern->selectivity-meta
  "Builds the metadata map used by the optimizer and explain.
  Includes :vars for greedy join ordering."
  [db stats pattern]
  (let [{:keys [score inputs]} (calculate-selectivity-with-details db stats pattern)]
    {:pattern pattern
     :score   (or score default-selectivity)
     :inputs  inputs
     :vars    (pattern-vars pattern)}))

(defn- pattern-meta-compare
  "Total order for pattern-meta maps: (score, then stable tie-breaker)."
  [{sa :score pa :pattern} {sb :score pb :pattern}]
  (let [c (compare sa sb)]
    (if (zero? c)
      (compare-triples pa pb)
      c)))

(defn- shares-var?
  "Returns true if `pattern-meta` shares at least one variable with `bound-vars`."
  [bound-vars {:keys [vars]}]
  (boolean (some bound-vars vars)))

(defn- pick-best
  "Returns the best element of `xs` by `cmp` (like (first (sort cmp xs)), but
  without sorting)."
  [cmp xs]
  (reduce (fn [best x]
            (if (neg? (cmp x best))
              x
              best))
          (first xs)
          (rest xs)))

(defn- remove-first
  "Remove the first element from vector `v` matching `pred`."
  [pred v]
  (if-let [idx (first (keep-indexed (fn [i x]
                                      (when (pred x) i))
                                    v))]
    (into (subvec v 0 idx) (subvec v (inc idx)))
    v))

(defn- greedy-order
  "Greedy join ordering:
  - Prefer patterns that share vars with what is already bound (avoid cartesian explosions).
  - Within that set, prefer the lowest selectivity score (then stable tie-breaker).

  Note: `bound-vars` is best-effort; it represents vars already in scope from
  previously executed patterns."
  [cmp bound-vars pattern-metas]
  (loop [bound     (or bound-vars #{})
         remaining pattern-metas
         ordered   []]
    (if (empty? remaining)
      ordered
      (let [candidates (filterv (partial shares-var? bound) remaining)
            pool       (if (seq candidates) candidates remaining)
            chosen     (pick-best cmp pool)]
        (recur (into bound (:vars chosen))
               (remove-first #(= % chosen) remaining)
               (conj ordered chosen))))))

(defn optimize-segment-with-metadata
  "Optimize a single segment and return patterns with their scores and detailed inputs.
   Returns vector of maps with :pattern, :score, and :inputs (for explain)."
  [db stats patterns bound-vars]
  (let [pattern-metas (mapv (partial pattern->selectivity-meta db stats) patterns)]
    (greedy-order pattern-meta-compare bound-vars pattern-metas)))

(defn- boundary-produced-vars
  "Best-effort: vars that could be introduced into the solution *after* executing
  a boundary pattern.

  We keep this conservative. Filters, for example, don't introduce vars."
  [pattern]
  (case (where/pattern-type pattern)
    (:bind :values :optional :union) (pattern-vars pattern)
    #{}))

(defn optimize-patterns-with-metadata
  "Reorder patterns for optimal execution and return rich metadata for explain.
   Returns map with:
   - :original - original where clause
   - :optimized - optimized pattern list (just patterns, for fast extraction)
   - :segments - segment info with patterns, scores, and inputs
   - :changed? - whether optimization changed the order

   This function does all the optimization work once, so explain can just format
   the results without recalculating selectivity scores."
  [db where-clause]
  (let [stats (:stats db)
        segments (split-by-optimization-boundaries where-clause)
        ;; Process each segment in order, carrying forward an approximate
        ;; 'vars-in-scope' set so later segments prefer patterns that join with
        ;; existing bindings.
        [processed-segments _vars-in-scope]
        (reduce
         (fn [[processed vars-in-scope] segment]
           (if (= :optimizable (:type segment))
             (let [patterns            (:data segment)
                   optimized-with-meta (optimize-segment-with-metadata db stats patterns vars-in-scope)
                   vars-in-scope'      (into vars-in-scope (mapcat :vars optimized-with-meta))]
               [(conj processed
                      {:type :optimizable
                       :original patterns
                       :optimized optimized-with-meta})
                vars-in-scope'])
             (let [pattern (:data segment)
                   vars-in-scope' (into vars-in-scope (boundary-produced-vars pattern))]
               [(conj processed
                      {:type :boundary
                       :pattern pattern})
                vars-in-scope'])))
         [[] #{}]
         segments)
        ;; Extract just the optimized patterns for the optimized clause
        optimized-clause
        (into []
              (mapcat (fn [segment]
                        (if (= :optimizable (:type segment))
                          (mapv :pattern (:optimized segment))
                          [(:pattern segment)])))
              processed-segments)
        changed? (not= where-clause optimized-clause)]
    {:original where-clause
     :optimized optimized-clause
     :segments processed-segments
     :changed? changed?}))

(defn- component->user-value
  "Convert an internal pattern component to user-readable format.
   Includes lang, datatype, and transaction metadata when present."
  [component compact-fn]
  (cond
    (nil? component)
    nil

    (where/unmatched-var? component)
    (str (where/get-variable component))

    (where/matched-iri? component)
    (let [iri (where/get-iri component)]
      (json-ld/compact iri compact-fn))

    (where/matched-value? component)
    (let [value       (where/get-value component)
          lang        (where/get-lang component)
          datatype    (where/get-datatype-iri component)
          transaction (where/get-transaction component)]
      ;; Only return a map with metadata if there's lang or transaction metadata
      ;; (datatype is common and not particularly interesting for display)
      (if (or lang transaction)
        (cond-> {:value value}
          lang        (assoc :lang lang)
          datatype    (assoc :datatype (json-ld/compact datatype compact-fn))
          transaction (assoc :t transaction))
        ;; No interesting metadata, just return the value
        value))

    :else
    (throw (ex-info (str "Unexpected component type: " (pr-str component))
                    {:component component}))))

(defn- cleanup-filter-data
  "Clean up filter pattern data for user display.
   Attempts to show the original filter expression if available via metadata."
  [f]
  (if-let [fns (some-> f meta :fns)]
    {:description "Filter expression"
     :expressions fns}
    {:description "Filter function"}))

(defn- cleanup-bind-data
  "Clean up bind pattern data for user display.
   Shows variable names and whether they use functions or static values."
  [bind-map compact-fn]
  (into {}
        (map (fn [[var-sym binding]]
               (let [var-name (str var-sym)]
                 [var-name
                  (if (contains? binding :fluree.db.query.exec.where/fn)
                    {:type "function"}
                    {:type "value"
                     :value (component->user-value binding compact-fn)})])))
        bind-map))

(defn- cleanup-values-data
  "Clean up values pattern data for user display.
   Shows inline solution bindings."
  [solutions compact-fn]
  {:description "Inline values"
   :solutions (mapv (fn [solution]
                      (into {}
                            (map (fn [[var-sym binding]]
                                   [(str var-sym) (component->user-value binding compact-fn)]))
                            solution))
                    solutions)})

(defn- cleanup-union-data
  "Clean up union pattern data for user display.
   Shows that it contains multiple alternative clauses."
  [clauses]
  {:description "Union of alternative patterns"
   :alternatives (count clauses)})

(defn- cleanup-optional-data
  "Clean up optional pattern data for user display."
  [_clause]
  {:description "Optional pattern group"})

(defn- pattern->user-format
  "Convert internal pattern to user-readable triple format"
  [pattern compact-fn]
  (let [ptype (where/pattern-type pattern)
        pdata (where/pattern-data pattern)]
    (case ptype
      :class
      (let [[s _ o] pdata]
        {:subject (component->user-value s compact-fn)
         :property const/iri-type
         :object (component->user-value o compact-fn)})

      :tuple
      (let [[s p o] pdata]
        {:subject (component->user-value s compact-fn)
         :property (component->user-value p compact-fn)
         :object (component->user-value o compact-fn)})

      :id
      {:subject (component->user-value pdata compact-fn)}

      :filter
      (cleanup-filter-data pdata)

      :bind
      (cleanup-bind-data pdata compact-fn)

      :values
      (cleanup-values-data pdata compact-fn)

      :union
      (cleanup-union-data pdata)

      :optional
      (cleanup-optional-data pdata)

      ;; Fallback for any other pattern types
      {:type ptype
       :description "Advanced pattern"})))

(defn- pattern-type->user-type
  "Convert internal pattern type to user-friendly type name"
  [ptype]
  (case ptype
    :tuple :triple
    ptype))

(defn- format-pattern-with-metadata
  "Format a pattern with its pre-calculated metadata for display.
   Takes pattern metadata map with :pattern, :score, :inputs."
  [pattern-meta compact-fn]
  (let [pattern (:pattern pattern-meta)
        score   (:score pattern-meta)
        inputs  (:inputs pattern-meta)
        ptype   (where/pattern-type pattern)]
    {:type        (pattern-type->user-type ptype)
     :pattern     (pattern->user-format pattern compact-fn)
     :selectivity score
     :inputs      inputs
     :optimizable (when score (pattern-type->user-type ptype))}))

(defn- format-segment-metadata
  "Format segment metadata for display in explain output."
  [segment compact-fn]
  (if (= :optimizable (:type segment))
    {:type     :optimizable
     :patterns (mapv #(format-pattern-with-metadata % compact-fn) (:optimized segment))}
    {:type    :boundary
     :pattern (pattern->user-format (:pattern segment) compact-fn)}))

(defn optimize-query
  "Optimize a parsed query using statistics if available.
   Returns the optimized query with patterns reordered for optimal execution.
   Uses fast path - extracts optimized patterns without formatting."
  [db parsed-query]
  (let [stats (:stats db)]
    (if (and stats (not-empty stats) (:where parsed-query))
      (let [{:keys [optimized]} (optimize-patterns-with-metadata db (:where parsed-query))]
        (assoc parsed-query :where optimized))
      parsed-query)))

(defn explain-query
  "Generate an execution plan for the query showing optimization details.
   Returns a query plan map with optimization information.
   Uses optimize-patterns-with-metadata to avoid redundant calculations."
  [db parsed-query]
  (let [stats            (:stats db)
        has-stat-counts? (and stats
                              (or (seq (:properties stats))
                                  (seq (:classes stats))))
        context          (:context parsed-query)
        compact-fn       (json-ld/compact-fn context)]
    (if-not has-stat-counts?
      {:query parsed-query
       :plan  {:optimization :none
               :reason       "No statistics available"
               :where-clause (:where parsed-query)}}
      (let [where-clause (:where parsed-query)]
        (if-not where-clause
          {:query parsed-query
           :plan  {:optimization :none
                   :reason       "No where clause"}}
          ;; Calculate optimization metadata once
          (let [{:keys [optimized segments changed?]}
                (optimize-patterns-with-metadata db where-clause)
                ;; Now just format the metadata for display (no recalculation)
                ;; For original, we need to compute the metadata for patterns in their original order
                original-with-meta (mapcat (fn [segment]
                                             (if (= :optimizable (:type segment))
                                               ;; Find scores from optimized segment (same patterns, different order)
                                               (let [score-map (into {} (map (fn [pm] [(:pattern pm) pm])
                                                                             (:optimized segment)))]
                                                 (map (fn [p] (or (get score-map p)
                                                                  {:pattern p :score nil :inputs nil}))
                                                      (:original segment)))
                                               [{:pattern (:pattern segment) :score nil :inputs nil}]))
                                           segments)
                original-explain   (mapv #(format-pattern-with-metadata % compact-fn) original-with-meta)
                optimized-explain (mapv #(format-pattern-with-metadata % compact-fn)
                                        (mapcat (fn [segment]
                                                  (if (= :optimizable (:type segment))
                                                    (:optimized segment)
                                                    [{:pattern (:pattern segment) :score nil :inputs nil}]))
                                                segments))
                segments-explain  (mapv #(format-segment-metadata % compact-fn) segments)]
            {:query (assoc parsed-query :where optimized)
             :plan  {:optimization (if changed? :reordered :unchanged)
                     :statistics   {:property-counts (count (:properties stats))
                                    :class-counts    (count (:classes stats))
                                    :total-flakes    (:flakes stats)
                                    :indexed-at-t    (:indexed stats)}
                     :original     original-explain
                     :optimized    optimized-explain
                     :segments     segments-explain}}))))))
