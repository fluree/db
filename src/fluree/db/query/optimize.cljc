(ns fluree.db.query.optimize
  (:require [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.exec.where :as where]))

;; Selectivity score constants
;; Lower score = more selective = execute first
(def ^:const highly-selective 0)
(def ^:const moderately-selective 10)
(def ^:const default-selectivity 1000)
(def ^:const full-scan 1.0e12)

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

(defn matched?
  "Check if a component is matched (has a bound value, IRI, or SID)"
  [component]
  (where/matched? component))

(defn unmatched?
  "Check if a component is unmatched (variable)"
  [component]
  (not (matched? component)))

(defn all-matched?
  "Check if all components are matched"
  [[s p o]]
  (and (matched? s) (matched? p) (matched? o)))

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

(defn calculate-selectivity
  "Calculate selectivity score for a pattern."
  [db stats pattern]
  (let [pattern-type (where/pattern-type pattern)
        pattern-data (where/pattern-data pattern)]

    (cond
      (or (nil? stats) (empty? stats))
      nil

      (= :id pattern-type)
      (if (matched? pattern-data)
        highly-selective
        moderately-selective)

      :else
      (let [[s p o] pattern-data]
        (cond
          (= :class pattern-type)
          (let [class-sid (get-sid db o)]
            (or (get-class-count stats class-sid) default-selectivity))

          (all-matched? [s p o])
          highly-selective

          (and (matched? s) (matched? p) (unmatched? o))
          (let [pred-sid (get-sid db p)]
            (estimate-bound-subject stats pred-sid))

          (and (unmatched? s) (matched? p) (unmatched? o))
          (let [pred-sid (get-sid db p)]
            (or (get-property-count stats pred-sid) default-selectivity))

          (and (unmatched? s) (matched? p) (matched? o))
          (let [pred-sid (get-sid db p)]
            (estimate-bound-value stats pred-sid))

          (and (unmatched? s) (unmatched? p) (unmatched? o))
          full-scan

          :else
          default-selectivity)))))

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
      (if (matched? pattern-data)
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

          (all-matched? [s p o])
          {:score highly-selective
           :inputs {:type :tuple-exact :all-matched? true}}

          (and (matched? s) (matched? p) (unmatched? o))
          (let [pred-sid (get-sid db p)
                count (get-property-count stats pred-sid)
                ndv-subjects (get-ndv-subjects stats pred-sid)
                selectivity (get-selectivity-subject stats pred-sid)
                score (estimate-bound-subject stats pred-sid)]
            {:score score
             :inputs (cond-> {:type :tuple
                              :pattern :bound-subject
                              :property-sid pred-sid
                              :count count
                              :ndv-subjects ndv-subjects
                              :selectivity selectivity
                              :used-subjects-ndv? (some? selectivity)}
                       (nil? selectivity)
                       (assoc :fallback? true
                              :reason (if count "NDV not available" "Count not available")))})

          (and (unmatched? s) (matched? p) (unmatched? o))
          (let [pred-sid (get-sid db p)
                count (get-property-count stats pred-sid)
                score (or count default-selectivity)]
            {:score score
             :inputs (cond-> {:type :tuple
                              :pattern :property-scan
                              :property-sid pred-sid
                              :count count}
                       (nil? count)
                       (assoc :fallback? true :reason "Count not available"))})

          (and (unmatched? s) (matched? p) (matched? o))
          (let [pred-sid (get-sid db p)
                count (get-property-count stats pred-sid)
                ndv-values (get-ndv-values stats pred-sid)
                selectivity (get-selectivity-value stats pred-sid)
                clamped? (and selectivity (< selectivity 1.0))
                score (estimate-bound-value stats pred-sid)]
            {:score score
             :inputs (cond-> {:type :tuple
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

          (and (unmatched? s) (unmatched? p) (unmatched? o))
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

(defn optimize-segment
  "Optimize a single segment by sorting patterns by selectivity.
   Uses compare-triples as tie-breaker for equal scores to ensure stable ordering."
  [db stats patterns]
  (if (or (nil? stats) (empty? stats))
    ;; No stats, return as-is
    patterns
    ;; Sort by selectivity (lower = more selective = execute first)
    ;; Use compare-triples as tie-breaker for equal scores
    (let [with-scores (mapv (fn [pattern]
                              {:pattern pattern
                               :score (or (calculate-selectivity db stats pattern)
                                          default-selectivity)})
                            patterns)
          cmp         (fn [{sa :score pa :pattern} {sb :score pb :pattern}]
                        (let [c (compare sa sb)]
                          (if (zero? c)
                            (compare-triples pa pb)
                            c)))
          sorted      (sort cmp with-scores)]
      (mapv :pattern sorted))))

(defn optimize-patterns
  "Reorder patterns for optimal execution based on statistics.
   Splits on optimization boundaries and optimizes each segment independently."
  [db where-clause]
  (let [stats (:stats db)]
    (if (or (nil? stats) (empty? stats))
      ;; No stats available, return as-is
      where-clause
      ;; Split into segments and optimize each
      (let [segments (split-by-optimization-boundaries where-clause)]
        (into []
              (mapcat (fn [segment]
                        (if (= :optimizable (:type segment))
                          (optimize-segment db stats (:data segment))
                          [(:data segment)])))
              segments)))))

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

    Returns a channel that will contain a query plan map with:
    - :query - The optimized query
    - :plan - Execution plan details including:
      - :optimization - :reordered, :unchanged, or :none
      - :statistics - Available statistics info
      - :original - Original pattern order with selectivity
      - :optimized - Optimized pattern order with selectivity
      - :segments - Pattern segments with boundaries
      - :changed? - Boolean indicating if patterns were reordered

    Parameters:
      db - The database (FlakeDB, AsyncDB, etc.)
      parsed-query - The parsed query from fql/parse-query

    Returns:
      Channel containing query plan map"))
