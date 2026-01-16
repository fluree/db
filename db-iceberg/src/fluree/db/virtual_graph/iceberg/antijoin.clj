(ns fluree.db.virtual-graph.iceberg.antijoin
  "Anti-join execution for EXISTS, NOT EXISTS, and MINUS patterns.

   These are SPARQL operators that correlate an outer query with an inner
   subquery to filter results."
  (:require [clojure.set]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util.log :as log]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; Anti-Join Execution (EXISTS, NOT EXISTS, MINUS)
;;; ---------------------------------------------------------------------------

(defn extract-pattern-vars
  "Extract all variable symbols from a sequence of WHERE patterns.

   Used to determine which outer solution bindings are relevant for
   correlated subquery memoization.

   Handles:
   - MapEntry patterns (from where/->pattern): [:tuple {:s {:var ?x} ...}]
   - Raw map patterns (SPARQL): {:s ?x :p \"pred\" :o \"val\"}
   - Vector patterns (SPARQL nested): [\"exists\" [{:s ?x ...}]]"
  [patterns]
  (if-not (sequential? patterns)
    #{}  ;; Return empty set for non-sequential inputs
    (into #{}
          (mapcat (fn [pattern]
                    (cond
                  ;; MapEntry pattern - use where accessors
                      (map-entry? pattern)
                      (let [ptype (where/pattern-type pattern)
                            pdata (where/pattern-data pattern)]
                        (case ptype
                          :tuple
                      ;; Extract vars from tuple pattern slots
                          (->> pdata
                               (keep (fn [[_slot m]]
                                       (when (and (map? m) (:var m))
                                         (:var m)))))
                      ;; Nested patterns - recurse
                          (:exists :not-exists :minus)
                          (extract-pattern-vars pdata)
                      ;; Other pattern types - no vars extracted
                          nil))

                  ;; Vector pattern - could be:
                  ;; 1. SPARQL nested like ["exists" [...]]
                  ;; 2. Tuple as [s-match p-match o-match] where matches have ::where/var
                      (vector? pattern)
                      (let [first-elem (first pattern)]
                        (cond
                      ;; Nested anti-join pattern
                          (or (keyword? first-elem) (string? first-elem))
                          (let [ptype (if (keyword? first-elem) first-elem (keyword first-elem))]
                            (when (#{:exists :not-exists :minus} ptype)
                              (extract-pattern-vars (second pattern))))

                      ;; Tuple as vector of match objects [s p o]
                          (map? first-elem)
                          (->> pattern
                               (keep (fn [match-obj]
                                       (when (map? match-obj)
                                     ;; Check for ::where/var in the match object
                                         (or (::where/var match-obj)
                                             (:var match-obj)
                                         ;; Handle namespaced key as keyword
                                             (get match-obj :fluree.db.query.exec.where/var))))))))

                  ;; Raw map pattern (tuple) - extract vars directly
                      (map? pattern)
                      (->> pattern
                           (keep (fn [[_slot m]]
                                   (cond
                                 ;; Match object with :var
                                     (and (map? m) (:var m))
                                     (:var m)
                                 ;; Direct symbol (SPARQL raw pattern)
                                     (symbol? m)
                                     m
                                     :else nil))))

                      :else nil))
                  patterns))))

(defn apply-exists
  "Apply EXISTS filter: keep solutions where inner pattern matches.

   EXISTS is a correlated subquery - the inner pattern uses bindings from
   the outer solution. A solution is kept if the inner pattern produces
   at least one result.

   SPARQL semantics per spec section 8.2.

   Performance optimization: Instead of executing the inner query per outer
   solution (expensive), we execute it ONCE with no correlations to get all
   possible matches, then use set membership tests. This converts EXISTS to
   a semi-join operation which is much more efficient.

   Args:
     solutions            - Sequence of outer solutions
     inner-patterns       - Patterns from the EXISTS clause
     execute-inner-fn     - Function to execute inner patterns: (fn [patterns base-solution] -> solutions)

   Returns filtered sequence of solutions."
  [solutions inner-patterns execute-inner-fn]
  (let [solutions-vec (vec solutions)]
    (if (empty? solutions-vec)
      solutions-vec
      ;; Find variables used in inner patterns
      (let [inner-vars (extract-pattern-vars inner-patterns)
            outer-keys (set (keys (first solutions-vec)))
            ;; Correlated vars are those in both outer solution and inner patterns
            correlated-vars (vec (clojure.set/intersection outer-keys inner-vars))]
        (log/debug "EXISTS semi-join:" {:inner-var-count (count inner-vars)
                                        :correlated-var-count (count correlated-vars)})
        (if (empty? correlated-vars)
          ;; No correlation - EXISTS evaluates to same result for all outer solutions
          ;; Execute once and keep all or none
          (let [inner-results (execute-inner-fn inner-patterns {})]
            (if (seq inner-results)
              solutions-vec  ;; Inner has results - keep all outer
              []))           ;; Inner empty - remove all outer
          ;; Has correlated vars - execute inner once, build index, do semi-join
          (let [;; Execute inner query once without outer bindings
                inner-results (vec (execute-inner-fn inner-patterns {}))
                ;; Build index: {[correlated-var-values] -> true}
                inner-index (into #{}
                                  (keep (fn [inner-sol]
                                          (let [vals (mapv #(get inner-sol %) correlated-vars)]
                                            (when (every? some? vals)
                                              vals))))
                                  inner-results)]
            (log/debug "EXISTS index built:" {:inner-count (count inner-results)
                                              :index-size (count inner-index)})
            ;; Filter outer solutions using index - O(1) lookup
            (filterv
             (fn [outer-sol]
               (let [outer-vals (mapv #(get outer-sol %) correlated-vars)]
                 (and (every? some? outer-vals)
                      (contains? inner-index outer-vals))))
             solutions-vec)))))))

(defn apply-not-exists
  "Apply NOT EXISTS filter: keep solutions where inner pattern does NOT match.

   NOT EXISTS is a correlated subquery - the inner pattern uses bindings from
   the outer solution. A solution is kept if the inner pattern produces
   NO results.

   SPARQL semantics per spec section 8.2.

   Performance optimization: Instead of executing the inner query per outer
   solution (expensive), we execute it ONCE with no correlations to get all
   possible matches, then use set membership tests. This converts NOT EXISTS
   to an anti-semi-join operation which is much more efficient.

   Args:
     solutions            - Sequence of outer solutions
     inner-patterns       - Patterns from the NOT EXISTS clause
     execute-inner-fn     - Function to execute inner patterns: (fn [patterns base-solution] -> solutions)

   Returns filtered sequence of solutions."
  [solutions inner-patterns execute-inner-fn]
  (let [solutions-vec (vec solutions)]
    (if (empty? solutions-vec)
      solutions-vec
      ;; Find variables used in inner patterns
      (let [inner-vars (extract-pattern-vars inner-patterns)
            outer-keys (set (keys (first solutions-vec)))
            ;; Correlated vars are those in both outer solution and inner patterns
            correlated-vars (vec (clojure.set/intersection outer-keys inner-vars))]
        (log/debug "NOT EXISTS anti-semi-join:" {:inner-var-count (count inner-vars)
                                                 :correlated-var-count (count correlated-vars)})
        (if (empty? correlated-vars)
          ;; No correlation - NOT EXISTS evaluates to same result for all outer solutions
          ;; Execute once and keep all or none
          (let [inner-results (execute-inner-fn inner-patterns {})]
            (if (seq inner-results)
              []              ;; Inner has results - remove all outer
              solutions-vec)) ;; Inner empty - keep all outer
          ;; Has correlated vars - execute inner once, build index, do anti-semi-join
          (let [;; Execute inner query once without outer bindings
                inner-results (vec (execute-inner-fn inner-patterns {}))
                ;; Build index: {[correlated-var-values] -> true}
                inner-index (into #{}
                                  (keep (fn [inner-sol]
                                          (let [vals (mapv #(get inner-sol %) correlated-vars)]
                                            (when (every? some? vals)
                                              vals))))
                                  inner-results)]
            (log/debug "NOT EXISTS index built:" {:inner-count (count inner-results)
                                                  :index-size (count inner-index)})
            ;; Filter outer solutions using index - O(1) lookup
            ;; Keep solutions NOT in the inner index
            (filterv
             (fn [outer-sol]
               (let [outer-vals (mapv #(get outer-sol %) correlated-vars)]
                 (or (some nil? outer-vals)  ;; Unbound var - not a match, keep
                     (not (contains? inner-index outer-vals)))))
             solutions-vec)))))))

(defn apply-minus
  "Apply MINUS set difference: remove solutions that match inner pattern.

   MINUS is NOT a correlated subquery - the inner pattern is executed
   independently. Then, for each outer solution, if there exists an inner
   solution with the same values for all shared variables, the outer
   solution is removed.

   SPARQL semantics per spec section 8.3:
   - Only shared variables are compared
   - Unbound variables in either solution are treated as non-matching

   Performance: Uses O(1) hash index lookup instead of O(inner) scan per outer.
   Shared variables are determined once from solution structure, then inner
   solutions are indexed by their shared-var values.

   Args:
     solutions            - Sequence of outer solutions
     inner-patterns       - Patterns from the MINUS clause
     execute-inner-fn     - Function to execute inner patterns: (fn [patterns base-solution] -> solutions)

   Returns filtered sequence of solutions."
  [solutions inner-patterns execute-inner-fn]
  ;; Execute inner pattern once (uncorrelated - no outer bindings)
  (let [inner-solutions (vec (execute-inner-fn inner-patterns {}))
        outer-solutions (vec solutions)]
    (cond
      ;; No inner solutions - keep all outer solutions
      (empty? inner-solutions)
      outer-solutions

      ;; No outer solutions - nothing to filter
      (empty? outer-solutions)
      outer-solutions

      :else
      ;; Determine shared vars from solution structure (consistent within each result set)
      (let [inner-keys (set (keys (first inner-solutions)))
            outer-keys (set (keys (first outer-solutions)))
            shared-vars (vec (clojure.set/intersection outer-keys inner-keys))]
        (if (empty? shared-vars)
          ;; No shared variables - nothing can match, keep all
          outer-solutions
          ;; Build hash index: {[shared-var-values] -> true}
          (let [inner-index (into #{}
                                  (keep (fn [inner-sol]
                                          (let [vals (mapv #(get inner-sol %) shared-vars)]
                                        ;; Only index if all shared vars are bound
                                            (when (every? some? vals)
                                              vals))))
                                  inner-solutions)]
            (log/debug "MINUS index built:" {:shared-vars shared-vars
                                             :inner-count (count inner-solutions)
                                             :index-size (count inner-index)})
            ;; Filter outer solutions - O(1) lookup per solution
            (filterv
             (fn [outer-sol]
               (let [outer-vals (mapv #(get outer-sol %) shared-vars)]
                  ;; Keep if: any shared var is unbound, OR values not in inner index
                 (or (some nil? outer-vals)
                     (not (contains? inner-index outer-vals)))))
             outer-solutions)))))))

(defn apply-anti-joins
  "Apply all anti-join patterns to solutions in sequence.

   Anti-joins are applied after the main query execution and before
   query modifiers (DISTINCT, ORDER BY, LIMIT).

   Args:
     solutions        - Sequence of solutions from main query
     anti-joins       - Vector of {:type :exists/:not-exists/:minus :patterns [...]}
     execute-inner-fn - Function to execute inner patterns

   Returns solutions after applying all anti-joins."
  [solutions anti-joins execute-inner-fn]
  (reduce
   (fn [sols {:keys [type patterns]}]
     (log/debug "Applying anti-join:" {:type type :pattern-count (count patterns)
                                       :input-solutions (count sols)})
     (let [result (case type
                    :exists (apply-exists sols patterns execute-inner-fn)
                    :not-exists (apply-not-exists sols patterns execute-inner-fn)
                    :minus (apply-minus sols patterns execute-inner-fn)
                    ;; Unknown type - pass through
                    (do (log/warn "Unknown anti-join type:" type)
                        sols))
           ;; Force realization to get accurate count for logging
           result-vec (vec result)]
       (log/debug "Anti-join result:" {:type type :output-solutions (count result-vec)})
       result-vec))
   solutions
   anti-joins))
