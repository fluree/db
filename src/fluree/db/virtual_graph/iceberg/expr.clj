(ns fluree.db.virtual-graph.iceberg.expr
  "Expression evaluation for residual FILTER and BIND operations.

   These are expressions that couldn't be pushed down to Iceberg and must
   be evaluated in Clojure after the table scan."
  (:require [fluree.db.query.exec.where :as where]
            [fluree.db.util.log :as log]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; Expression Evaluation (Residual FILTER + BIND)
;;; ---------------------------------------------------------------------------

(defn- apply-filter-fn
  "Apply a pre-compiled filter function to a solution.
   Returns the solution if filter passes, nil otherwise.

   Filter functions from eval.cljc expect solutions with match objects
   (symbol keys to {::where/val, ::where/datatype-iri, ...}).
   Iceberg solutions already have this format via row->solution."
  [solution filter-fn]
  (try
    (when (filter-fn solution)
      solution)
    (catch Exception e
      (log/debug "Filter evaluation error:" (ex-message e))
      nil)))

(defn- apply-filters
  "Apply all compiled filter functions to solutions.
   Works with both eager (vec) and lazy (seq) inputs.

   Args:
     solutions    - Sequence of solution maps
     filter-specs - Vector of {:fn compiled-filter-fn, :meta pattern-metadata}"
  [solutions filter-specs]
  (if (seq filter-specs)
    (let [filter-fns (map :fn filter-specs)]
      (filter (fn [sol]
                (every? #(apply-filter-fn sol %) filter-fns))
              solutions))
    solutions))

(defn- apply-bind-spec
  "Apply a BIND spec to a solution, adding new variable bindings.

   Spec is a map {var-sym {::where/var v, ::where/fn f}} from the BIND pattern.
   For each binding:
   - If ::where/fn is present, evaluate the function and bind result
   - Otherwise, it's a static binding

   Args:
     solution  - Current solution map
     bind-spec - Map of {var-sym -> bind-info}"
  [solution bind-spec]
  (reduce-kv
   (fn [sol var-sym bind-info]
     (let [f (::where/fn bind-info)]
       (if f
         (try
           (let [result (f sol)
                 result-mch (where/typed-val->mch (where/unmatched-var var-sym) result)]
             (or (where/update-solution-binding sol var-sym result-mch)
                 (assoc sol ::invalidated true)))
           (catch Exception e
             (log/debug "BIND evaluation error for" var-sym ":" (ex-message e))
             (assoc sol ::invalidated true)))
         ;; Static binding - bind-info is already a match object
         (or (where/update-solution-binding sol var-sym bind-info)
             (assoc sol ::invalidated true)))))
   solution
   bind-spec))

(defn- apply-binds
  "Apply all BIND specs to solutions.
   Solutions marked ::invalidated are removed.

   Args:
     solutions  - Sequence of solution maps
     bind-specs - Vector of bind specs (each a map {var-sym -> bind-info})"
  [solutions bind-specs]
  (if (seq bind-specs)
    (->> solutions
         (map (fn [sol] (reduce apply-bind-spec sol bind-specs)))
         (remove ::invalidated))
    solutions))

(defn apply-expression-evaluators
  "Apply residual BIND and FILTER evaluators to solutions.

   This is called in -finalize after Iceberg scan but before anti-joins
   and aggregation. Order: BIND first (to introduce variables that may
   be needed for correlated EXISTS/NOT EXISTS), then FILTER.

   Args:
     solutions   - Sequence of solution maps from Iceberg scan
     evaluators  - Map {:filters [...] :binds [...]}"
  [solutions evaluators]
  (if (or (seq (:filters evaluators)) (seq (:binds evaluators)))
    (do
      (log/debug "Applying expression evaluators:"
                 {:filters (count (:filters evaluators))
                  :binds (count (:binds evaluators))
                  :input-count (if (counted? solutions) (count solutions) "lazy")})
      (let [;; Apply BINDs first to introduce new variables
            with-binds (apply-binds solutions (:binds evaluators))
            ;; Then apply FILTERs
            filtered (apply-filters with-binds (:filters evaluators))]
        (log/debug "Expression evaluation complete")
        filtered))
    solutions))
