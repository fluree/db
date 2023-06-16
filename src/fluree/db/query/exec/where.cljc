(ns fluree.db.query.exec.where
  (:require [fluree.db.query.range :as query-range]
            [clojure.core.async :as async :refer [>! go]]
            [fluree.db.flake :as flake]
            [fluree.db.fuel :as fuel]
            [fluree.db.util.async :refer [<?]]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.datatype :as datatype]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const])
  #?(:clj (:import (clojure.lang MapEntry))))

#?(:clj (set! *warn-on-reflection* true))

(defn reference?
  [dt]
  (= dt const/$xsd:anyURI))

(defn idx-for
  [s p o o-dt]
  (cond
    s         :spot
    (and p o) :post
    p         :psot
    o         (if (reference? o-dt)
                :opst
                (throw (ex-info (str "Illegal reference object value" (::var o))
                                {:status 400 :error :db/invalid-query})))
    :else     :spot))

(defn resolve-flake-range
  ([db fuel-tracker error-ch components]
   (resolve-flake-range db fuel-tracker nil error-ch components))

  ([{:keys [conn t] :as db} fuel-tracker flake-xf error-ch components]
   (let [out-ch               (async/chan)
         [s-cmp p-cmp o-cmp]  components
         {s ::val, s-fn ::fn} s-cmp
         {p ::val, p-fn ::fn} p-cmp
         {o    ::val
          o-fn ::fn
          o-dt ::datatype}    o-cmp]
     (go
       (try* (let [s*          (if (and s (not (number? s)))
                                 (<? (dbproto/-subid db s true))
                                 s)
                   [o* o-dt*]  (if-let [o-iri (::iri o)]
                                 [(<? (dbproto/-subid db o-iri true)) const/$xsd:anyURI]
                                 [o o-dt])
                   idx         (idx-for s* p o* o-dt*)
                   idx-root    (get db idx)
                   novelty     (get-in db [:novelty idx])
                   start-flake (flake/create s* p o* o-dt* nil nil util/min-integer)
                   end-flake   (flake/create s* p o* o-dt* nil nil util/max-integer)
                   track-fuel  (when fuel-tracker
                                 (fuel/track fuel-tracker))
                   flake-xf*   (->> [flake-xf track-fuel]
                                    (remove nil?)
                                    (apply comp))
                   opts        (cond-> {:idx         idx
                                        :from-t      t
                                        :to-t        t
                                        :start-test  >=
                                        :start-flake start-flake
                                        :end-test    <=
                                        :end-flake   end-flake
                                        :flake-xf    flake-xf*}
                                 s-fn (assoc :subject-fn s-fn)
                                 p-fn (assoc :predicate-fn p-fn)
                                 o-fn (assoc :object-fn o-fn))]
               (-> (query-range/resolve-flake-slices conn idx-root novelty
                                                     error-ch opts)
                   (->> (query-range/filter-authorized db start-flake end-flake
                                                       error-ch))
                   (async/pipe out-ch)))
             (catch* e
                     (log/error e "Error resolving flake range")
                     (>! error-ch e))))
     out-ch)))

(defn unmatched
  ([] {})
  ([var-sym]
   {::var var-sym}))

(defn match-value
  ([m x dt]
   (assoc m
     ::val x
     ::datatype dt)))

(defn anonymous-value
  "Build a pattern that already matches an explicit value."
  ([v]
   (let [dt (datatype/infer v)]
     (anonymous-value v dt)))
  ([v dt]
   (-> (unmatched)
       (match-value v dt))))

(defn matched?
  [component]
  (-> component ::val boolean))

(def unmatched?
  "Returns true if the triple pattern component `component` represents a variable
  without an associated value."
  (complement matched?))

(defn ->pattern
  "Build a new non-tuple match pattern of type `typ`."
  [typ data]
  #?(:clj  (MapEntry/create typ data)
     :cljs (MapEntry. typ data nil)))

(defn ->ident
  "Build a pattern that already matches the two-tuple database identifier `x`"
  [x]
  {::ident x})

(defn ->iri-ref
  [x]
  {::iri x})

(defn ->function
  "Build a query function specification for the variable `var` out of the
  parsed function `f`."
  [var f]
  (-> var
      unmatched
      (assoc ::fn f)))

(defn ->predicate
  "Build a pattern that already matches the explicit predicate value `value`."
  ([value]
   (anonymous-value value))
  ([value recur-n]
   (-> value
       ->predicate
       (assoc ::recur recur-n))))

(defn ->full-text
  "Build a full text predicate pattern match."
  [pred]
  {::full-text pred})

(defn ->where-clause
  "Build a pattern that matches all the patterns in the supplied `patterns`
  collection and filters any matches for variables appearing as a key in the
  supplied `filters` map with the filter specification found in the value of the
  filters map for that variable, if the `filters` map is provided."
  ([patterns]
   {::patterns patterns})
  ([patterns filters]
   (cond-> (->where-clause patterns)
           (seq filters) (assoc ::filters filters))))

(defn pattern-type
  [pattern]
  (if (map-entry? pattern)
    (key pattern)
    :tuple))

(defmulti match-pattern
  "Return a channel that will contain all pattern match solutions from flakes in
   `db` that are compatible with the initial solution `solution` and matches the
   additional where-clause pattern `pattern`."
  (fn [_db _fuel-tracker _solution pattern _filters _error-ch]
    (pattern-type pattern)))

(defn assign-matched-values
  "Assigns the value of any variables within the supplied `triple-pattern` that
  were previously matched in the supplied solution map `solution` to their
  values from `solution`. If a variable in `triple-pattern` does not have a
  match in `solution`, but does appear as a key in the filter specification map
  `filters`, the variable's match filter function within `triple-pattern` is set
  to the value associated with that variable from the `filter` specification
  map."
  [triple-pattern solution filters]
  (mapv (fn [component]
          (if-let [variable (::var component)]
            (let [match (get solution variable)]
              (if-let [value (::val match)]
                (let [dt (::datatype match)]
                  (match-value component value dt))
                (let [filter-fn (some->> (get filters variable)
                                         (map ::fn)
                                         (map (fn [f]
                                                (partial f solution)))
                                         (apply every-pred))]
                  (assoc component ::fn filter-fn))))
            component))
        triple-pattern))

(defn match-subject
  "Matches the subject of the supplied `flake` to the triple subject pattern
  component `s-match`, and marks the matched pattern component as a URI data
  type."
  [s-match flake]
  (match-value s-match (flake/s flake) const/$xsd:anyURI))

(defn match-predicate
  "Matches the predicate of the supplied `flake` to the triple predicate pattern
  component `p-match`, and marks the matched pattern component as a URI data
  type."
  [p-match flake]
  (match-value p-match (flake/p flake) const/$xsd:anyURI))

(defn match-object
  "Matches the object and data type of the supplied `flake` to the triple object
  pattern component `o-match`."
  [o-match flake]
  (match-value o-match (flake/o flake) (flake/dt flake)))

(defn match-flake
  "Assigns the unmatched variables within the supplied `triple-pattern` to their
  corresponding values from `flake` in the supplied match `solution`."
  [solution triple-pattern flake]
  (let [[s p o] triple-pattern]
    (cond-> solution
            (unmatched? s) (assoc (::var s) (match-subject s flake))
            (unmatched? p) (assoc (::var p) (match-predicate p flake))
            (unmatched? o) (assoc (::var o) (match-object o flake)))))

(defn get-equivalent-properties
  [db prop]
  (-> db
      (get-in [:schema :pred prop :equivalentProperty])
      not-empty))

(defn recursive-resolve-flake-range
  [db [s p o] recur-n fuel-tracker error-ch]
  (let [{first-s ::val} s
        result-ch (async/chan)]
    (go
      (loop [stack [[(if (and first-s (not (number? first-s)))
                       (<? (dbproto/-subid db first-s true))
                       first-s) recur-n]]]
        (let [[new-query-sid recur-r :as next]  (first stack)]
          (if (and next
                   (not= 0 recur-r))
            (let [fs (<? (resolve-flake-range db fuel-tracker error-ch [(assoc s ::val new-query-sid) p o]))]
              (async/>! result-ch fs)
              (recur (into (rest stack) (map (fn [f] [(flake/o f) (dec recur-r)]) fs))))
            (async/close! result-ch)))))
    result-ch))


(defmethod match-pattern :tuple
  [db fuel-tracker solution pattern filters error-ch]
  (let [[s p o]  (assign-matched-values pattern solution filters)
        match-ch (async/chan 2 (comp cat
                                     (map (fn [flake]
                                            (match-flake solution pattern flake)))))
        p-val    (::val p)
        recur-n (::recur p)]
    (if-let [props (and p-val (get-equivalent-properties db p-val))]
      (let [prop-ch (async/to-chan! (conj props p-val))]
        (async/pipeline-async 2
                              match-ch
                              (fn [prop ch]
                                (let [p* (assoc p ::val prop)]
                                  (-> db
                                      (resolve-flake-range fuel-tracker error-ch [s p* o])
                                      (async/pipe ch))))
                              prop-ch))

      (if recur-n
        (-> db
            (recursive-resolve-flake-range [s p o] recur-n fuel-tracker error-ch)
            (async/pipe match-ch))
        (-> db
            (resolve-flake-range fuel-tracker error-ch [s p o])
            (async/pipe match-ch))))

    match-ch))

(defn with-distinct-subjects
  "Return a transducer that filters a stream of flakes by removing any flakes with
  subject ids repeated from previously processed flakes."
  []
  (fn [rf]
    (let [seen-sids (volatile! #{})]
      (fn
        ;; Initialization: do nothing but initialize the supplied reducing fn
        ([]
         (rf))

        ;; Iteration: keep track of subject ids seen; only pass flakes with new
        ;; subject ids through to the supplied reducing fn.
        ([result f]
         (let [sid (flake/s f)]
           (if (contains? @seen-sids sid)
             result
             (do (vswap! seen-sids conj sid)
                 (rf result f)))))

        ;; Termination: do nothing but terminate the supplied reducing fn
        ([result]
         (rf result))))))

(defmethod match-pattern :class
  [db fuel-tracker solution pattern filters error-ch]
  (let [triple   (val pattern)
        [s p o] (assign-matched-values triple solution filters)
        cls      (::val o)
        classes  (into [cls] (dbproto/-class-prop db :subclasses cls))
        class-ch (async/to-chan! classes)
        match-ch (async/chan 2 (comp cat
                                     (with-distinct-subjects)
                                     (map (fn [flake]
                                            (match-flake solution triple flake)))))]
    (async/pipeline-async 2
                          match-ch
                          (fn [cls ch]
                            (-> (resolve-flake-range db fuel-tracker error-ch [s p (assoc o ::val cls)])
                                (async/pipe ch)))
                          class-ch)
    match-ch))

(defn with-constraint
  "Return a channel of all solutions from `db` that extend from the solutions in
  `solution-ch` and also match the where-clause pattern `pattern`."
  [db fuel-tracker pattern filters error-ch solution-ch]
  (let [out-ch (async/chan 2)]
    (async/pipeline-async 2
                          out-ch
                          (fn [solution ch]
                            (-> (match-pattern db fuel-tracker solution pattern filters error-ch)
                                (async/pipe ch)))
                          solution-ch)
    out-ch))

(defn match-clause
  "Returns a channel that will eventually contain all match solutions in `db`
  extending from `solution` that also match all the patterns in the parsed where
  clause collection `clause`."
  [db fuel-tracker solution clause error-ch]
  (let [initial-ch (async/to-chan! [solution])
        filters    (::filters clause)
        patterns   (::patterns clause)]
    (reduce (fn [solution-ch pattern]
              (with-constraint db fuel-tracker pattern filters error-ch solution-ch))
            initial-ch patterns)))

(defmethod match-pattern :union
  [db fuel-tracker solution pattern _ error-ch]
  (let [clauses   (val pattern)
        clause-ch (async/to-chan! clauses)
        out-ch    (async/chan 2)]
    (async/pipeline-async 2
                          out-ch
                          (fn [clause ch]
                            (-> (match-clause db fuel-tracker solution clause error-ch)
                                (async/pipe ch)))
                          clause-ch)
    out-ch))

(defn with-default
  "Return a transducer that transforms an input stream of solutions to include the
  `default-solution` if and only if the stream was empty."
  [default-solution]
  (fn [rf]
    (let [solutions? (volatile! false)]
      (fn
        ;; Initialization: do nothing but initialize the supplied reducing fn.
        ([]
         (rf))

        ;; Iteration: mark that a solution was processed, and pass it to the supplied
        ;; reducing fn.
        ([result solution]
         (do (vreset! solutions? true)
             (rf result solution)))

        ;; Termination: if no other solutions were processed, then process the
        ;; default-solution with the supplied reducing fn before terminating it;
        ;; terminate as normal otherwise.
        ([result]
         (if @solutions?
           (rf result)
           (do (vreset! solutions? true) ; mark that a solution was processed in
                                         ; case the reducing fn is terminated
                                         ; again as can happen with buffers.
               (-> result
                   (rf default-solution)
                   rf))))))))

(defmethod match-pattern :optional
  [db fuel-tracker solution pattern _ error-ch]
  (let [clause (val pattern)
        opt-ch (async/chan 2 (with-default solution))]
    (-> (match-clause db fuel-tracker solution clause error-ch)
        (async/pipe opt-ch))))

(defn add-fn-result-to-solution
  [solution var-name result]
  (let [dt (datatype/infer result)]
    (assoc solution var-name {::var var-name ::val result ::datatype dt})))

(defmethod match-pattern :bind
  [_db fuel-tracker solution pattern _ error-ch]
  (let [bind (val pattern)]
    (go
      (let [result
            (reduce (fn [solution* b]
                      (let [f        (::fn b)
                            var-name (::var b)]
                        (try*
                         (->> (f solution)
                              (add-fn-result-to-solution solution* var-name))
                         (catch* e (update solution* ::errors conj e)))))
                    solution (vals bind))]
        (when-let [errors (::errors result)]
          (async/onto-chan! error-ch errors))
        result))))

(def blank-solution {})

(defn search
  [db q fuel-tracker error-ch]
  (let [where-clause      (:where q)
        initial-solutions (-> q
                              :values
                              not-empty
                              (or [blank-solution]))
        out-ch            (async/chan)]
    (async/pipeline-async 2
                          out-ch
                          (fn [initial-solution ch]
                            (-> (match-clause db fuel-tracker initial-solution where-clause error-ch)
                                (async/pipe ch)))
                          (async/to-chan! initial-solutions))
    out-ch))
