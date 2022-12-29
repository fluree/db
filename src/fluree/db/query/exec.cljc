(ns fluree.db.query.exec
  (:require [clojure.spec.alpha :as spec]
            [clojure.string :as str]
            [fluree.json-ld :as json-ld]
            [fluree.db.query.json-ld.response :as json-ld-resp]
            [fluree.db.query.range :as query-range]
            [clojure.core.async :as async :refer [<! >! go go-loop]]
            [fluree.db.flake :as flake]
            [fluree.db.util.async :refer [<? go-try merge-into?]]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.query.analytical-filter :as filter]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const])
  (:import (clojure.lang MapEntry)))

#?(:clj (set! *warn-on-reflection* true))

(defn idx-for
  [s p o]
  (cond
    s         :spot
    (and p o) :post
    p         :psot
    o         :opst
    :else     :spot))

(defn resolve-flake-range
  [{:keys [conn t] :as db} error-ch components]
  (let [[s p o]          (map ::val components)
        [s-fn p-fn o-fn] (map ::fn components)
        idx              (idx-for s p o)
        idx-root         (get db idx)
        novelty          (get-in db [:novelty idx])
        start-flake      (flake/create s p o nil nil nil util/min-integer)
        end-flake        (flake/create s p o nil nil nil util/max-integer)
        opts             (cond-> {:idx         idx
                                  :from-t      t
                                  :to-t        t
                                  :start-test  >=
                                  :start-flake start-flake
                                  :end-test    <=
                                  :end-flake   end-flake}
                           s-fn (assoc :subject-fn s-fn)
                           p-fn (assoc :predicate-fn p-fn)
                           o-fn (assoc :object-fn o-fn))]
    (query-range/resolve-flake-slices conn idx-root novelty error-ch opts)))

(defn ->pattern
  [typ data]
  (MapEntry/create typ data))

(defn ->where-clause
  ([patterns]
   {::patterns patterns})
  ([patterns filters]
   (-> patterns
       ->where-clause
       (assoc ::filters filters))))

(defn pattern-type
  [pattern]
  (if (map-entry? pattern)
    (key pattern)
    :tuple))

(defmulti match-pattern
  "Return a channel that will contain all solutions from flakes in `db` that are
  compatible with the initial solution `solution` and matches the additional
  where-clause pattern `pattern`."
  (fn [db solution pattern filters error-ch]
    (pattern-type pattern)))

(defn get-value
  [solution variable]
  (-> solution
      (get variable)
      ::val))

(defn assign-tuple
  [tuple solution filters]
  (mapv (fn [component]
          (if-let [variable (::var component)]
            (let [value     (get-value solution variable)
                  filter-fn (some->> (get filters variable)
                                     (and (nil? value))
                                     (map ::fn)
                                     (apply every-pred))]
              (cond-> component
                value     (assoc ::val value)
                filter-fn (assoc ::fn filter-fn)))
            component))
        tuple))

(defn unbound?
  [component]
  (and (::var component)
       (not (::val component))))

(defn bind-subject
  [s-pattern flake]
  (assoc s-pattern
         ::val      (flake/s flake)
         ::datatype const/$xsd:anyURI))

(defn bind-predicate
  [p-pattern flake]
  (assoc p-pattern
         ::val      (flake/p flake)
         ::datatype const/$xsd:anyURI))

(defn bind-object
  [o-pattern flake]
  (assoc o-pattern
         ::val      (flake/o flake)
         ::datatype (flake/dt flake)))

(defn bind-flake
  [solution pattern flake]
  (let [[s p o] pattern]
    (cond-> solution
      (unbound? s) (assoc (::var s) (bind-subject s flake))
      (unbound? p) (assoc (::var p) (bind-predicate p flake))
      (unbound? o) (assoc (::var o) (bind-object o flake)))))

(defmethod match-pattern :tuple
  [db solution pattern filters error-ch]
  (let [cur-vals (assign-tuple pattern solution filters)
        flake-ch (resolve-flake-range db error-ch cur-vals)
        match-ch (async/chan 2 (comp cat
                                     (map (fn [flake]
                                            (bind-flake solution pattern flake)))))]
    (async/pipe flake-ch match-ch)))

(defmethod match-pattern :iri
  [db solution pattern filters error-ch]
  (let [tuple    (val pattern)
        cur-vals (assign-tuple tuple solution filters)
        flake-ch (resolve-flake-range db error-ch cur-vals)
        match-ch (async/chan 2 (comp cat
                                     (map (fn [flake]
                                            (bind-flake solution tuple flake)))))]
    (async/pipe flake-ch match-ch)))

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
  [db solution pattern filters error-ch]
  (let [tuple    (val pattern)
        [s p o]  (assign-tuple tuple solution filters)
        cls      (::val o)
        classes  (into [cls] (dbproto/-class-prop db :subclasses cls))
        class-ch (async/to-chan! classes)
        match-ch (async/chan 2 (comp cat
                                     (with-distinct-subjects)
                                     (map (fn [flake]
                                            (bind-flake solution tuple flake)))))]
    (async/pipeline-async 2
                          match-ch
                          (fn [cls ch]
                            (-> (resolve-flake-range db error-ch [s p (assoc o ::val cls)])
                                (async/pipe ch)))
                          class-ch)
    match-ch))

(defn with-constraint
  "Return a channel of all solutions from `db` that extend from the solutions in
  `solution-ch` and also match the where-clause pattern `pattern`."
  [db pattern filters error-ch solution-ch]
  (let [out-ch (async/chan 2)]
    (async/pipeline-async 2
                          out-ch
                          (fn [solution ch]
                            (-> (match-pattern db solution pattern filters error-ch)
                                (async/pipe ch)))
                          solution-ch)
    out-ch))

(defn match-clause
  "Returns a channel that will eventually contain all match solutions in `db`
  extending from `solution` that also match the parsed where clause `clause`."
  [db solution clause error-ch]
  (let [initial-ch (async/to-chan! [solution])
        filters    (::filters clause)
        patterns   (::patterns clause)]
    (reduce (fn [solution-ch pattern]
              (with-constraint db pattern filters error-ch solution-ch))
            initial-ch patterns)))

(defmethod match-pattern :union
  [db solution pattern _ error-ch]
  (let [clauses   (val pattern)
        clause-ch (async/to-chan! clauses)
        out-ch    (async/chan 2)]
    (async/pipeline-async 2
                          out-ch
                          (fn [clause ch]
                            (-> (match-clause db solution clause error-ch)
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
  [db solution pattern _ error-ch]
  (let [clause (val pattern)
        opt-ch (async/chan 2 (with-default solution))]
    (-> (match-clause db solution clause error-ch)
        (async/pipe opt-ch))))

(def blank-solution {})

(defn where
  [db q error-ch]
  (let [where-clause     (:where q)
        initial-solution (or (:vars q)
                             blank-solution)]
    (match-clause db initial-solution where-clause error-ch)))

(defn split-solution-by
  [variables solution]
  (let [group-key   (mapv (fn [v]
                            (-> (get solution v)
                                (select-keys [::val ::datatype])))
                          variables)
        grouped-val (apply dissoc solution variables)]
    [group-key grouped-val]))

(defn assoc-coll
  [m k v]
  (update m k (fn [coll]
                (-> coll
                    (or [])
                    (conj v)))))

(defn group-solution
  [groups [group-key grouped-val]]
  (assoc-coll groups group-key grouped-val))

(defn merge-with-colls
  [m1 m2]
  (reduce (fn [merged k]
            (let [v (get m2 k)]
              (assoc-coll merged k v)))
          m1 (keys m2)))

(defn unwind-groups
  [grouping groups]
  (reduce-kv (fn [solutions group-key grouped-vals]
               (let [merged-vals (->> grouped-vals
                                      (reduce merge-with-colls {})
                                      (reduce-kv (fn [m k v]
                                                   (assoc m k {::var       k
                                                               ::val       v
                                                               ::datatype ::grouping}))
                                                 {}))
                     solution    (into merged-vals
                                       (map vector grouping group-key))]
                 (conj solutions solution)))
             [] groups))

(defn group
  [grouping solution-ch]
  (if grouping
    (-> (async/transduce (map (partial split-solution-by grouping))
                         (completing group-solution
                                     (partial unwind-groups grouping))
                         {}
                         solution-ch)
        (async/pipe (async/chan 2 cat)))
    solution-ch))

(defn compare-vals
  [x-val x-dt y-val y-dt]
  (let [dt-cmp (compare x-dt y-dt)]
    (if (zero? dt-cmp)
      (compare x-val y-val)
      dt-cmp)))

(defn compare-solutions-by
  [variable direction x y]
  (let [x-var (get x variable)
        x-val (::val x-var)
        x-dt  (::datatype x-var)

        y-var (get y variable)
        y-val (::val y-var)
        y-dt  (::datatype y-var)]
    (if (= direction :asc)
      (compare-vals x-val x-dt y-val y-dt)
      (compare-vals y-val y-dt x-val x-dt))))

(defn compare-solutions
  [ordering x y]
  (reduce (fn [comparison [variable direction]]
            (let [cmp (compare-solutions-by variable direction x y)]
              (if (zero? cmp)
                comparison
                (reduced cmp))))
          0 ordering))

(defn order
  [ordering solution-ch]
  (if ordering
    (let [comparator (partial compare-solutions ordering)
          coll-ch    (async/into [] solution-ch)
          ordered-ch (async/chan 2 (comp (map (partial sort comparator))
                                         cat))]
      (async/pipe coll-ch ordered-ch))
    solution-ch))

(defn offset
  [n solution-ch]
  (if n
    (async/pipe solution-ch
                (async/chan 2 (drop n)))
    solution-ch))

(defn limit
  [n solution-ch]
  (if n
    (async/take n solution-ch)
    solution-ch))

(defmulti display
  (fn [match db select-cache compact]
    (::datatype match)))

(defmethod display :default
  [match _ _ _]
  (go (::val match)))

(defmethod display const/$xsd:anyURI
  [match db select-cache compact]
  (go-try
   (let [v (::val match)]
     (if-let [cached (get @select-cache v)]
       cached
       (let [iri (<? (dbproto/-iri db (::val match) compact))]
         (vswap! select-cache assoc v iri)
         iri)))))

(defmethod display ::grouping
  [match db select-cache compact]
  (let [group (::val match)]
    (->> group
         (map (fn [grouped-val]
                (display grouped-val db select-cache compact)))
         (async/map vector))))

(defmulti format
  (fn [selector db select-cache compact solution]
    (if (map? selector)
      (::selector selector)
      :var)))

(defmethod format :var
  [variable db select-cache compact solution]
  (-> solution
      (get variable)
      (display db select-cache compact)))

(defn ->aggregate-selector
  [variable function]
  {::selector :aggregate
   ::variable variable
   ::function function})

(defmethod format :aggregate
  [{::keys [variable function]} db select-cache compact solution]
  (go-try
   (let [group (<? (format variable db select-cache compact solution))]
     (function group))))

(defn ->subgraph-selector
  [variable selection spec depth]
  {::selector  :subgraph
   ::variable  variable
   ::selection selection
   ::depth     depth
   ::spec      spec})

(defmethod format :subgraph
  [{::keys [variable selection depth spec]} db select-cache compact solution]
  (go-try
   (let [sid    (-> solution
                    (get variable)
                    ::val)
         flakes (<? (query-range/index-range db :spot = [sid]))]
     ;; TODO: Replace these nils with fuel values when we turn fuel back on
     (<? (json-ld-resp/flakes->res db select-cache compact nil nil spec 0 flakes)))))

(defn select-values
  [db select-cache compact solution selectors]
  (go-loop [selectors selectors
            values     []]
    (if-let [selector (first selectors)]
      (let [value (<? (format selector db select-cache compact solution))]
        (recur (rest selectors)
               (conj values value)))
      values)))

(defn select
  [db q solution-ch]
  (let [compact      (->> q :context json-ld/compact-fn)
        selectors    (or (:select q)
                         (:selectOne q))
        select-cache (volatile! {})
        select-ch    (async/chan)]
    (async/pipeline-async 1
                          select-ch
                          (fn [solution ch]
                            (-> (select-values db select-cache compact solution selectors)
                                (async/pipe ch)))
                          solution-ch)
    select-ch))

(defn collect-results
  [q result-ch]
  (if (:selectOne q)
    (async/take 1 result-ch)
    (async/into [] result-ch)))

(defn execute
  [db q]
  (let [error-ch (async/chan)]
    (->> (where db q error-ch)
         (group (:group-by q))
         (order (:order-by q))
         (offset (:offset q))
         (limit (:limit q))
         (select db q)
         (collect-results q))))
