(ns fluree.db.query.exec
  (:require [clojure.spec.alpha :as spec]
            [clojure.string :as str]
            [fluree.json-ld :as json-ld]
            [fluree.db.query.range :as query-range]
            [clojure.core.async :as async :refer [<! >! go go-loop]]
            [fluree.db.flake :as flake]
            [fluree.db.util.async :refer [<? go-try merge-into?]]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.query.analytical-filter :as filter]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const]))

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
  [{:keys [conn t] :as db} error-ch [s p o]]
  (let [idx         (idx-for s p o)
        idx-root    (get db idx)
        novelty     (get-in db [:novelty idx])
        start-flake (flake/create s p o nil nil nil util/min-integer)
        end-flake   (flake/create s p o nil nil nil util/max-integer)
        #_#_obj-filter  (some-> o :filter filter/extract-combined-filter)
        opts        (cond-> {:idx         idx
                             :from-t      t
                             :to-t        t
                             :start-test  >=
                             :start-flake start-flake
                             :end-test    <=
                             :end-flake   end-flake}
                      #_#_obj-filter (assoc :object-fn obj-filter))]
    (query-range/resolve-flake-slices conn idx-root novelty error-ch opts)))

(defmulti match-pattern
  "Return a channel that will contain all solutions from flakes in `db` that are
  compatible with the initial solution `solution` and matches the additional
  where-clause pattern `pattern`."
  (fn [db solution pattern error-ch]
    (if (map-entry? pattern)
      (key pattern)
      :tuple)))

(defn get-value
  [solution variable]
  (-> solution
      (get variable)
      ::val))

(defn with-values
  [tuple solution]
  (mapv (fn [component]
          (if-let [variable (::var component)]
            (let [value (get-value solution variable)]
              (cond-> component
                value (assoc ::val value)))
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
  [db solution pattern error-ch]
  (let [flake-ch (->> (with-values pattern solution)
                      (map ::val)
                      (resolve-flake-range db error-ch))
        match-ch (async/chan 2 (comp cat
                                     (map (fn [flake]
                                            (bind-flake solution pattern flake)))))]
    (async/pipe flake-ch match-ch)))

(defmethod match-pattern :iri
  [db solution pattern error-ch]
  (let [flake-ch (->> (with-values pattern solution)
                      (map ::val)
                      (resolve-flake-range db error-ch))
        match-ch (async/chan 2 (comp cat
                                     (map (fn [flake]
                                            (bind-flake solution pattern flake)))))]
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
  [db solution pattern error-ch]
  (let [tuple    (val pattern)
        [s p o]  (map ::val (with-values tuple solution))
        classes  (into [o] (dbproto/-class-prop db :subclasses o))
        class-ch (async/to-chan! classes)
        match-ch (async/chan 2 (comp cat
                                     (with-distinct-subjects)
                                     (map (fn [flake]
                                            (bind-flake solution tuple flake)))))]
    (async/pipeline-async 2
                          match-ch
                          (fn [cls ch]
                            (-> (resolve-flake-range db error-ch [s p cls])
                                (async/pipe ch)))
                          class-ch)
    match-ch))

(defn with-constraint
  "Return a channel of all solutions from `db` that extend from the solutions in
  `solution-ch` and also match the where-clause pattern `pattern`."
  [db pattern error-ch solution-ch]
  (let [out-ch (async/chan 2)]
    (async/pipeline-async 2
                          out-ch
                          (fn [solution ch]
                            (-> (match-pattern db solution pattern error-ch)
                                (async/pipe ch)))
                          solution-ch)
    out-ch))

(defn match-clause
  "Returns a channel that will eventually contain all match solutions in `db`
  extending from `solution` that also match all the where-clause patterns in the
  collection `clause`."
  [db solution clause error-ch]
  (let [initial-ch (async/to-chan! [solution])]
    (reduce (fn [solution-ch pattern]
              (with-constraint db pattern error-ch solution-ch))
            initial-ch clause)))

(defmethod match-pattern :union
  [db solution pattern error-ch]
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
  [db solution pattern error-ch]
  (let [clause (val pattern)
        opt-ch (async/chan 2 (with-default solution))]
    (-> (match-clause db solution clause error-ch)
        (async/pipe opt-ch))))

(def blank-solution {})

(defn where
  [db context where-clause error-ch]
  (match-clause db blank-solution where-clause error-ch))

(defn split-solution-by
  [variables solution]
  (let [values    (mapv (partial get-value solution)
                        variables)]
    [values solution]))

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
               (let [merged-vals (reduce merge-with-colls {} grouped-vals)
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

(defmulti display
  (fn [v db compact]
    (::datatype v)))

(defmethod display :default
  [v _ _]
  (go (::val v)))

(defmethod display const/$xsd:anyURI
  [v db compact]
  (dbproto/-iri db (::val v) compact))

(defn select-values
  [db compact solution selectors]
  (go-loop [selectors selectors
            values     []]
    (if-let [selector (first selectors)]
      (let [value (<? (-> solution
                          (get selector)
                          (display db compact)))]
        (recur (rest selectors)
               (conj values value)))
      values)))

(defn select
  [db compact selectors solution-ch]
  (let [select-ch (async/chan)]
    (async/pipeline-async 2
                          select-ch
                          (fn [solution ch]
                            (-> (select-values db compact solution selectors)
                                (async/pipe ch)))
                          solution-ch)
    select-ch))

(defn execute
  [db q]
  (let [error-ch (async/chan)
        context  (:context q)
        compact  (json-ld/compact-fn context)]
    (->> (where db context (:where q) error-ch)
         (group (:group-by q))
         (select db compact (:select q))
         (async/into []))))
