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

(defmulti match-flakes
  (fn [db result pattern error-ch]
    (if (map-entry? pattern)
      (key pattern)
      :tuple)))

(defn with-values
  [tuple values]
  (mapv (fn [component]
          (if-let [variable (::var component)]
            (let [value (get values variable)]
              (cond-> component
                value (assoc ::val value)))
            component))
        tuple))

(defn unbound?
  [component]
  (and (::var component)
       (not (::val component))))

(defn bind-flake
  [result pattern flake]
  (let [[s p o] pattern]
    (cond-> result
      (unbound? s) (assoc (::var s) (flake/s flake))
      (unbound? p) (assoc (::var p) (flake/p flake))
      (unbound? o) (assoc (::var o) (flake/o flake)))))

(defmethod match-flakes :tuple
  [db result pattern error-ch]
  (let [flake-ch (->> (with-values pattern result)
                      (map ::val)
                      (resolve-flake-range db error-ch))
        match-ch (async/chan 2 (comp cat
                                     (map (fn [flake]
                                            (bind-flake result pattern flake)))))]
    (async/pipe flake-ch match-ch)))

(defn with-distinct-subjects
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

(defmethod match-flakes :class
  [db result pattern error-ch]
  (let [tuple    (val pattern)
        [s p o]  (map ::val (with-values tuple result))
        classes  (into [o] (dbproto/-class-prop db :subclasses o))
        class-ch (async/to-chan! classes)
        match-ch (async/chan 2 (comp cat
                                     (with-distinct-subjects)
                                     (map (fn [flake]
                                            (bind-flake result tuple flake)))))]
    (async/pipeline-async 2
                          match-ch
                          (fn [cls ch]
                            (-> (resolve-flake-range db error-ch [s p cls])
                                (async/pipe ch)))
                          class-ch)
    match-ch))

(defn with-constraint
  [db pattern error-ch result-ch]
  (let [out-ch (async/chan 2)]
    (async/pipeline-async 2
                          out-ch
                          (fn [result ch]
                            (-> (match-flakes db result pattern error-ch)
                                (async/pipe ch)))
                          result-ch)
    out-ch))

(def blank-result {})

(defn where
  [db context error-ch patterns]
  (let [initial-ch (async/to-chan! [blank-result])]
    (reduce (fn [result-ch pattern]
              (with-constraint db pattern error-ch result-ch))
            initial-ch patterns)))

(defn select-values
  [result selectors]
  (reduce (fn [values selector]
            (conj values (get result selector)))
          [] selectors))

(defn select
  [selectors result-ch]
  (async/transduce (map (fn [result]
                          (select-values result selectors)))
                   conj
                   []
                   result-ch))

(defn execute
  [db q]
  (let [error-ch (async/chan)
        context (:context q)]
    (->> (where db context error-ch (:where q))
         (select (:select q)))))
