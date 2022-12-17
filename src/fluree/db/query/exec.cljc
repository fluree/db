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

(defmulti constrain
  (fn [db result constraint error-ch]
    (if (map? constraint)
      (-> constraint keys first)
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
  [result constraint flake]
  (let [[s p o] constraint]
    (cond-> result
      (unbound? s) (assoc (::var s) (flake/s flake))
      (unbound? p) (assoc (::var p) (flake/p flake))
      (unbound? o) (assoc (::var o) (flake/o flake)))))

(defmethod constrain :tuple
  [db result constraint error-ch]
  (let [flake-ch      (->> (with-values constraint result)
                           (mapv ::val)
                           (resolve-flake-range db error-ch))
        constraint-ch (async/chan 4 (comp cat
                                          (map (fn [flake]
                                                 (bind-flake result constraint flake)))))]
    (async/pipe flake-ch constraint-ch)))

(defn with-constraint
  [db constraint error-ch result-ch]
  (let [out-ch (async/chan 4)]
    (async/pipeline-async 4
                          out-ch
                          (fn [result ch]
                            (async/pipe (constrain db result constraint error-ch)
                                        ch))
                          result-ch)
    out-ch))

(def empty-result {})

(defn where
  [db context error-ch constraints]
  (let [initial-ch (async/to-chan! [empty-result])]
    (reduce (fn [result-ch constraint]
              (with-constraint db constraint error-ch result-ch))
            initial-ch constraints)))

(defn select-values
  [result selectors]
  (reduce (fn [values selector]
            (conj values (get result selector)))
          [] selectors))

(defn select
  [selectors result-ch]
  (let [select-ch (async/chan 4 (map (fn [result]
                                       (select-values result selectors))))]
    (async/pipe result-ch select-ch)))

(defn query
  [db q]
  (let [error-ch (async/chan)
        context (:context q)]
    (->> (where db context error-ch (:where q))
         (select (:select q))
         (async/into []))))
