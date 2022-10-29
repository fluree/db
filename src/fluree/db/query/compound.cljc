(ns fluree.db.query.compound
  (:require [clojure.set :as set]
            [fluree.db.query.range :as query-range]
            [clojure.core.async :as async]
            #?(:clj [fluree.db.full-text :as full-text])
            [fluree.db.time-travel :as time-travel]
            [fluree.db.util.async :refer [<? go-try merge-into?]]
            [fluree.db.util.core :as util]
            [fluree.db.flake :as flake]
            [fluree.db.query.analytical-wikidata :as wikidata]
            [fluree.db.query.analytical-filter :as filter]
            [fluree.db.query.union :as union]
            [clojure.string :as str]
            [fluree.db.util.log :as log :include-macros true]
            #?(:cljs [cljs.reader])
            [fluree.db.dbproto :as dbproto]
            [fluree.db.query.analytical-parse :as parse]
            [fluree.db.dbproto :as db-proto]))

#?(:clj (set! *warn-on-reflection* true))

(defn query-range-opts
  [idx t s p o]
  (let [start-flake (flake/create s p o nil nil nil util/min-integer)
        end-flake   (flake/create s p o nil nil nil util/max-integer)]
    {:idx         idx
     :from-t      t
     :to-t        t
     :start-test  >=
     :start-flake start-flake
     :end-test    <=
     :end-flake   end-flake
     :object-fn   nil}))


(defn next-chunk-s
  [{:keys [conn] :as db} error-ch next-in {:keys [in-n] :as s} p idx t flake-x-form passthrough-fn]
  (let [out-ch   (async/chan)
        idx-root (get db idx)
        novelty  (get-in db [:novelty idx])]
    (async/go
      (loop [[in-item & r] next-in]
        (if in-item
          (let [pass-vals (when passthrough-fn
                            (passthrough-fn in-item))
                {pid :value} p
                sid       (nth in-item in-n)
                opts      (query-range-opts idx t sid pid nil)
                in-ch     (query-range/resolve-flake-slices conn idx-root novelty error-ch opts)]
            ;; pull all subject results off chan, push on out-ch
            (loop []
              (when-let [next-chunk (async/<! in-ch)]
                (let [result (cond->> (sequence flake-x-form next-chunk)
                                      pass-vals (map #(concat % pass-vals)))]
                  (async/>! out-ch result)
                  (recur))))
            (recur r))
          (async/close! out-ch))))
    out-ch))


(defn get-chan
  [db prev-chan error-ch clause t]
  (let [out-ch (async/chan 2)
        {:keys [s p o idx flake-x-form passthrough-fn]} clause
        {s-var :variable, s-in-n :in-n} s
        {o-var :variable, o-in-n :in-n} o]
    (async/go
      (loop []
        (if-let [next-in (async/<! prev-chan)]
          (let []
            (if s-in-n
              (let [s-vals-chan (next-chunk-s db error-ch next-in s p idx t flake-x-form passthrough-fn)]
                (loop []
                  (when-let [next-s (async/<! s-vals-chan)]
                    (async/>! out-ch next-s)
                    (recur)))))
            (recur))
          (async/close! out-ch))))
    out-ch))


(defmulti get-clause-res (fn [_ _ {:keys [type] :as _clause} _ _ _ _ _]
                           type))

(defmethod get-clause-res :class
  [{:keys [conn] :as db} prev-chan clause t vars fuel max-fuel error-ch]
  (let [out-ch      (async/chan 2)
        {:keys [s p o idx flake-x-form]} clause
        {pid :value} p
        {s-var :variable} s
        {o-var :variable} o
        s*          (or (:value s)
                        (get vars s-var))
        o*          (or (:value o)
                        (get vars o-var))
        subclasses  (db-proto/-class-prop db :subclasses o*)
        all-classes (into [o*] subclasses)
        idx-root    (get db idx)
        novelty     (get-in db [:novelty idx])]
    (async/go
      (loop [[next-class & rest-classes] all-classes
             all-seen #{}]
        (if next-class
          (let [class-opts (query-range-opts idx t s* pid next-class)
                class-chan (query-range/resolve-flake-slices conn idx-root novelty error-ch class-opts)
                ;; exhaust class, return all seen sids for the class
                class-seen (loop [class-seen []]
                             (let [next-res (async/<! class-chan)]
                               (if next-res
                                 (let [next-res* (remove #(all-seen (flake/s %)) next-res)
                                       next-out  (sequence flake-x-form next-res*)]
                                   (when (seq next-out)
                                     (async/>! out-ch next-out))
                                   (recur (conj class-seen (mapv flake/s next-res*))))
                                 class-seen)))]
            ;; integrate class-seen into all-seen
            ;; class-seen will be a vector of lists of subject ids [(123 456) (45) (7 9 34) ...]
            (recur rest-classes (reduce #(into %1 %2) all-seen class-seen)))
          (async/close! out-ch))))
    out-ch))

(defmethod get-clause-res :tuple
  [{:keys [conn] :as db} prev-chan clause t vars fuel max-fuel error-ch]
  (let [out-ch   (async/chan 2)
        {:keys [s p o idx flake-x-form]} clause
        {pid :value} p
        {s-var :variable} s
        {o-var :variable} o
        s*       (or (:value s)
                     (get vars s-var))
        o*       (or (:value o)
                     (get vars o-var))
        opts     (query-range-opts idx t s* pid o*)
        idx-root (get db idx)
        novelty  (get-in db [:novelty idx])
        range-ch (query-range/resolve-flake-slices conn idx-root novelty error-ch opts)]
    (async/go
      (loop []
        (let [next-res (async/<! range-ch)]
          (if next-res
            (let [next-out (sequence flake-x-form next-res)]
              (async/>! out-ch next-out)
              (recur))
            (async/close! out-ch)))))
    out-ch))

(defn resolve-where-clause
  [{:keys [t] :as db} {:keys [where vars] :as _parsed-query} error-ch fuel max-fuel]
  (let [initial-chan (get-clause-res db nil (first where) t vars fuel max-fuel error-ch)]
    (loop [[clause & r] (rest where)
           prev-chan initial-chan]
      ;; TODO - get 't' from query!
      (if clause
        (let [out-chan (get-chan db prev-chan error-ch clause t)]
          (recur r out-chan))
        prev-chan))))

(defn order+group-results
  "Ordering must first consume all results and then sort."
  [results-ch error-ch fuel max-fuel {:keys [comparator] :as _order-by} {:keys [grouping-fn] :as _group-by}]
  (async/go
    (let [results (loop [results []]
                    (if-let [next-res (async/<! results-ch)]
                      (recur (into results next-res))
                      results))]
      (cond-> (sort comparator results)
              grouping-fn grouping-fn))))


(defn where
  [parsed-query error-ch fuel max-fuel db]
  (let [{:keys [order-by group-by]} parsed-query
        where-results (resolve-where-clause db parsed-query error-ch fuel max-fuel)
        out-ch        (cond-> where-results
                              order-by (order+group-results error-ch fuel max-fuel order-by group-by))]
    out-ch))
