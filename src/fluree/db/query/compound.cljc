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
            [fluree.db.query.analytical-parse :as parse]))

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
                sid   (nth in-item in-n)
                opts  (query-range-opts idx t sid p nil)
                in-ch (query-range/resolve-flake-slices conn idx-root novelty error-ch opts)]
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


(defmulti get-clause-res (fn [_ _ {:keys [type] :as _clause} _ _ _ _ _ _]
                           type))

(defmethod get-clause-res :tuple
  [{:keys [conn] :as db} prev-chan clause t vars fuel max-fuel error-ch opts]
  (let [out-ch      (async/chan 2)
        {:keys [s p o idx flake-x-form]} clause
        {s-var :variable} s
        {o-var :variable} o
        s*          (or (:value s)
                        (get vars s-var))
        o*          (or (:value o)
                        (get vars o-var))
        start-flake (flake/create s* p o* nil nil nil util/min-integer)
        end-flake   (flake/create s* p o* nil nil nil util/max-integer)
        opts        (assoc opts
                      :idx idx
                      :from-t t
                      :to-t t
                      :start-test >=
                      :start-flake start-flake
                      :end-test <=
                      :end-flake end-flake
                      :object-fn nil)
        idx-root    (get db idx)
        novelty     (get-in db [:novelty idx])
        range-ch    (query-range/resolve-flake-slices conn idx-root novelty error-ch opts)]
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
  [{:keys [t] :as db} {:keys [where vars] :as _q-map} error-ch fuel max-fuel opts]
  (let [initial-chan (get-clause-res db nil (first where) t vars fuel max-fuel error-ch opts)]
    (loop [[clause & r] (rest where)
           prev-chan initial-chan]
      ;; TODO - get 't' from query!
      (if clause
        (let [out-chan (get-chan db prev-chan error-ch clause t)]
          (recur r out-chan))
        prev-chan))))

(defn where
  [q-map error-ch fuel max-fuel db opts]
  (let [{:keys [ident-vars where optional filter]} q-map
        where-ch (resolve-where-clause db q-map error-ch fuel max-fuel opts)
        ;optional-res (if optional
        ;               (<? (optional->left-outer-joins db q-map optional where-res fuel max-fuel opts))
        ;               where-res)
        ;filter-res   (if filter
        ;               (tuples->filtered optional-res filter nil)
        ;               optional-res)

        ]
    where-ch))
