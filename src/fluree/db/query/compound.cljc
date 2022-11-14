(ns fluree.db.query.compound
  (:require [fluree.db.query.range :as query-range]
            [fluree.db.util.async :refer [<? go-try merge-into?]]
            [clojure.core.async :as async :refer [<!]]
            [fluree.db.util.core :as util]
            [fluree.db.flake :as flake]
            [clojure.string :as str]
            [fluree.db.util.log :as log :include-macros true]
            #?(:cljs [cljs.reader])
            [fluree.db.dbproto :as db-proto]
            [fluree.db.constants :as const]))

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
  [{:keys [conn] :as db} error-ch next-in optional? {:keys [in-n] :as s} p idx t flake-x-form passthrough-fn out-ch]
  (let [idx-root (get db idx)
        novelty  (get-in db [:novelty idx])]
    (async/go
      (loop [[in-item & r] next-in]
        (if in-item
          (let [pass-vals (when passthrough-fn
                            (passthrough-fn in-item))
                {pid :value} p
                sid       (nth in-item in-n)
                sid*      (if (vector? sid)
                            (let [[sid-val datatype] sid]
                              ;; in a mixed datatype response (e.g. some IRIs, some strings), need to filter out any non-IRI
                              (when (= datatype const/$xsd:anyURI)
                                sid-val))
                            sid)]
            (when sid
              (let [xfs   (cond-> [flake-x-form]
                            pass-vals (conj (map #(concat % pass-vals))))
                    xf    (apply comp xfs)
                    opts  (-> (query-range-opts idx t sid* pid nil)
                              (assoc :flake-xf xf))
                    results (async/<! (->> (query-range/resolve-flake-slices conn idx-root novelty error-ch opts)
                                           (async/reduce into [])))]
                ;; pull all subject results off chan, push on out-ch
                (if (seq results)
                  (async/>! out-ch results)
                  (when optional?
                    (async/>! out-ch (sequence xf [(flake/parts->Flake [sid* pid])]))))))
            (recur r))
          (async/close! out-ch))))
    out-ch))

(defn refine-results
  [result-ch {:keys [t] :as db} error-ch clause]
  (let [{:keys [s p idx flake-x-form passthrough-fn optional?]} clause]
    (if (:in-n s)
      (let [refine-next-chunk (fn [next-in ch]
                              (next-chunk-s db error-ch next-in optional? s p idx t flake-x-form passthrough-fn ch))
            refined-ch (async/chan 2)]
        (async/pipeline-async 2 refined-ch refine-next-chunk result-ch)
        refined-ch)
      result-ch)))

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
  (let [out-ch (async/chan 2)]
    (async/go
      (let [{:keys [s p o idx flake-x-form]} clause
            {pid :value} p
            {s-var :variable, s-val :value} s
            {o-var :variable} o
            s*       (if s-val
                       (if (number? s-val)
                         s-val
                         (<? (db-proto/-subid db s-val)))
                       (get vars s-var))
            o*       (or (:value o)
                         (get vars o-var))
            opts     (-> (query-range-opts idx t s* pid o*)
                         (assoc :flake-xf flake-x-form))
            idx-root (get db idx)
            novelty  (get-in db [:novelty idx])
            range-ch (query-range/resolve-flake-slices conn idx-root novelty error-ch opts)]
        (if (and s-val (nil? s*))                           ;; this means the iri provided for 's' doesn't exist, close
          (async/close! out-ch)
          (async/pipe range-ch out-ch))))
    out-ch))

(defn resolve-where-clause
  [{:keys [t] :as db} {:keys [where vars] :as _parsed-query} error-ch fuel max-fuel]
  (let [initial-chan (get-clause-res db nil (first where) t vars fuel max-fuel error-ch)]
    (reduce (fn [result-ch clause]
              (refine-results result-ch db error-ch clause))
            initial-chan (rest where))))

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
  [db parsed-query fuel max-fuel error-ch]
  (let [{:keys [order-by group-by]} parsed-query
        where-results (->> (resolve-where-clause db parsed-query error-ch fuel max-fuel)
                           (async/reduce into []))
        out-ch        (cond-> where-results
                        order-by (order+group-results error-ch fuel max-fuel order-by group-by))]
    out-ch))
