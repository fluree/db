(ns fluree.db.query.compound
  (:require [fluree.db.query.range :as query-range]
            [clojure.core.async :as async]
            [fluree.db.util.async :refer [<? go-try merge-into?]]
            [fluree.db.util.core :as util]
            [fluree.db.flake :as flake]
            [fluree.db.query.analytical-filter :as filter]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const]))

#?(:clj (set! *warn-on-reflection* true))

(defn query-range-opts
  [idx t s p {:keys [filter] :as o}]
  (let [start-flake (flake/create s p o nil nil nil util/min-integer)
        end-flake   (flake/create s p o nil nil nil util/max-integer)]
    {:idx         idx
     :from-t      t
     :to-t        t
     :start-test  >=
     :start-flake start-flake
     :end-test    <=
     :end-flake   end-flake
     :object-fn (filter/extract-combined-filter filter)}))


(defn process-in-item
  [{:keys [conn] :as db} in-item in-n idx idx-root t novelty passthrough-fn p o flake-x-form optional? error-ch out-ch]
  (async/go
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
      (if sid
        (let [xfs   (cond-> [flake-x-form]
                      pass-vals (conj (map #(concat % pass-vals))))
              xf    (apply comp xfs)
              opts  (-> (query-range-opts idx t sid* pid o)
                        (assoc :flake-xf xf)) ]
          (async/pipe (->> (query-range/resolve-flake-slices conn idx-root novelty error-ch opts)
                           (async/transduce cat
                                            (completing conj
                                                        (fn [res]
                                                          (if (and (empty? res)
                                                                   optional?)
                                                            (into [] xf [(flake/parts->Flake [sid* pid])])
                                                            res)))
                                            []))
                      out-ch))
        (async/close! out-ch)))))

(defn next-chunk-s
  [{:keys [conn] :as db} error-ch next-in optional? {:keys [in-n] :as s} p o idx t flake-x-form passthrough-fn]
  (let [out-ch   (async/chan)
        idx-root (get db idx)
        novelty  (get-in db [:novelty idx])]
    (async/pipeline-async 2
                          out-ch
                          (fn [in-item ch]
                            (process-in-item db in-item in-n idx idx-root t novelty passthrough-fn p o flake-x-form
                                             optional? error-ch ch))
                          (async/to-chan! next-in))
    out-ch))

(defn where-clause-tuple-chunk
  "Processes a chunk of input to a tuple where clause, and pushes output to out-chan."
  [db next-in clause t error-ch]
  (let [{:keys [s p o idx flake-x-form passthrough-fn optional? nils-fn]} clause
        {s-var :variable, s-in-n :in-n} s
        {o-var :variable, o-in-n :in-n} o
        out-ch (if nils-fn
                 (async/chan 2 (map nils-fn))
                 (async/chan 2))]
    (if s-in-n
      (let [s-vals-ch (next-chunk-s db error-ch next-in optional? s p o idx t flake-x-form passthrough-fn)]
        (async/pipe s-vals-ch out-ch))
      (async/close! out-ch))
    out-ch))


(defn where-clause-chan
  "Takes next where clause and returns and output channel with chunked results."
  [db clause t prev-chan error-ch]
  (let [out-ch (async/chan 2)]
    (async/pipeline-async 2
                          out-ch
                          (fn [next-in ch]
                            (async/pipe (where-clause-tuple-chunk db next-in clause t
                                                                  error-ch)
                                        ch))
                          prev-chan)
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
        subclasses  (dbproto/-class-prop db :subclasses o*)
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
                         (<? (dbproto/-subid db s-val)))
                       (get vars s-var))]
        (if (and s-val (nil? s*)) ; this means the iri provided for 's' doesn't exist, close
          (async/close! out-ch)
          (let [{o-var :variable} o
                o*       (or (:value o)
                             (get vars o-var))
                opts     (-> (query-range-opts idx t s* pid o*)
                             (assoc :flake-xf flake-x-form))
                idx-root (get db idx)
                novelty  (get-in db [:novelty idx])
                range-ch (query-range/resolve-flake-slices conn idx-root novelty error-ch opts)]
            (async/pipe range-ch out-ch)))))
    out-ch))

(defn process-union
  [db prev-chan error-ch clause t]
  (let [out-ch (async/chan 2)
        [union1 union2] (:where clause)]
    (async/pipeline-async 2
                          out-ch
                          (fn [next-in ch]
                            (let [ch1 (where-clause-tuple-chunk db next-in (first union1) t error-ch)
                                  ch2 (where-clause-tuple-chunk db next-in (first union2) t error-ch)]
                              (-> (async/merge [ch1 ch2])
                                  (async/pipe ch))))
                          prev-chan)
    out-ch))


(defn where
  [{:keys [t] :as db} {:keys [where vars] :as _parsed-query} fuel max-fuel error-ch]
  (let [initial-chan (get-clause-res db nil (first where) t vars fuel max-fuel error-ch)]
    (loop [[clause & r] (rest where)
           prev-chan initial-chan]
      ;; TODO - get 't' from query!
      (if clause
        (let [out-chan (case (:type clause)
                         (:class :tuple :iri) (where-clause-chan db clause t  prev-chan error-ch)
                         :optional :TODO
                         :union (process-union db prev-chan error-ch clause t))]
          (recur r out-chan))
        prev-chan))))
