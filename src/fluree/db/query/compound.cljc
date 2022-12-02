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

(defn resolve-flake-range
  [{:keys [conn] :as db} idx t s p o flake-xf error-ch]
  (let [idx-root    (get db idx)
        novelty     (get-in db [:novelty idx])
        start-flake (flake/create s p o nil nil nil util/min-integer)
        end-flake   (flake/create s p o nil nil nil util/max-integer)
        obj-filter  (some-> o :filter filter/extract-combined-filter)
        opts        (cond-> {:idx         idx
                             :from-t      t
                             :to-t        t
                             :start-test  >=
                             :start-flake start-flake
                             :end-test    <=
                             :end-flake   end-flake}
                      obj-filter (assoc :object-fn obj-filter)
                      flake-xf   (assoc :flake-xf flake-xf))]
    (query-range/resolve-flake-slices conn idx-root novelty error-ch opts)))

(defn with-optional
  [res xf sid pid]
  (if (empty? res)
    (into [] xf [(flake/parts->Flake [sid pid])])
    res))

(defn parse-sid
  [sid]
  (if (vector? sid)
    (let [[sid-val datatype] sid]
      ;; in a mixed datatype response (e.g. some IRIs, some strings), need to
      ;; filter out any non-IRI
      (when (= datatype const/$xsd:anyURI)
        sid-val))
    sid))

(defn process-in-item
  [{:keys [conn] :as db} in-item in-n idx t passthrough-fn p o flake-x-form optional? error-ch out-ch]
  (async/go
    (let [{pid :value} p]
      (if-let [sid (some-> in-item (nth in-n) parse-sid)]
        (let [xfs (cond-> [flake-x-form]
                    passthrough-fn (conj (map (fn [result]
                                                (concat result (passthrough-fn in-item))))))
              xf  (apply comp xfs)]
          (async/pipe (->> (resolve-flake-range db idx t sid pid o xf error-ch)
                           (async/transduce cat
                                            (completing conj
                                                        (fn [res]
                                                          (cond-> res
                                                            optional? (with-optional xf sid pid))))
                                            []))
                      out-ch))
        (async/close! out-ch)))))

(defn next-chunk-s
  [{:keys [conn] :as db} error-ch next-in optional? {:keys [in-n] :as s} p o idx t flake-x-form passthrough-fn]
  (let [out-ch   (async/chan)]
    (async/pipeline-async 2
                          out-ch
                          (fn [in-item ch]
                            (process-in-item db in-item in-n idx t passthrough-fn p o
                                             flake-x-form optional? error-ch ch))
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

(defn with-distinct-subjects
  []
  (fn [xf]
    (let [seen (volatile! #{})]
      (fn
        ([]
         (xf))

        ([result flake-chunk]
         (let [seen-before @seen]
           (if-let [flakes (->> flake-chunk
                                (remove (fn [f]
                                          (contains? seen-before
                                                     (flake/s f))))
                                seq)]
             (do (vswap! seen into (map flake/s) flakes)
                 (xf result flakes))
             result)))

        ([result]
         (xf result))))))

(defmethod get-clause-res :class
  [{:keys [conn] :as db} prev-chan clause t vars fuel max-fuel error-ch]
  (let [{:keys [s p o idx flake-x-form]} clause
        {pid :value} p
        {s-var :variable} s
        {o-var :variable} o

        s* (or (:value s)
               (get vars s-var))
        o* (or (:value o)
               (get vars o-var))

        subclasses  (dbproto/-class-prop db :subclasses o*)
        all-classes (into [o*] subclasses)

        out-ch (async/chan 2 (with-distinct-subjects))]

    (->> (async/to-chan! all-classes)
         (async/pipeline-async 2
                               out-ch
                               (fn [class ch]
                                 (async/pipe (resolve-flake-range db idx t s* pid class
                                                                  nil error-ch)
                                             ch))))
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
                range-ch (resolve-flake-range db idx t s* pid o* flake-x-form error-ch)]
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
