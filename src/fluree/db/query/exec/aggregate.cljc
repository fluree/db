(ns fluree.db.query.exec.aggregate
  "Streaming aggregate protocol and implementations for incremental group aggregation."
  (:refer-clojure :exclude [ratio?])
  (:require [fluree.db.constants :as const]
            [fluree.db.datatype :as datatype]
            [fluree.db.query.exec.where :as where])
  #?(:clj (:import (java.time LocalDateTime OffsetDateTime LocalDate OffsetTime LocalTime
                              ZoneOffset))))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol StreamingAggregator
  "Protocol for streaming/incremental aggregate computation.
  Implementations are stateless - state is managed externally."
  (initialize [this] "Return initial accumulator state")
  (step [this state tv] "Update state with new typed-value, return new state")
  (finalize [this state] "Convert final state to result typed-val"))

(defrecord CountAggregator []
  StreamingAggregator
  (initialize [_] 0)
  (step [_ state tv]
    (if (some-> tv :value some?)
      (inc state)
      state))
  (finalize [_ state]
    (where/->typed-val state)))

(defrecord CountStarAggregator []
  StreamingAggregator
  (initialize [_] 0)
  (step [_ state _tv] (inc state))
  (finalize [_ state] (where/->typed-val state)))

(defrecord CountDistinctAggregator []
  StreamingAggregator
  (initialize [_] (transient #{}))
  (step [_ state tv]
    (if (some-> tv :value some?)
      (conj! state tv)
      state))
  (finalize [_ state]
    (where/->typed-val (count (persistent! state)))))

(defrecord SumAggregator []
  StreamingAggregator
  (initialize [_] nil)
  (step [_ state tv]
    (let [v (:value tv)]
      (if (some? v)
        (if (nil? state) v (+ state v))
        state)))
  (finalize [_ state]
    (where/->typed-val (or state 0))))

#?(:clj
   (defn ratio?
     [x]
     (clojure.core/ratio? x))
   :cljs
   (defn ratio?
     [_]
     false))

(defrecord AvgAggregator []
  StreamingAggregator
  (initialize [_] {:sum nil :cnt 0})
  (step [_ {:keys [sum cnt]} tv]
    (let [v (:value tv)]
      (if (some? v)
        {:sum (if (nil? sum) v (+ sum v))
         :cnt (inc cnt)}
        {:sum sum :cnt cnt})))
  (finalize [_ {:keys [sum cnt]}]
    (let [raw (if (pos? cnt) (/ sum cnt) 0)
          res (if (ratio? raw) (double raw) raw)]
      (where/->typed-val res))))

;; Typed value comparison (mirrors fluree.db.query.exec.eval/compare*)

(def comparable-numeric-datatypes
  #{const/iri-xsd-decimal
    const/iri-xsd-double
    const/iri-xsd-integer
    const/iri-long
    const/iri-xsd-int
    const/iri-xsd-byte
    const/iri-xsd-short
    const/iri-xsd-float
    const/iri-xsd-unsignedLong
    const/iri-xsd-unsignedInt
    const/iri-xsd-unsignedShort
    const/iri-xsd-positiveInteger
    const/iri-xsd-nonPositiveInteger
    const/iri-xsd-negativeInteger
    const/iri-xsd-nonNegativeInteger})

(def comparable-string-datatypes
  #{const/iri-id
    const/iri-anyURI
    const/iri-string
    const/iri-xsd-normalizedString
    const/iri-lang-string
    const/iri-xsd-token})

(def comparable-time-datatypes
  #{const/iri-xsd-dateTime
    const/iri-xsd-date})

#?(:clj (defmulti ->offset-date-time
          #(when-let [t (#{OffsetDateTime LocalDateTime LocalDate} (type %))]
             t)))
#?(:clj (defmethod ->offset-date-time OffsetDateTime
          [^OffsetDateTime datetime]
          datetime))
#?(:clj (defmethod ->offset-date-time LocalDateTime
          [^LocalDateTime datetime]
          (.atOffset datetime ZoneOffset/UTC)))
#?(:clj (defmethod ->offset-date-time LocalDate
          [^LocalDate date]
          (.atOffset (.atStartOfDay date) ZoneOffset/UTC)))
#?(:clj (defmethod ->offset-date-time :default
          [x]
          (throw (ex-info "Cannot convert value to OffsetDateTime."
                          {:value  x
                           :status 400
                           :error  :db/invalid-fn-call}))))

#?(:clj (defmulti ->offset-time
          #(when-let [t (#{OffsetTime LocalTime} (type %))]
             t)))
#?(:clj (defmethod ->offset-time OffsetTime
          [^OffsetTime time]
          time))
#?(:clj (defmethod ->offset-time LocalTime
          [^LocalTime time]
          (.atOffset time ZoneOffset/UTC)))
#?(:clj (defmethod ->offset-time :default
          [x]
          (throw (ex-info "Cannot convert value to OffsetTime."
                          {:value  x
                           :status 400
                           :error  :db/invalid-fn-call}))))

(defn compare*
  [{val-a :value dt-a :datatype-iri}
   {val-b :value dt-b :datatype-iri}]
  (let [dt-a (or dt-a (datatype/infer-iri val-a))
        dt-b (or dt-b (datatype/infer-iri val-b))]
    (cond
      ;; can compare across types
      (or (and (contains? comparable-numeric-datatypes dt-a)
               (contains? comparable-numeric-datatypes dt-b))
          (and (contains? comparable-string-datatypes dt-a)
               (contains? comparable-string-datatypes dt-b)))
      (compare val-a val-b)

      ;; datetimes need to be converted to OffsetDateTimes for proper comparison
      (and (contains? comparable-time-datatypes dt-a)
           (contains? comparable-time-datatypes dt-b))
      #?(:clj (compare (->offset-date-time val-a) (->offset-date-time val-b))
         :cljs (compare val-a val-b))

      ;; same types compare
      (= dt-a dt-b)
      (compare val-a val-b)

      :else
      (throw (ex-info (str "Incomparable datatypes: " dt-a " and " dt-b)
                      {:a      val-a :a-dt dt-a
                       :b      val-b :b-dt dt-b
                       :status 400
                       :error  :db/invalid-query})))))

(defrecord MinAggregator []
  StreamingAggregator
  (initialize [_] nil)
  (step [_ state tv]
    (cond
      (nil? (some-> tv :value)) state
      (nil? state) tv
      (neg? (compare* tv state)) tv
      :else state))
  (finalize [_ state]
    (or state (where/->typed-val nil))))

(defrecord MaxAggregator []
  StreamingAggregator
  (initialize [_] nil)
  (step [_ state tv]
    (cond
      (nil? (some-> tv :value)) state
      (nil? state) tv
      (pos? (compare* tv state)) tv
      :else state))
  (finalize [_ state]
    (or state (where/->typed-val nil))))

(defn agg-step
  "Backwards-compatible wrapper for older call sites.
  Prefer `step`."
  [aggregator state tv]
  (step aggregator state tv))

(def streaming-aggregators
  "Map of aggregate op symbols to their aggregator instances.
  Instances are stateless and can be shared."
  {'count          (->CountAggregator)
   'count-star     (->CountStarAggregator)
   'count-distinct (->CountDistinctAggregator)
   'sum            (->SumAggregator)
   'avg            (->AvgAggregator)
   'min            (->MinAggregator)
   'max            (->MaxAggregator)})

(defn streaming-aggregator
  "Returns streaming aggregator for `op`, or nil."
  [op]
  (get streaming-aggregators op))
