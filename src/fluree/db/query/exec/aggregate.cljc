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
  Implementations encapsulate their own state."
  (step [this tv] "Update aggregator with new typed-value, return new aggregator")
  (complete [this] "Convert accumulated state to result typed-val"))

(defrecord CountAggregator [tally]
  StreamingAggregator
  (step [this tv]
    (if (some-> tv :value some?)
      (update this :tally inc)
      this))
  (complete [this]
    (where/->typed-val (:tally this))))

(defn count-aggregator
  []
  (->CountAggregator 0))

(defrecord CountStarAggregator [tally]
  StreamingAggregator
  (step [this _tv] (update this :tally inc))
  (complete [this] (where/->typed-val (:tally this))))

(defn count-star-aggregator
  []
  (->CountStarAggregator 0))

(defrecord CountDistinctAggregator [seen]
  StreamingAggregator
  (step [this tv]
    (if (some-> tv :value some?)
      (update this :seen conj tv)
      this))
  (complete [this]
    (where/->typed-val (count (:seen this)))))

(defn count-distinct-aggregator
  []
  (->CountDistinctAggregator #{}))

(defrecord SumAggregator [total]
  StreamingAggregator
  (step [this tv]
    (let [v (:value tv)]
      (if (some? v)
        (update this :total #(if (nil? %) v (+ % v)))
        this)))
  (complete [this]
    (where/->typed-val (or (:total this) 0))))

(defn sum-aggregator
  []
  (->SumAggregator nil))

#?(:clj
   (defn ratio?
     [x]
     (clojure.core/ratio? x))
   :cljs
   (defn ratio?
     [_]
     false))

(defrecord AvgAggregator [sum cnt]
  StreamingAggregator
  (step [this tv]
    (let [v (:value tv)]
      (if (some? v)
        (-> this
            (update :sum #(if (nil? %) v (+ % v)))
            (update :cnt inc))
        this)))
  (complete [this]
    (let [s   (:sum this)
          c   (:cnt this)
          raw (if (pos? c) (/ s c) 0)
          res (if (ratio? raw) (double raw) raw)]
      (where/->typed-val res))))

(defn avg-aggregator
  []
  (->AvgAggregator nil 0))

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

#?(:clj
   (defprotocol OffsetDateTimeConverter
     (->offset-date-time [this])))

#?(:clj
   (extend-protocol OffsetDateTimeConverter
     OffsetDateTime
     (->offset-date-time
       [^OffsetDateTime this]
       this)

     LocalDateTime
     (->offset-date-time
       [^LocalDateTime this]
       (.atOffset this ZoneOffset/UTC))

     LocalDate
     (->offset-date-time
       [^LocalDate this]
       (-> this .atStartOfDay (.atOffset ZoneOffset/UTC)))

     Object
     (->offset-date-time
       [this]
       (throw (ex-info "Cannot convert value to OffsetDateTime."
                       {:value  this
                        :status 400
                        :error  :db/invalid-fn-call})))))

#?(:clj
   (defprotocol OffsetTimeConverter
     (->offset-time [this])))

#?(:clj
   (extend-protocol OffsetTimeConverter
     OffsetTime
     (->offset-time
       [^OffsetTime this]
       this)

     LocalTime
     (->offset-time
       [^LocalTime this]
       (.atOffset this ZoneOffset/UTC))

     Object
     (->offset-time
       [this]
       (throw (ex-info "Cannot convert value to OffsetTime."
                       {:value  this
                        :status 400
                        :error  :db/invalid-fn-call})))))

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

(defrecord MinAggregator [current]
  StreamingAggregator
  (step [this tv]
    (let [cur (:current this)]
      (cond
        (nil? (some-> tv :value)) this
        (nil? cur) (assoc this :current tv)
        (neg? (compare* tv cur)) (assoc this :current tv)
        :else this)))
  (complete [this]
    (or (:current this) (where/->typed-val nil))))

(defn min-aggregator
  []
  (->MinAggregator nil))

(defrecord MaxAggregator [current]
  StreamingAggregator
  (step [this tv]
    (let [cur (:current this)]
      (cond
        (nil? (some-> tv :value)) this
        (nil? cur) (assoc this :current tv)
        (pos? (compare* tv cur)) (assoc this :current tv)
        :else this)))
  (complete [this]
    (or (:current this) (where/->typed-val nil))))

(defn max-aggregator
  []
  (->MaxAggregator nil))

(def streaming-aggregators
  "Map of aggregate op symbols to their aggregator constructor functions."
  {'count          count-aggregator
   'count-star     count-star-aggregator
   'count-distinct count-distinct-aggregator
   'sum            sum-aggregator
   'avg            avg-aggregator
   'min            min-aggregator
   'max            max-aggregator})

(defn streaming-aggregator
  "Returns streaming aggregator constructor for `op`, or nil.
  Call the returned function with no args to get a fresh aggregator instance."
  [op]
  (get streaming-aggregators op))
