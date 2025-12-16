(ns fluree.db.query.exec.eval
  (:refer-clojure :exclude [compile rand concat replace max min
                            #?(:clj ratio?) #?@(:cljs [uuid -count divide])])
  (:require #?@(:clj [[sci.core :as sci]])
            [clojure.math :as math]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :as walk :refer [postwalk]]
            [fluree.crypto :as crypto]
            [fluree.db.constants :as const]
            [fluree.db.datatype :as datatype]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.exec.group :as group]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util :as util]
            [fluree.db.util.graalvm :as graalvm]
            [fluree.db.util.log :as log]
            [fluree.db.vector.scoring :as score]
            [fluree.json-ld :as json-ld])
  #?(:clj (:import (java.time LocalDateTime OffsetDateTime LocalDate OffsetTime LocalTime
                              ZoneId ZoneOffset))))

#?(:clj (set! *warn-on-reflection* true))

#?(:clj
   (defn ratio?
     [x]
     (clojure.core/ratio? x))

   :cljs  ; ClojureScript doesn't support ratios)
   (defn ratio?
     [_]
     false))

(defn sum
  [coll]
  (where/->typed-val (reduce + (mapv :value coll))))

(defn avg
  [coll]
  (let [coll (mapv :value coll)
        res (/ (reduce + coll)
               (count coll))]
    (where/->typed-val
     (if (ratio? res)
       (double res)
       res))))

(defn median
  [coll]
  (let [terms (sort (mapv :value coll))
        size  (count coll)
        med   (bit-shift-right size 1)
        res   (cond-> (nth terms med)
                (even? size)
                (-> (+ (nth terms (dec med)))
                    (/ 2)))]
    (where/->typed-val
     (if (ratio? res)
       (double res)
       res))))

(defn variance
  [coll]
  (let [mean (avg coll)
        sum  (sum (for [x coll
                        :let [delta (- (:value x) (:value mean))]]
                    (* delta delta)))
        res  (/ (:value sum) (count coll))]
    (where/->typed-val
     (if (ratio? res)
       (double res)
       res))))

(defn stddev
  [coll]
  (where/->typed-val
   (Math/sqrt (:value (variance coll)))))

(defn count-distinct
  [coll]
  (where/->typed-val
   (count (distinct coll))))

(defn -count
  [coll]
  (where/->typed-val (count (keep :value coll))))

(defn count-star
  [coll]
  (where/->typed-val (count coll)))

;; Streaming aggregate descriptors (incremental group aggregation)

(declare compare*)

(def streaming-aggregate-registry
  "Streaming aggregate registry: op -> {:init :step :final}."
  {'count        {:init  (fn [] 0)
                  :step  (fn [state tv]
                           (if (some-> tv :value some?)
                             (inc state)
                             state))
                  :final (fn [state]
                           (where/->typed-val state))}

   'count-star   {:init  (fn [] 0)
                  :step  (fn [state _tv]
                           (inc state))
                  :final (fn [state]
                           (where/->typed-val state))}

   'count-distinct {:init  (fn [] (transient #{}))
                    :step  (fn [state tv]
                             (if (some-> tv :value some?)
                               (conj! state tv)
                               state))
                    :final (fn [state]
                             (where/->typed-val
                              (count (persistent! state))))}

   'sum          {:init  (fn [] nil)
                  :step  (fn [state tv]
                           (let [v (:value tv)]
                             (if (some? v)
                               (if (nil? state)
                                 v
                                 (+ state v))
                               state)))
                  :final (fn [state]
                           (where/->typed-val (or state 0)))}

   'avg          {:init  (fn [] {:sum nil :cnt 0})
                  :step  (fn [{:keys [sum cnt]} tv]
                           (let [v (:value tv)]
                             (if (some? v)
                               {:sum (if (nil? sum) v (+ sum v))
                                :cnt (inc cnt)}
                               {:sum sum :cnt cnt})))
                  :final (fn [{:keys [sum cnt]}]
                           (let [raw (if (pos? cnt)
                                       (/ sum cnt)
                                       0)
                                 res (if (ratio? raw)
                                       (double raw)
                                       raw)]
                             (where/->typed-val res)))}

   'min          {:init  (fn [] nil)
                  :step  (fn [state tv]
                           (cond
                             (nil? (some-> tv :value)) state
                             (nil? state) tv
                             (neg? (compare* tv state)) tv
                             :else state))
                  :final (fn [state]
                           (or state (where/->typed-val nil)))}

   'max          {:init  (fn [] nil)
                  :step  (fn [state tv]
                           (cond
                             (nil? (some-> tv :value)) state
                             (nil? state) tv
                             (pos? (compare* tv state)) tv
                             :else state))
                  :final (fn [state]
                           (or state (where/->typed-val nil)))}})

(defn streaming-agg-descriptor
  "Returns streaming aggregate descriptor for `op`, or nil."
  [op]
  (get streaming-aggregate-registry op))

(defn groupconcat
  "GroupConcat is a set function which performs a string concatenation across the values
  of an expression with a group. The order of the strings is not specified. The
  separator character used in the concatenation may be given with the scalar argument
  SEPARATOR.

  If the separator scalar argument is absent from GROUP_CONCAT then it is taken to be
  the space character, unicode codepoint U+0020."
  ([coll]
   (groupconcat coll (where/->typed-val " ")))
  ([coll separator]
   (where/->typed-val (str/join (:value separator) (mapv :value coll)))))

(defn sample
  [{n :value} coll]
  (->> coll
       shuffle
       (take n)
       vec))

(defn sample1
  [coll]
  (->> coll (sample (where/->typed-val 1)) first))

(defmacro coalesce
  "Evaluates args in order. The result of the first arg not to return error gets returned."
  ([] (throw (ex-info "COALESCE evaluation failed on all forms." {:status 400 :error :db/invalid-query})))
  ([arg] `(let [res# (try ~arg (catch Exception e# nil))]
            (if (nil? (:value res#))
              (throw (ex-info "Coalesce evaluation failed on all forms." {:status 400 :error :db/invalid-query}))
              res#)))
  ([arg & args]
   `(let [res# (try ~arg (catch Exception e# nil))]
      (if (nil? (:value res#))
        (coalesce ~@args)
        res#))))

(defn ceil
  [{n :value}]
  (where/->typed-val (cond (= n (int n)) n
                           (> n 0) (-> n int inc)
                           (< n 0) (-> n int))))

(defn floor
  [{n :value}]
  (where/->typed-val (cond (= n (int n)) n
                           (> n 0) (-> n int)
                           (< n 0) (-> n int dec))))

(defn bound
  [{x :value}]
  (where/->typed-val (some? x)))

(defn -not
  [{x :value}]
  (where/->typed-val (not x)))

(defmacro -and
  "Equivalent to and"
  ([] (where/->typed-val true))
  ([x] x)
  ([x & next]
   `(let [and# ~x]
      (if (:value and#) (-and ~@next) ~x))))

(defmacro -or
  "Equivalent to or"
  ([] (where/->typed-val nil))
  ([x] x)
  ([x & next]
   `(let [or# ~x]
      (if (:value or#) ~x (-or ~@next)))))

(defn strStarts
  [{s :value} {substr :value}]
  (where/->typed-val (str/starts-with? s substr)))

(defn strEnds
  [{s :value} {substr :value}]
  (where/->typed-val (str/ends-with? s substr)))

(defn subStr
  ;; The index of the first character in a string is 1.
  ([{s :value} {start :value}]
   (where/->typed-val (subs s (dec start))))
  ([{s :value} {start :value} {length :value}]
   (let [start (dec start)]
     (where/->typed-val (subs s start (clojure.core/min (+ start length) (count s)))))))

(defn strLen
  [{s :value}]
  (where/->typed-val (count s)))

(defn ucase
  [{s :value}]
  (where/->typed-val (str/upper-case s)))

(defn lcase
  [{s :value}]
  (where/->typed-val (str/lower-case s)))

(defn contains
  [{s :value} {substr :value}]
  (where/->typed-val (str/includes? s substr)))

(defn strBefore
  [{s :value} {substr :value}]
  (let [[before :as split] (str/split s (re-pattern substr))]
    (where/->typed-val
     (if (> (count split) 1)
       before
       ""))))

(defn strAfter
  [{s :value} {substr :value}]
  (let [split (str/split s (re-pattern substr))]
    (where/->typed-val
     (if (> (count split) 1)
       (last split)
       ""))))

(defn concat
  [& strs]
  (where/->typed-val (apply str (mapv :value strs))))

(defn lang
  [tv]
  (where/->typed-val (or (:lang tv) "") const/iri-string))

(defn str-lang
  [{lexical-form :value} {langtag :value}]
  (where/->typed-val (str lexical-form) const/iri-lang-string langtag))

(defn datatype
  [tv]
  (where/->typed-val (:datatype-iri tv) const/iri-id))

(defn str-dt
  [{lexical-form :value} {datatype-iri :value}]
  (where/->typed-val lexical-form datatype-iri))

(def context-var
  (symbol "$-CONTEXT"))

(defmacro iri
  [tv]
  `(where/->typed-val (json-ld/expand-iri (:value ~tv) ~context-var) const/iri-id))

(defn is-iri
  [tv]
  (where/->typed-val (= (:datatype-iri tv) const/iri-id)))

(defn is-literal
  [tv]
  (where/->typed-val (not= (:datatype-iri tv) const/iri-id)))

(defn bnode
  []
  (where/->typed-val (iri/new-blank-node-id) const/iri-id))

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

(defn typed-equal
  ([_]
   (where/->typed-val true))
  ([x y]
   (where/->typed-val (zero? (compare* x y))))
  ([x y & more]
   (reduce (fn [result [a b]]
             (if (:value result)
               (where/->typed-val (zero? (compare* a b)))
               (reduced result)))
           (where/->typed-val (zero? (compare* x y)))
           (partition 2 1 (into [y] more)))))

(defn typed-not-equal
  ([_]
   (where/->typed-val false))
  ([x y] (where/->typed-val (not (zero? (compare* x y)))))
  ([x y & more]
   (reduce (fn [result [a b]]
             (if (:value result)
               (where/->typed-val (not (zero? (compare* a b))))
               (reduced result)))
           (where/->typed-val (not (zero? (compare* x y))))
           (partition 2 1 (into [y] more)))))

(defn less-than
  [a b]
  (where/->typed-val (neg? (compare* a b))))

(defn less-than-or-equal
  [a b]
  (where/->typed-val
   (or (= a b)
       (neg? (compare* a b)))))

(defn greater-than
  [a b]
  (where/->typed-val (pos? (compare* a b))))

(defn greater-than-or-equal
  [a b]
  (where/->typed-val
   (or (= a b)
       (pos? (compare* a b)))))

(defn max
  [coll]
  (let [compare-fn (fn [a b]
                     (if (pos? (compare* a b))
                       a
                       b))]
    (reduce compare-fn coll)))

(defn min
  [coll]
  (let [compare-fn (fn [a b]
                     (if (neg? (compare* a b))
                       a
                       b))]
    (reduce compare-fn coll)))

(defn regex
  [{text :value} {pattern :value}]
  (where/->typed-val (boolean (re-find (re-pattern pattern) text))))

(defn replace
  [{s :value} {pattern :value} {replacement :value}]
  (where/->typed-val (str/replace s (re-pattern pattern) replacement)))

(defn rand
  []
  (where/->typed-val (clojure.core/rand)))

(defn now
  []
  (where/->typed-val
   #?(:clj (OffsetDateTime/now (ZoneId/of "UTC"))
      :cljs (js/Date.))
   const/iri-xsd-dateTime))

;; TODO - date functions below all look incorrect for CLJS - should (string? datetime) be (map? datetime)?
(defn year
  [x]
  (where/->typed-val
   #?(:clj  (.getYear ^OffsetDateTime (->offset-date-time (:value x)))
      :cljs (.getFullYear (if (string? (:value x))
                            (datatype/coerce (:value x) (:datatype-iri x))
                            x)))))

(defn month
  [x]
  (where/->typed-val
   #?(:clj  (.getMonthValue ^OffsetDateTime (->offset-date-time (:value x)))
      :cljs (.getMonth (if (string? (:value x))
                         (datatype/coerce (:value x) (:datatype-iri x))
                         x)))))

(defn day
  [x]
  (where/->typed-val
   #?(:clj  (.getDayOfMonth ^OffsetDateTime (->offset-date-time (:value x)))
      :cljs (.getDate (if (string? (:value x))
                        (datatype/coerce (:value x) (:datatype-iri x))
                        x)))))

(defn hours
  [x]
  (where/->typed-val
   #?(:clj
      (condp contains? (:datatype-iri x)
        #{const/iri-xsd-dateTime const/iri-xsd-date}
        (.getHour ^OffsetDateTime (->offset-date-time (:value x)))
        #{const/iri-xsd-time}
        (.getHour ^OffsetTime (->offset-time (:value x))))
      :cljs
      (.getHours (if (string? (:value x))
                   (datatype/coerce (:value x) (:datatype-iri x))
                   x)))))

(defn minutes
  [x]
  (where/->typed-val
   #?(:clj  (condp contains? (:datatype-iri x)
              #{const/iri-xsd-dateTime const/iri-xsd-date}
              (.getMinute ^OffsetDateTime (->offset-date-time (:value x)))
              #{const/iri-xsd-time}
              (.getMinute ^OffsetTime (->offset-time (:value x))))
      :cljs (.getMinutes (if (string? (:value x))
                           (datatype/coerce (:value x) (:datatype-iri x))
                           x)))))

(defn seconds
  [x]
  (where/->typed-val
   #?(:clj (condp contains? (:datatype-iri x)
             #{const/iri-xsd-dateTime const/iri-xsd-date}
             (.getSecond ^OffsetDateTime (->offset-date-time (:value x)))
             #{const/iri-xsd-time}
             (.getSecond ^OffsetTime (->offset-time (:value x))))
      :cljs (.getSeconds (if (string? (:value x))
                           (datatype/coerce (:value x) (:datatype-iri x))
                           x)))))

;; TODO - CLJS datetime is not a variable present in the function
(defn tz
  [x]
  (where/->typed-val
   #?(:clj (condp contains? (:datatype-iri x)
             #{const/iri-xsd-dateTime const/iri-xsd-date}
             (.toString (.getOffset ^OffsetDateTime (->offset-date-time (:value x))))
             #{const/iri-xsd-time}
             (.toString (.getOffset ^OffsetTime (->offset-time (:value x)))))
      :cljs (.getTimeZoneOffset ^js/Date (if (string? (:value x))
                                           (datatype/coerce (:value x) (:datatype-iri x))
                                           x)))))

(defn sha256
  [{x :value}]
  (where/->typed-val (crypto/sha2-256 x)))

(defn sha512
  [{x :value}]
  (where/->typed-val (crypto/sha2-512 x)))

(defn uuid
  []
  (where/->typed-val (str "urn:uuid:" (random-uuid)) const/iri-id))

(defn struuid
  []
  (where/->typed-val (str (random-uuid))))

(defn isNumeric
  [{x :value}]
  (where/->typed-val (number? x)))

(defn isBlank
  [{x :value}]
  (where/->typed-val
   (and (string? x)
        (str/starts-with? x "_:"))))

(defn sparql-str
  [{x :value}]
  (where/->typed-val (str x)))

(defn in
  [{term :value} expressions]
  (where/->typed-val (contains? (set (mapv :value expressions)) term)))

(defn as*
  [val var]
  (log/trace "as binding value:" val "to variable:" var)
  (if (where/variable? var)
    val ; only needs to return the value b/c we store the binding variable in the AsSelector
    (throw
     (ex-info
      (str "second arg to `as` must be a query variable (e.g. ?foo); provided:"
           (pr-str var))
      {:status 400, :error :db/invalid-query}))))

(defmacro as
  [val var]
  `(as* ~val '~var))

(defn plus
  ([] (where/->typed-val 0))
  ([{x :value}]  (where/->typed-val x))
  ([{x :value} {y :value}] (where/->typed-val (+ x y)))
  ([{x :value} {y :value} & more]
   (where/->typed-val (reduce + (+ x y) (mapv :value more)))))

(defn minus
  ([{x :value}]  (where/->typed-val (- x)))
  ([{x :value} {y :value}] (where/->typed-val (- x y)))
  ([{x :value} {y :value} & more]
   (where/->typed-val (reduce - (- x y) (mapv :value more)))))

(defn multiply
  ([] (where/->typed-val 1))
  ([{x :value}]  (where/->typed-val x))
  ([{x :value} {y :value}] (where/->typed-val (* x y)))
  ([{x :value} {y :value} & more]
   (where/->typed-val (reduce * (* x y) (mapv :value more)))))

(defn divide
  ([{x :value}]  (where/->typed-val (/ 1 x)))
  ([{x :value} {y :value}] (where/->typed-val (/ x y)))
  ([{x :value} {y :value} & more]
   (where/->typed-val (reduce / (/ x y) (mapv :value more)))))

(defn quotient
  [{num :value} {div :value}]
  (where/->typed-val (quot num div)))

(defn power
  [{base :value} {power :value}]
  (where/->typed-val (math/pow base power)))

(defn untyped-equal
  ([_]  (where/->typed-val true))
  ([{x :value} {y :value}] (where/->typed-val (= x y)))
  ([{x :value} {y :value} & more]
   (where/->typed-val (apply = x y (mapv :value more)))))

(defn untyped-not-equal
  ([_]  (where/->typed-val false))
  ([{x :value} {y :value}] (where/->typed-val (not= x y)))
  ([{x :value} {y :value} & more]
   (where/->typed-val (apply not= x y (mapv :value more)))))

(defn absolute-value
  [{x :value}]
  (where/->typed-val (abs x)))

(defn round
  [{a :value}]
  (where/->typed-val (math/round a)))

(defmacro -if
  [test then else]
  `(if (:value ~test)
     ~then
     ~else))

(defn -nil?
  [{x :value}]
  (where/->typed-val (nil? x)))

(defn dotProduct
  [{v1 :value} {v2 :value}]
  (where/->typed-val
   (score/dot-product v1 v2)))

(defn cosineSimilarity
  [{v1 :value} {v2 :value}]
  (where/->typed-val
   (score/cosine-similarity v1 v2)))

(defn euclidianDistance
  [{v1 :value} {v2 :value}]
  (where/->typed-val
   (score/euclidian-distance v1 v2)))

(def qualified-symbols
  '{!              fluree.db.query.exec.eval/-not
    ||             fluree.db.query.exec.eval/-or
    &&             fluree.db.query.exec.eval/-and
    +              fluree.db.query.exec.eval/plus
    -              fluree.db.query.exec.eval/minus
    *              fluree.db.query.exec.eval/multiply
    /              fluree.db.query.exec.eval/divide
    =              fluree.db.query.exec.eval/untyped-equal
    <              fluree.db.query.exec.eval/less-than
    <=             fluree.db.query.exec.eval/less-than-or-equal
    >              fluree.db.query.exec.eval/greater-than
    >=             fluree.db.query.exec.eval/greater-than-or-equal
    abs            fluree.db.query.exec.eval/absolute-value
    as             fluree.db.query.exec.eval/as
    and            fluree.db.query.exec.eval/-and
    avg            fluree.db.query.exec.eval/avg
    bnode          fluree.db.query.exec.eval/bnode
    bound          fluree.db.query.exec.eval/bound
    ceil           fluree.db.query.exec.eval/ceil
    coalesce       fluree.db.query.exec.eval/coalesce
    concat         fluree.db.query.exec.eval/concat
    contains       fluree.db.query.exec.eval/contains
    count-distinct fluree.db.query.exec.eval/count-distinct
    count          fluree.db.query.exec.eval/-count
    count-star     fluree.db.query.exec.eval/count-star
    datatype       fluree.db.query.exec.eval/datatype
    equal          fluree.db.query.exec.eval/typed-equal
    floor          fluree.db.query.exec.eval/floor
    groupconcat    fluree.db.query.exec.eval/groupconcat
    if             fluree.db.query.exec.eval/-if
    in             fluree.db.query.exec.eval/in
    iri            fluree.db.query.exec.eval/iri
    is-iri         fluree.db.query.exec.eval/is-iri
    is-literal     fluree.db.query.exec.eval/is-literal
    lang           fluree.db.query.exec.eval/lang
    lcase          fluree.db.query.exec.eval/lcase
    median         fluree.db.query.exec.eval/median
    nil?           fluree.db.query.exec.eval/-nil?
    not            fluree.db.query.exec.eval/-not
    not=           fluree.db.query.exec.eval/untyped-not-equal
    not-equal      fluree.db.query.exec.eval/typed-not-equal
    now            fluree.db.query.exec.eval/now
    or             fluree.db.query.exec.eval/-or
    power          fluree.db.query.exec.eval/power
    quot           fluree.db.query.exec.eval/quotient
    rand           fluree.db.query.exec.eval/rand
    regex          fluree.db.query.exec.eval/regex
    replace        fluree.db.query.exec.eval/replace
    round          fluree.db.query.exec.eval/round
    sample         fluree.db.query.exec.eval/sample
    sample1        fluree.db.query.exec.eval/sample1
    stddev         fluree.db.query.exec.eval/stddev
    strAfter       fluree.db.query.exec.eval/strAfter
    strBefore      fluree.db.query.exec.eval/strBefore
    strEnds        fluree.db.query.exec.eval/strEnds
    strLen         fluree.db.query.exec.eval/strLen
    strStarts      fluree.db.query.exec.eval/strStarts
    subStr         fluree.db.query.exec.eval/subStr
    sum            fluree.db.query.exec.eval/sum
    ucase          fluree.db.query.exec.eval/ucase
    variance       fluree.db.query.exec.eval/variance
    year           fluree.db.query.exec.eval/year
    month          fluree.db.query.exec.eval/month
    day            fluree.db.query.exec.eval/day
    hours          fluree.db.query.exec.eval/hours
    minutes        fluree.db.query.exec.eval/minutes
    seconds        fluree.db.query.exec.eval/seconds
    tz             fluree.db.query.exec.eval/tz
    sha256         fluree.db.query.exec.eval/sha256
    sha512         fluree.db.query.exec.eval/sha512
    uuid           fluree.db.query.exec.eval/uuid
    struuid        fluree.db.query.exec.eval/struuid
    str-dt         fluree.db.query.exec.eval/str-dt
    str-lang       fluree.db.query.exec.eval/str-lang
    isNumeric      fluree.db.query.exec.eval/isNumeric
    isBlank        fluree.db.query.exec.eval/isBlank
    str            fluree.db.query.exec.eval/sparql-str
    max            fluree.db.query.exec.eval/max
    min            fluree.db.query.exec.eval/min

    dotProduct         fluree.db.query.exec.eval/dotProduct
    cosineSimilarity  fluree.db.query.exec.eval/cosineSimilarity
    euclidianDistance fluree.db.query.exec.eval/euclidianDistance})

;;; =============================================================================
;;; SCI (Small Clojure Interpreter) Support for GraalVM
;;; =============================================================================
;;; This section contains all SCI-related code used for evaluating Clojure forms
;;; in GraalVM native images where regular eval is not available.

;; Forward declaration for functions referenced in SCI context setup
(declare find-grouped-val)

;; SCI context for GraalVM-compatible code evaluation
#?(:clj
   (defn create-sci-context []
     (let [;; Separate macros from functions
           macro-symbols #{'coalesce 'as '-and '-or 'iri '-if 'if}

           ;; Define macro replacements once
           -if-fn (fn [test then else] (if (:value test) then else))
           as-fn (fn [expr _alias] expr)
           -and-fn (fn [& args]
                     (reduce (fn [result x]
                               (if (:value result) x result))
                             (where/->typed-val true)
                             args))
           -or-fn (fn [& args]
                    (reduce (fn [result x]
                              (if (:value result) result x))
                            (where/->typed-val nil)
                            args))

           ;; iri function for SCI
           ;; This is the base two-parameter function used after transformation
           iri-fn-base (fn [{value :value} ctx]
                         (let [expanded (if (= const/iri-type value)
                                          const/iri-type
                                          (json-ld/expand-iri value ctx))]
                           (where/->typed-val expanded const/iri-id)))

           ;; Build eval namespace in two steps
           ;; 1) Seed with a few explicit entries
           eval-ns-fns (reduce-kv (fn [acc _k v]
                                    (let [unqualified-name (symbol (name v))
                                          var-val @(resolve v)]
                                      (assoc acc unqualified-name var-val)))
                                  {'compare* compare*
                                   'find-grouped-val find-grouped-val
                                   'iri-fn-base iri-fn-base}
                                  (apply dissoc qualified-symbols macro-symbols))

           ;; 2) Add macro replacements
           eval-ns-fns (assoc eval-ns-fns
                              'as as-fn
                              '-if -if-fn
                              '-and -and-fn
                              '-or -or-fn)

           ;; 3) For a few dynamic functions, allow with-redefs to affect SCI calls
           now-wrapper     (when-let [v (resolve 'fluree.db.query.exec.eval/now)]
                             (fn [] (var-get v)))
           uuid-wrapper    (when-let [v (resolve 'fluree.db.query.exec.eval/uuid)]
                             (fn [] (var-get v)))
           struuid-wrapper (when-let [v (resolve 'fluree.db.query.exec.eval/struuid)]
                             (fn [] (var-get v)))

           eval-ns-fns (cond-> eval-ns-fns
                         now-wrapper
                         (assoc 'now (fn [] ((now-wrapper))))
                         uuid-wrapper
                         (assoc 'uuid (fn [] ((uuid-wrapper))))
                         struuid-wrapper
                         (assoc 'struuid (fn [] ((struuid-wrapper)))))

           ;; Build other namespaces
           where-ns-fns {'->typed-val where/->typed-val
                         'get-datatype-iri where/get-datatype-iri
                         'get-binding where/get-binding
                         'variable? where/variable?
                         'mch->typed-val where/mch->typed-val}

           json-ld-fns {'expand-iri json-ld/expand-iri
                        'parse-context json-ld/parse-context}

           const-ns {'iri-id const/iri-id
                     ;; String datatypes needed for comparisons
                     'iri-string const/iri-string
                     'iri-anyURI const/iri-anyURI
                     'iri-xsd-normalizedString const/iri-xsd-normalizedString
                     'iri-lang-string const/iri-lang-string
                     'iri-xsd-token const/iri-xsd-token
                     ;; Numeric datatypes
                     'iri-xsd-decimal const/iri-xsd-decimal
                     'iri-xsd-double const/iri-xsd-double
                     'iri-xsd-integer const/iri-xsd-integer
                     'iri-long const/iri-long
                     'iri-xsd-int const/iri-xsd-int
                     'iri-xsd-byte const/iri-xsd-byte
                     'iri-xsd-short const/iri-xsd-short
                     'iri-xsd-float const/iri-xsd-float
                     'iri-xsd-unsignedLong const/iri-xsd-unsignedLong
                     'iri-xsd-unsignedInt const/iri-xsd-unsignedInt
                     'iri-xsd-unsignedShort const/iri-xsd-unsignedShort
                     'iri-xsd-positiveInteger const/iri-xsd-positiveInteger
                     'iri-xsd-nonPositiveInteger const/iri-xsd-nonPositiveInteger
                     'iri-xsd-negativeInteger const/iri-xsd-negativeInteger
                     'iri-xsd-nonNegativeInteger const/iri-xsd-nonNegativeInteger
                     ;; Time datatypes
                     'iri-xsd-dateTime const/iri-xsd-dateTime
                     'iri-xsd-date const/iri-xsd-date
                     ;; Boolean datatype
                     'iri-xsd-boolean const/iri-xsd-boolean
                      ;; RDF type
                     'iri-rdf-type const/iri-rdf-type}

            ;; Build clojure.core map from a small explicit allowlist to reduce maintenance
           core-allowlist '[instance? boolean? string? number? keyword?
                            int? pos-int? nat-int? map? vector? sequential?
                            list? set? coll? fn? nil? some? contains? empty?
                            not-empty every? some filter remove first second rest next
                            last butlast take drop take-while drop-while nth count get
                            get-in assoc dissoc update keys vals merge select-keys into
                            conj concat mapv reduce partition group-by sort sort-by reverse
                            distinct flatten zipmap frequencies range repeat repeatedly iterate
                            cycle interleave interpose str subs re-find re-matches re-pattern
                            re-seq inc dec + - * / quot rem mod abs max min compare
                            = not= < > <= >= zero? pos? neg? even? odd? true? false? identity
                            constantly comp complement partial
                            name namespace symbol keyword apply
                            pr-str shuffle]

           core-fns (let [m (into {}
                                  (keep (fn [sym]
                                          (when-let [v (ns-resolve 'clojure.core sym)]
                                            [(symbol (name sym)) (var-get v)])))
                                  core-allowlist)]
                      (assoc m `format #?(:clj format :cljs str)))]

       (sci/init {:namespaces {'fluree.db.query.exec.eval eval-ns-fns
                               'fluree.db.query.exec.where where-ns-fns
                               'fluree.json-ld json-ld-fns
                               'fluree.db.constants const-ns
                               'clojure.core core-fns
                               'user {}}
                  :bindings {;; Make constants available
                             'fluree.db.constants/iri-id const/iri-id}}))))

;; Singleton SCI context - created once and reused
;; Defined unconditionally but only used in GraalVM builds
#?(:clj
   (defonce ^:private sci-context-singleton
     (delay (create-sci-context))))

;; GraalVM-specific evaluation function
#?(:clj
   (defn eval-graalvm-with-context
     "Evaluates a form in SCI with context bindings for GraalVM builds."
     [form ctx]
     (let [ctx-with-bindings (sci/merge-opts @sci-context-singleton
                                             {:bindings {'$-CONTEXT ctx
                                                         'fluree.db.query.exec.eval/$-CONTEXT ctx}})]
       (sci/eval-form ctx-with-bindings form))))

;; Enhanced eval-form that accepts context for GraalVM builds
#?(:clj
   (defmacro eval-form-with-context
     "Evaluates a form with additional context bindings for GraalVM builds.
      For JVM builds, ignores the context and uses regular eval."
     [form ctx]
     (graalvm/if-graalvm
      ;; GraalVM branch - use our dedicated function
      `(eval-graalvm-with-context ~form ~ctx)
      ;; JVM branch - use direct eval, ignoring context
      `(eval ~form))))

;;; =============================================================================
;;; Query Compilation Support
;;; =============================================================================

(def allowed-aggregate-fns
  '#{avg ceil count count-star count-distinct distinct floor groupconcat
     median max min rand sample sample1 stddev str sum variance})

(def allowed-scalar-fns
  '#{&& || ! > < >= <= = equal not-equal + - * / quot and bound coalesce if
     nil? as not not= or re-find re-pattern in power

     ;; string fns
     strStarts strEnds subStr strLen ucase lcase contains strBefore strAfter
     concat regex replace

     ;; numeric fns
     abs round ceil floor rand

     ;; datetime fns
     now year month day hours minutes seconds tz

     ;; hash fns
     sha256 sha512

     ;; rdf term fns
     uuid struuid isNumeric isBlank str iri lang datatype is-iri is-literal
     str-lang str-dt bnode

     ;; vector scoring fns
     dotProduct cosineSimilarity euclidianDistance})

(def allowed-symbols
  (set/union allowed-aggregate-fns allowed-scalar-fns))

(defn symbols
  [code]
  (postwalk (fn [x]
              (if (coll? x)
                (apply set/union x)
                (if (symbol? x)
                  #{x}
                  #{})))
            code))

(defn variables
  "Returns the set of items within the arbitrary data structure `code` that
  are variables."
  [code]
  (->> code
       symbols
       (filter where/variable?)))

(defn qualify
  [sym allow-aggregates?]
  (let [allowed-fns (if allow-aggregates?
                      allowed-symbols
                      allowed-scalar-fns)]
    (if (contains? allowed-fns sym)
      (let [qsym (get qualified-symbols sym sym)]
        (log/trace "qualified symbol" sym "as" qsym)
        qsym)
      (let [err-msg (if (and (not allow-aggregates?)
                             (contains? allowed-aggregate-fns sym))
                      (str "Aggregate function " sym " is only valid for grouped values")
                      (str "Query function references illegal symbol: " sym))]
        (throw (ex-info err-msg
                        {:status 400, :error :db/invalid-query}))))))

(defn check-for-count-star
  [[f first-arg & r :as fn-expr] count-star-sym]
  (if (= '[count * nil] [f first-arg r])
    (list 'count-star count-star-sym)
    fn-expr))

;; Helper function to transform iri calls to include context
(defn transform-iri-calls
  "Transforms (iri x) calls to (iri-fn-base x $-CONTEXT) for GraalVM builds."
  [form]
  (if (graalvm/build?)
    (walk/postwalk
     (fn [x]
       (if (and (sequential? x)
                (= 'fluree.db.query.exec.eval/iri (first x))
                (= 2 (count x)))
         `(fluree.db.query.exec.eval/iri-fn-base ~(second x) ~context-var)
         x))
     form)
    form))

(defn coerce
  [count-star-sym allow-aggregates? ctx x]
  (cond
    ;; set literal (for "in")
    (vector? x)
    (mapv (partial coerce count-star-sym allow-aggregates? ctx) x)

    ;; function expression
    (sequential? x)
    (->> (check-for-count-star x count-star-sym)
         (map (partial coerce count-star-sym allow-aggregates? ctx)))

    ;; value map
    (map? x)
    (let [expanded-data (-> (json-ld/expand {const/iri-data x} ctx)
                            (util/get-first const/iri-data))
          id            (util/get-id expanded-data)
          value         (util/get-value expanded-data)
          type          (util/get-types expanded-data)
          language      (util/get-lang expanded-data)]
      (if id
        (where/->typed-val id const/iri-id)
        (where/->typed-val value type language)))

    (where/variable? x)
    x

    (symbol? x)
    (qualify x allow-aggregates?)

    ;; simple literal
    (not (sequential? x))
    (where/->typed-val x)

    :else
    x))

(defn find-grouped-val
  "Used for (count *). In an aggregate, the ::group/grouping matches will all have the
  same number of matches as a value, so we just take the first one."
  [solution]
  (loop [[mch & r] (vals solution)]
    (if mch
      (if (= (::where/datatype-iri mch) :fluree.db.query.exec.group/grouping)
        mch
        (recur r))
      (throw (ex-info "Cannot apply count to wildcard without using group-by."
                      {:status 400 :error :db/invalid-query})))))

(def soln-sym 'solution)

(defn bind-variables
  [count-star-sym var-syms ctx]
  (into `[~context-var ~ctx
          ~'$-CONTEXT ~ctx]  ; Also bind $-CONTEXT for SCI
        (mapcat (fn [var]
                  `[mch# ~(if (= var count-star-sym)
                            `(find-grouped-val ~soln-sym)
                            `(get ~soln-sym (quote ~var)))
                    ;; convert match to TypedValue
                    ~var (if (= ::group/grouping (where/get-datatype-iri mch#))
                           (mapv where/mch->typed-val (where/get-binding mch#))
                           (where/mch->typed-val mch#))]))
        var-syms))

#?(:clj
   (defmacro parse-qualified-code
     "Parses qualified code, applying GraalVM-specific transformations when needed.
      For GraalVM builds, expands iri macro calls to their full form since SCI
      doesn't support runtime macro expansion. Decision is made at compile time."
     [code count-star-sym ctx allow-aggregates?]
     (if (graalvm/build?)
       ;; GraalVM/SCI build - coerce then expand iri macro calls
       `(let [qualified-code# (coerce ~count-star-sym ~allow-aggregates? ~ctx ~code)]
          (walk/postwalk
           (fn [form#]
             (if (and (seq? form#)
                      (= 'fluree.db.query.exec.eval/iri (first form#))
                      (= 2 (count form#)))
               ;; Replace (fluree.db.query.exec.eval/iri x) with the expanded form
               `(fluree.db.query.exec.where/->typed-val
                 (fluree.json-ld/expand-iri
                  (:value ~(second form#))
                  ~context-var)
                 fluree.db.constants/iri-id)
               form#))
           qualified-code#))
       ;; Regular JVM build - just coerce
       `(coerce ~count-star-sym ~allow-aggregates? ~ctx ~code))))

(defn compile*
  [code ctx allow-aggregates?]
  (let [count-star-sym (gensym "?$-STAR")
        qualified-code (parse-qualified-code code count-star-sym ctx allow-aggregates?)
        vars           (variables qualified-code)
        bdg            (bind-variables count-star-sym vars ctx)]
    `(fn [~soln-sym]
       (let ~bdg
         ~qualified-code))))

(defn compile
  ([code ctx] (compile code ctx true))
  ([code ctx allow-aggregates?]
   (let [fn-code (compile* code ctx allow-aggregates?)]
     (log/trace "compiled fn:" fn-code)
     #?(:clj (eval-form-with-context fn-code ctx)
        :cljs (throw (ex-info "eval not supported in ClojureScript" {:code fn-code}))))))

(defn compile-filter
  [code var ctx]
  (let [f        (compile code ctx)
        soln-sym 'solution
        filter-fn-code `(fn [~soln-sym ~var]
                          (-> ~soln-sym
                              (assoc (quote ~var) ~var)
                              ~f
                              :value))]
    #?(:clj (eval-form-with-context filter-fn-code ctx)
       :cljs (throw (ex-info "eval not supported in ClojureScript" {:code filter-fn-code})))))
