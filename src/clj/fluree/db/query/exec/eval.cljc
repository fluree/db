(ns fluree.db.query.exec.eval
  (:refer-clojure :exclude [compile rand concat replace max min
                            #?(:clj ratio? :cljs uuid)])
  (:require [fluree.db.query.exec.group :as group]
            [fluree.db.query.exec.where :as where]
            [fluree.db.vector.scoring :as score]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld]
            [fluree.db.json-ld.iri :as iri]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :refer [postwalk]]
            [fluree.db.datatype :as datatype]
            [fluree.crypto :as crypto]
            [fluree.db.constants :as const]
            [clojure.math :as math])
  #?(:clj (:import (java.time Instant OffsetDateTime LocalDateTime))))

#?(:clj (set! *warn-on-reflection* true))

(defn ratio?
  [x]
  #?(:clj  (clojure.core/ratio? x)
     :cljs false)) ; ClojureScript doesn't support ratios

(defn sum
  [coll]
  (where/->typed-val (reduce + (mapv :value coll))))

(defn avg
  [coll]
  (let [coll (mapv :value coll)
        res (/ (sum coll)
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
  (let [mean (avg (mapv :value coll))
        sum  (sum (for [x coll
                        :let [delta (- x mean)]]
                    (* delta delta)))
        res  (/ sum (count coll))]
    (where/->typed-val
      (if (ratio? res)
        (double res)
        res))))

(defn stddev
  [coll]
  (where/->typed-val
    (Math/sqrt (:value (variance coll)))))

(defn max
  [coll]
  (where/->typed-val
    (apply clojure.core/max (mapv :value coll))))

(defn min
  [coll]
  (where/->typed-val
    (apply clojure.core/min (mapv :value coll))))

(defn ceil
  [{n :value}]
  (where/->typed-val (cond (= n (int n)) n
                           (> n 0) (-> n int inc)
                           (< n 0) (-> n int))))

(defn count-distinct
  [coll]
  (where/->typed-val
    (count (distinct coll))))

(defn -count
  [coll]
  (where/->typed-val (count coll)))

(defn floor
  [{n :value}]
  (where/->typed-val (cond (= n (int n)) n
                           (> n 0) (-> n int)
                           (< n 0) (-> n int dec))))

(def groupconcat clojure.core/concat)

(defn sample
  [{n :value} coll]
  (->> coll
       shuffle
       (take n)
       vec))

(defn sample1
  [coll]
  (->> coll (sample 1) first))

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
   `(let [and# (:value ~x)]
      (where/->typed-val (if and# (and (:value ~@next)) and#)))))

(defmacro -or
  "Equivalent to or"
  ([] (where/->typed-val nil))
  ([x] x)
  ([x & next]
   `(let [or# (:value ~x)]
      (where/->typed-val (if or# or# (or (:value ~@next)))))))

(defn now
  []
  (where/->typed-val #?(:clj (Instant/now)
                        :cljs (js/Date.))
                     const/iri-xsd-dateTime))

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
  (where/->typed-val (:lang tv) const/iri-string))

(defn datatype
  [tv]
  (where/->typed-val (:datatype-iri tv) const/iri-id))

(def context-var
  (symbol "$-CONTEXT"))

(defmacro iri
  [tv]
  `(where/->typed-val (json-ld/expand-iri (:value ~tv) ~context-var) const/iri-id))

(def numeric-datatypes
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

(def string-datatypes
  #{const/iri-string
    const/iri-xsd-normalizedString
    const/iri-lang-string
    const/iri-xsd-token})

(def comparable-datatypes
  (set/union
    numeric-datatypes
    string-datatypes
    #{const/iri-xsd-boolean
      const/iri-anyURI
      const/iri-id
      const/iri-xsd-dateTime
      const/iri-xsd-date
      const/iri-xsd-time}))

(defn compare*
  [val-a dt-a val-b dt-b]
  (let [dt-a (or dt-a (datatype/infer-iri val-a))
        dt-b (or dt-b (datatype/infer-iri val-b))]
    (cond
      ;; can compare across types
      (or (and (contains? numeric-datatypes dt-a)
               (contains? numeric-datatypes dt-b))
          (and (contains? string-datatypes dt-a)
               (contains? string-datatypes dt-b)))
      (compare val-a val-b)

      ;; can compare with same type
      (and (= dt-a dt-b)
           (contains? comparable-datatypes dt-a))
      (compare val-a val-b)

      :else
      (throw (ex-info (str "Incomparable datatypes: " dt-a " and " dt-b)
                      {:a      val-a :a-dt dt-a
                       :b      val-b :b-dt dt-b
                       :status 400
                       :error  :db/invalid-query})))))

(defn less-than
  [{a :value a-dt :datatype-iri}
   {b :value b-dt :datatype-iri}]
  (where/->typed-val (neg? (compare* a a-dt b b-dt))))

(defn less-than-or-equal
  [{a :value a-dt :datatype-iri}
   {b :value b-dt :datatype-iri}]
  (where/->typed-val
    (or (= a b)
        (neg? (compare* a a-dt b b-dt)))))

(defn greater-than
  [{a :value a-dt :datatype-iri}
   {b :value b-dt :datatype-iri}]
  (where/->typed-val (pos? (compare* a a-dt b b-dt))))

(defn greater-than-or-equal
  [{a :value a-dt :datatype-iri}
   {b :value b-dt :datatype-iri}]
  (where/->typed-val
    (or (= a b)
        (pos? (compare* a a-dt b b-dt)))))

(defn regex
  [{text :value} {pattern :value}]
  (where/->typed-val (boolean (re-find (re-pattern pattern) text))))

(defn replace
  [{s :value} {pattern :value} {replacement :value}]
  (where/->typed-val (str/replace s (re-pattern pattern) replacement)))

(defn rand
  []
  (where/->typed-val (clojure.core/rand)))

(defn year
  [{datetime :value}]
  (where/->typed-val
    #?(:clj  (.getYear ^LocalDateTime datetime)
       :cljs (.getFullYear datetime))))

(defn month
  [{datetime :value}]
  (where/->typed-val
    #?(:clj  (.getMonthValue ^LocalDateTime datetime)
       :cljs (.getMonth datetime))))

(defn day
  [{datetime :value}]
  (where/->typed-val
    #?(:clj  (.getDayOfMonth ^LocalDateTime datetime)
       :cljs (.getDate datetime))))

(defn hours
  [{datetime :value}]
  (where/->typed-val
    #?(:clj  (.getHour ^LocalDateTime datetime)
       :cljs (.getHours datetime))))

(defn minutes
  [{datetime :value}]
  (where/->typed-val
    #?(:clj  (.getMinute ^LocalDateTime datetime)
       :cljs (.getMinutes datetime))))

(defn seconds
  [{datetime :value}]
  (where/->typed-val
    #?(:clj  (.getSecond ^LocalDateTime datetime)
       :cljs (.getSeconds datetime))))

(defn tz
  [{datetime :value}]
  (where/->typed-val
    #?(:clj  (.toString (.getOffset ^OffsetDateTime datetime))
       :cljs (.getTimeZoneOffset ^js/Date datetime))))

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

(defn equal
  ([{x :value}]  (where/->typed-val true))
  ([{x :value} {y :value}] (where/->typed-val (= x y)))
  ([{x :value} {y :value} & more]
   (where/->typed-val (apply = x y (mapv :value more)))))

(defn not-equal
  ([{x :value}]  (where/->typed-val false))
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

(defn dotproduct
  [{v1 :value} {v2 :value}]
  (where/->typed-val
    (score/dotproduct v1 v2)))

(defn cosine-similarity
  [{v1 :value} {v2 :value}]
  (where/->typed-val
    (score/cosine-similarity v1 v2 )))

(defn euclidean-distance
  [{v1 :value} {v2 :value}]
  (where/->typed-val
    (score/euclidian-distance v1 v2 )))

(def qualified-symbols
  '{!              fluree.db.query.exec.eval/-not
    ||             fluree.db.query.exec.eval/-or
    &&             fluree.db.query.exec.eval/-and
    +              fluree.db.query.exec.eval/plus
    -              fluree.db.query.exec.eval/minus
    *              fluree.db.query.exec.eval/multiply
    /              fluree.db.query.exec.eval/divide
    =              fluree.db.query.exec.eval/equal
    <              fluree.db.query.exec.eval/less-than
    <=             fluree.db.query.exec.eval/less-than-or-equal
    >              fluree.db.query.exec.eval/greater-than
    >=             fluree.db.query.exec.eval/greater-than-or-equal
    abs            fluree.db.query.exec.eval/absolute-value
    as             fluree.db.query.exec.eval/as
    and            fluree.db.query.exec.eval/-and
    avg            fluree.db.query.exec.eval/avg
    bound          fluree.db.query.exec.eval/bound
    ceil           fluree.db.query.exec.eval/ceil
    coalesce       fluree.db.query.exec.eval/coalesce
    concat         fluree.db.query.exec.eval/concat
    contains       fluree.db.query.exec.eval/contains
    count-distinct fluree.db.query.exec.eval/count-distinct
    count          fluree.db.query.exec.eval/-count
    datatype       fluree.db.query.exec.eval/datatype
    floor          fluree.db.query.exec.eval/floor
    groupconcat    fluree.db.query.exec.eval/groupconcat
    if             fluree.db.query.exec.eval/-if
    in             fluree.db.query.exec.eval/in
    iri            fluree.db.query.exec.eval/iri
    lang           fluree.db.query.exec.eval/lang
    lcase          fluree.db.query.exec.eval/lcase
    median         fluree.db.query.exec.eval/median
    nil?           fluree.db.query.exec.eval/-nil?
    not            fluree.db.query.exec.eval/-not
    not=           fluree.db.query.exec.eval/not-equal
    now            fluree.db.query.exec.eval/now
    or             fluree.db.query.exec.eval/-or
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
    isNumeric      fluree.db.query.exec.eval/isNumeric
    isBlank        fluree.db.query.exec.eval/isBlank
    str            fluree.db.query.exec.eval/sparql-str
    max            fluree.db.query.exec.eval/max
    min            fluree.db.query.exec.eval/min

    dotproduct         fluree.db.query.exec.eval/dotproduct
    cosine-similarity  fluree.db.query.exec.eval/cosine-similarity
    euclidian-distance fluree.db.query.exec.eval/euclidean-distance})

(def allowed-aggregate-fns
  '#{avg ceil count count-distinct distinct floor groupconcat
     median max min rand sample sample1 stddev str sum variance})

(def allowed-scalar-fns
  '#{&& || ! > < >= <= = + - * / quot and bound coalesce datatype if iri lang
     nil? as not not= or re-find re-pattern in

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
     uuid struuid isNumeric isBlank str

     ;; vector scoring fns
     dotproduct cosine-similarity euclidian-distance})

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

(defn coerce
  [code allow-aggregates?]
  (postwalk (fn [x]
              (cond (where/variable? x)
                    x

                    (symbol? x)
                    (qualify x allow-aggregates?)

                    ;; literal
                    (not (sequential? x))
                    (where/->TypedValue x (datatype/infer-iri x) nil)

                    :else
                    x))
            code))

(defn mch->typed-val
  [{::where/keys [val iri datatype-iri meta]}]
  (where/->typed-val (or iri val) (if iri const/iri-id datatype-iri) (:lang meta)))

(defn bind-variables
  [soln-sym var-syms ctx]
  (into `[~context-var ~ctx]
        (mapcat (fn [var]
                  `[mch# (get ~soln-sym (quote ~var))
                    ;; convert match to TypedValue
                    ~var (if (= ::group/grouping (::where/datatype-iri mch#))
                           (mapv mch->typed-val (where/get-binding mch#))
                           (mch->typed-val mch#))]))
        var-syms))

(defn compile*
  [code ctx allow-aggregates?]
  (let [qualified-code (coerce code allow-aggregates?)
        vars           (variables qualified-code)
        soln-sym       'solution
        bdg            (bind-variables soln-sym vars ctx)]
    `(fn [~soln-sym]
       (let ~bdg
         ~qualified-code))))

(defn compile
  ([code ctx] (compile code ctx true))
  ([code ctx allow-aggregates?]
   (let [fn-code (compile* code ctx allow-aggregates?)]
     (log/trace "compiled fn:" fn-code)
     (eval fn-code))))

(defn compile-filter
  [code var ctx]
  (let [f        (compile code ctx)
        soln-sym 'solution]
    (eval `(fn [~soln-sym ~var]
             (-> ~soln-sym
                 (assoc (quote ~var) ~var)
                 ~f)))))
