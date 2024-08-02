(ns fluree.db.query.exec.eval
  (:refer-clojure :exclude [compile rand concat replace max min
                            #?(:clj ratio? :cljs uuid)])
  (:require [fluree.db.query.exec.group :as group]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util.log :as log]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :refer [postwalk]]
            [clojure.math]
            [fluree.db.datatype :as datatype]
            [fluree.crypto :as crypto]
            [fluree.db.constants :as const])
  #?(:clj (:import (java.time Instant OffsetDateTime LocalDateTime))))

#?(:clj (set! *warn-on-reflection* true))

(defn ratio?
  [x]
  #?(:clj  (clojure.core/ratio? x)
     :cljs false)) ; ClojureScript doesn't support ratios

(defn sum
  [coll]
  (reduce + coll))

(defn avg
  [coll]
  (let [res (/ (sum coll)
               (count coll))]
    (if (ratio? res)
      (double res)
      res)))

(defn median
  [coll]
  (let [terms (sort coll)
        size  (count coll)
        med   (bit-shift-right size 1)
        res   (cond-> (nth terms med)
                      (even? size)
                      (-> (+ (nth terms (dec med)))
                          (/ 2)))]
    (if (ratio? res)
      (double res)
      res)))

(defn variance
  [coll]
  (let [mean (avg coll)
        sum  (sum (for [x coll
                        :let [delta (- x mean)]]
                    (* delta delta)))
        res  (/ sum (count coll))]
    (if (ratio? res)
      (double res)
      res)))

(defn stddev
  [coll]
  (Math/sqrt (variance coll)))

(defn max
  [coll]
  (apply clojure.core/max coll))

(defn min
  [coll]
  (apply clojure.core/min coll))

(defn ceil
  [n]
  (cond (= n (int n)) n
        (> n 0) (-> n int inc)
        (< n 0) (-> n int)))

(def count-distinct
  (comp count distinct))

(defn floor
  [n]
  (cond (= n (int n)) n
        (> n 0) (-> n int)
        (< n 0) (-> n int dec)))

(def groupconcat clojure.core/concat)

(defn sample
  [n coll]
  (->> coll
       shuffle
       (take n)
       vec))

(defn sample1
  [coll]
  (->> coll (sample 1) first))

(def allowed-aggregate-fns
  '#{avg ceil count count-distinct distinct floor groupconcat
     median max min rand sample sample1 stddev str sum variance})

(defmacro coalesce
  "Evaluates args in order. The result of the first arg not to return error gets returned."
  ([] (throw (ex-info "COALESCE evaluation failed on all forms." {:status 400 :error :db/invalid-query})))
  ([arg] `(let [res# (try ~arg (catch Exception e# nil))]
            (if (nil? res#)
              (throw (ex-info "Coalesce evaluation failed on all forms." {:status 400 :error :db/invalid-query})) res#)))
  ([arg & args]
   `(let [res# (try ~arg (catch Exception e# nil))]
      (if (nil? res#)
        (coalesce ~@args) res#))))

(def bound some?)

(def ! not)

(defmacro &&
  "Equivalent to and"
  ([] true)
  ([x] x)
  ([x & next]
   `(let [and# ~x]
      (if and# (and ~@next) and#))))

(defmacro ||
  "Equivalent to or"
  ([] nil)
  ([x] x)
  ([x & next]
   `(let [or# ~x]
      (if or# or# (or ~@next)))))

(defn now
  []
  #?(:clj  (str (Instant/now))
     :cljs (.toISOString (js/Date.))))

(defn strStarts
  [s substr]
  (str/starts-with? s substr))

(defn strEnds
  [s substr]
  (str/ends-with? s substr))

(defn subStr
  ;; The index of the first character in a string is 1.
  ([s start]
   (subs s (dec start)))
  ([s start length]
   (let [start (dec start)]
     (subs s start (clojure.core/min (+ start length) (count s))))))

(defn strLen
  [s]
  (count s))

(defn ucase
  [s]
  (str/upper-case s))

(defn lcase
  [s]
  (str/lower-case s))

(defn contains
  [s substr]
  (str/includes? s substr))

(defn strBefore
  [s substr]
  (let [[before :as split] (str/split s (re-pattern substr))]
    (if (> (count split) 1)
      before
      "")))

(defn strAfter
  [s substr]
  (let [split (str/split s (re-pattern substr))]
    (if (> (count split) 1)
      (last split)
      "")))

(defn concat
  [& strs]
  (apply str strs))

(defn var->lang-var
  [var]
  (-> var
      (str "$-LANG")
      symbol))

(defn var->dt-var
  [var]
  (-> var
      (str "$-DATATYPE")
      symbol))

(defmacro lang
  [var]
  (var->lang-var var))

(defmacro datatype
  [var]
  (var->dt-var var))

(defn regex
  [text pattern]
  (boolean (re-find (re-pattern pattern) text)))

(defn replace
  [s pattern replacement]
  (str/replace s (re-pattern pattern) replacement))

(defn rand
  []
  (clojure.core/rand))

(defn year
  [datetime-string]
  (let [datetime (datatype/coerce datetime-string const/$xsd:dateTime)]
    #?(:clj  (.getYear ^LocalDateTime datetime)
       :cljs (.getFullYear datetime))))

(defn month
  [datetime-string]
  (let [datetime (datatype/coerce datetime-string const/$xsd:dateTime)]
    #?(:clj  (.getMonthValue ^LocalDateTime datetime)
       :cljs (.getMonth datetime))))

(defn day
  [datetime-string]
  (let [datetime (datatype/coerce datetime-string const/$xsd:dateTime)]
    #?(:clj  (.getDayOfMonth ^LocalDateTime datetime)
       :cljs (.getDate datetime))))

(defn hours
  [datetime-string]
  (let [datetime (datatype/coerce datetime-string const/$xsd:dateTime)]
    #?(:clj  (.getHour ^LocalDateTime datetime)
       :cljs (.getHours datetime))))

(defn minutes
  [datetime-string]
  (let [datetime (datatype/coerce datetime-string const/$xsd:dateTime)]
    #?(:clj  (.getMinute ^LocalDateTime datetime)
       :cljs (.getMinutes datetime))))

(defn seconds
  [datetime-string]
  (let [datetime (datatype/coerce datetime-string const/$xsd:dateTime)]
    #?(:clj  (.getSecond ^LocalDateTime datetime)
       :cljs (.getSeconds datetime))))

(defn tz
  [datetime-string]
  (let [datetime (datatype/coerce datetime-string const/$xsd:dateTime)]
    #?(:clj  (.toString (.getOffset ^OffsetDateTime datetime))
       :cljs (.getTimeZoneOffset ^js/Date datetime))))

(defn sha256
  [x]
  (crypto/sha2-256 x))

(defn sha512
  [x]
  (crypto/sha2-512 x))

(defn uuid
  []
  (str "urn:uuid:" (random-uuid)))

(defn struuid
  []
  (str (random-uuid)))

(defn isNumeric
  [x]
  (number? x))

(defn isBlank
  [x]
  (and (string? x)
       (str/starts-with? x "_:")))

(defn sparql-str
  [x]
  (str x))

(defn in
  [term expressions]
  (contains? (set expressions) term))

(def allowed-scalar-fns
  '#{&& || ! > < >= <= = + - * / quot and bound coalesce datatype if lang nil?
     as not not= or re-find re-pattern in

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
     uuid struuid isNumeric isBlank str})


(def allowed-symbols
  (set/union allowed-aggregate-fns allowed-scalar-fns))

(def qualified-symbols
  '{
    !              fluree.db.query.exec.eval/!
    ||             fluree.db.query.exec.eval/||
    &&             fluree.db.query.exec.eval/&&
    abs            clojure.core/abs
    as             fluree.db.query.exec.eval/as
    avg            fluree.db.query.exec.eval/avg
    bound          fluree.db.query.exec.eval/bound
    ceil           fluree.db.query.exec.eval/ceil
    coalesce       fluree.db.query.exec.eval/coalesce
    concat         fluree.db.query.exec.eval/concat
    contains       fluree.db.query.exec.eval/contains
    count-distinct fluree.db.query.exec.eval/count-distinct
    count          clojure.core/count
    datatype       fluree.db.query.exec.eval/datatype
    floor          fluree.db.query.exec.eval/floor
    groupconcat    fluree.db.query.exec.eval/groupconcat
    in             fluree.db.query.exec.eval/in
    lang           fluree.db.query.exec.eval/lang
    lcase          fluree.db.query.exec.eval/lcase
    median         fluree.db.query.exec.eval/median
    now            fluree.db.query.exec.eval/now
    rand           fluree.db.query.exec.eval/rand
    regex          fluree.db.query.exec.eval/regex
    replace        fluree.db.query.exec.eval/replace
    round          clojure.math/round
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
    min            fluree.db.query.exec.eval/min})


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
              (if (and (symbol? x)
                       (not (where/variable? x)))
                (qualify x allow-aggregates?)
                x))
            code))

(defn bind-variables
  [soln-sym var-syms]
  (into []
        (mapcat (fn [var]
                  (let [dt-var   (var->dt-var var)
                        lang-var (var->lang-var var)]
                    `[mch#      (get ~soln-sym (quote ~var))
                      ~dt-var   (where/get-datatype-iri mch#)
                      ~lang-var (-> mch# ::where/meta :lang (or ""))
                      ~var      (cond->> (where/get-binding mch#)
                                  (= ~dt-var ::group/grouping)
                                  (mapv where/get-value))])))
        var-syms))

(defn compile
  ([code] (compile code true))
  ([code allow-aggregates?]
   (let [qualified-code (coerce code allow-aggregates?)
         vars           (variables qualified-code)
         soln-sym       'solution
         bdg            (bind-variables soln-sym vars)
         fn-code        `(fn [~soln-sym]
                           (log/trace "fn solution:" ~soln-sym)
                           (log/trace "fn bindings:" (quote ~bdg))
                           (let ~bdg
                             ~qualified-code))]
     (log/trace "compiled fn:" fn-code)
     (eval fn-code))))

(defn compile-filter
  [code var]
  (let [f        (compile code)
        soln-sym 'solution]
    (eval `(fn [~soln-sym ~var]
             (-> ~soln-sym
                 (assoc (quote ~var) ~var)
                 ~f)))))
