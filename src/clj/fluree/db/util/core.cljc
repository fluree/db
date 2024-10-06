(ns fluree.db.util.core
  (:require [clojure.string :as str]
            #?@(:clj [[fluree.db.util.clj-exceptions :as clj-exceptions]
                      [fluree.db.util.cljs-exceptions :as cljs-exceptions]]))
  #?(:cljs (:require-macros [fluree.db.util.core :refer [case+]]))
  #?(:clj (:import (java.util UUID Date)
                   (java.time Instant OffsetDateTime ZoneId)
                   (java.time.format DateTimeFormatter)
                   (java.net URLEncoder URLDecoder)))
  (:refer-clojure :exclude [vswap!]))

#?(:clj (set! *warn-on-reflection* true))


(def ^:const max-long #?(:clj  (Long/MAX_VALUE)
                         :cljs 9007199254740991))           ;; 2^53-1 for javascript
(def ^:const min-long #?(:clj  (Long/MIN_VALUE)
                         :cljs -9007199254740991))
(def ^:const max-integer 2147483647)
(def ^:const min-integer -2147483647)

(defn cljs-env?
  "Take the &env from a macro, and tell whether we are expanding into cljs."
  [env]
  (boolean (:ns env)))

#?(:clj
   (defmacro if-cljs
     "Return then if we are generating cljs code and else for Clojure code.
     https://groups.google.com/d/msg/clojurescript/iBY5HaQda4A/w1lAQi9_AwsJ"
     [then else]
     (if (cljs-env? &env) then else)))


#?(:clj
   (defmacro try-catchall
     "A cross-platform variant of try-catch that catches all exceptions.
   Does not (yet) support finally, and does not need or want an exception class."
     [& body]
     (let [try-body (butlast body)
           [catch sym & catch-body :as catch-form] (last body)]
       (assert (= catch 'catch))
       (assert (symbol? sym))
       `(if-cljs
            (try ~@try-body (~'catch js/Object ~sym ~@catch-body))
            (try ~@try-body (~'catch Throwable ~sym ~@catch-body))))))

(declare catch*)

#?(:clj
   (defmacro try*
     "Like try but supports catch*. catch* is like catch but supports CLJ/CLJS
     with less boilerplate. In CLJ it catches `Exception`. In CLJS it catches
     `:default`.

     Use it like this: `(try* ... (catch* err (handle-err err)))`.
     Also supports an optional finally clause."
     [& body]
     `(if-cljs
        (cljs-exceptions/try* ~@body)
        (clj-exceptions/try* ~@body))))

(defn index-of
  "Returns index integer (n) of item within a Vector.
  If item cannot be found, returns nil."
  [^clojure.lang.PersistentVector coll value]
  #?(:clj  (let [n (.indexOf coll value)]
             (if (< n 0)
               nil
               n))
     :cljs (some (fn [[item idx]] (when (= value item) idx))
                 (partition 2 (interleave coll (range))))))

(defn date->millis
  "Given a date, returns epoch millis if possible."
  [date]
  (cond
    (string? date)
    #?(:clj  (-> (Instant/parse date)
                 (.toEpochMilli))
       :cljs (-> (js/Date.parse date)
                 (.getTime)))

    (number? date)
    date

    #?@(:clj  [(instance? Instant date)
               (.toEpochMilli ^Instant date)

               (instance? Date date)
               (.getTime ^Date date)]
        :cljs [(instance? js/Date date)
               (.getTime date)])

    :else
    (throw (ex-info (str "Invalid date: " (pr-str date))
                    {:status 400 :error :db/invalid-date}))))


(defn current-time-millis
  "Returns current time in epoch milliseonds for closure/script"
  []
  #?(:clj  (System/currentTimeMillis)
     :cljs (js/Date.now)))

(defn current-time-iso
  "Returns current time as string for ISO-8601 format"
  []
  #?(:clj  (str (Instant/now))
     :cljs (.toISOString (js/Date.))))

(defn response-time-formatted
  "Returns response time, formatted as string. Must provide start time of request
   for clj as (System/nanoTime), or for cljs epoch milliseconds"
  [start-time]
  #?(:clj  (-> (- (System/nanoTime) start-time)
               (/ 1000000)
               (#(format "%.2fms" (float %))))
     :cljs (-> (- (current-time-millis) start-time)
               (str "ms"))))


(defn deep-merge [v & vs]
  (letfn [(rec-merge [v1 v2]
            (if (and (map? v1) (map? v2))
              (merge-with deep-merge v1 v2)
              v2))]
    (if (some identity vs)
      (reduce #(rec-merge %1 %2) v vs)
      v)))


(defn email?
  [email]
  (re-matches #"^[\w-\+]+(\.[\w]+)*@[\w-]+(\.[\w]+)*(\.[a-z]{2,})$" email))


(defn pred-ident?
  "Tests if an predicate identity two-tuple
  in form of [pred-name-or-id pred-value]"
  [x]
  (and (sequential? x)
       (= 2 (count x))
       (string? (first x))))


(defn temp-ident?
  [x]
  (string? x))


(defn str->int
  "Converts string to integer. Assumes you've already verified the string is
  parsable to an integer."
  [s]
  #?(:clj  (Integer/parseInt s)
     :cljs (js/parseInt s)))

(defn str->long
  "Converts string to long integer. Assumes you've already verified the string is
  parsable to a long.

  Note JS only has precision to 2^53-1, so this will not work for larger numbers."
  [s]
  #?(:clj  (Long/parseLong s)
     :cljs (js/parseInt s)))

(defn keyword->str
  "Converts a keyword to string. Can safely be called on a
  string which will return itself."
  [k]
  (cond
    (keyword? k) (subs (str k) 1)
    (string? k) k
    :else (throw (ex-info (str "Cannot convert type " (type k) " to string: " (pr-str k))
                          {:status 500 :error :db/unexpected-error}))))

(defn str->keyword
  "Converts a string to a keyword, checking to see if
  the string starts with a ':', which it strips before converting."
  [s]
  (cond
    (string? s) (if (str/starts-with? s ":")
                  (keyword (subs s 1))
                  (keyword s))
    (keyword? s) s
    :else (throw (ex-info (str "Cannot convert type " (type s) " to keyword: " (pr-str s))
                          {:status 500 :error :db/unexpected-error}))))

(defn keywordize-keys
  "Does simple (top-level keys only) keywordize-keys if the key is a string."
  [m]
  (reduce-kv
    (fn [acc k v]
      (if (string? k)
        (assoc acc (keyword k) v)
        (assoc acc k v)))
    {} m))

(defn stringify-keys
  "Does simple (top-level keys only) conversion of keyword keys to strings.
  This only takes the 'name' value of keywords, not the namespace. Could do
  namespace too, but nothing currently needs that. Used mostly for serializing
  properly to JSON."
  [m]
  (reduce-kv
    (fn [acc k v]
      (if (keyword? k)
        (assoc acc (name k) v)
        (assoc acc k v)))
    {} m))


(defn str->epoch-ms
  "Takes time as a string and returns epoch millis."
  [time-str]
  (try
    #?(:clj  (.toEpochMilli (Instant/parse time-str))
       :cljs (js/Date.parse time-str))
    (catch #?(:clj Exception :cljs :default) _
      (throw (ex-info (str "Invalid time string. Ensure format is ISO-8601 compatible. Provided: " (pr-str time-str))
                      {:status 400
                       :error  :db/invalid-time})))))

(defn epoch-ms->iso-8601-str
  "Takes milliseconds since the epoch and returns an ISO-8601 formatted string
  for that datetime. Optionally takes a ZoneId string (e.g. 'America/Denver')."
  ([millis] (epoch-ms->iso-8601-str millis "Z"))
  ([millis zone-id]
   #?(:clj  (-> millis Instant/ofEpochMilli
                (OffsetDateTime/ofInstant (ZoneId/of zone-id))
                (.format DateTimeFormatter/ISO_OFFSET_DATE_TIME))
      :cljs (-> millis js/Date. .toISOString))))

(defn trunc
  "Truncate string s to n characters."
  [s n]
  (if (< (count s) n)
    s
    (str (subs s 0 n) " ...")))

#?(:clj
   (defmacro some-of
     ([] nil)
     ([x] x)
     ([x & more]
      `(let [x# ~x] (if (nil? x#) (some-of ~@more) x#)))))

(defn filter-vals
  "Filters map k/v pairs dropping any where predicate applied to value is false."
  [pred m]
  (reduce-kv (fn [m k v] (if (pred v) (assoc m k v) m)) {} m))

(defn without-nils
  "Remove all keys from a map that have nil or empty collection values."
  [m]
  (filter-vals #(if (coll? %) (not-empty %) (some? %)) m))

(defn inclusive-range
  "Like range, but includes start/end values."
  ([] (range))
  ([end] (range (inc end)))
  ([start end] (range start (inc end)))
  ([start end step] (range start (+ end step) step)))


(defn exception?
  "x-platform, returns true if is an exception"
  [x]
  (instance? #?(:clj Throwable :cljs js/Error) x))


(defn url-encode
  [string]
  #?(:clj  (some-> string str (URLEncoder/encode "UTF-8") (.replace "+" "%20"))
     :cljs (some-> string str (js/encodeURIComponent) (.replace "+" "%20"))))

(defn url-decode
  ([string] (url-decode string "UTF-8"))
  ([string ^String encoding]
   #?(:clj  (some-> string str (URLDecoder/decode encoding))
      :cljs (some-> string str (js/decodeURIComponent)))))


(defn map-invert
  [m]
  (reduce (fn [m [k v]] (assoc m v k)) {} m))


(defn zero-pad
  "Zero pads x"
  [x pad]
  (loop [s (str x)]
    (if (< #?(:clj (.length s) :cljs (.-length s)) pad)
      (recur (str "0" s))
      s)))

(defn conjv
  "Like conj, but if collection is nil creates a new vector instead of list.
  Not built to handle variable arity values"
  [coll x]
  (if (nil? coll)
    (vector x)
    (conj coll x)))

(defn conjs
  "Like conj, but if collection is nil creates a new set instead of list.
  Not built to handle variable arity values"
  [coll x]
  (if (nil? coll)
    #{x}
    (conj coll x)))

(defn sequential
  "Returns input wrapped in a vector if not already sequential."
  [x]
  (if (sequential? x)
    x
    [x]))

#?(:clj
   (defmacro condps
     "Takes an expression and a set of clauses.
     Each clause can take the form of either:

     unary-predicate-fn? result-expr
     (unary-predicate-fn?-1 ... unary-predicate-fn?-N) result-expr

     For each clause, (unary-predicate-fn? expr) is evalated (for each
     unary-predicate-fn? in the clause when >1 is given). If it returns logical
     true, the clause is a match.

     Similar to condp but takes unary predicates instead of binary and allows
     multiple predicates to be supplied in a list similar to case."
     [expr & clauses]
     (let [gexpr (gensym "expr__")
           emit  (fn emit [expr args]
                   (let [[[a b :as clause] more] (split-at 2 args)
                         n (count clause)]
                     (case n
                       0 `(throw (IllegalArgumentException.
                                   (str "No matching clause: " ~expr)))
                       1 a
                       (let [preds (if (and (coll? a)
                                            (not (= 'fn* (first a)))
                                            (not (= 'fn (first a))))
                                     (vec a)
                                     [a])]
                         `(if ((apply some-fn ~preds) ~expr)
                            ~b
                            ~(emit expr more))))))]
       `(let [~gexpr ~expr]
          ~(emit gexpr clauses)))))

#?(:clj
   (defn- eval-dispatch
     [d]
     (if (list? d)
       (map eval d)
       (eval d))))

#?(:clj
   (defmacro case+
     "Same as case, but evaluates dispatch values, needed for referring to
     class and def'ed constants as well as java.util.Enum instances.

     NB: If you have all `:const` or literal dispatch values you can use either regular
  old `cljs.core/case` if you are in cljs-only code, as those get inlined and work fine,
  or `fluree.db.util.clj-const/case` and `fluree.db.util.cljs-const/case` if you are in
  cljc.

  calling context/dispatch value

  |      | literal | :const                        | anything else |
  |------+---------+-------------------------------+---------------|
  | cljs | case    | case                          | case+         |
  | clj  | case    | fluree.db.util.clj-const/case | case+         |
  | cljc | case    | fluree.db.util.clj-const/case | case+         |"
     [value & clauses]
     (let [clauses       (partition 2 2 nil clauses)
           default       (when (-> clauses last count (= 1))
                           (last clauses))
           clauses       (if default (drop-last clauses) clauses)]
       (if-cljs
         `(condp = ~value
            ~@(concat clauses default))
         `(case ~value
            ~@(concat (->> clauses
                           (map #(-> % first eval-dispatch (list (second %))))
                           (mapcat identity))
                      default))))))


(defn vswap!
  "This silly fn exists to work around a bug in go macros where they sometimes clobber
  type hints and issue reflection warnings. The vswap! macro uses interop so those forms
  get macroexpanded into the go block. You'll then see reflection warnings for reset
  deref. By letting the macro expand into this fn instead, it avoids the go bug.
  I've filed a JIRA issue here: https://clojure.atlassian.net/browse/ASYNC-240
  NB: I couldn't figure out how to get a var-arg version working so this only supports
  0-3 args. I didn't see any usages in here that need more than 2, but note well and
  feel free to add additional arities if needed (but maybe see if that linked bug has
  been fixed first in which case delete this thing with a vengeance and remove the
  refer-clojure exclude in the ns form).
  - WSM 2021-08-26"
  ([vol f]
   (clojure.core/vswap! vol f))
  ([vol f arg1]
   (clojure.core/vswap! vol f arg1))
  ([vol f arg1 arg2]
   (clojure.core/vswap! vol f arg1 arg2))
  ([vol f arg1 arg2 arg3]
   (clojure.core/vswap! vol f arg1 arg2 arg3)))

(defn get-first
  [json-ld k]
  (let [v (get json-ld k)]
    (if (sequential? v)
      (first v)
      v)))

(defn get-types
  [json-ld]
  (or (:type json-ld)
      (get json-ld "@type")))

(defn of-type?
  "Returns true if the provided json-ld node is of the provided type."
  [json-ld rdf-type]
  (->> json-ld
       get-types
       (some #(= % rdf-type))))

(defn get-value
  [val]
  (if (map? val)
    (or (:value val)
        (get val "@value"))
    val))

(defn get-first-value
  [json-ld k]
  (-> json-ld
      (get-first k)
      get-value))

(defn get-values
  [json-ld k]
  (mapv get-value (get json-ld k)))

(defn get-id
  [json-ld]
  (or (:id json-ld)
      (get json-ld "@id")))

(defn get-first-id
  [json-ld k]
  (-> json-ld
      (get-first k)
      get-id))

(defn unwrap-singleton
  ([coll]
   (if (= 1 (count coll))
     (first coll)
     coll))

  ([iri context coll]
   (if (#{:list :set} (-> context (get iri) :container))
     coll
     (unwrap-singleton coll))))

(defn unwrap-list
  "If values are contained in a @list, unwraps them.
  @list can look like:
  {ex:someProperty [{@list [ex:val1 ex:val2]}]}
  or in single-cardinality form:
  {ex:someProperty {@list [ex:val1 ex:val2]}}

  If @list is not present, return original 'vals' argument."
  [vals]
  (let [first-val (if (sequential? vals)
                    (first vals)
                    vals)
        list-vals (when (map? first-val)
                    (or (:list first-val)
                        (get first-val "@list")))]
    (or list-vals
        vals)))

(defn get-all-ids
  "Returns all @id values for a given key in a json-ld node.

  If values are contained in a @list, unwraps them.

  Elides any scalar values (those without an @id key)."
  [json-ld k]
  (some->> (get json-ld k)
           unwrap-list
           (keep get-id)))

(defn parse-opts
  [opts]
  (let [other-keys    (->> opts keys (remove #{:max-fuel :maxFuel}))
        max-fuel-opts {:max-fuel (or (:max-fuel opts) (:maxFuel opts))}
        merged-opts   (merge max-fuel-opts (select-keys opts other-keys))]
    (if (or (:max-fuel merged-opts) (:meta merged-opts))
      (assoc merged-opts ::track-fuel? true)
      merged-opts)))

(defn cartesian-merge
  "Like a cartesian product, but performs a map
  merge across all possilble combinations
  of collections."
  [colls]
  (if (empty? colls)
    '(())
    (for [more (cartesian-merge (rest colls))
          x    (first colls)]
      (merge x more))))
