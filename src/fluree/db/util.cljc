(ns fluree.db.util
  #?(:clj (:require [clojure.string :as str]
                    [fluree.db.util.clj-exceptions :as clj-exceptions]
                    [fluree.db.util.cljs-exceptions :as cljs-exceptions]))
  #?(:cljs (:require-macros [fluree.db.util :refer [case+]]))
  #?(:clj (:import (java.time Instant OffsetDateTime ZoneId)
                   (java.time.format DateTimeFormatter)))
  (:refer-clojure :exclude [vswap!]))

#?(:clj (set! *warn-on-reflection* true))

(def ^:const max-long #?(:clj  Long/MAX_VALUE
                         :cljs 9007199254740991))           ;; 2^53-1 for javascript
(def ^:const min-long #?(:clj  Long/MIN_VALUE
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
           [catch sym & catch-body :as _catch-form] (last body)]
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

(defn machine-id
  "Returns a machine identifier in format 'hostname:pid'.

  This identifier is used to track which process is performing operations
  like indexing, allowing detection of stale/failed processes across a
  distributed system.

  Attempts to use environment-specific identifiers when available:
  - AWS Lambda: Uses AWS_LAMBDA_LOG_STREAM_NAME (unique per invocation)
  - AWS EC2: Uses HOSTNAME env var (often instance ID)
  - Kubernetes: Uses pod hostname (typically pod name)
  - Docker: Uses container hostname
  - Traditional servers: Uses system hostname via InetAddress

  Examples:
    - 'server-1.example.com:12345' (traditional server)
    - 'fluree-pod-abc123:1' (Kubernetes pod)
    - '2021/01/01/[$LATEST]abc123:1' (AWS Lambda)
    - 'i-0abcdef1234567890:12345' (EC2 with instance ID as hostname)
    - 'localhost:54321' (development)"
  []
  #?(:clj  (let [;; Try AWS Lambda log stream name first, then hostname, then fallback
                 hostname (or (System/getenv "AWS_LAMBDA_LOG_STREAM_NAME")
                              (System/getenv "HOSTNAME")
                              (try
                                (.getHostName (java.net.InetAddress/getLocalHost))
                                (catch Exception _
                                  "unknown")))
                 pid      (try
                            (-> (java.lang.management.ManagementFactory/getRuntimeMXBean)
                                (.getName)
                                (str/split #"@")
                                first)
                            (catch Exception _
                              "unknown"))]
             (str hostname ":" pid))
     :cljs (if (exists? js/process)
             ;; Node.js environment - prefer AWS Lambda log stream, then hostname
             (let [hostname (or (.-AWS_LAMBDA_LOG_STREAM_NAME js/process.env)
                                (.-HOSTNAME js/process.env)
                                (when (exists? js/os)
                                  (try (.hostname js/os)
                                       (catch js/Error _ "node")))
                                "node")
                   pid      (.-pid js/process)]
               (str hostname ":" pid))
             ;; Browser environment - use timestamp-based identifier
             (str "browser:" (js/Date.now)))))

(defn response-time-formatted
  "Returns response time, formatted as string. Must provide start time of request
   for clj as (System/nanoTime), or for cljs epoch milliseconds"
  [start-time]
  #?(:clj  (-> (- (System/nanoTime) start-time)
               (/ 1000000)
               (#(format "%.2fms" (float %))))
     :cljs (-> (- (current-time-millis) start-time)
               (str "ms"))))

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
  #?(:clj
     ([millis zone-id]
      (-> millis Instant/ofEpochMilli
          (OffsetDateTime/ofInstant (ZoneId/of zone-id))
          (.format DateTimeFormatter/ISO_OFFSET_DATE_TIME)))

     :cljs
     ([millis _]
      (-> millis js/Date. .toISOString))))

(defn filter-vals
  "Filters map k/v pairs dropping any where predicate applied to value is false."
  [pred m]
  (reduce-kv (fn [m k v] (if (pred v) (assoc m k v) m)) {} m))

(defn without-nils
  "Remove all keys from a map that have nil or empty collection values."
  [m]
  (filter-vals #(if (coll? %) (not-empty %) (some? %)) m))

(defn exception?
  "x-platform, returns true if is an exception"
  [x]
  (instance? #?(:clj Throwable :cljs js/Error) x))

(defn conjv
  "Like conj, but if collection is nil creates a new vector instead of list.
  Not built to handle variable arity values"
  [coll x]
  (if (nil? coll)
    (vector x)
    (conj coll x)))

(defn sequential
  "Returns input wrapped in a vector if not already sequential."
  [x]
  (if (sequential? x)
    x
    [x]))

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
  [jsonld k]
  (let [v (get jsonld k)]
    (if (sequential? v)
      (first v)
      v)))

(defn get-types
  [jsonld]
  (get jsonld "@type"))

(defn of-type?
  "Returns true if the provided json-ld node is of the provided type."
  [jsonld rdf-type]
  (->> jsonld
       get-types
       sequential
       (some #(= % rdf-type))))

(defn get-value
  [val]
  (if (map? val)
    (get val "@value")
    val))

(defn get-list
  [v-map]
  (get v-map "@list"))

(defn get-lang
  [v-map]
  (get v-map "@language"))

(defn get-datatype
  [node]
  (when (contains? node "@value")
    (get-types node)))

(defn get-first-value
  [jsonld k]
  (-> jsonld
      (get-first k)
      get-value))

(defn get-values
  [jsonld k]
  (mapv get-value (get jsonld k)))

(defn get-id
  [jsonld]
  (get jsonld "@id"))

(defn get-first-id
  [jsonld k]
  (-> jsonld
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
  the @list can look like:
  {ex:someProperty [{@list [ex:val1 ex:val2]}]}
  or in single-cardinality form:
  {ex:someProperty {@list [ex:val1 ex:val2]}}

  If @list is not present, return original 'vals' argument wrapped in a sequence if not already.
  Always returns a sequence."
  [vals]
  (let [first-val (if (sequential? vals)
                    (first vals)
                    vals)
        list-vals (when (map? first-val)
                    (get first-val "@list"))]
    (sequential (or list-vals vals))))

(defn get-all-ids
  "Returns all @id values for a given key in a jsonld node.

  If values are contained in a @list, unwraps them.

  Elides any scalar values (those without an @id key)."
  [jsonld k]
  (some->> (get jsonld k)
           unwrap-list
           (keep get-id)))

(defn get-graph
  [jsonld]
  (if (contains? jsonld "@graph")
    (get jsonld "@graph")
    jsonld))
