(ns fluree.db.util.core
  (:require [clojure.string :as str]
            #?(:clj  [clojure.core.async :refer [go <!] :as async]
               :cljs [cljs.core.async :refer [go <! put!] :as async])
            #?(:cljs [cljs.js :as cljs])
            #?@(:clj [[fluree.db.util.clj-exceptions :as clj-exceptions]
                      [fluree.db.util.cljs-exceptions :as cljs-exceptions]]))
  #?(:clj  (:import (java.util UUID Date)
                    (java.time Instant)
                    (java.net URLEncoder URLDecoder))
     :cljs (:refer-clojure :exclude [random-uuid])))


;; javascript is 2^53 - 1
(def ^:const max-long #?(:clj  (Long/MAX_VALUE)
                         :cljs (- 2r11111111111111111111111111111111111111111111111111111 1)))
(def ^:const min-long (- max-long))
(def ^:const max-integer 2r1111111111111111111111111111111)
(def ^:const min-integer (- max-integer))

(defn cljs-env?
  "Take the &env from a macro, and tell whether we are expanding into cljs."
  [env]
  (boolean (:ns env)))

(defmacro if-cljs
  "Return then if we are generating cljs code and else for Clojure code.
   https://groups.google.com/d/msg/clojurescript/iBY5HaQda4A/w1lAQi9_AwsJ"
  [then else]
  (if (cljs-env? &env) then else))

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
       (try ~@try-body (~'catch Throwable ~sym ~@catch-body)))))

(declare catch*)

(defmacro try*
  "Like try but supports catch*. catch* is like catch but supports CLJ/CLJS with
  less boilerplate. In CLJ it catches `Exception`. In CLJS it catches `:default`.
  Use it like this: `(try* ... (catch* err (handle-err err)))`.
  Also supports an optional finally clause."
  [& body]
  `(if-cljs
     (cljs-exceptions/try* ~@body)
     (clj-exceptions/try* ~@body)))

;; index-of from: https://gist.github.com/fgui/48443e08844e42c674cd
(defn index-of [coll value]
  (some (fn [[item idx]] (if (= value item) idx))
        (partition 2 (interleave coll (iterate inc 0)))))

(defn random-uuid []
  "Generates random UUID in both clojure/script"
  #?(:clj  (UUID/randomUUID)
     :cljs (clojure.core/random-uuid)))

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
               (.toEpochMilli date)

               (instance? Date date)
               (.getTime date)]
        :cljs [(instance? js/Date date)
               (.getTime date)])

    :else
    (throw (ex-info (str "Invalid date: " (pr-str date))
                    {:status 400 :error :db/invalid-date}))))


(defn current-time-millis
  "Returns current time in epoch milliseonds for closure/script"
  []
  #?(:clj  (System/currentTimeMillis)
     :cljs (.getTime (js/Date.))))

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


(defn subj-ident?
  "Tests if an _id is a numeric or predicate-ident"
  [x]
  (or (int? x)
      (pred-ident? x)))


(defn str->int
  "Converts string to integer. Assumes you've already verified the string is
  parsable to an integer."
  [s]
  #?(:clj  (Integer/parseInt s)
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


(defn str->epoch-ms
  "Takes time as a string and returns an java.time.Instant."
  [time-str]
  (try
    #?(:clj  (.toEpochMilli (Instant/parse time-str))
       :cljs (js/Date.parse time-str))
    (catch #?(:clj Exception :cljs :default) _
      (throw (ex-info (str "Invalid time string. Ensure format is ISO-8601 compatible. Provided: " (pr-str time-str))
                      {:status 400
                       :error  :db/invalid-time})))))

(defn trunc
  "Truncate string s to n characters."
  [s n]
  (if (< (count s) n)
    s
    (str (subs s 0 n) " ...")))

(defmacro some-of
  ([] nil)
  ([x] x)
  ([x & more]
   `(let [x# ~x] (if (nil? x#) (some-of ~@more) x#))))

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
  "x-platform, returns true if is an execption"
  [x]
  (instance? #?(:clj Throwable :cljs js/Error) x))


(defn url-encode
  [string]
  #?(:clj  (some-> string str (URLEncoder/encode "UTF-8") (.replace "+" "%20"))
     :cljs (some-> string str (js/encodeURIComponent) (.replace "+" "%20"))))

(defn url-decode
  ([string] (url-decode string "UTF-8"))
  ([string encoding]
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
