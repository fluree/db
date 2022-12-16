(ns fluree.common.util
  (:require [clojure.string :as str])
  #?(:clj (:import (java.time Instant))))

(defn string->bytes
  "Convert `s` to bytes."
  [s]
  #?(:clj (.getBytes ^String s)
     :cljs (js/Uint8Array. (js/Buffer.from s "utf8"))))

(defn ensure-trailing-slash
  "If `s` ends with a slash, returns it, otherwise appends \"/\"."
  [s]
  (if (str/ends-with? s "/")
    s
    (str s "/")))

(defn current-time-iso
  "Returns current time as string for ISO-8601 format"
  []
  #?(:clj  (str (Instant/now))
     :cljs (.toISOString (js/Date.))))

(defn exception?
  "Cross-platform, returns true if is an exception"
  [x]
  (instance? #?(:clj Throwable :cljs js/Error) x))
