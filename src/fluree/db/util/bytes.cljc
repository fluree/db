(ns fluree.db.util.bytes
  (:require #?(:clj [byte-streams :as bs])
            #?(:cljs [goog.crypt :as gcrypt])))

#?(:clj (set! *warn-on-reflection* true))


(defn string->UTF8
  [x]
  #?(:clj  (.getBytes ^String x "UTF-8")
     :cljs (gcrypt/stringToUtf8ByteArray x)))


(defn to-reader
  [x]
  #?(:clj  (bs/to-reader x)
     :cljs (throw (js/Error. "bytes/to-reader not supported in javascript."))))


(defn UTF8->string
  [arr]
  #?(:clj  (String. (byte-array arr) "UTF8")
     :cljs (gcrypt/utf8ByteArrayToString (apply array arr))))

