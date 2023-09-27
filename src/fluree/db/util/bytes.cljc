(ns fluree.db.util.bytes
  #?(:cljs (:require [goog.crypt :as gcrypt])))

#?(:clj (set! *warn-on-reflection* true))


(defn string->UTF8
  [x]
  #?(:clj  (.getBytes ^String x "UTF-8")
     :cljs (gcrypt/stringToUtf8ByteArray x)))


(defn UTF8->string
  [arr]
  #?(:clj  (String. (byte-array arr) "UTF8")
     :cljs (gcrypt/utf8ByteArrayToString (apply array arr))))


(defn UTF8->long
  [utf8]
  (if (> (count utf8) 8)
    (throw (ex-info "Can't encode more than 8 bytes into a Long"
                    {:value utf8, :status 500, :error :db/unexpected-error}))
    (loop [result   0
           [b & bs] utf8]
      (if b
        (recur (bit-or (bit-shift-left result 8)
                       (bit-and b 0xFF))
               bs)
        result))))


(defn long->UTF8
  [n]
  (loop [result '()
         n'     n]
    (if (= 0 n')
      #?(:clj (byte-array result)
         :cljs (.from js/Int8Array result))
      (recur (conj result (bit-and n' 0xFF))
             (bit-shift-right n' 8)))))

(defn long-encode-str
  [s]
  (-> s string->UTF8 UTF8->long))

(defn str-decode-long
  [n]
  (-> n long->UTF8 UTF8->string))
