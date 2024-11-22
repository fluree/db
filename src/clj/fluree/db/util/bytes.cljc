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
  (let [size (count utf8)]
    (if (> size 8)
      (throw (ex-info "Can't encode more than 8 bytes into a Long"
                      {:value utf8, :status 500, :error :db/unexpected-error}))
      (loop [result   0
             [b & bs] utf8]
        (if b
          (recur (bit-or (bit-shift-left result 8)
                         (bit-and b 0xFF))
                 bs)
          (if (< size 8)
            (let [diff (- 8 size)]
              (bit-shift-left result (* diff 8)))
            result))))))


(defn long->UTF8
  [l]
  (->> [56 48 40 32 24 16 8 0] ;; byte offsets
       (map (fn [i] (bit-and (bit-shift-right l i) 0xFF)))
       (remove zero?) ;; get rid of trailing padding
       #?(:clj (byte-array)
          :cljs (.from js/Int8Array))))
