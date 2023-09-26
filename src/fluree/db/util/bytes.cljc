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


(defn UTF8->long
  [utf8]
  (if (> (count utf8) 8)
    (throw (ex-info "Can't encode more than 8 bytes into a Long"
                    {:value utf8, :status 500, :error :db/unexpected-error}))
    (loop [result   0
           [b & bs] utf8]
      (if b
        (recur (bit-or (bit-shift-left result 8)
                       (bit-and b 255))
               bs)
        result))))

(defn long->UTF8
  [n]
  (loop [result '()
         n'     n]
    (if (= 0 n')
      #?(:clj (byte-array result)
         :cljs (.from js/Uint8Array result))
      (recur (conj result (bit-and n' 255))
             (bit-shift-right n' 8)))))
