(ns fluree.db.flake
  (:refer-clojure :exclude [split-at sorted-set-by take])
  (:require [clojure.data.avl :as avl]
            [fluree.db.constants :as const]
            #?(:clj [abracad.avro :as avro]))
  #?(:cljs (:require-macros [fluree.db.flake :refer [combine-cmp]])))

#?(:clj (set! *warn-on-reflection* true))

;; maximum number of collections. 19 bits - 524,287 - javascript 9 bits - 511
(def ^:const MAX-COLLECTION-ID #?(:clj  2r1111111111111111111
                                  :cljs 2r111111111))
;; maximum number of subject indexes within a given collection. 44 bits - 17,592,186,044,415
;; javascript, 44 bits - 1
(def ^:const MAX-COLL-SUBJECTS #?(:clj  2r11111111111111111111111111111111111111111111
                                  :cljs (- 2r11111111111111111111111111111111111111111111 1)))

(declare equiv-flake assoc-flake get-flake-val nth-flake)

(defn lshift
  [n bits]
  #?(:clj  (bit-shift-left n bits)
     :cljs (* n (js/Math.pow 2 bits))))


(defn ->sid
  "Converts a collection id and a number (n) into a subject-id."
  [cid n]
  (+ (lshift cid 44) n))


(defn ->sid-checked
  "Like ->sid, but checks that cid and n are within allowable range."
  [cid n]
  (assert (< 0 cid MAX-COLLECTION-ID) (str "Collection id is out of allowable range of 0 - " MAX-COLLECTION-ID))
  (assert (< 0 n MAX-COLL-SUBJECTS) (str "Subject number is out of allowable range of 0 - " MAX-COLL-SUBJECTS))
  (->sid cid n))


(defn min-subject-id
  "For a given collection id, returns the min subject id that is allowed."
  [cid]
  (->sid cid 0))


(defn max-subject-id
  "For a given collection id, returns the max subject id that is allowed."
  [cid]
  (->sid cid MAX-COLL-SUBJECTS))


(def ^:const MIN-PREDICATE-ID (min-subject-id const/$_predicate))
(def ^:const MAX-PREDICATE-ID (max-subject-id const/$_predicate))


(defn sid->cid
  "Will return a collection id from a subject-id."
  [sid]
  #?(:clj  (bit-shift-right sid 44)
     :cljs (js/Math.floor (lshift sid -44))))


(defn sid->i
  "Returns the subject index from a subject-id."
  [sid]
  (- sid (lshift (sid->cid sid) 44)))


(deftype Flake [s p o t op m]
  #?@(:clj  [avro/AvroSerializable
             (schema-name [_] "fluree.Flake")
             (field-get [f field] (get f field))
             (field-list [_] #{:s :p :o :t :op :m})

             clojure.lang.Seqable
             (seq [f] (list (.-s f) (.-p f) (.-o f) (.-t f) (.-op f) (.-m f)))

             clojure.lang.Indexed
             (nth [f i] (nth-flake f i nil))
             (nth [f i not-found] (nth-flake f i not-found))

             clojure.lang.ILookup
             (valAt [f k] (get-flake-val f k nil))
             (valAt [f k not-found] (get-flake-val f k not-found))

             clojure.lang.IPersistentCollection
             (equiv [f o] (and (instance? Flake o) (equiv-flake f o)))
             (empty [f] (throw (UnsupportedOperationException. "empty is not supported on Flake")))
             (count [f] 6)
             (cons [f [k v]] (assoc-flake f k v))

             clojure.lang.IPersistentMap
             (assocEx [f k v] (UnsupportedOperationException. "assocEx is not supported on Flake"))
             (without [f k] (UnsupportedOperationException. "without is not supported on Flake"))

             clojure.lang.Associative
             (entryAt [f k] (some->> (get f k nil) (clojure.lang.MapEntry k)))
             (containsKey [_ k] (boolean (#{:s :p :o :t :op :m} k)))
             (assoc [f k v] (assoc-flake f k v))

             Object
             (hashCode [f] (hash (seq f)))

             clojure.lang.IHashEq
             (hasheq [f] (hash (seq f)))

             java.lang.Iterable
             (iterator [this]
               (let [xs (clojure.lang.Box. (seq this))]
                 (reify java.util.Iterator
                   (next [this]
                     (locking xs
                       (if-let [v (.-val xs)]
                         (let [x (first v)]
                           (set! (.-val xs) (next v))
                           x)
                         (throw
                           (java.util.NoSuchElementException.
                             "no more elements in VecSeq iterator")))))
                   (hasNext [this]
                     (locking xs
                       (not (nil? (.-val xs)))))
                   (remove [this]
                     (throw (UnsupportedOperationException. "remove is not supported on Flake"))))))

             java.util.Collection
             (contains [this o] (boolean (some #(= % o) this)))
             (containsAll [this c] (every? #(.contains this %) c))
             (isEmpty [_] false)
             (toArray [this] (into-array Object this))]

      :cljs [ILookup
             (-lookup [this k] (get-flake-val this k nil))
             (-lookup [this k not-found] (get-flake-val this k not-found))

             IIndexed
             (-nth [this i] (nth-flake this i nil))
             (-nth [this i not-found] (nth-flake this i not-found))

             ISeqable
             (-seq [this] (list (.-s this) (.-p this) (.-o this) (.-t this) (.-op this) (.-m this)))

             IHash
             (-hash [this] (hash (seq this)))

             IEquiv
             (-equiv [this o] (and (instance? Flake o) (equiv-flake this o)))

             IAssociative
             (-assoc [this k v] (assoc-flake this k v))

             IPrintWithWriter
             (-pr-writer [^Flake f writer opts]
                         (pr-sequential-writer writer pr-writer
                                               "#Flake [" " " "]"
                                               opts [(.-s f) (.-p f) (.-o f) (.-t f) (.-op f) (.-m f)]))]))


#?(:clj (defmethod print-method Flake [^Flake f, ^java.io.Writer w]
          (.write w (str "#Flake "))
          (binding [*out* w]
            (pr [(.-s f) (.-p f) (.-o f) (.-t f) (.-op f) (.-m f)]))))


(defn- equiv-flake
  [^Flake f ^Flake o]
  (and (= (.-s f) (.-s o))
       (= (.-p f) (.-p o))
       (= (.-o f) (.-o o))))

(defn parts->Flake
  "Used primarily to generate flakes for comparator. If you wish to
  generate a flake for other purposes, be sure to supply all components."
  ([[s p o t op m]]
   (->Flake s p o t op m))
  ([[s p o t op m] default-tx]
   (->Flake s p o (or t default-tx) op m))
  ([[s p o t op m] default-tx default-op]
   (->Flake s p o (or t default-tx) (or op default-op) m)))


(defn Flake->parts
  [^Flake flake]
  [(.-s flake) (.-p flake) (.-o flake) (.-t flake) (.-op flake) (.-m flake)])


(defn- assoc-flake
  "Assoc for Flakes"
  [^Flake flake k v]
  (let [[s p o t op m] (Flake->parts flake)]
    (case k
      :s (->Flake v p o t op m)
      :p (->Flake s v o t op m)
      :o (->Flake s p v t op m)
      :t (->Flake s p o v op m)
      :op (->Flake s p o t v m)
      :m (->Flake s p o t op v)
      #?(:clj  (throw (IllegalArgumentException. (str "Flake does not contain key: " k)))
         :cljs (throw (js/Error. (str "Flake does not contain key: " k)))))))


(defn- get-flake-val
  [^Flake flake k not-found]
  (case k
    :s (.-s flake) "s" (.-s flake)
    :p (.-p flake) "p" (.-p flake)
    :o (.-o flake) "o" (.-o flake)
    :t (.-t flake) "t" (.-t flake)
    :op (.-op flake) "op" (.-op flake)
    :m (.-m flake) "m" (.-m flake)
    not-found))


(defn- nth-flake
  "Gets position i in flake."
  [^Flake flake i not-found]
  (let [ii (int i)]
    (case ii 0 (.-s flake)
             1 (.-p flake)
             2 (.-o flake)
             3 (.-t flake)
             4 (.-op flake)
             5 (.-m flake)
             (or not-found
                 #?(:clj  (throw (IndexOutOfBoundsException.))
                    :cljs (throw (js/Error. (str "Index " i " out of bounds for flake: " flake))))))))


#?(:clj
   (defmacro combine-cmp [& comps]
     (loop [comps (reverse comps)
            res   (num 0)]
       (if (not-empty comps)
         (recur
           (next comps)
           `(let [c# ~(first comps)]
              (if (== 0 c#)
                ~res
                c#)))
         res))))


(defn cmp-val [o1 o2]
  (if (and (some? o1) (some? o2))
    (compare o1 o2)
    0))


(defn cc-cmp-class [x]
  (if (string? x)
    "string"
    "number"))

;; if possibly doing cross-type value comparison, use this instead
(defn cmp-val-xtype [o1 o2]
  (if (and (some? o1) (some? o2))
    (let [o1-str   (cc-cmp-class o1)
          o2-str   (cc-cmp-class o2)
          type-cmp (compare o1-str o2-str)]
      (if (= 0 type-cmp)
        (compare o1 o2)
        type-cmp))
    0))


(defn cmp-bool [b1 b2]
  (if (and (boolean? b1) (boolean? b2))
    #?(:clj (Boolean/compare b1 b2) :cljs (compare b1 b2))
    0))

(defn cmp-meta [m1 m2]
  "Meta will always be a map or nil, but can be searched using an integer to
  perform effective range scans if needed.
  i.e. (Integer/MIN_VALUE) to (Integer/MAX_VALUE) will always include all meta values."
  (let [m1h (if (int? m1) m1 (hash m1))
        m2h (if (int? m2) m2 (hash m2))]
    #?(:clj (Integer/compare m1h m2h) :cljs (- m1h m2h))))


(defn cmp-pred [p1 p2]
  (if (and p1 p2)
    #?(:clj (Long/compare p1 p2) :cljs (- p1 p2))
    0))

(defn cmp-long [l1 l2]
  (if (and l1 l2)
    #?(:clj (Long/compare l1 l2) :cljs (- l1 l2))
    0))


(defn cmp-flakes-spot [^Flake f1, ^Flake f2]
  (combine-cmp
    (cmp-long (.-s f2) (.-s f1))                            ;; reversed
    (cmp-pred (.-p f1) (.-p f2))
    (cmp-val (.-o f1) (.-o f2))
    (cmp-meta (.-m f1) (.-m f2))))


(defn cmp-flakes-psot [^Flake f1, ^Flake f2]
  (combine-cmp
    (cmp-pred (.-p f1) (.-p f2))
    (cmp-long (.-s f2) (.-s f1))                            ;; reversed
    (cmp-val (.-o f1) (.-o f2))
    (cmp-meta (.-m f1) (.-m f2))))


(defn cmp-flakes-post [^Flake f1, ^Flake f2]
  (combine-cmp
    (cmp-pred (.-p f1) (.-p f2))
    (cmp-val (.-o f1) (.-o f2))
    (cmp-long (.-s f2) (.-s f1))                            ;; reversed
    (cmp-meta (.-m f1) (.-m f2))))

;; note that opst sorts values in reverse order (as they are subjects)
(defn cmp-flakes-opst [^Flake f1, ^Flake f2]
  (combine-cmp
    (cmp-long (.-o f2) (.-o f1))                            ;; reversed
    (cmp-long (.-p f1) (.-p f2))
    (cmp-long (.-s f2) (.-s f1))                            ;; reversed
    (cmp-meta (.-m f1) (.-m f2))))



;; when we look up an item in history, we can quickly find the relevant items, then apply changes in reverse
;; the alternative would be to reverse an entire node, which might work better for generic caching purposes.

(defn cmp-flakes-spot-novelty [^Flake f1, ^Flake f2]
  (combine-cmp
    (cmp-long (.-s f2) (.-s f1))                            ;; reversed
    (cmp-pred (.-p f1) (.-p f2))
    (cmp-val-xtype (.-o f1) (.-o f2))
    (cmp-long (.-t f2) (.-t f1))                            ;; reversed
    (cmp-bool (.-op f1) (.-op f2))
    (cmp-meta (.-m f1) (.-m f2))))


(defn cmp-flakes-psot-novelty [^Flake f1, ^Flake f2]
  (combine-cmp
    (cmp-pred (.-p f1) (.-p f2))
    (cmp-long (.-s f2) (.-s f1))                            ;; reversed
    (cmp-val-xtype (.-o f1) (.-o f2))
    (cmp-long (.-t f2) (.-t f1))                            ;; reversed
    (cmp-bool (.-op f1) (.-op f2))
    (cmp-meta (.-m f1) (.-m f2))))


(defn cmp-flakes-post-novelty [^Flake f1, ^Flake f2]
  (combine-cmp
    (cmp-pred (.-p f1) (.-p f2))
    (cmp-val-xtype (.-o f1) (.-o f2))
    (cmp-long (.-s f2) (.-s f1))                            ;; reversed
    (cmp-long (.-t f2) (.-t f1))                            ;; reversed
    (cmp-bool (.-op f1) (.-op f2))
    (cmp-meta (.-m f1) (.-m f2))))


(defn cmp-flakes-opst-novelty [^Flake f1, ^Flake f2]
  (combine-cmp
    (cmp-long (.-o f2) (.-o f1))                            ;; reversed
    (cmp-pred (.-p f1) (.-p f2))
    (cmp-long (.-s f2) (.-s f1))                            ;; reversed
    (cmp-long (.-t f2) (.-t f1))                            ;; reversed
    (cmp-bool (.-op f1) (.-op f2))
    (cmp-meta (.-m f1) (.-m f2))))


(defn cmp-flakes-block [^Flake f1, ^Flake f2]
  "Comparison for flakes in blocks.
  Like cmp-flakes-spot-novelty, but 't' is moved up front."
  (combine-cmp
    (cmp-long (.-t f2) (.-t f1))                            ;; reversed
    (cmp-long (.-s f2) (.-s f1))                            ;; reversed
    (cmp-pred (.-p f1) (.-p f2))
    (cmp-val-xtype (.-o f1) (.-o f2))
    (cmp-bool (.-op f1) (.-op f2))
    (cmp-meta (.-m f1) (.-m f2))))


(defn cmp-flakes-history
  "Note this is not suitable for a set, only a vector/list."
  [^Flake f1, ^Flake f2]
  (combine-cmp
    (cmp-long (.-t f1) (.-t f2))
    #?(:clj  (Boolean/compare (.-op f2) (.-op f1))
       :cljs (compare (.-op f2) (.-op f1)))))


(defn cmp-history-quick-reverse-sort
  "Sorts by transaction time in ascending order (newest first), then by
  the boolean operation descending so assertions (true) come before retractions (false)
  so that we can 're-play' the log in reverse order to come up with historical states.
  Suitable only for sorting a vector, not a sorted set."
  [^Flake f1, ^Flake f2]
  (combine-cmp
    (cmp-long (.-t f1) (.-t f2))
    #?(:clj  (Boolean/compare (.-op f2) (.-op f1))
       :cljs (compare (.-op f2) (.-op f1)))))


(defn new-flake
  [& parts]
  (let [[s p o t op m] parts]
    (->Flake s p o t op m)))


(defn flip-flake
  "Takes a flake and returns one with the provided block and .-op flipped from true/false.
  Don't over-ride no-history, even if no-history for this predicate has changed. New inserts
  will have the no-history flag, but we need the old inserts to be properly retracted in the txlog."
  ([^Flake flake]
   (->Flake (.-s flake) (.-p flake) (.-o flake) (.-t flake) (not (.-op flake)) (.-m flake)))
  ([^Flake flake t]
   (->Flake (.-s flake) (.-p flake) (.-o flake) t (not (.-op flake)) (.-m flake))))

(defn change-t
  "Takes a flake and returns one with the provided block and .-op flipped from true/false.
  Don't over-ride no-history, even if no-history for this predicate has changed. New inserts
  will have the no-history flag, but we need the old inserts to be properly retracted in the txlog."
  ([^Flake flake t]
   (->Flake (.-s flake) (.-p flake) (.-o flake) t (.-op flake) (.-m flake))))



(defn slice
  "From and to are Flakes"
  [ss from to]
  (cond
    (and from to) (avl/subrange ss >= from <= to)
    (nil? from) (avl/subrange ss <= to)
    (nil? to) (avl/subrange ss >= from)
    :else (throw (ex-info "Unexpected error performing slice, both from and to conditions are nil. Please report."
                          {:status 500
                           :error  :db/unexpected-error}))))

(defn lookup
  [ss start-flake end-flake]
  ;(log/warn "index-range-flakes" {:start-test >= :start-flake start-flake :end-test <= :end-flake end-flake})
  (avl/subrange ss >= start-flake <= end-flake))

(defn subrange
  [ss start-test start-flake end-test end-flake]
  (avl/subrange ss start-test start-flake end-test end-flake))


(defn split-at
  [n ss]
  (avl/split-at n ss))


(defn split-by-flake
  "Splits a sorted set at a given flake. If there is an exact match for flake,
  puts it in the left-side. Primarily for use with last-flake."
  [^Flake f ss]
  (let [[l e r] (avl/split-key f ss)]
    [(if e (conj l e) l) r]))


(defn sorted-set-by
  [comparator & flakes]
  (apply avl/sorted-set-by comparator flakes))


(defn size-flake
  "Base size of a flake is 38 bytes... then add size for 'o' and 'm'.
  Flakes have the following:
    - s - 8 bytes
    - p - 8 bytes
    - o - ??
    - t - 8 bytes
    - add? - 1 byte
    - m - 1 byte + ??
    - header - 12 bytes - object header...

  Objects will be rounded up to nearest 8 bytes... we don't do this here as
  it should be 'close enough'
  reference: https://www.javamex.com/tutorials/memory/string_memory_usage.shtml"
  [^Flake f]
  (let [o (.-o f)]
    (+ 37 #?(:clj  (condp = (type o)
                     String (+ 38 (* 2 (count o)))
                     Long 8
                     Double 8
                     Integer 4
                     Float 4
                     Boolean 1
                     ;; else
                     (count (pr-str o)))
             :cljs (count (pr-str o)))
       (if (nil? (.-m f))
         1
         (* 2 (count (pr-str (.-m f))))))))


(defn size-bytes
  "Returns approx number of bytes in a collection of flakes."
  [flakes]
  (reduce #(+ %1 (size-flake %2)) 0 flakes))


(defn size-kb
  "Like size-bytes, but kb.
  Rounds down for simplicity, as bytes is just an estimate anyhow."
  [flakes]
  (-> (size-bytes flakes)
      (/ 1000)
      (double)
      (Math/round)))


(defn take
  "Takes n flakes from a sorted flake set, retaining the set itself."
  [n flake-set]
  (if (>= n (count flake-set))
    flake-set
    (let [k (nth flake-set n)]
      (first (avl/split-key k flake-set)))))

