(ns fluree.db.flake
  (:refer-clojure :exclude [split-at sorted-set-by sorted-map-by take last])
  (:require [clojure.data.avl :as avl]
            [fluree.db.constants :as const]
            [fluree.db.util.core :as util]
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

(def inc-t
  "Increments a transaction value"
  dec)

(def dec-t
  "Decrements a transaction value"
  inc)

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

(defn s
  [^Flake f]
  (.-s f))

(defn p
  [^Flake f]
  (.-p f))

(defn o
  [^Flake f]
  (.-o f))

(defn t
  [^Flake f]
  (.-t f))

(defn op
  [^Flake f]
  (.-op f))

(defn m
  [^Flake f]
  (.-m f))

(defn- equiv-flake
  [f other]
  (and (= (s f) (s other))
       (= (p f) (p other))
       (= (o f) (o other))))

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
  [flake]
  [(s flake) (p flake) (o flake) (t flake) (op flake) (m flake)])

(def maximum
  "The largest flake possible"
  (->Flake util/max-long 0 util/max-long 0 true nil))

(defn- assoc-flake
  "Assoc for Flakes"
  [flake k v]
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
  [flake k not-found]
  (case k
    :s (s flake) "s" (s flake)
    :p (p flake) "p" (p flake)
    :o (o flake) "o" (o flake)
    :t (t flake) "t" (t flake)
    :op (op flake) "op" (op flake)
    :m (m flake) "m" (m flake)
    not-found))


(defn- nth-flake
  "Gets position i in flake."
  [flake i not-found]
  (let [ii (int i)]
    (case ii 0 (s flake)
             1 (p flake)
             2 (o flake)
             3 (t flake)
             4 (op flake)
             5 (m flake)
             (or not-found
                 #?(:clj  (throw (IndexOutOfBoundsException.))
                    :cljs (throw (js/Error. (str "Index " ii " out of bounds for flake: " flake))))))))

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

(defn cmp-val-xtype
  "Use this instead of `cmp-val` if possibly doing cross-type value comparison"
  [o1 o2]
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

(defn cmp-meta
  "Meta will always be a map or nil, but can be searched using an integer to
  perform effective range scans if needed. i.e. (Integer/MIN_VALUE)
  to (Integer/MAX_VALUE) will always include all meta values."
  [m1 m2]
  (let [m1h (if (int? m1) m1 (hash m1))
        m2h (if (int? m2) m2 (hash m2))]
    #?(:clj (Integer/compare m1h m2h) :cljs (- m1h m2h))))


(defn cmp-long [l1 l2]
  (if (and l1 l2)
    #?(:clj (Long/compare l1 l2) :cljs (- l1 l2))
    0))

(defn cmp-subj
  "Comparator for subject values. The supplied values are reversed before the
  comparison to account for the decreasing sort order of subjects"
  [s1 s2]
  (cmp-long s2 s1))

(defn cmp-pred [p1 p2]
  (cmp-long p1 p2))

(defn cmp-tx
  "Comparator for transaction values. The supplied values are reversed before the
  comparison to account for the decreasing sort order of transactions"
  [t1 t2]
  (cmp-long t2 t1))

(defn cmp-obj
  [o1 o2]
  (cmp-val-xtype o1 o2))

(defn cmp-op
  [op1 op2]
  (cmp-bool op1 op2))

(defn cmp-flakes-spot [f1 f2]
  (combine-cmp
    (cmp-subj (s f1) (s f2))
    (cmp-pred (p f1) (p f2))
    (cmp-obj (o f1) (o f2))
    (cmp-tx (t f1) (t f2))
    (cmp-bool (op f1) (op f2))
    (cmp-meta (m f1) (m f2))))

(defn cmp-flakes-psot [f1 f2]
  (combine-cmp
    (cmp-pred (p f1) (p f2))
    (cmp-subj (s f1) (s f2))
    (cmp-obj (o f1) (o f2))
    (cmp-tx (t f1) (t f2))
    (cmp-bool (op f1) (op f2))
    (cmp-meta (m f1) (m f2))))


(defn cmp-flakes-post [f1 f2]
  (combine-cmp
    (cmp-pred (p f1) (p f2))
    (cmp-obj (o f1) (o f2))
    (cmp-subj (s f1) (s f2))
    (cmp-tx (t f1) (t f2))
    (cmp-bool (op f1) (op f2))
    (cmp-meta (m f1) (m f2))))


(defn cmp-flakes-opst [f1 f2]
  (combine-cmp
    (cmp-subj (o f1) (o f2))
    (cmp-pred (p f1) (p f2))
    (cmp-subj (s f1) (s f2))
    (cmp-tx (t f1) (t f2))
    (cmp-bool (op f1) (op f2))
    (cmp-meta (m f1) (m f2))))


(defn cmp-flakes-block
  "Comparison for flakes in blocks. Like cmp-flakes-spot, but with 't'
  moved up front."
  [f1 f2]
  (combine-cmp
   (cmp-tx (t f1) (t f2))
   (cmp-subj (s f1) (s f2))
   (cmp-pred (p f1) (p f2))
   (cmp-obj (o f1) (o f2))
   (cmp-bool (op f1) (op f2))
   (cmp-meta (m f1) (m f2))))


(defn cmp-flakes-history
  "Note this is not suitable for a set, only a vector/list."
  [f1 f2]
  (combine-cmp
    (cmp-long (t f1) (t f2))
    #?(:clj  (Boolean/compare (op f2) (op f1))
       :cljs (compare (op f2) (op f1)))))


(defn cmp-history-quick-reverse-sort
  "Sorts by transaction time in ascending order (newest first), then by
  the boolean operation descending so assertions (true) come before retractions (false)
  so that we can 're-play' the log in reverse order to come up with historical states.
  Suitable only for sorting a vector, not a sorted set."
  [f1 f2]
  (combine-cmp
    (cmp-long (t f1) (t f2))
    #?(:clj  (Boolean/compare (op f2) (op f1))
       :cljs (compare (op f2) (op f1)))))


(defn new-flake
  [& parts]
  (let [[s p o t op m] parts]
    (->Flake s p o t op m)))


(defn flip-flake
  "Takes a flake and returns one with the provided block and op flipped from true/false.
  Don't over-ride no-history, even if no-history for this predicate has changed. New inserts
  will have the no-history flag, but we need the old inserts to be properly retracted in the txlog."
  ([flake]
   (->Flake (s flake) (p flake) (o flake) (t flake) (not (op flake)) (m flake)))
  ([flake tx]
   (->Flake (s flake) (p flake) (o flake) tx (not (op flake)) (m flake))))

(defn change-t
  "Takes a flake and returns one with the provided block and op flipped from true/false.
  Don't over-ride no-history, even if no-history for this predicate has changed. New inserts
  will have the no-history flag, but we need the old inserts to be properly retracted in the txlog."
  ([flake t]
   (->Flake (s flake) (p flake) (o flake) t (op flake) (m flake))))



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

(defn match-spot
  "Returns all matching flakes to a specific subject, and optionaly also a predicate if provided
  Must be provided with subject/predicate integer ids, no lookups are performed."
  [ss sid pid]
  (if pid
    (avl/subrange ss >= (->Flake sid pid nil nil nil nil)
                  <= (->Flake sid (inc pid) "" nil nil nil))
    (avl/subrange ss > (->Flake (inc sid) MAX-COLL-SUBJECTS nil nil nil nil)
                  < (->Flake (dec sid) -1 nil nil nil nil))))


(defn match-post
  "Returns all matching flakes to a predicate + object match."
  [ss pid o]
  (avl/subrange ss
                >= (->Flake util/max-long pid o nil nil nil)
                <= (->Flake 0 pid o nil nil nil)))

(defn match-tspo
  "Returns all matching flakes to a specific 't' value."
  [ss t]
  (avl/subrange ss
                >= (->Flake util/max-long nil nil t nil nil)
                <= (->Flake util/min-long nil nil t nil nil)))

(defn lookup
  [ss start-flake end-flake]
  (avl/subrange ss >= start-flake <= end-flake))

(defn subrange
  ([ss test flake]
   (avl/subrange ss test flake))
  ([ss start-test start-flake end-test end-flake]
   (avl/subrange ss start-test start-flake end-test end-flake)))


(defn split-at
  [n ss]
  (avl/split-at n ss))

(defn lower-than-all?
  [f ss]
  (let [[lower e _] (avl/split-key f ss)]
    (and (nil? e)
         (empty? lower))))

(defn higher-than-all?
  [f ss]
  (let [[_ e upper] (avl/split-key f ss)]
    (and (nil? e)
         (empty? upper))))

(defn split-by-flake
  "Splits a sorted set at a given flake. If there is an exact match for flake,
  puts it in the left-side. Primarily for use with last-flake."
  [f ss]
  (let [[l e r] (avl/split-key f ss)]
    [(if e (conj l e) l) r]))


(defn sorted-set-by
  [comparator & flakes]
  (apply avl/sorted-set-by comparator flakes))

(defn sorted-map-by
  [comparator & entries]
  (apply avl/sorted-map-by comparator entries))

(defn transient-reduce
  [reducer ss coll]
  (->> coll
       (reduce reducer (transient ss))
       persistent!))

(defn conj-all
  "Adds all flakes in the `to-add` collection from the AVL-backed sorted flake set
  `sorted-set`. This function uses transients for intermediate set values for
  better performance because of the slower batched update performance of
  AVL-backed sorted sets."
  [ss to-add]
  (transient-reduce conj! ss to-add))

(defn disj-all
  "Removes all flakes in the `to-remove` collection from the AVL-backed sorted
  flake set `sorted-set`. This function uses transients for intermediate set
  values for better performance because of the slower batched update performance
  of AVL-backed sorted sets."
  [ss to-remove]
  (transient-reduce disj! ss to-remove))

(defn assoc-all
  [sm entries]
  (transient-reduce (fn [m [k v]]
                      (assoc! m k v))
                    sm entries))

(defn dissoc-all
  [sm ks]
  (transient-reduce dissoc! sm ks))

(defn last
  "Returns the last item in `ss` in constant time as long as `ss` is a sorted
  set."
  [ss]
  (->> ss rseq first))

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
  (let [o (o f)]
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
       (if (nil? (m f))
         1
         (* 2 (count (pr-str (m f))))))))


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
