(ns fluree.db.flake
  (:refer-clojure :exclude [split-at sorted-set-by sorted-map-by take last])
  (:require [me.tonsky.persistent-sorted-set :as pss]
            [me.tonsky.persistent-sorted-set.arrays :as arrays]
            [fluree.db.constants :as const]
            [fluree.db.util.core :as util])
  #?(:cljs (:require-macros [fluree.db.flake :refer [combine-cmp]]))
  (:import (me.tonsky.persistent_sorted_set PersistentSortedSet)))

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


(deftype Flake [s p o dt t op m]
  #?@(:clj  [clojure.lang.Seqable
             (seq [f] (list (.-s f) (.-p f) (.-o f) (.-dt f) (.-t f) (.-op f) (.-m f)))

             clojure.lang.Indexed
             (nth [f i] (nth-flake f i nil))
             (nth [f i not-found] (nth-flake f i not-found))

             clojure.lang.ILookup
             (valAt [f k] (get-flake-val f k nil))
             (valAt [f k not-found] (get-flake-val f k not-found))

             clojure.lang.IPersistentCollection
             (equiv [f o] (and (instance? Flake o) (equiv-flake f o)))
             (empty [f] (throw (UnsupportedOperationException. "empty is not supported on Flake")))
             (count [f] 7)
             (cons [f [k v]] (assoc-flake f k v))

             clojure.lang.IPersistentMap
             (assocEx [f k v] (UnsupportedOperationException. "assocEx is not supported on Flake"))
             (without [f k] (UnsupportedOperationException. "without is not supported on Flake"))

             clojure.lang.Associative
             (entryAt [f k] (some->> (get f k nil) (clojure.lang.MapEntry k)))
             (containsKey [_ k] (boolean (#{:s :p :o :dt :t :op :m} k)))
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
             (-seq [this] (list (.-s this) (.-p this) (.-o this) (.-dt this) (.-t this) (.-op this) (.-m this)))

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
                                               opts [(.-s f) (.-p f) (.-o f) (.-dt f) (.-t f) (.-op f) (.-m f)]))]))


#?(:clj (defmethod print-method Flake [^Flake f, ^java.io.Writer w]
          (.write w (str "#Flake "))
          (binding [*out* w]
            (pr [(.-s f) (.-p f) (.-o f) (.-dt f) (.-t f) (.-op f) (.-m f)]))))

(defn s
  [^Flake f]
  (.-s f))

(defn p
  [^Flake f]
  (.-p f))

(defn o
  [^Flake f]
  (.-o f))

(defn dt
  [^Flake f]
  (.-dt f))

(defn t
  [^Flake f]
  (.-t f))

(defn op
  [^Flake f]
  (.-op f))

(defn m
  [^Flake f]
  (.-m f))

(defn flake?
  [x]
  (instance? Flake x))

(defn- equiv-flake
  [f other]
  (and (= (s f) (s other))
       (= (p f) (p other))
       (= (o f) (o other))
       (= (dt f) (dt other))))

(defn parts->Flake
  "Used primarily to generate flakes for comparator. If you wish to
  generate a flake for other purposes, be sure to supply all components."
  ([[s p o dt t op m]]
   (->Flake s p o dt t op m))
  ([[s p o dt t op m] default-tx]
   (->Flake s p o dt (or t default-tx) op m))
  ([[s p o dt t op m] default-tx default-op]
   (->Flake s p o dt (or t default-tx) (or op default-op) m)))

(defn create
  "Creates a new flake from parts"
  [s p o dt t op m]
  (->Flake s p o dt t op m))


(defn Flake->parts
  [flake]
  [(s flake) (p flake) (o flake) (dt flake) (t flake) (op flake) (m flake)])

(def maximum
  "The largest flake possible"
  (->Flake util/max-long util/max-long util/max-long
           util/max-integer util/min-integer true util/max-integer))

(def minimum
  (->Flake util/min-long util/min-long util/min-long
           util/min-integer 0 false util/min-integer))

(defn- assoc-flake
  "Assoc for Flakes"
  [flake k v]
  (let [[s p o dt t op m] (Flake->parts flake)]
    (case k
      :s (->Flake v p o dt t op m)
      :p (->Flake s v o dt t op m)
      :o (->Flake s p v dt t op m)
      :dt (->Flake s p o v t op m)
      :t (->Flake s p o dt v op m)
      :op (->Flake s p o dt t v m)
      :m (->Flake s p o dt t op v)
      #?(:clj  (throw (IllegalArgumentException. (str "Flake does not contain key: " k)))
         :cljs (throw (js/Error. (str "Flake does not contain key: " k)))))))


(defn- get-flake-val
  [flake k not-found]
  (case k
    :s (s flake) "s" (s flake)
    :p (p flake) "p" (p flake)
    :o (o flake) "o" (o flake)
    :dt (dt flake) "dt" (dt flake)
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
             3 (dt flake)
             4 (t flake)
             5 (op flake)
             6 (m flake)
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

(defn cmp-dt
  "Used within cmp-obj to compare data types in more edge cases"
  [dt1 dt2]
  (if (and dt1 dt2)
    (compare dt1 dt2)
    0))

(defn cmp-obj
  [o1 dt1 o2 dt2]
  (if (and (some? o1) (some? o2))
    (cond
      ;; same data types (common case), just standard compare
      (= dt1 dt2)
      ;; TODO this does a generic compare, might boost performance if further look at common types and call specific comparator fns (e.g. boolean, long, etc.)
      (compare o1 o2)

      ;; different data types, but strings
      (and (string? o1)
           (string? o2))
      (let [s-cmp (compare o1 o2)]
        (if (= 0 s-cmp)                                     ;; could be identical values, but different data types
          (cmp-dt dt1 dt2)
          s-cmp))

      ;; different data types, but numbers
      (and (number? o1)
           (number? o2))
      (let [s-cmp (compare o1 o2)]
        (if (= 0 s-cmp)                                     ;; could be identical values, but different data types
          (cmp-dt dt1 dt2)
          s-cmp))

      ;; different data types, not comparable
      :else
      (cmp-dt dt1 dt2))
    0))


(defn cmp-op
  [op1 op2]
  (cmp-bool op1 op2))

(defn cmp-flakes-spot [f1 f2]
  (combine-cmp
    (cmp-subj (s f1) (s f2))
    (cmp-pred (p f1) (p f2))
    (cmp-obj (o f1) (dt f1) (o f2) (dt f2))
    (cmp-tx (t f1) (t f2))
    (cmp-bool (op f1) (op f2))
    (cmp-meta (m f1) (m f2))))

(defn cmp-flakes-psot [f1 f2]
  (combine-cmp
    (cmp-pred (p f1) (p f2))
    (cmp-subj (s f1) (s f2))
    (cmp-obj (o f1) (dt f1) (o f2) (dt f2))
    (cmp-tx (t f1) (t f2))
    (cmp-bool (op f1) (op f2))
    (cmp-meta (m f1) (m f2))))


(defn cmp-flakes-post [f1 f2]
  (combine-cmp
    (cmp-pred (p f1) (p f2))
    (cmp-obj (o f1) (dt f1) (o f2) (dt f2))
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
    (cmp-obj (o f1) (dt f1) (o f2) (dt f2))
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


(defn flip-flake
  "Takes a flake and returns one with the provided block and op flipped from true/false.
  Don't over-ride no-history, even if no-history for this predicate has changed. New inserts
  will have the no-history flag, but we need the old inserts to be properly retracted in the txlog."
  ([flake]
   (->Flake (s flake) (p flake) (o flake) (dt flake) (t flake) (not (op flake)) (m flake)))
  ([flake t]
   (->Flake (s flake) (p flake) (o flake) (dt flake) t (not (op flake)) (m flake))))

(defn slice
  "From and to are Flakes"
  [ss from to]
  (pss/slice ss from to))

(defn rslice
  [ss to from]
  (pss/rslice ss to from))

(defn sorted-set-by
  "Create a new sorted set according to `comparator`. If a collection of flakes or
  index nodes is supplied as the `elts` argument, the returned set will contain
  all of the elements in the collection. If the `elts` collection is supplied,
  it *must* already be sorted."
  ([comparator]
   (pss/sorted-set-by comparator))
  ([comparator elts]
   (->> elts
        arrays/into-array
        (pss/from-sorted-array comparator))))

(defn match-tspo
  "Returns all matching flakes to a specific 't' value."
  [ss t]
  (pss/slice ss
             (->Flake util/max-long nil nil nil t nil nil)
             (->Flake util/min-long nil nil nil t nil nil)))

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
    - dt - 4 bytes
    - t - 8 bytes
    - add? - 1 byte
    - m - 1 byte + ??
    - header - 12 bytes - object header...

  Objects will be rounded up to nearest 8 bytes... we don't do this here as
  it should be 'close enough'
  reference: https://www.javamex.com/tutorials/memory/string_memory_usage.shtml"
  [^Flake f]
  (let [o      (o f)
        dt     (int (dt f))
        o-size (util/case+ dt
                 const/$xsd:string (* 2 (count o))
                 const/$xsd:anyURI 8
                 const/$xsd:boolean 1
                 const/$xsd:long 8
                 const/$xsd:int 4
                 const/$xsd:short 2
                 const/$xsd:double 8
                 const/$xsd:float 4
                 const/$xsd:byte 1
                 ;; else
                 (if (number? o)
                   8
                   (if (string? o)
                     (* 2 (count o))
                     (* 2 (count (pr-str o))))))]
    (cond-> (+ 42 o-size)
            (m f) (* 2 (count (pr-str (m f)))))))


(defn size-bytes
  "Returns approx number of bytes in a collection of flakes."
  [flakes]
  (reduce (fn [size f] (+ size (size-flake f))) 0 flakes))


(defn size-kb
  "Like size-bytes, but kb.
  Rounds down for simplicity, as bytes is just an estimate anyhow."
  [flakes]
  (-> (size-bytes flakes)
      (/ 1000)
      (double)
      (Math/round)))
