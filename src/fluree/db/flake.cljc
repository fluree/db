(ns fluree.db.flake
  (:refer-clojure :exclude [partition-by remove split-at sorted-set-by sorted-map-by take last])
  (:require #?(:clj [clojure.pprint :as pprint])
            [clojure.data.avl :as avl]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.util.core :as util])
  #?(:cljs (:require-macros [fluree.db.flake :refer [combine-cmp]])))

#?(:clj (set! *warn-on-reflection* true))

(declare equiv-flake assoc-flake get-flake-val nth-flake)

(def min-s iri/min-sid)
(def max-s iri/max-sid)
(def min-p iri/min-sid)
(def max-p iri/max-sid)
(def min-dt iri/min-sid)
(def max-dt iri/max-sid)
(def ^:const min-t 0)
(def ^:const max-t util/max-long)
(def ^:const min-op false)
(def ^:const max-op true)
(def ^:const min-meta util/min-integer)
(def ^:const max-meta util/max-integer)

(deftype Flake [s p o dt t op m -clj-meta]
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

             clojure.lang.IMeta
             (meta [_] -clj-meta)

             clojure.lang.IObj
             (withMeta [_ clj-meta] (Flake. s p o dt t op m clj-meta))

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

             IMeta
             (-meta [this] -clj-meta)

             IWithMeta
             (-with-meta [f clj-meta] (Flake. (.-s f) (.-p f) (.-o f) (.-dt f) (.-t f) (.-op f) (.-m f) clj-meta))

             IPrintWithWriter
             (-pr-writer [^Flake f writer opts]
                         (pr-sequential-writer writer pr-writer
                                               "#fluree/Flake [" " " "]"
                                               opts [(.-s f) (.-p f) (.-o f) (.-dt f) (.-t f) (.-op f) (.-m f)]))]))

#?(:clj (defmethod print-method Flake [^Flake f, ^java.io.Writer w]
          (.write w (str "#fluree/Flake "))
          (binding [*out* w]
            (pr [(.-s f) (.-p f) (.-o f) (.-dt f) (.-t f) (.-op f) (.-m f)]))))

#?(:clj (defmethod pprint/simple-dispatch Flake [^Flake f]
          (pr f)))

(defn create
  "Creates a new flake from parts"
  [s p o dt t op m]
  (->Flake s p o dt t op m nil))

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
   (create s p o dt t op m))
  ([[s p o dt t op m] default-tx]
   (create s p o dt (or t default-tx) op m))
  ([[s p o dt t op m] default-tx default-op]
   (create s p o dt (or t default-tx) (or op default-op) m)))

(defn Flake->parts
  [flake]
  [(s flake) (p flake) (o flake) (dt flake) (t flake) (op flake) (m flake)])

(def subj-pos 0)
(def pred-pos 1)
(def obj-pos 2)
(def dt-pos 3)
(def t-pos 4)
(def op-pos 5)
(def m-pos 6)

(def maximum
  (create max-s max-p max-s max-dt max-t max-op max-meta))

(def minimum
  (create min-s min-p min-s min-dt min-t min-op min-meta))

(defn- assoc-flake
  "Assoc for Flakes"
  ([flake k v]
   (let [[s p o dt t op m] (Flake->parts flake)]
     (case k
       :s (create v p o dt t op m)
       :p (create s v o dt t op m)
       :o (create s p v dt t op m)
       :dt (create s p o v t op m)
       :t (create s p o dt v op m)
       :op (create s p o dt t v m)
       :m (create s p o dt t op v)
       #?(:clj  (throw (IllegalArgumentException. (str "Flake does not contain key: " k)))
          :cljs (throw (js/Error. (str "Flake does not contain key: " k)))))))
  ([flake k v & kvs]
   (let [ret (assoc-flake flake k v)]
     (if kvs
       (if (next kvs)
         (recur ret (first kvs) (second kvs) (nnext kvs))
         (throw (ex-info "assoc-flake expects even number of arguments after map/vector, found odd number"
                         {:status 500 :error :db/system})))
       ret))))

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
  (if (and (some? b1) (some? b2))
    #?(:clj (Boolean/compare b1 b2) :cljs (compare b1 b2))
    0))

(defn hash-meta
  [m]
  (if (int? m)
    m
    (hash m)))

(defn cmp-meta
  "Meta will always be a map or nil, but can be searched using an integer to
  perform effective range scans if needed. i.e. (Integer/MIN_VALUE)
  to (Integer/MAX_VALUE) will always include all meta values."
  [m1 m2]
  (let [m1h (hash-meta m1)
        m2h (hash-meta m2)]
    #?(:clj (Integer/compare m1h m2h) :cljs (- m1h m2h))))

(defn cmp-long [l1 l2]
  (if (and l1 l2)
    #?(:clj (Long/compare l1 l2) :cljs (- l1 l2))
    0))

(defn cmp-sid
  [sid1 sid2]
  (if (and sid1 sid2)
    (compare sid1 sid2)
    0))

(defn cmp-subj
  "Compare two subject values by comparing them as SIDs."
  [s1 s2]
  (cmp-sid s1 s2))

(defn cmp-pred [p1 p2]
  (cmp-sid p1 p2))

(defn cmp-tx
  "Compare two transaction values by comparing them as long numbers."
  [t1 t2]
  (cmp-long t1 t2))

(defn cmp-dt
  "Used within cmp-obj to compare data types in more edge cases"
  [dt1 dt2]
  (if (and dt1 dt2)
    (cmp-sid dt1 dt2)
    0))

(defn cmp-obj
  [o1 dt1 o2 dt2]
  (if (and (some? o1) (some? o2))
    (cond
      ;; same data types (common case), just standard compare
      (= dt1 dt2)
      ;; TODO this does a generic compare, might boost performance if further
      ;; look at common types and call specific comparator fns (e.g. boolean,
      ;; long, etc.)
      (compare o1 o2)

      ;; different data types, but strings
      (and (string? o1)
           (string? o2))
      (let [s-cmp (compare o1 o2)]
        (if (= 0 s-cmp) ; could be identical values, but different data types
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
   (cmp-op (op f1) (op f2))
   (cmp-meta (m f1) (m f2))))

(defn cmp-flakes-post [f1 f2]
  (combine-cmp
   (cmp-pred (p f1) (p f2))
   (cmp-obj (o f1) (dt f1) (o f2) (dt f2))
   (cmp-subj (s f1) (s f2))
   (cmp-tx (t f1) (t f2))
   (cmp-op (op f1) (op f2))
   (cmp-meta (m f1) (m f2))))

(defn cmp-flakes-opst [f1 f2]
  (combine-cmp
   (cmp-subj (o f1) (o f2))
   (cmp-pred (p f1) (p f2))
   (cmp-subj (s f1) (s f2))
   (cmp-tx (t f1) (t f2))
   (cmp-op (op f1) (op f2))
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
   (cmp-op (op f1) (op f2))
   (cmp-meta (m f1) (m f2))))

(defn flip-flake
  "Takes a flake and returns one with the provided block and op flipped from true/false.
  Don't over-ride no-history, even if no-history for this predicate has changed. New inserts
  will have the no-history flag, but we need the old inserts to be properly retracted in the txlog."
  ([flake]
   (assoc-flake flake :op (not (op flake))))
  ([flake t]
   (assoc-flake flake :t t :op (not (op flake)))))

(defn next-t
  [t]
  (inc (or t 0)))

(defn prev-t
  [t]
  (when (and t (> t 0))
    (dec t)))

(defn t-before?
  [t1 t2]
  (if (and t1 t2)
    (< t1 t2)
    (nil? t1)))

(defn t-after?
  [t1 t2]
  (if (and t1 t2)
    (> t1 t2)
    (some? t1)))

(defn match-tspo
  "Returns all matching flakes to a specific 't' value."
  [ss t]
  (avl/subrange ss
                >= (create min-s nil nil nil t nil nil)
                <= (create max-s nil nil nil t nil nil)))

(defn subrange
  ([ss test flake]
   (avl/subrange ss test flake))
  ([ss start-test start-flake end-test end-flake]
   (avl/subrange ss start-test start-flake end-test end-flake)))

(defn nearest
  [ss test f]
  (avl/nearest ss test f))

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

(defn sorted-set-by
  [comparator & flakes]
  (apply avl/sorted-set-by comparator flakes))

(defn sorted-map-by
  [comparator & entries]
  (apply avl/sorted-map-by comparator entries))

(defn revise
  "Changes the composition of the sorted set `ss` by adding all the flakes in the
  `to-add` collection and removing all flakes in the `to-remove` collection."
  [ss to-add to-remove]
  (let [trans   (transient ss)
        removed (loop [[f & r] to-remove
                       t-set   trans]
                  (if f
                    (recur r (disj! t-set f))
                    t-set))
        added   (loop [[f & r] to-add
                       t-set   removed]
                  (if f
                    (recur r (conj! t-set f))
                    t-set))]
    (persistent! added)))

(defn last
  "Returns the last item in `ss` in constant time as long as `ss` is a sorted
  set."
  [ss]
  (->> ss rseq first))

(def ^:const flake-size-base (+ 30 ;;sid
                                30 ;;pid
                                30 ;;dt size
                                8 ;; t size
                                ))

(defn size-flake
  "Base size of a flake is 38 bytes... then add size for 'o' and 'm'.
  Flakes have the following:
    - s, p, dt - sid size as returned by fluree.db.json-ld.iri/measure-sid
    - o - ??
    - t - 8 bytes
    - add? - 1 byte
    - m - 1 byte + ??
    - header - 12 bytes - object header...

  Objects will be rounded up to nearest 8 bytes... we don't do this here as
  it should be 'close enough'
  reference: https://www.javamex.com/tutorials/memory/string_memory_usage.shtml"
  [^Flake f]
  (let [o       (o f)
        o-size  (#?@(:clj (util/case+)
                     :cljs (condp =)) (dt f)
                                      const/$xsd:string (* 2 (count o))
                                      const/$id 30
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
    (cond-> (+ flake-size-base o-size)
      (m f) (* 2 (count (pr-str (m f)))))))

(defn size-bytes
  "Returns approx number of bytes in a collection of flakes."
  [flakes]
  (reduce (fn [size f] (+ size (size-flake f))) 0 flakes))

(defn class-flake?
  [f]
  (= const/$rdf:type (p f)))

(defn ref-flake?
  [f]
  (= const/$id (dt f)))
