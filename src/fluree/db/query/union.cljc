(ns fluree.db.query.union
  (:require [fluree.db.util.core :as util]
            [clojure.set :as set]
            [fluree.db.util.log :as log]))


(defn intersecting-keys-tuples
  [a-tuples b-tuples]
  (let [a-keys (-> a-tuples :headers set)
        b-keys (-> b-tuples :headers)]
    (reduce (fn [acc key]
              (if (a-keys key)
                (conj acc key)
                acc))
            [] b-keys)))


(defn find-match+row-nums
  "Given a single tuple from A, a-idxs, b-idxs, b-not-idxs, and b-tuples, return any tuples in b that match.
  Along with their row-numbers"
  [a-tuple a-idxs b-tuples b-idxs b-not-idxs]
  (let [a-tuple-part (map #(nth a-tuple %) a-idxs)]
    (reduce-kv (fn [[acc b-rows] row b-tuple]
                 (if (= a-tuple-part (map #(nth b-tuple %) b-idxs))
                   [(conj (or acc []) (concat a-tuple (map #(nth b-tuple %) b-not-idxs))) (conj b-rows row)]
                   [acc b-rows]))
               [nil #{}] (into [] b-tuples))))


(defn nil-pad-tuples
  "Pads n columns with nil values on either left or right hand side of tuples"
  [tuples pad-n side]
  (let [pad-seq (->> (repeat nil)                           ;; infinite list of nil values
                     (take pad-n)                           ;; take on n number as per padding
                     repeat)]                               ;; make resulting padded list infinite
    (doall
      (case side
        :right (map concat tuples pad-seq)
        :left (map concat pad-seq tuples)))))


(defn non-intersecting
  "Non-intersecting means there are no common variable bindings between the two tuples.
  The result of this operation can be quite fast as it is just a concatenation of all results with
  null values all columns that are not in each result set respectively."
  [a-tuples b-tuples]
  (let [a-headers (:headers a-tuples)
        b-headers (:headers b-tuples)
        c-tuples  (concat (nil-pad-tuples (:tuples a-tuples) (count b-headers) :right)
                          (nil-pad-tuples (:tuples b-tuples) (count a-headers) :left))]
    {:headers (concat a-headers b-headers)
     :vars    (merge (:vars a-tuples) (:vars b-tuples))
     :tuples  c-tuples}))


(defn tuple-positions
  "Returns a list containing the tuple-index for each provided subset header based on tuple headers.
  i.e. if the tuple headers are (?x ?y ?z ?c) and the subset headers given is (?c ?y), it will
  return (3 1) as the respective index points for those tuples."
  [all-headers subset-headers]
  (map #(util/index-of all-headers %) subset-headers))


(defn all-intersecting
  "Case where every variable is intersecting in union of two tuple sets."
  [a-tuples b-tuples]
  (let [a-headers (:headers a-tuples)
        b-headers (:headers b-tuples)
        a-data    (:tuples a-tuples)
        b-data    (if (= a-headers b-headers)
                    (:tuples b-tuples)                      ;; variable ordering is identical, no need to re-sort
                    (let [positions (tuple-positions b-headers a-headers)]
                      (map #(map (fn [i] (get % i)) positions) (:tuples b-tuples))))
        b-pos-map (apply hash-map (interleave b-data (range)))
        c-data    (loop [a-items    a-data
                         b-pos-map* b-pos-map]
                    (if (empty? a-items)
                      (concat a-data (->> b-pos-map* (sort-by val) keys))
                      (->> (first a-items)
                           (dissoc b-pos-map*)
                           (recur (rest a-items)))))]
    {:headers a-headers
     :vars    (merge (:vars a-tuples) (:vars b-tuples))
     :tuples  c-data}))


(defn b-data-map
  "For situation where just some variables in a union intersect between the a-tuples and b-tuples,
  creates a map of b-tuples where key is intersecting variables for fast lookup, and value is a
  map of original tuple along with :idx which is the original order (row number) of b-tuples
  to ensure consistent ordering.

  Matches between a-tuples and b-tuples will remove entries from this map and merge results, entries
  left in this map will be items in b-tuples that did not match a-tuples."
  [b-common-idx b-data]
  (loop [b-data* b-data
         i       0
         acc     {}]
    (if (empty? b-data*)
      acc
      (let [tuple  (vec (first b-data*))
            common (map #(get tuple %) b-common-idx)]
        (recur (rest b-data*)
               (inc i)
               (assoc acc common {:idx   i
                                  :tuple tuple}))))))


(defn intersecting
  "With common-headers already calculated, takes a list of headers and returns a
   3-tuple of:
    - common indexes, i.e (3, 1)
    - not common indexes, i.e. (0, 2)
    - not common headers, i.e. (?x, ?y)"
  [common-headers headers]
  (let [common-key?    (into #{} common-headers)
        not-common     (filter #(not (common-key? %)) headers)
        common-idx     (tuple-positions headers common-headers)
        not-common-idx (tuple-positions headers not-common)]
    [common-idx not-common-idx not-common]))


(defn some-intersecting
  "Some headers from a and/or b tuples intersect. Need to build out all columns.
  Where common values exist for intersecting headers, merge into more complete rows.
  Otherwise pad superset of headers as results with nil values for both a and b as
  applicable."
  [common-headers a-tuples b-tuples]
  (let [a-headers    (:headers a-tuples)
        b-headers    (:headers b-tuples)
        a-data       (:tuples a-tuples)
        b-data       (:tuples b-tuples)
        a-common-idx (map #(util/index-of (:headers a-tuples) %) common-headers)
        [b-common-idx b-only-idx b-only-headers] (intersecting common-headers b-headers)
        b-pos-map    (b-data-map b-common-idx b-data)
        a-nil-pad    (vec (repeat (count a-headers) nil))
        b-nil-pad    (repeat (count b-only-headers) nil)
        a-pad-fn     (fn [b-tuple]
                       (concat
                         (reduce
                           (fn [acc [a-idx b-idx]]
                             (assoc acc a-idx (get b-tuple b-idx)))
                           a-nil-pad
                           (partition 2 (interleave a-common-idx b-common-idx)))
                         (map #(get b-tuple %) b-only-idx)))
        c-data       (loop [a-data*    a-data
                            b-pos-map* b-pos-map
                            acc        []]
                       (if (empty? a-data*)
                         (let [only-b (->> (vals b-pos-map*)
                                           (sort-by :idx)
                                           (map #(a-pad-fn (:tuple %))))]
                           (concat acc only-b))
                         (let [a-item (vec (first a-data*))
                               match  (map #(get a-item %) a-common-idx)]
                           (if-let [b-match (get b-pos-map* match)]
                             (recur (rest a-data*)
                                    (dissoc b-pos-map* match)
                                    (conj acc (concat a-item
                                                      (map #(get (:tuple b-match) %) b-only-idx))))
                             (recur (rest a-data*)
                                    b-pos-map*
                                    (conj acc (concat a-item
                                                      b-nil-pad)))))))]
    {:headers (concat a-headers b-only-headers)
     :vars    (merge (:vars a-tuples) (:vars b-tuples))
     :tuples  c-data}))


(defn results
  "Returns union (outer join) results of two statements."
  [a-tuples b-tuples]
  (let [common-keys   (intersecting-keys-tuples a-tuples b-tuples)
        intersections (cond
                        ;; no overlapping variables, will just create a big list with all columns + nils for non-values
                        (zero? (count common-keys)) :none
                        ;; every variable overlaps, just need to merge the two. Retain ordering consistency by always using a-tuples first
                        (= (count common-keys)
                           (count (:headers a-tuples))
                           (count (:headers b-tuples))) :all
                        ;; only some of the variables intersect, need to apply more granular approach
                        :else :some)]
    (case intersections
      :none (non-intersecting a-tuples b-tuples)
      :all (all-intersecting a-tuples b-tuples)
      :some (some-intersecting common-keys a-tuples b-tuples))))