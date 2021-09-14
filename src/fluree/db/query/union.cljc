(ns fluree.db.query.union
  (:require [fluree.db.util.core :as util]))


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


(defn- b-data-map
  "For situation where just some variables in a union intersect between the a-tuples and b-tuples,
  creates a map of b-tuples where key is intersecting variables for fast lookup, and value is a
  *list* of map(s) of original tuple along with :idx which is the original order (row number) of b-tuples
  to ensure consistent ordering.

  There can be multiple matches in b-tuples for a key, as the other non-matching variables differ.

  Matches between a-tuples and b-tuples will remove entries from this map and merge results, entries
  remaining in this map will be items in b-tuples that did not match a-tuples."
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
               (update acc common conj {:idx   i
                                        :tuple tuple}))))))


(defn flatten-b-data-map
  "Takes a b-data map as per above and re-flattens it into a list of tuples in the original order.

  This is used after removing matching entries in b-data-map, so this will often end up being a subset
  of the original b-data-map."
  [b-data-map]
  (->> (vals b-data-map)
       (apply concat)
       (sort-by :idx)
       (map :tuple)))


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


(defn- pad-b-tuples
  "B-tuples that do not match and get merged with an a-tuple must be concatenated to the
  list in the order of the combined tuple headers. This will take a list of b-tuples and
  put nil values into all columns that are unique to a.

  If a-headers were [?x ?y ?z] and b-headers were [?y ?a ?b] the combined headers will be:
  [?x ?y ?z ?a ?b]

  Because these are b-only values, ?x and ?z will always be nil.
  If we have a b-tuple of [42 36 49] we'd want a final result of [nil 42 nil 36 49]"
  [b-tuples a-headers a-common-idx b-common-idx b-only-idx]
  (let [a-nil-pad      (vec (repeat (count a-headers) nil)) ;; tuple of a-headers length with all nil vals
        a->b-positions (partition 2 (interleave a-common-idx b-common-idx))] ;; for common indexes, tuples of position in a-header to position in b-header
    (map
      (fn [b-tuple]
        (let [a-headers-merged (reduce
                                 (fn [acc [a-idx b-idx]]
                                   (assoc acc a-idx (get b-tuple b-idx)))
                                 a-nil-pad
                                 a->b-positions)]
          (concat
            a-headers-merged
            (map #(get b-tuple %) b-only-idx))))
      b-tuples)))


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
        b-nil-pad    (repeat (count b-only-headers) nil)
        c-data       (loop [a-data*    a-data
                            b-pos-map* b-pos-map
                            acc        []]
                       (if (empty? a-data*)
                         (concat acc (-> b-pos-map*
                                         flatten-b-data-map
                                         (pad-b-tuples a-headers a-common-idx b-common-idx b-only-idx)))
                         (let [a-item    (vec (first a-data*))
                               match-key (map #(get a-item %) a-common-idx)]
                           (if-let [b-matches (get b-pos-map* match-key)]
                             (recur (rest a-data*)
                                    (dissoc b-pos-map* match-key)
                                    (reduce
                                      (fn [acc* b-match]
                                        (conj acc*
                                              (concat a-item
                                                      (map #(get (:tuple b-match) %) b-only-idx))))
                                      acc b-matches))
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