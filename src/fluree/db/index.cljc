(ns fluree.db.index
  (:refer-clojure :exclude [resolve])
  (:require [clojure.data.avl :as avl]
            [fluree.db.flake :as flake]
            #?(:clj  [clojure.core.async :refer [chan go <! >!] :as async]
               :cljs [cljs.core.async :refer [chan go <!] :as async])
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.conn.cache :as conn-cache]))

(def default-comparators
  "Map of default index comparators for the five index types"
  {:spot flake/cmp-flakes-spot
   :psot flake/cmp-flakes-psot
   :post flake/cmp-flakes-post
   :opst flake/cmp-flakes-opst
   :tspo flake/cmp-flakes-block})

(def types
  "The five possible index orderings based on the subject, predicate, object,
  and transaction flake attributes"
  (-> default-comparators keys set))

(defn leaf?
  "Returns `true` if `node` is a map for a leaf node"
  [node]
  (-> node :leaf true?))

(defn branch?
  "Returns `true` if `node` is a map for branch node"
  [node]
  (-> node :leaf false?))

(defprotocol Resolver
  (resolve [r node]
    "Populate the supplied index branch or leaf node maps with either the child
     node attributes or the flakes they store, respectively."))

(defn try-resolve
  [r error-ch node]
  (go
    (try* (<? (resolve r node))
          (catch* e
                  (log/error e
                             "Error resolving index node:"
                             (select-keys node [:id :network :ledger-id]))
                  (>! error-ch e)))))

(defn resolved?
  "Returns `true` if the data associated with the index node map `node` is fully
  resolved from storage"
  [node]
  (cond
    (leaf? node)   (not (nil? (:flakes node)))
    (branch? node) (not (nil? (:children node)))))

(defn unresolve
  "Clear the populated child node attributes from the supplied `node` map if it
  represents a branch, or the populated flakes if `node` represents a leaf."
  [node]
  (cond
    (leaf? node)   (dissoc node :flakes)
    (branch? node) (dissoc node :children)))

(defn lookup
  [branch flake]
  (when (and (branch? branch)
             (resolved? branch))
    (let [{:keys [children]} branch]
      (-> children
          (avl/nearest <= flake)
          (or (first children))
          val))))

(defn lookup-leaf
  [r branch flake]
  (go-try
   (when (and (branch? branch)
              (resolved? branch))
     (loop [child (lookup branch flake)]
       (if (leaf? child)
         child
         (recur (<? (resolve r child))))))))


(defn add-flakes
  [leaf flakes]
  (let [new-leaf (-> leaf
                     (update :flakes flake/conj-all flakes)
                     (update :size (fn [size]
                                     (->> flakes
                                          (map flake/size-flake)
                                          (reduce + size)))))
        new-first (or (some-> new-leaf :flakes first)
                      flake/maximum)]
    (assoc new-leaf :first new-first)))

(defn rem-flakes
  [leaf flakes]
  (let [new-leaf (-> leaf
                     (update :flakes flake/disj-all flakes)
                     (update :size (fn [size]
                                     (->> flakes
                                          (map flake/size-flake)
                                          (reduce - size)))))
        new-first (or (some-> new-leaf :flakes first)
                      flake/maximum)]
    (assoc new-leaf :first new-first)))

(defn empty-leaf
  "Returns a blank leaf node map for the provided `network`, `ledger-id`, and index
  comparator `cmp`."
  [network ledger-id cmp]
  {:comparator cmp
   :network network
   :ledger-id ledger-id
   :id :empty
   :tempid (random-uuid)
   :leaf true
   :first flake/maximum
   :rhs nil
   :size 0
   :block 0
   :t 0
   :leftmost? true})

(defn new-leaf
  [network ledger-id cmp flakes]
  (let [empty-set (flake/sorted-set-by cmp)]
    (-> (empty-leaf network ledger-id cmp)
        (assoc :flakes empty-set)
        (add-flakes flakes))))

(defn descendant?
  "Checks if the `node` passed in the second argument is a descendant of the
  `branch` passed in the first argument"
  [{:keys [rhs leftmost?], cmp :comparator, first-flake :first, :as branch}
   {node-first :first, node-rhs :rhs, :as node}]
  (if-not (branch? branch)
    false
    (and (or leftmost?
             (not (pos? (cmp first-flake node-first))))
         (or (nil? rhs)
             (and (not (nil? node-rhs))
                  (not (pos? (cmp node-rhs rhs))))))))

(defn child-entry
  [{:keys [first] :as node}]
  (let [child-node (unresolve node)]
    [first child-node]))

(defn child-map
  "Returns avl sorted map whose keys are the first flakes of the index node
  sequence `child-nodes`, and whose values are the corresponding nodes from
  `child-nodes`."
  [cmp & child-nodes]
  (->> child-nodes
       (mapcat child-entry)
       (apply flake/sorted-map-by cmp)))

(defn empty-branch
  "Returns a blank branch node which contains a single empty leaf node for the
  provided `network`, `ledger-id`, and index comparator `cmp`."
  [network ledger-id cmp]
  (let [child-node (empty-leaf network ledger-id cmp)
        children   (child-map cmp child-node)]
    {:comparator cmp
     :network network
     :ledger-id ledger-id
     :id :empty
     :tempid (random-uuid)
     :leaf false
     :first flake/maximum
     :rhs nil
     :children children
     :size 0
     :block 0
     :t 0
     :leftmost? true}))

(defn reset-children
  [{:keys [comparator size] :as branch} new-child-nodes]
  (let [new-kids  (apply child-map comparator new-child-nodes)
        new-first (or (some-> new-kids first key)
                      flake/maximum)
        new-size  (->> new-child-nodes
                       (map :size)
                       (reduce + size))]
    (assoc branch :first new-first, :size new-size, :children new-kids)))

(defn new-branch
  [network ledger-id cmp child-nodes]
  (-> (empty-branch network ledger-id cmp)
      (reset-children child-nodes)))

(defn after-t?
  "Returns `true` if `flake` has a transaction value after the provided `t`"
  [t flake]
  (< (flake/t flake) t))

(defn before-t?
  "Returns `true` if `flake` has a transaction value before the provided `t`"
  [t flake]
  (> (flake/t flake) t))

(defn filter-after
  "Returns a sequence containing only flakes from the flake set `flakes` with
  transaction values after the provided `t`."
  [t flakes]
  (filter (partial after-t? t) flakes))

(defn flakes-through
  "Returns an avl-subset of the avl-set `flakes` with transaction values on or
  before the provided `t`."
  [t flakes]
  (->> flakes
       (filter-after t)
       (flake/disj-all flakes)))

(defn novelty-subrange
  [{:keys [rhs leftmost?], first-flake :first, :as node} through-t novelty]
  (log/trace "novelty-subrange: first-flake:" first-flake "\nrhs:" rhs "\nleftmost?" leftmost?)
  (let [subrange (cond
                   ;; standard case: both left and right boundaries
                   (and rhs (not leftmost?))
                   (avl/subrange novelty > first-flake <= rhs)

                   ;; right only boundary
                   (and rhs leftmost?)
                   (avl/subrange novelty <= rhs)

                   ;; left only boundary
                   (and (nil? rhs) (not leftmost?))
                   (avl/subrange novelty > first-flake)

                   ;; no boundary
                   (and (nil? rhs) leftmost?)
                   novelty)]
    (flakes-through through-t subrange)))

(defn stale-by
  "Returns a sequence of flakes from the sorted set `flakes` that are out of date
  by the transaction `from-t` because `flakes` contains another flake with the same
  subject and predicate and a t-value later than that flake but on or before `from-t`."
  [from-t flakes]
  (->> flakes
       (remove (partial after-t? from-t))
       (partition-by (juxt flake/s flake/p flake/o))
       (mapcat (fn [flakes]
                 ;; if the last flake for a subject/predicate/object combo is an assert,
                 ;; then everything before that is stale (object is necessary for
                 ;; multicardinality flakes)
                 (let [last-flake (last flakes)]
                   (if (flake/op last-flake)
                     (butlast flakes)
                     flakes))))))

(defn t-range
  "Returns a sorted set of flakes that are not out of date between the
  transactions `from-t` and `to-t`."
  ([{:keys [flakes] leaf-t :t :as leaf} novelty from-t to-t]
   (let [latest       (cond-> flakes
                        (> leaf-t to-t)
                        (flake/conj-all (novelty-subrange leaf to-t novelty)))
         stale-flakes (stale-by from-t latest)
         subsequent   (filter-after to-t latest)
         out-of-range (concat stale-flakes subsequent)]
     (flake/disj-all latest out-of-range))))

(defrecord CachedTRangeResolver [node-resolver novelty from-t to-t lru-cache-atom]
  Resolver
  (resolve [_ {:keys [id tempid tt-id] :as node}]
    (if (branch? node)
      (resolve node-resolver node)
      (conn-cache/lru-lookup
        lru-cache-atom
        [::t-range id tempid tt-id from-t to-t]
        (fn [_]
          (go-try
            (let [resolved (<? (resolve node-resolver node))
                  flakes   (t-range resolved novelty from-t to-t)]
              (-> resolved
                  (dissoc :t)
                  (assoc :from-t from-t
                         :to-t   to-t
                         :flakes  flakes)))))))))

(defn history-t-range
  "Returns a sorted set of flakes between the transactions `from-t` and `to-t`."
  ([{:keys [flakes] leaf-t :t :as leaf} novelty from-t to-t]
   (let [latest       (cond-> flakes
                        (> leaf-t to-t)
                        (flake/conj-all (novelty-subrange leaf to-t novelty)))
         ;; flakes that happen after to-t
         subsequent   (filter-after to-t latest)
         ;; flakes that happen before from-t
         previous     (filter (partial before-t? from-t) latest)
         out-of-range (concat subsequent previous)]
     (flake/disj-all latest out-of-range))))

(defrecord CachedHistoryRangeResolver [node-resolver novelty from-t to-t lru-cache-atom]
  Resolver
  (resolve [_ {:keys [id tempid tt-id] :as node}]
    (if (branch? node)
      (resolve node-resolver node)
      (conn-cache/lru-lookup
        lru-cache-atom
        [::history-t-range id tempid tt-id from-t to-t]
        (fn [_]
          (go-try
            (let [resolved (<? (resolve node-resolver node))
                  flakes   (history-t-range resolved novelty from-t to-t)]
              (-> resolved
                  (dissoc :t)
                  (assoc :from-t from-t
                         :to-t   to-t
                         :flakes  flakes)))))))))

(defn at-t
  "Find the value of `leaf` at transaction `t` by adding new flakes from
  `idx-novelty` to `leaf` if `t` is newer than `leaf`, or removing flakes later
  than `t` from `leaf` if `t` is older than `leaf`."
  [{:keys [rhs leftmost? flakes], leaf-t :t, :as leaf} t idx-novelty]
  (if (= leaf-t t)
    leaf
    (cond-> leaf
      (> leaf-t t)
      (add-flakes (novelty-subrange leaf t idx-novelty))

      (< leaf-t t)
      (rem-flakes (filter-after t flakes))

      true
      (assoc :t t))))

(defn- mark-expanded
  [node]
  (assoc node ::expanded true))

(defn- unmark-expanded
  [node]
  (dissoc node ::expanded))

(defn- expanded?
  [node]
  (-> node ::expanded true?))

(defn resolve-when
  [r resolve? error-ch node]
  (if (resolve? node)
    (try-resolve r error-ch node)
    (doto (chan)
      (async/put! node))))

(defn resolve-children-when
  [r resolve? error-ch branch]
  (if (resolved? branch)
    (->> branch
         :children
         (map (fn [[_ child]]
                (resolve-when r resolve? error-ch child)))
         (async/map vector))
    (go [])))

(defn tree-chan
  "Returns a channel that will eventually contain the stream of index nodes
  descended from `root` in depth-first order. `resolve?` is a boolean function
  that will be applied to each node to determine whether or not the data
  associated with that node will be resolved from disk using the supplied
  `Resolver` `r`. `include?` is a boolean function that will be applied to each
  node to determine if it will be included in the final output node stream, `n`
  is an optional parameter specifying the number of nodes to load concurrently,
  and `xf` is an optional transducer that will transform the output stream if
  supplied."
  ([r root resolve? include? error-ch]
   (tree-chan r root resolve? include? 1 identity error-ch))
  ([r root resolve? include? n xf error-ch]
   (let [out (chan n xf)]
     (go
       (let [root-node (<! (resolve-when r resolve? error-ch root))]
         (loop [stack [root-node]]
           (when-let [node (peek stack)]
             (let [stack* (pop stack)]
               (if (or (leaf? node)
                       (expanded? node))
                 (do (when (include? node)
                       (>! out (unmark-expanded node)))
                     (recur stack*))
                 (let [children (<! (resolve-children-when r resolve? error-ch node))
                       stack**  (-> stack*
                                    (conj (mark-expanded node))
                                    (into (rseq children)))]
                   (recur stack**))))))
         (async/close! out)))
     out)))
