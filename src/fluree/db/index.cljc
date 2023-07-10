(ns fluree.db.index
  (:refer-clojure :exclude [resolve])
  (:require [fluree.db.flake :as flake]
            #?(:clj  [clojure.core.async :refer [chan go <! >!] :as async]
               :cljs [cljs.core.async :refer [chan go <! >!] :as async])
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

(defn resolved?
  "Returns `true` if the data associated with the index node map `node` is fully
  resolved from storage"
  [node]
  (cond
    (leaf? node)   (not (nil? (:flakes node)))
    (branch? node) (not (nil? (:children node)))))

(defn resolved-leaf?
  [node]
  (and (leaf? node)
       (resolved? node)))

(defn unresolve
  "Clear the populated child node attributes from the supplied `node` map if it
  represents a branch, or the populated flakes if `node` represents a leaf."
  [node]
  (cond
    (leaf? node)   (dissoc node :flakes)
    (branch? node) (dissoc node :children)))

(defn add-flakes
  [leaf flakes]
  (let [new-leaf  (-> leaf
                      (update :flakes flake/conj-all flakes)
                      (update :size (fn [size]
                                      (->> flakes
                                           (map flake/size-flake)
                                           (reduce + size)))))
        new-first (some-> new-leaf :flakes first)]
    (assoc new-leaf :first new-first)))

(defn rem-flakes
  [leaf flakes]
  (let [new-leaf  (-> leaf
                      (update :flakes flake/disj-all flakes)
                      (update :size (fn [size]
                                      (->> flakes
                                           (map flake/size-flake)
                                           (reduce - size)))))
        new-first (some-> new-leaf :flakes first)]
    (assoc new-leaf :first new-first)))

(defn ->node-comparator
  "Return an index node comparator that compares nodes by considering two nodes
  equal if their flake intervals overlap. Otherwise, a node is considered lower
  than another if that node's flake interval is lower. The interval comparisons
  are based on the supplied flake comparator `flake-cmp`."
  [flake-cmp]
  (fn [node-x node-y]
    (let [rhs-x   (:rhs node-x)
          first-y (:first node-y)]
      (if (and rhs-x
               first-y
               (<= (flake-cmp rhs-x first-y)
                   0))
        -1
        (let [first-x (:first node-x)
              rhs-y   (:rhs node-y)]
          (if (and first-x
                   rhs-y
                   (>= (flake-cmp first-x rhs-y)
                       0))
            1
            0))))))

(defn sorted-node-set-by
  ([cmp]
   (let [node-cmp (->node-comparator cmp)]
     (flake/sorted-set-by node-cmp)))
  ([cmp nodes]
   (let [node-cmp (->node-comparator cmp)]
     (flake/sorted-set-by node-cmp nodes))))

(defn empty-leaf
  "Returns a blank leaf node map for the provided `ledger-alias` and index
  comparator `cmp`."
  [ledger-alias cmp]
  {:comparator   cmp
   :ledger-alias ledger-alias
   :id           :empty
   :tempid       (random-uuid)
   :leaf         true
   :first        nil
   :rhs          nil
   :size         0
   :t            0})

(defn leftmost?
  [node]
  (-> node :first nil?))

(defn rightmost?
  [node]
  (-> node :rhs nil?))

(defn descendant?
  "Checks if the `node` passed in the second argument is a descendant of the
  `branch` passed in the first argument"
  [{:keys [rhs], cmp :comparator, first-flake :first, :as branch}
   {node-first :first, node-rhs :rhs, :as node}]
  (if-not (branch? branch)
    false
    (and (or (leftmost? branch)
             (and (not (leftmost? node))
                  (not (pos? (cmp first-flake node-first)))))
         (or (rightmost? branch)
             (and (not (rightmost? node))
                  (not (pos? (cmp node-rhs rhs))))))))

(defn empty-branch
  "Returns a blank branch node which contains a single empty leaf node for the
  provided `ledger-alias` and index comparator `cmp`."
  [ledger-alias cmp]
  (let [child-node (empty-leaf ledger-alias cmp)
        children   (sorted-node-set-by cmp [child-node])]
    {:comparator   cmp
     :ledger-alias ledger-alias
     :id           :empty
     :tempid       (random-uuid)
     :leaf         false
     :first        nil
     :rhs          nil
     :children     children
     :size         0
     :t            0}))

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
  [{:keys [rhs], first-flake :first, :as node} through-t novelty]
  (log/trace "novelty-subrange: first-flake:" first-flake "\nrhs:" rhs)
  (let [subrange (if (and (leftmost? node)
                          (rightmost? node))
                   novelty
                   (flake/slice novelty first-flake rhs))]
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

(defn resolve-t-range
  [resolver node novelty from-t to-t]
  (go-try
    (let [resolved (<? (resolve resolver node))
          flakes   (t-range resolved novelty from-t to-t)]
      (-> resolved
          (dissoc :t)
          (assoc :from-t from-t
                 :to-t   to-t
                 :flakes  flakes)))))

(defrecord TRangeResolver [node-resolver novelty from-t to-t]
  Resolver
  (resolve [_ node]
    (if (branch? node)
      (resolve node-resolver node)
      (resolve-t-range node-resolver node novelty from-t to-t))))

(defrecord CachedTRangeResolver [node-resolver novelty from-t to-t lru-cache-atom]
  Resolver
  (resolve [_ {:keys [id tempid tt-id] :as node}]
    (if (branch? node)
      (resolve node-resolver node)
      (conn-cache/lru-lookup
        lru-cache-atom
        [::t-range id tempid tt-id from-t to-t]
        (fn [_]
          (resolve-t-range node-resolver node novelty from-t to-t))))))

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

(defn- mark-expanded
  [node]
  (assoc node ::expanded true))

(defn- unmark-expanded
  [node]
  (dissoc node ::expanded))

(defn- expanded?
  [node]
  (-> node ::expanded true?))

(defn trim-left?
  [cmp first-flake start-flake]
  (if (some? first-flake)
    (if (some? start-flake)
      (> 0 (cmp start-flake first-flake))
      false)
    (if (some? start-flake)
      true
      false)))

(defn trim-right?
  [cmp rhs end-flake]
  (if (some? rhs)
    (if (some? end-flake)
      (> 0 (cmp rhs end-flake))
      false)
    (if (some? end-flake)
      true
      false)))

(defn trim-branch
  [branch new-first new-rhs]
  (let [start-node {:first new-first, :rhs new-first}
        end-node   {:first new-rhs, :rhs new-rhs}]
    ;; Take an rslice here because `tree-chan` expects a branch node's children
    ;; to be resolved in reverse order to ensure that the node sequence returned
    ;; from `tree-chan` is in depth-first order.
    (update branch :children flake/rslice end-node start-node)))

(defn trim-leaf
  [leaf new-first new-rhs]
  (update leaf :flakes flake/slice new-first new-rhs))

(defn trim-node
  [node start-flake end-flake]
  (let [cmp         (:comparator node)
        first-flake (:first node)
        rhs         (:rhs node)
        new-first   (when (trim-left? cmp first-flake start-flake)
                      start-flake)
        new-rhs     (when (trim-right? cmp rhs end-flake)
                      end-flake)]
    (if (leaf? node)
      (if (or new-first new-rhs)
        (trim-leaf node new-first new-rhs)
        node)
      (trim-branch node new-first new-rhs)))) ; always trim branches to reverse
                                              ; their children

(defn try-resolve
  [r start-flake end-flake error-ch node]
  (go
    (try* (let [resolved (<? (resolve r node))]
            (trim-node resolved start-flake end-flake))
          (catch* e
                  (log/error e
                             "Error resolving index node:"
                             (select-keys node [:id :ledger-alias]))
                  (>! error-ch e)))))

(defn resolve-when
  [r start-flake end-flake resolve? error-ch node]
  (if (resolve? node)
    (try-resolve r start-flake end-flake error-ch node)
    (doto (chan)
      (async/put! node))))

(defn resolve-children-when
  [r start-flake end-flake resolve? error-ch branch]
  (if (resolved? branch)
    (->> branch
         :children
         (map (fn [child]
                (resolve-when r start-flake end-flake resolve? error-ch child)))
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
  ([r root resolve? error-ch]
   (tree-chan r root resolve? 1 identity error-ch))
  ([r root resolve? n xf error-ch]
   (tree-chan r root nil nil resolve? n xf error-ch))
  ([r root start-flake end-flake resolve? n xf error-ch]
   (let [out (chan n xf)]
     (go
       (let [root-node (<! (resolve-when r start-flake end-flake resolve? error-ch root))]
         (loop [stack [root-node]]
           (when-let [node (peek stack)]
             (let [stack* (pop stack)]
               (if (or (leaf? node)
                       (expanded? node))
                 (do (>! out (unmark-expanded node))
                     (recur stack*))
                 (let [children (<! (resolve-children-when r start-flake end-flake resolve? error-ch node))
                       stack**  (-> stack*
                                    (conj (mark-expanded node))
                                    (into children))]
                   (recur stack**))))))
         (async/close! out)))
     out)))
