(ns fluree.db.flake.index
  (:refer-clojure :exclude [resolve])
  (:require [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            #?(:clj  [clojure.core.async :refer [chan go <! >!] :as async]
               :cljs [cljs.core.async :refer [chan go <! >!] :as async])
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.cache :as cache]))

(def comparators
  "Map of default index comparators for the four index types"
  {:spot flake/cmp-flakes-spot
   :post flake/cmp-flakes-post
   :opst flake/cmp-flakes-opst
   :tspo flake/cmp-flakes-block})

(def types
  "The four possible index orderings based on the subject, predicate, object,
  and transaction flake attributes"
  (-> comparators keys set))

(defn reference?
  [dt]
  (= dt const/$id))

(defn for-components
  "Returns the index that should be used to scan for flakes that match the
  supplied flake components `s` `p` `o` and `o-dt` given when of these supplied
  components are non-nil."
  [s p o o-dt]
  (cond
    s     :spot
    p     :post
    o     (if (reference? o-dt)
            :opst
            (throw (ex-info (str "Illegal reference object value" (::var o))
                            {:status 400 :error :db/invalid-query})))
    :else :spot))

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
  "Returns a blank leaf node map for the provided `ledger-alias` and index
  comparator `cmp`."
  [ledger-alias cmp]
  {:comparator   cmp
   :ledger-alias ledger-alias
   :id           :empty
   :tempid       (random-uuid)
   :leaf         true
   :first        flake/maximum
   :rhs          nil
   :size         0
   :t            0
   :leftmost?    true})

(defn descendant?
  "Checks if the `node` passed in the second argument is a descendant of the
  `branch` passed in the first argument"
  [{:keys [rhs leftmost?], cmp :comparator, first-flake :first, :as branch}
   {node-first :first, node-rhs :rhs, :as _node}]
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
  [cmp child-nodes]
  (->> child-nodes
       (mapcat child-entry)
       (apply flake/sorted-map-by cmp)))

(defn empty-branch
  "Returns a blank branch node which contains a single empty leaf node for the
  provided `ledger-alias` and index comparator `cmp`."
  [ledger-alias cmp]
  (let [child-node (empty-leaf ledger-alias cmp)
        children   (child-map cmp [child-node])]
    {:comparator   cmp
     :ledger-alias ledger-alias
     :id           :empty
     :tempid       (random-uuid)
     :leaf         false
     :first        flake/maximum
     :rhs          nil
     :children     children
     :size         0
     :t            0
     :leftmost?    true}))

(defn after-t?
  "Returns `true` if `flake` has a transaction value after the provided `t`"
  [t flake]
  (flake/t-after? (flake/t flake) t))

(defn before-t?
  "Returns `true` if `flake` has a transaction value before the provided `t`"
  [t flake]
  (flake/t-before? (flake/t flake) t))

(defn filter-after
  "Returns a sequence containing only flakes from the flake set `flakes` with
  transaction values after the provided `t`."
  [t flakes]
  (filter (partial after-t? t) flakes))

(defn filter-before
  [t flakes]
  (filter (partial before-t? t) flakes))

(defn flakes-through
  "Returns an avl-subset of the avl-set `flakes` with transaction values on or
  before the provided `t`."
  [t flakes]
  (->> flakes
       (filter-after t)
       (flake/disj-all flakes)))

(defn flakes-after
  [t flakes]
  (->> flakes
       (filter-before (flake/next-t t))
       (flake/disj-all flakes)))

(defn novelty-subrange
  [{:keys [rhs leftmost?], first-flake :first, :as _node} through-t novelty]
  (log/trace "novelty-subrange: first-flake:" first-flake "\nrhs:" rhs "\nleftmost?" leftmost?)
  (let [subrange (cond
                   ;; standard case: both left and right boundaries
                   (and rhs (not leftmost?))
                   (flake/subrange novelty > first-flake <= rhs)

                   ;; right only boundary
                   (and rhs leftmost?)
                   (flake/subrange novelty <= rhs)

                   ;; left only boundary
                   (and (nil? rhs) (not leftmost?))
                   (flake/subrange novelty > first-flake)

                   ;; no boundary
                   (and (nil? rhs) leftmost?)
                   novelty)]
    (flakes-through through-t subrange)))

(def meta-hash
  (comp flake/hash-meta flake/m))

(def fact-content
  "Function to extract the content being asserted or retracted by a flake."
  (juxt flake/s flake/p flake/o flake/dt meta-hash))

(defn stale-by
  "Returns a vector of flakes from the sorted set `flakes` that are out of date by
  the transaction `from-t` because `flakes` contains another flake with the same
  subject and predicate and a t-value later than that flake but on or before
  `from-t`."
  [from-t flakes]
  (->> flakes
       (flake/remove (partial after-t? from-t))
       (flake/partition-by fact-content)
       (mapcat (fn [flakes]
                 ;; if the last flake pertaining to a unique
                 ;; fact is an assert, then every flake before
                 ;; that is stale. If that item is a retract,
                 ;; then all the flakes are stale.
                 (let [last-flake (flake/last flakes)]
                   (if (flake/op last-flake)
                     (disj flakes last-flake)
                     flakes))))))

(defn t-range
  "Returns a sorted set of flakes that are not out of date between the
  transactions `from-t` and `to-t`."
  ([{:keys [flakes] leaf-t :t :as leaf} novelty from-t to-t]
   (let [latest       (if (> to-t leaf-t)
                        (flake/conj-all flakes (novelty-subrange leaf to-t novelty))
                        flakes)
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

(defrecord CachedTRangeResolver [node-resolver novelty from-t to-t cache]
  Resolver
  (resolve [_ {:keys [id tempid tt-id] :as node}]
    (if (branch? node)
      (resolve node-resolver node)
      (cache/lru-lookup
        cache
        [::t-range id tempid tt-id from-t to-t]
        (fn [_]
          (resolve-t-range node-resolver node novelty from-t to-t))))))

(defn index-store->t-range-resolver
  [{:keys [cache] :as idx-store} novelty from-t to-t]
  (->CachedTRangeResolver idx-store novelty from-t to-t cache))

(defn history-t-range
  "Returns a sorted set of flakes between the transactions `from-t` and `to-t`."
  [{:keys [flakes] leaf-t :t :as leaf} novelty from-t to-t]
  (let [latest       (if (> to-t leaf-t)
                       (flake/conj-all flakes (novelty-subrange leaf to-t novelty))
                       flakes)
        ;; flakes that happen after to-t
        subsequent   (filter-after to-t latest)
        ;; flakes that happen before from-t
        previous     (filter (partial before-t? from-t) latest)
        out-of-range (concat subsequent previous)]
    (flake/disj-all latest out-of-range)))

(defrecord CachedHistoryRangeResolver [node-resolver novelty from-t to-t lru-cache-atom]
  Resolver
  (resolve [_ {:keys [id tempid tt-id] :as node}]
    (if (branch? node)
      (resolve node-resolver node)
      (cache/lru-lookup
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
      (< leaf-t t)
      (add-flakes (novelty-subrange leaf t idx-novelty))

      (> leaf-t t)
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

(defn trim-leaf
  "Remove flakes from the index leaf node `leaf` that are outside of the interval
  defined by `start-flake` and `end-flake`. nil values for either `start-flake`
  or `end-flake` makes that side of the interval unlimited."
  [leaf start-flake end-flake]
  (cond
    (and start-flake end-flake)
    (update leaf :flakes flake/subrange >= start-flake <= end-flake)

    start-flake
    (update leaf :flakes flake/subrange >= start-flake)

    end-flake
    (update leaf :flakes flake/subrange <= end-flake)

    :else
    leaf))

(defn trim-branch
  "Remove child nodes from the index branch node `branch` that do not contain
  flakes in the interval defined by `start-flake` and `end-flake`. nil values
  for either `start-flake` or `end-flake` makes that side of the interval
  unlimited."
  [{:keys [children] :as branch} start-flake end-flake]
  (let [start-key (some->> start-flake (flake/nearest children <=) key)
        end-key   (some->> end-flake (flake/nearest children <=) key)]
    (cond
      (and start-key end-key)
      (update branch :children flake/subrange >= start-key <= end-key)

      start-key
      (update branch :children flake/subrange >= start-key)

      end-key
      (update branch :children flake/subrange <= end-key)

      :else
      branch)))

(defn trim-node
  "Remove flakes or children from the index leaf or branch node `node` that are
  outside of the interval defined by `start-flake` and `end-flake`. nil values
  for either `start-flake` or `end-flake` makes that side of the interval
  unlimited."
  [node start-flake end-flake]
  (if (leaf? node)
    (trim-leaf node start-flake end-flake)
    (trim-branch node start-flake end-flake)))

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
         (map (fn [[_ child]]
                (resolve-when r start-flake end-flake resolve? error-ch child)))
         (async/map vector))
    (go [])))

(defn tree-chan
  "Returns a channel that will eventually contain the stream of index nodes
  descended from `root` in depth-first order. `resolve?` is a boolean function
  that will be applied to each node to determine whether or not the data
  associated with that node will be resolved from disk using the supplied
  `Resolver` `r`. `start-flake` and `end-flake` are flakes for which only nodes
  that contain flakes within the interval defined by them will be considered,
  `n` is an optional parameter specifying the number of nodes to load
  concurrently, and `xf` is an optional transducer that will transform the
  output stream if supplied."
  ([r root resolve? error-ch]
   (tree-chan r root resolve? 1 identity error-ch))
  ([r root resolve? n xf error-ch]
   (tree-chan r root nil nil resolve? n xf error-ch))
  ([r root start-flake end-flake resolve? n xf error-ch]
   (let [out (chan n xf)]
     (go
       (let [root-node (<! (resolve-when r start-flake end-flake
                                         resolve? error-ch root))]
         (loop [stack [root-node]]
           (when-let [node (peek stack)]
             (let [stack* (pop stack)]
               (if (or (leaf? node)
                       (expanded? node))
                 (do (>! out (unmark-expanded node))
                     (recur stack*))
                 (let [children (<! (resolve-children-when r start-flake end-flake
                                                           resolve? error-ch node))
                       stack**  (-> stack*
                                    (conj (mark-expanded node))
                                    (into (rseq children)))] ; reverse children
                                                             ; to ensure final
                                                             ; nodes are in
                                                             ; depth-first order
                   (recur stack**))))))
         (async/close! out)))
     out)))
