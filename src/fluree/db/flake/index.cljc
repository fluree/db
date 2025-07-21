(ns fluree.db.flake.index
  (:refer-clojure :exclude [resolve])
  (:require #?(:clj  [clojure.core.async :refer [chan go <! >!] :as async]
               :cljs [cljs.core.async :refer [chan go <! >!] :as async])
            [fluree.db.cache :as cache]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log :include-macros true]))

(def comparators
  "Map of default index comparators for the four index types"
  (array-map ;; when using futures, can base other calcs on :spot (e.g. size), so ensure comes first
   :spot flake/cmp-flakes-spot
   :post flake/cmp-flakes-post
   :opst flake/cmp-flakes-opst
   :tspo flake/cmp-flakes-block))

(def types
  "The four possible index orderings based on the subject, predicate, object,
  and transaction flake attributes"
  (-> comparators keys vec))

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
                     (update :flakes into flakes)
                     (update :size (fn [size]
                                     (->> flakes
                                          (map flake/size-flake)
                                          (reduce + size)))))
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
  [flake t]
  (flake/t-after? (flake/t flake) t))

(defn before-t?
  "Returns `true` if `flake` has a transaction value before the provided `t`"
  [flake t]
  (flake/t-before? (flake/t flake) t))

(defn filter-by-t
  "Returns a subset of the sorted flake set `flakes` consisting of all elements
  that the supplied predicate function `pred` returns `true` when applied to
  that element and the supplied t value `t`."
  [pred t flakes]
  (loop [[f & r] flakes
         flakes* (transient flakes)]
    (if f
      (if (pred f t)
        (recur r flakes*)
        (recur r (disj! flakes* f)))
      (persistent! flakes*))))

(defn filter-after
  "Returns a flake set containing only flakes from the flake set `flakes` with
  transaction values after the provided `t`."
  [t flakes]
  (filter-by-t after-t? t flakes))

(defn flakes-from
  "Returns a subset of the flake set `flakes` with transaction values greater than
  or equal to `t`."
  [t flakes]
  (filter-after (flake/prev-t t) flakes))

(defn filter-before
  "Returns a subset of the flake set `flakes` with transaction values less than
  `t`."
  [t flakes]
  (filter-by-t before-t? t flakes))

(defn flakes-through
  "Returns an avl-subset of the avl-set `flakes` with transaction values on or
  before the provided `t`."
  [t flakes]
  (filter-before (flake/next-t t) flakes))

(defn novelty-subrange
  [{:keys [rhs leftmost?], first-flake :first, :as _node} through-t novelty-t novelty]
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
    (if (= novelty-t through-t)
      subrange
      (flakes-through through-t subrange))))

(def meta-hash
  (comp flake/hash-meta flake/m))

(def fact-content
  "Function to extract the fact being asserted or retracted by a flake, ignoring
  the `t` value."
  (juxt flake/s flake/p flake/o flake/dt meta-hash))

(defn remove-stale-flakes
  "Removes all flake retractions, along with the flakes they retract, as well as
  all but the latest duplicate flake assertions, from the sorted set.

  Approach is to iterate through the flakes in reverse order, keeping track of
  facts already seen. Then, any flake encountered with the same fact content of
  a previously seen flake is removed from the set as well as any retractions."
  [flakes]
  (loop [to-check (rseq flakes)
         checked  #{}
         flakes*  (transient flakes)]
    (if-let [next-flake (first to-check)]
      (let [r    (rest to-check)
            fact (fact-content next-flake)]
        (if (contains? checked fact)
          (recur r checked (disj! flakes* next-flake))
          (let [checked* (conj checked fact)]
            (if (flake/op next-flake)
              (recur r checked* flakes*)
              (recur r checked* (disj! flakes* next-flake))))))
      (persistent! flakes*))))

(defn t-range
  "Returns a sorted set of flakes that are not out of date between the
  transactions `from-t` and `to-t`."
  [{:keys [flakes] leaf-t :t :as leaf} novelty-t novelty to-t]
  (let [latest (cond
                 (> to-t leaf-t)
                 (into flakes (novelty-subrange leaf to-t novelty-t novelty))

                 (= to-t leaf-t)
                 flakes

                 (< to-t leaf-t)
                 (flakes-through to-t flakes))]
    (remove-stale-flakes latest)))

(defn resolve-t-range
  [resolver node novelty-t novelty to-t]
  (go-try
    (let [resolved (<? (resolve resolver node))
          flakes   (t-range resolved novelty-t novelty to-t)]
      (-> resolved
          (dissoc :t)
          (assoc :to-t   to-t
                 :flakes  flakes)))))

(defrecord TRangeResolver [node-resolver novelty-t novelty to-t]
  Resolver
  (resolve [_ node]
    (if (branch? node)
      (resolve node-resolver node)
      (resolve-t-range node-resolver node novelty-t novelty to-t))))

(defrecord CachedTRangeResolver [node-resolver novelty-t novelty to-t cache]
  Resolver
  (resolve [_ {:keys [id tempid tt-id] :as node}]
    (if (branch? node)
      (resolve node-resolver node)
      (cache/lru-lookup
       cache
       [::t-range id tempid tt-id to-t]
       (fn [_]
         (resolve-t-range node-resolver node novelty-t novelty to-t))))))

(defn index-catalog->t-range-resolver
  [{:keys [cache] :as idx-store} novelty-t novelty to-t]
  (->CachedTRangeResolver idx-store novelty-t novelty to-t cache))

(defn history-t-range
  "Returns a sorted set of flakes between the transactions `from-t` and `to-t`."
  [{:keys [flakes] leaf-t :t :as leaf} novelty-t novelty from-t to-t]
  (let [latest (if (> to-t leaf-t)
                 (into flakes (novelty-subrange leaf to-t novelty-t novelty))
                 (flakes-through to-t flakes))]
    (flakes-from from-t latest)))

(defrecord CachedHistoryRangeResolver [node-resolver novelty-t novelty from-t to-t lru-cache-atom]
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
                 flakes   (history-t-range resolved novelty-t novelty from-t to-t)]
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
