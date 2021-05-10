(ns fluree.db.index
  (:require [clojure.data.avl :as avl]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            #?(:clj  [clojure.core.async :refer [go <!] :as async]
               :cljs [cljs.core.async :refer [go <!] :as async])
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]))

(def types
  "The five possible index orderings based on the subject, predicate, object,
  and transaction flake attributes"
  #{:spot :psot :post :opst :tspo})

(defrecord IndexConfig [index-type comparator historyComparator])

#?(:clj
   (defmethod print-method IndexConfig [^IndexConfig config, ^java.io.Writer w]
     (.write w (str "#FdbIndexConfig "))
     (binding [*out* w]
       (pr {:idx-type (:index-type config)}))))

(def default-configs
  "Map of default index configuration objects for the five index types"
  {:spot (map->IndexConfig {:index-type :spot
                            :comparator flake/cmp-flakes-spot-novelty})
   :psot (map->IndexConfig {:index-type :psot
                            :comparator flake/cmp-flakes-psot-novelty})
   :post (map->IndexConfig {:index-type :post
                            :comparator flake/cmp-flakes-post-novelty})
   :opst (map->IndexConfig {:index-type :opst
                            :comparator flake/cmp-flakes-opst-novelty})
   :tspo (map->IndexConfig {:index-type :tspo
                            :comparator flake/cmp-flakes-block})})

(defn node?
  [x]
  (not (-> x :leaf nil?)))

(defn leaf?
  [node]
  (-> node :leaf true?))

(defn branch?
  [node]
  (-> node :leaf false?))

(defn first-flake
  [node]
  (cond
    (leaf? node)   (:first-flake node)
    (branch? node) (-> node :children first key)))

(defn rhs
  [node]
  (:rhs node))

(defn resolved?
  [node]
  (cond
    (leaf? node)   (not (nil? (:flakes node)))
    (branch? node) (not (nil? (:children node)))))

(defn lookup
  [node flake]
  (if (and (branch? node)
           (resolved? node))
    (let [{:keys [children]} node]
      (-> children
          (avl/nearest <= flake)
          (or (first children))
          val))
    (throw (ex-info (str "lookup is only supported on resolved branch nodes.")
                    {:status 500, :error :db/unexpected-error,
                     ::node node}))))

(defn lookup-after
  [node flake]
  (if (and (branch? node)
           (resolved? node))
    (let [{:keys [children]} node]
      (-> children
          (avl/nearest > flake)
          (or (last children))
          val))
    (throw (ex-info (str "lookup-after is only supported on resolved branch nodes.")
                    {:status 500, :error :db/unexpected-error,
                     ::node node}))))

(defn lookup-leaf
  [node flake]
  (go-try
   (if (and (branch? node)
            (resolved? node))
     (loop [child (lookup node flake)]
       (if (leaf? child)
         child
         (recur (<? (resolve child)))))
     (ex-info (str "lookup-leaf is only supported on resolved branch nodes.")
              {:status 500, :error :db/unexpected-error,
               ::node node}))))

(defn lookup-leaf-after
  [node flake]
  (go-try
   (if (and (branch? node)
            (resolved? node))
     (loop [child (lookup-after node flake)]
       (if (leaf? child)
         child
         (recur (<? (resolve child)))))
     (ex-info (str "lookup-leaf is only supported on resolved branch nodes.")
              {:status 500, :error :db/unexpected-error,
               ::node node}))))

(defn empty-leaf
  [network dbid idx-config]
  (let [first-flake flake/maximum]
    {:config idx-config,
     :network network,
     :dbid dbid,
     :id :empty,
     :leaf true,
     :first-flake first-flake,
     :rhs nil,
     :size 0,
     :block 0,
     :t 0,
     :tt-id nil,
     :leftmost? true}))

(defn child-entry
  [{:keys [first-flake] :as node}]
  [first-flake node])

(defn child-map
  [cmp & child-nodes]
  (->> child-nodes
       (mapcat child-entry)
       (apply avl/sorted-map-by cmp)))

(defn empty-branch
  ([conn network dbid idx-type]
   (empty-branch conn default-configs network dbid idx-type))
  ([conn index-configs network dbid idx-type]
   (let [idx-config (get index-configs idx-type)
         _          (assert idx-config (str "No index config found for index: " idx-type))
         comparator (:historyComparator idx-config)
         _          (assert comparator (str "No index comparator found for index: " idx-type))

         child-node (empty-leaf network dbid idx-config)
         children   (child-map comparator child-node)]
     {:config idx-config
      :network network
      :dbid dbid
      :id :empty
      :leaf false
      :first-flake first-flake
      :rhs nil
      :children children
      :size 0
      :block 0
      :t 0
      :tt-id nil,
      :leftmost? true})))

(defn after-t?
  [t flake]
  (-> flake flake/t (< t)))

(defn filter-after
  [t flakes]
  (filter (partial after-t? t) flakes))

(defn flakes-through
  [t flakes]
  (->> flakes
       (filter-after t)
       (flake/disj-all flakes)))

(defn remove-latest
  [[first-flake & other-flakes]]
  (last (reduce (fn [[latest rest] flake]
                  (if (pos? (flake/cmp-tx latest flake))
                    [latest (conj rest flake)]
                    [flake (conj rest latest)]))
                [first-flake #{}]
                other-flakes)))

(defn flakes-before
  [t flakes]
  (->> flakes
       (group-by (fn [f]
                   [flake/s flake/p flake/o]))
       vals
       (mapcat #(->> %
                     (filter (complement (partial after-t? t)))
                     remove-latest))))

(defn flake-tx-range
  [from-t to-t flakes]
  (let [out-of-range (concat (flakes-before from-t flakes)
                             (filter-after to-t flakes))]
    (flake/disj-all flakes out-of-range)))

(defn as-of
  [t flakes]
  (flake-tx-range t t flakes))

(defn novelty-subrange
  [novelty first-flake rhs leftmost?]
  (cond
    ;; standard case.. both left and right boundaries
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
    novelty))

(defn source-novelty-t
  "Given a novelty set, a first-flake and rhs flake boundary,
  returns novelty subrange as a collection.

  If through-t is specified, will return novelty only through the
  specified t."
  ([novelty first-flake rhs leftmost?]
   (source-novelty-t novelty first-flake rhs leftmost? nil))
  ([novelty first-flake rhs leftmost? through-t]
   (let [subrange (novelty-subrange novelty first-flake rhs leftmost?)]
     (cond-> subrange
       through-t (flake/disj-all (filter-after through-t subrange))))))

(defn node-subrange
  [{:keys [first-flake rhs leftmost?] :as node} t flakes]
  (source-novelty-t flakes first-flake rhs leftmost? t))

(defn novelty-flakes-before
  [{:keys [rhs leftmost? flakes], node-t :t, :as node} t idx-novelty remove-preds]
  (let [f-flake            (first-flake node)
        subrange-through-t (source-novelty-t idx-novelty f-flake rhs leftmost? t)]
    (filter (fn [f]
              (and (true? (flake/op f))
                   (not (contains? remove-preds (flake/p f)))))
            subrange-through-t)))

(defn value-at-t
  "Find the value of `node` at transaction `t` by adding new flakes from
  `idx-novelty` to `node` if `t` is newer than `node`, or removing flakes later
  than `t` from `node` if `t` is older than `node`."
  [{:keys [rhs leftmost? flakes], node-t :t, :as node} t idx-novelty remove-preds]
  (if (= node-t t)
    node
    (cond-> node
      (> node-t t)
      (update :flakes flake/conj-all (novelty-flakes-before node t idx-novelty remove-preds))

      (< node-t t)
      (update :flakes flake/disj-all (filter-after t flakes))

      :finally
      (assoc :t t))))

(defn flakes-at-t
  "Find the value of `node` at transaction `t` by adding new flakes from
  `idx-novelty` to `node` if `t` is newer than `node`, or removing flakes later
  than `t` from `node` if `t` is older than `node`."
  [{:keys [flakes], node-t :t, :as node} t idx-novelty remove-preds]
  (cond-> flakes
    (> node-t t)
    (flake/conj-all (novelty-flakes-before t node idx-novelty remove-preds))

    (< node-t t)
    (flake/disj-all (filter-after t flakes))))
