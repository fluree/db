(ns fluree.db.flake.index.rebalance
  (:require [clojure.core.async :as async :refer [<! >! go go-loop]]
            [fluree.db.flake :as flake]
            [fluree.db.flake.index :as index]
            [fluree.db.flake.index.novelty :refer [update-node-id]]
            [fluree.db.flake.index.storage :as storage]
            [fluree.db.util :refer [try* catch*]]
            [fluree.db.util.async :refer [<?]]
            [fluree.db.util.log :as log]))

(defn partition-flakes
  [target-size]
  (fn [xf]
    (let [current-chunk (volatile! [])
          current-size  (volatile! 0)]
      (fn
        ([]
         (xf))

        ([result f]
         (let [f-size     (flake/size-flake f)
               total-size (vswap! current-size + f-size)]
           (if (<= total-size target-size)
             (do (vswap! current-chunk conj f)
                 result)
             (let [chunk @current-chunk]
               (vreset! current-chunk [])
               (vreset! current-size 0)
               (xf result chunk)))))

        ([result]
         (if-let [chunk (not-empty @current-chunk)]
           (do (vreset! current-chunk [])
               (vreset! current-size 0)
               (-> result
                   (xf chunk)
                   xf))
           (xf result)))))))

(defn build-leaves
  [ledger-alias t cmp]
  (fn [xf]
    (let [last-leaf (volatile! nil)]
      (fn
        ([]
         (xf))

        ([result flakes]
         (let [first-flake (first flakes)
               flake-size  (flake/size-bytes flakes)
               flake-set   (apply flake/sorted-set-by cmp flakes)
               next-leaf   (assoc (index/empty-leaf ledger-alias cmp)
                                  :first first-flake
                                  :flakes flake-set
                                  :size flake-size
                                  :t t
                                  :id (random-uuid))]
           (if-let [leaf @last-leaf]
             (let [leaf*      (assoc leaf :rhs first-flake)
                   next-leaf* (assoc next-leaf :leftmost? false)]
               (vreset! last-leaf next-leaf*)
               (xf result leaf*))
             (let [next-leaf* (assoc next-leaf :leftmost? true)]
               (vreset! last-leaf next-leaf*)
               result))))

        ([result]
         (if-let [leaf @last-leaf]
           (let [leaf* (assoc leaf :rhs nil)]
             (vreset! last-leaf nil)
             (-> result
                 (xf leaf*)
                 xf))
           (xf result)))))))

(defn rebalance-leaves-xf
  [ledger-alias t target-size flake-xf cmp]
  (let [flake-xf* (or flake-xf identity)]
    (comp :flakes
          cat
          flake-xf*
          (partition-flakes target-size)
          (build-leaves ledger-alias t cmp))))

(def always
  (constantly true))

(def only-leaves
  (filter index/leaf?))

(defn rebalance-leaves
  [{:keys [alias t index-catalog] :as db} idx target-size flake-xf error-ch]
  (let [root    (get db idx)
        cmp     (get index/comparators idx)
        leaf-xf (comp only-leaves
                      (rebalance-leaves-xf alias t target-size flake-xf cmp))]
    (index/tree-chan index-catalog root always 4 leaf-xf error-ch)))

(defn write-leaf
  [{:keys [alias index-catalog] :as _db} idx leaf error-ch]
  (go
    (try*
      (let [write-response (<? (storage/write-leaf index-catalog alias idx leaf))]
        (-> leaf
            (update-node-id write-response)
            index/unresolve))
      (catch* e
        (log/error! ::rebalanced-leaf-write e {:msg "Error writing rebalanced flake index leaf node"})
        (log/error e "Error writing rebalanced flake index leaf node")
        (>! error-ch e)))))

(defn write-branch
  [{:keys [alias index-catalog] :as _db} idx branch error-ch]
  (go
    (try*
      (let [write-response (<? (storage/write-branch index-catalog alias idx branch))]
        (-> branch
            (update-node-id write-response)
            index/unresolve))
      (catch* e
        (log/error! ::rebalanced-branch-write e {:msg "Error writing rebalanced flake index branch node"})
        (log/error e "Error writing rebalanced flake index branch node")
        (>! error-ch e)))))

(defn write-node
  [db idx node error-ch]
  (if (index/leaf? node)
    (write-leaf db idx node error-ch)
    (write-branch db idx node error-ch)))

(defn write-nodes
  [db idx error-ch node-ch]
  (go-loop [written-nodes []]
    (if-let [node (<! node-ch)]
      (let [written-node (<! (write-node db idx node error-ch))]
        (recur (conj written-nodes written-node)))
      written-nodes)))

(defn build-branches
  [{:keys [alias t] :as _db} idx]
  (let [cmp (get index/comparators idx)]
    (fn [xf]
      (let [last-branch (volatile! nil)]
        (fn
          ([]
           (xf))

          ([result nodes]
           (let [child-map   (index/child-map cmp nodes)
                 first-flake (->> child-map first key)
                 total-size  (transduce (map :size) + nodes)
                 next-branch (assoc (index/empty-branch alias cmp)
                                    :first first-flake
                                    :children child-map
                                    :size total-size
                                    :t t
                                    :id (random-uuid))]
             (if-let [branch @last-branch]
               (let [branch*      (assoc branch :rhs first-flake)
                     next-branch* (assoc next-branch :leftmost? false)]
                 (vreset! last-branch next-branch*)
                 (xf result branch*))
               (let [next-branch* (assoc next-branch :leftmost? true)]
                 (vreset! last-branch next-branch*)
                 result))))

          ([result]
           (if-let [branch @last-branch]
             (let [branch* (assoc branch :rhs nil)]
               (vreset! last-branch nil)
               (-> result
                   (xf branch*)
                   xf))
             (xf result))))))))

(defn homogenize-leaves
  [db idx leaf-size flake-xf error-ch]
  (->> (rebalance-leaves db idx leaf-size flake-xf error-ch)
       (write-nodes db idx error-ch)))

(defn homogenize-branches
  [db idx branch-size error-ch child-nodes]
  (go
    (let [branch-xf (comp (partition-all branch-size)
                          (build-branches db idx))
          branch-ch (async/chan 4 branch-xf)]
      (async/onto-chan! branch-ch child-nodes)
      (write-nodes db idx error-ch branch-ch))))

(defn homogenize-index
  [db idx leaf-size branch-size flake-xf error-ch]
  (go
    (let [leaves (<! (homogenize-leaves db idx leaf-size flake-xf error-ch))]
      (loop [branches (<! (homogenize-branches db idx branch-size error-ch leaves))]
        (if (= (count branches) 1)
          (let [root (first branches)]
            {:idx idx, :root root})
          (recur (<! (homogenize-branches db idx branch-size error-ch branches))))))))

(defn homogenize
  ([db leaf-size branch-size flake-xf error-ch]
   (->> index/types
        (map (fn [idx]
               (homogenize-index db idx leaf-size branch-size flake-xf error-ch)))
        async/merge
        (async/reduce (fn [db {:keys [idx root]}]
                        (assoc db idx root))
                      db))))
