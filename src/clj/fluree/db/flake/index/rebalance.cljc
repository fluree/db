(ns fluree.db.flake.index.rebalance
  (:require [clojure.core.async :as async :refer [<! >! go go-loop]]
            [fluree.db.flake :as flake]
            [fluree.db.flake.index :as index]
            [fluree.db.flake.index.storage :as storage]
            [fluree.db.flake.index.novelty :refer [update-node-id]]
            [fluree.db.util.core :refer [try* catch*]]
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
  [ledger-alias t target-size cmp]
  (comp :flakes
        cat
        (partition-flakes target-size)
        (build-leaves ledger-alias t cmp)))

(def always
  (constantly true))

(def only-leaves
  (filter index/leaf?))

(defn rebalance-leaves
  [{:keys [alias conn t] :as db} idx target-size error-ch]
  (let [root    (get db idx)
        cmp     (get index/comparators idx)
        leaf-xf (comp only-leaves
                      (rebalance-leaves-xf alias t target-size cmp))]
    (index/tree-chan conn root always 4 leaf-xf error-ch)))

(defn write-leaf
  [db idx leaf error-ch]
  (go
    (try*
      (let [write-response (<? (storage/write-leaf db idx leaf))]
        (-> leaf
            (update-node-id write-response)
            index/unresolve))
      (catch* e
              (log/error e "Error writing rebalanced flake index leaf node")
              (>! error-ch e)))))

(defn write-branch
  [db idx branch error-ch]
  (go
    (try*
      (let [write-response (<? (storage/write-branch db idx branch))]
        (-> branch
            (update-node-id write-response)
            index/unresolve))
      (catch* e
              (log/error e "Error writing rebalanced flake index branch node")
              (>! error-ch e)))))

(defn write-node
  [db idx node error-ch]
  (if (index/leaf? node)
    (write-leaf db idx node error-ch)
    (write-branch db idx node error-ch)))

(defn write-nodes
  [db idx error-ch node-ch]
  (let [out-ch (async/chan)]
    (go-loop []
      (if-let [node (<! node-ch)]
        (let [written-node (<! (write-node db idx node error-ch))]
          (>! out-ch written-node)
          (recur))
        (async/close! out-ch)))
    out-ch))
