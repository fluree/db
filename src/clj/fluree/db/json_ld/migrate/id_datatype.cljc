(ns fluree.db.json-ld.migrate.id-datatype
  (:require [clojure.core.async :as async :refer [<! go]]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.flake.index.rebalance :as rebalance]
            [fluree.db.flake.index :as index]))

(defn migrate-flake
  [f]
  (if (= (flake/dt f) const/$xsd:anyURI)
    (assoc f :dt const/$id)
    f))

(defn migrate-leaves-xf
  [ledger-alias t cmp]
  (comp :flakes
        cat
        (map migrate-flake)
        (rebalance/build-leaves ledger-alias t cmp)))

(defn migrate-leaves
  [{:keys [alias conn t] :as db} idx error-ch]
  (let [root    (get db idx)
        cmp     (get index/comparators idx)
        leaf-xf (comp rebalance/only-leaves
                      (migrate-leaves-xf alias t cmp))]
    (->> (index/tree-chan conn root rebalance/always 4 leaf-xf error-ch)
         (rebalance/write-nodes db idx error-ch))))

(defn migrate-index
  [db idx branch-size error-ch]
  (go
    (let [leaves (<! (migrate-leaves db idx error-ch))]
      (loop [branches (<! (rebalance/homogenize-branches db idx branch-size error-ch leaves))]
        (if (= (count branches) 1)
          (let [root (first branches)]
            {:idx idx, :root root})
          (recur (<! (rebalance/homogenize-branches db idx branch-size error-ch branches))))))))

(defn migrate
  [db branch-size error-ch]
  (->> index/types
       (map (fn [idx]
              (migrate-index db idx branch-size error-ch)))
       async/merge
       (async/reduce (fn [db {:keys [idx root]}]
                       (assoc db idx root))
                     db)))
