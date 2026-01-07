(ns fluree.db.flake.index.batched-prefix-range-lookup-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.flake :as flake]
            [fluree.db.flake.index :as index]))

(deftest batched-prefix-range-lookup-seek-spanning-test
  (testing ":seek mode accumulates matches across leaves for spanning lookups"
    (let [cmp flake/cmp-flakes-spot
          ;; Two resolved leaves with a gap between rhs and next :first.
          leaf1-flakes (apply flake/sorted-set-by cmp
                              [(flake/create 1 1 1 1 1 false 0)
                               (flake/create 1 1 1 1 1 false 1)
                               (flake/create 1 1 1 1 1 false 2)])
          leaf2-flakes (apply flake/sorted-set-by cmp
                              [(flake/create 1 1 1 1 1 false 10)
                               (flake/create 1 1 1 1 1 false 11)])
          leaf1 {:leaf true
                 :first (flake/create 1 1 1 1 1 false 0)
                 :rhs   (flake/create 1 1 1 1 1 false 2)
                 :flakes leaf1-flakes}
          leaf2 {:leaf true
                 :first (flake/create 1 1 1 1 1 false 10)
                 :rhs   nil
                 :flakes leaf2-flakes}
          root  {:leaf false
                 :first (flake/create 1 1 1 1 1 false 0)
                 :rhs nil
                 :comparator cmp
                 :children (flake/sorted-map-by cmp
                                                (:first leaf1) leaf1
                                                (:first leaf2) leaf2)}
          r     (reify index/Resolver
                  (resolve [_ node] node))
          error-ch (async/chan 1)
          ;; One lookup spans both leaves; one is contained in leaf1.
          l1 [:contained]
          l2 [:spanning]
          lookup->range {l1 [(flake/create 1 1 1 1 1 false 0) (flake/create 1 1 1 1 1 false 2)]
                         l2 [(flake/create 1 1 1 1 1 false 1) (flake/create 1 1 1 1 1 false 11)]}
          lookups [l1 l2]
          out-ch (index/batched-prefix-range-lookup r root lookups lookup->range error-ch {:mode :seek :buffer 10})
          results (loop [acc {}]
                    (if-let [[lk flakes] (async/<!! out-ch)]
                      (recur (assoc acc lk flakes))
                      acc))]
      (is (contains? results l1))
      (is (contains? results l2))
      (is (= 3 (count (get results l1))) "contained lookup should include all leaf1 matches")
      (is (= 4 (count (get results l2))) "spanning lookup should include matches from both leaves")
      (is (nil? (async/poll! error-ch)) "no errors should be emitted"))))


