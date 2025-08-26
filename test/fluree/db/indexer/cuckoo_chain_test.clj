(ns fluree.db.indexer.cuckoo-chain-test
  "Test suite for cuckoo filter chain functionality, growth, and collision handling."
  (:require [alphabase.core :as alphabase]
            [clojure.test :refer [deftest testing is]]
            [fluree.crypto :as crypto]
            [fluree.db.indexer.cuckoo :as cuckoo]))

(defn- test-hash
  "Create a valid base32 hash for testing from a string."
  [s]
  (-> s
      crypto/sha2-256  ; Returns hex string
      alphabase/hex->bytes  ; Convert hex to bytes
      alphabase/bytes->base32))  ; Already lowercase from fluree crypto

(deftest chain-growth-test
  (testing "Chain grows when capacity is exceeded"
    ;; Create a filter chain with very small capacity for testing
    ;; Note: minimum bucket count is 16, so actual capacity is 16*4*0.95 = ~60 items
    (let [small-capacity 10
          small-filter (cuckoo/create-filter small-capacity)
          chain (cuckoo/single-filter->chain small-filter)
          items (map #(test-hash (str "item-" %)) (range 100))  ; Use 100 items to ensure overflow
          ;; Add items until we exceed capacity and force chain growth
          chain-with-items (reduce cuckoo/add-item-chain chain items)
          filter-count (count (:filters chain-with-items))]

      (testing "Multiple filters created when capacity exceeded"
        (is (> filter-count 1)
            (str "Should have multiple filters, got " filter-count)))

      (testing "All items are findable across chain"
        (doseq [item items]
          (is (cuckoo/contains-hash-chain? chain-with-items item)
              (str "Should find " item " in chain"))))

      (testing "Items not added are not found"
        (is (not (cuckoo/contains-hash-chain? chain-with-items (test-hash "not-in-chain")))))

      (testing "Remove items across multiple filters"
        (let [items-to-remove (take 50 items)
              items-to-keep (drop 50 items)
              chain-after-remove (reduce cuckoo/remove-item-chain
                                         chain-with-items
                                         items-to-remove)]

            ;; Removed items should not be found
          (doseq [item items-to-remove]
            (is (not (cuckoo/contains-hash-chain? chain-after-remove item))
                (str item " should be removed")))

            ;; Kept items should still be found
          (doseq [item items-to-keep]
            (is (cuckoo/contains-hash-chain? chain-after-remove item)
                (str item " should still be present")))

            ;; Empty filters should be removed
          (testing "Empty filters are cleaned up"
            (let [filters-after (:filters chain-after-remove)]
              (is (every? #(pos? (:count %)) filters-after)
                  "No empty filters should remain"))))))))

(deftest collision-handling-test
  (testing "Handle items with similar patterns"
    (let [chain (cuckoo/create-filter-chain)
          ;; Create items that might collide
          base-items [(test-hash "aaaa") (test-hash "aaab") (test-hash "aaac")
                      (test-hash "aaad") (test-hash "aaae")
                      (test-hash "baaa") (test-hash "baab") (test-hash "baac")
                      (test-hash "baad") (test-hash "baae")]
          chain-with-items (reduce cuckoo/add-item-chain chain base-items)]

      (testing "All similar items are stored"
        (doseq [item base-items]
          (is (cuckoo/contains-hash-chain? chain-with-items item)
              (str item " should be found"))))

      (testing "Removing one item doesn't affect similar items"
        (let [item-to-remove (test-hash "aaaa")
              similar-items (remove #{item-to-remove} base-items)
              chain-after-remove (cuckoo/remove-item-chain chain-with-items item-to-remove)]

          (is (not (cuckoo/contains-hash-chain? chain-after-remove item-to-remove))
              "Removed item should not be found")

          (doseq [item similar-items]
            (is (cuckoo/contains-hash-chain? chain-after-remove item)
                (str item " should still be found after removing similar item"))))))))

(deftest chain-serialization-test
  (testing "Chain serialization and deserialization"
    (let [small-filter (cuckoo/create-filter 5)
          chain (cuckoo/single-filter->chain small-filter)
          items (map #(test-hash (str "serialize-" %)) (range 20))
          chain-with-items (reduce cuckoo/add-item-chain chain items)
          serialized (cuckoo/serialize chain-with-items)
          deserialized (cuckoo/deserialize serialized)]

      (testing "Deserialized chain has correct structure"
        (is (= (:version deserialized) 2)))

      (testing "All items findable after deserialization"
        (doseq [item items]
          (is (cuckoo/contains-hash-chain? deserialized item)
              (str item " should be found after deserialization"))))

      (testing "Items can be added to deserialized chain"
        (let [new-item (test-hash "after-deserialize")
              updated (cuckoo/add-item-chain deserialized new-item)]
          (is (cuckoo/contains-hash-chain? updated new-item))
          (is (every? #(cuckoo/contains-hash-chain? updated %) items)
              "Original items still present"))))))

(deftest proactive-growth-test
  (testing "Chain grows proactively at 90% capacity"
    (let [;; Create chain with small filter for testing
          ;; Minimum is 16 buckets * 4 = 64 capacity
          ;; 90% of 64 = 57.6, so should trigger growth at 58 items
          small-filter (cuckoo/create-filter 20)
          chain (cuckoo/single-filter->chain small-filter)
          ;; Add items to get close to 90% capacity (57 items)
          items (map #(test-hash (str "proactive-" %)) (range 57))
          chain-with-items (reduce cuckoo/add-item-chain chain items)]

      (testing "Should have only one filter before 90%"
        (is (= 1 (count (:filters chain-with-items)))))

      ;; Add one more item to trigger proactive growth (58th item = >90%)
      (let [chain-after (cuckoo/add-item-chain chain-with-items (test-hash "trigger-growth"))]
        (testing "Should proactively create second filter at 90% capacity"
          (is (= 2 (count (:filters chain-after))))
          (is (= 58 (-> chain-after cuckoo/get-chain-stats :total-count))))

        (testing "All items still findable after growth"
          (doseq [item items]
            (is (cuckoo/contains-hash-chain? chain-after item)))
          (is (cuckoo/contains-hash-chain? chain-after (test-hash "trigger-growth"))))))))

(deftest edge-cases-test
  (testing "Edge cases for chain operations"
    (testing "Empty chain operations"
      (let [empty-chain (cuckoo/create-filter-chain)]
        (is (not (cuckoo/contains-hash-chain? empty-chain (test-hash "anything"))))
        ;; Removing from empty chain should return the same chain structure
        (let [after-remove (cuckoo/remove-item-chain empty-chain (test-hash "anything"))]
          (is (= (:version after-remove) (:version empty-chain))
              "Version should be unchanged")
          (is (= (count (:filters after-remove)) (count (:filters empty-chain)))
              "Filter count should be unchanged"))))

    (testing "Single item chain"
      (let [chain (cuckoo/create-filter-chain)
            single-chain (cuckoo/add-item-chain chain (test-hash "only-item"))]
        (is (cuckoo/contains-hash-chain? single-chain (test-hash "only-item")))

        (let [empty-again (cuckoo/remove-item-chain single-chain (test-hash "only-item"))]
          ;; After removing the only item, should have empty filter
          (is (or (empty? (:filters empty-again))
                  (zero? (-> empty-again :filters first :count)))
              "Chain should be empty after removing only item"))))))