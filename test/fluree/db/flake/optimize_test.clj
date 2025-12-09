(ns fluree.db.flake.optimize-test
  "Unit tests for query optimization functions.
  Integration tests for optimization behavior are in explain_test.clj"
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.flake.optimize :as optimize]))

(deftest optimizable-pattern-test
  (testing "Pattern type recognition using where/pattern-type"
    ;; Patterns are map-entries (created by iterating over a map)
    ;; where/pattern-type returns the key if it's a map-entry, :tuple otherwise

    (let [;; Create map-entries by taking first from a map
          tuple-pattern (first {:tuple [:s :p :o]})
          class-pattern (first {:class [:s :p :o]})
          id-pattern    (first {:id "ex:alice"})
          filter-pattern (first {:filter "fn"})
          bind-pattern   (first {:bind "var"})]

      (is (optimize/optimizable-pattern? tuple-pattern)
          "tuple patterns are optimizable")
      (is (optimize/optimizable-pattern? class-pattern)
          "class patterns are optimizable")
      (is (optimize/optimizable-pattern? id-pattern)
          "id patterns are optimizable")
      (is (not (optimize/optimizable-pattern? filter-pattern))
          "filter patterns are not optimizable")
      (is (not (optimize/optimizable-pattern? bind-pattern))
          "bind patterns are not optimizable"))))

(deftest split-boundaries-test
  (testing "Split by optimization boundaries"
    ;; Test the split-by-optimization-boundaries function directly
    ;; Patterns are map-entries - create them from maps
    (let [tuple-pattern (first {:tuple [:s1 :p1 :o1]})
          class-pattern (first {:class [:s2 :p2 :o2]})
          filter-pattern (first {:filter "fn"})
          tuple-pattern2 (first {:tuple [:s3 :p3 :o3]})

          where-clause [tuple-pattern class-pattern filter-pattern tuple-pattern2]
          result (optimize/split-by-optimization-boundaries where-clause)]

      (is (= 3 (count result))
          "Should split into 3 segments: [tuple class] [filter] [tuple]")

      (is (= :optimizable (:type (first result)))
          "First segment should be optimizable")
      (is (= 2 (count (:data (first result))))
          "First segment should contain 2 patterns")

      (is (= :boundary (:type (second result)))
          "Second segment should be boundary")

      (is (= :optimizable (:type (nth result 2)))
          "Third segment should be optimizable")
      (is (= 1 (count (:data (nth result 2))))
          "Third segment should contain 1 pattern"))))
