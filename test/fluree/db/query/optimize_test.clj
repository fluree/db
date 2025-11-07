(ns fluree.db.query.optimize-test
  "Unit tests for query optimization functions.
  Integration tests for optimization behavior are in explain_test.clj"
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.optimize :as optimize]))

(deftest optimizable-pattern-test
  (testing "Pattern type recognition using where/pattern-type"
    (let [tuple-pattern  [:s :p :o]
          class-pattern  (where/->pattern :class [:s :p :o])
          id-pattern     (where/->pattern :id "ex:alice")
          filter-pattern (where/->pattern :filter "fn")
          bind-pattern   (where/->pattern :bind ["?var" ["value"]])]

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
    (let [tuple-pattern  [:s1 :p1 :o1]
          class-pattern  (where/->pattern :class [:s2 :p2 :o2])
          filter-pattern (where/->pattern :filter "fn")
          tuple-pattern2 [:s3 :p3 :o3]

          where-clause [tuple-pattern class-pattern filter-pattern tuple-pattern2]
          result       (optimize/segment-clause where-clause)]

      (is (= 3 (count result))
          "Should split into 3 segments: [tuple class] [filter] [tuple]")

      (is (= :optimizable (:type (first result)))
          "First segment should be optimizable")
      (is (= 2 (count (:patterns (first result))))
          "First segment should contain 2 patterns")

      (is (= :boundary (:type (second result)))
          "Second segment should be boundary")

      (is (= :optimizable (:type (nth result 2)))
          "Third segment should be optimizable")
      (is (= 1 (count (:patterns (nth result 2))))
          "Third segment should contain 1 pattern"))))
