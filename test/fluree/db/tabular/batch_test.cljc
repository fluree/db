(ns fluree.db.tabular.batch-test
  "Tests for IBatch protocol implementations."
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.tabular.batch :as batch]))

;;; ---------------------------------------------------------------------------
;;; RowBatch Tests
;;; ---------------------------------------------------------------------------

(def sample-rows
  [{"id" 1 "name" "Alice" "region" "US" "amount" 1000.0}
   {"id" 2 "name" "Bob" "region" "EU" "amount" 2500.5}
   {"id" 3 "name" "Charlie" "region" "US" "amount" 750.25}
   {"id" 4 "name" "Diana" "region" "APAC" "amount" 3200.0}])

(deftest row-batch-row-count-test
  (testing "row-count returns correct count"
    (let [b (batch/wrap-rows sample-rows)]
      (is (= 4 (batch/row-count b))))))

(deftest row-batch-column-names-test
  (testing "column-names returns all column names"
    (let [b (batch/wrap-rows sample-rows)]
      (is (= #{"id" "name" "region" "amount"}
             (set (batch/column-names b)))))))

(deftest row-batch-column-test
  (testing "column returns values for specified column"
    (let [b (batch/wrap-rows sample-rows)]
      (is (= [1 2 3 4] (vec (batch/column b "id"))))
      (is (= ["Alice" "Bob" "Charlie" "Diana"] (vec (batch/column b "name"))))
      (is (= ["US" "EU" "US" "APAC"] (vec (batch/column b "region")))))))

(deftest row-batch-select-columns-test
  (testing "select-columns returns batch with only specified columns"
    (let [b (batch/wrap-rows sample-rows)
          b2 (batch/select-columns b ["name" "region"])]
      (is (= 4 (batch/row-count b2)))
      (is (= #{"name" "region"} (set (batch/column-names b2))))
      (is (= [{"name" "Alice" "region" "US"}
              {"name" "Bob" "region" "EU"}
              {"name" "Charlie" "region" "US"}
              {"name" "Diana" "region" "APAC"}]
             (vec (batch/to-row-seq b2)))))))

(deftest row-batch-slice-test
  (testing "slice returns batch with subset of rows"
    (let [b (batch/wrap-rows sample-rows)
          b2 (batch/slice b 1 3)]
      (is (= 2 (batch/row-count b2)))
      (is (= [2 3] (vec (batch/column b2 "id"))))
      (is (= ["Bob" "Charlie"] (vec (batch/column b2 "name")))))))

(deftest row-batch-to-row-seq-test
  (testing "to-row-seq returns original rows"
    (let [b (batch/wrap-rows sample-rows)]
      (is (= sample-rows (vec (batch/to-row-seq b)))))))

(deftest batch-seq->rows-test
  (testing "batch-seq->rows flattens multiple batches"
    (let [b1 (batch/wrap-rows (take 2 sample-rows))
          b2 (batch/wrap-rows (drop 2 sample-rows))
          all-rows (vec (batch/batch-seq->rows [b1 b2]))]
      (is (= 4 (count all-rows)))
      (is (= sample-rows all-rows)))))

(deftest empty-batch-test
  (testing "empty batch handles edge cases"
    (let [b (batch/wrap-rows [])]
      (is (= 0 (batch/row-count b)))
      (is (nil? (batch/column-names b)))
      (is (= [] (vec (batch/to-row-seq b)))))))
