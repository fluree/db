(ns fluree.db.full-text-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.flake :as flake]
            [fluree.db.full-text :as full-text]))

(deftest full-text-index-test
  (testing "full-text index"
    (let [lang :en]
      (with-open [idx (full-text/memory-index lang)]
        (testing "after initialization"
          (-> idx full-text/writer .close)
          (testing "populated with predicates for a subject"
            (let [cid       12345
                  subj-num  10
                  subj-id   (flake/->sid cid subj-num)
                  pred-vals {1001 "foo", 1002 "bar"}]
              (with-open [wrtr (full-text/writer idx)]
                (full-text/put-subject idx wrtr subj-id pred-vals)
                (.commit wrtr)
                (let [subject-under-test (full-text/get-subject idx subj-id)]
                  (is (and (= "foo" (:1001 subject-under-test))
                           (= "bar" (:1002 subject-under-test)))
                      "populated subject can be retrieved")))
              (testing "when updating a single predicate"
                (let [pred-update {1001 "baz"}]
                  (with-open [wrtr (full-text/writer idx)]
                    (full-text/put-subject idx wrtr subj-id pred-update)
                    (.commit wrtr)
                    (let [subject-under-test (full-text/get-subject idx subj-id)]
                      (is (= "baz" (:1001 subject-under-test))
                          "the updated predicate can be retrieved")
                      (is (= "bar" (:1002 subject-under-test))
                          "unchanged predicates are retained"))))))))))))
