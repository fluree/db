(ns fluree.db.full-text-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.flake :as flake]
            [fluree.db.full-text :as full-text])
  (:import (java.io Closeable)
           (org.apache.lucene.index IndexWriter)))

(deftest full-text-index-test
  (testing "full-text index"
    (let [lang :en]
      (with-open [idx (full-text/memory-index lang)]
        (testing "after initialization"
          (doto (full-text/writer idx)
            .commit
            .close)
          (testing "populated with predicates for a subject"
            (let [cid       12345
                  subj-num  10
                  subj-id   (flake/->sid cid subj-num)
                  text-1    "it is raining right now"
                  text-2    "the sun is shining"
                  pred-vals {1001 text-1, 1002 text-2}]
              (with-open [wrtr (full-text/writer idx)]
                (full-text/put-subject idx wrtr subj-id pred-vals)
                (.commit wrtr)
                (let [subject-under-test (full-text/get-subject idx subj-id)]
                  (is (and (= text-1 (:1001 subject-under-test))
                           (= text-2 (:1002 subject-under-test)))
                      "populated subject can be retrieved")))
              (testing "search"
                )
              (testing "when updating a single predicate"
                (let [text-update "the rain has stopped"
                      pred-update {1001 text-update}]
                  (with-open [wrtr (full-text/writer idx)]
                    (full-text/put-subject idx wrtr subj-id pred-update)
                    (.commit wrtr)
                    (let [subject-under-test (full-text/get-subject idx subj-id)]
                      (is (= text-update (:1001 subject-under-test))
                          "the updated predicate can be retrieved")
                      (is (= text-2 (:1002 subject-under-test))
                          "unchanged predicates are retained")
                      (is (not (-> subject-under-test vals set (contains? text-1)))
                          "previous predicate values are not retained"))))))))))))
