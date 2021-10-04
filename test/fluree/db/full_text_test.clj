(ns fluree.db.full-text-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :refer [IFlureeDb]]
            [fluree.db.flake :as flake]
            [fluree.db.full-text :as full-text])
  (:import (java.io Closeable)
           (org.apache.lucene.index IndexWriter)))

(def user-coll-id (inc const/$numSystemCollections))
(def user-coll-name "user")

(def handle-pred-id 200)
(def handle-pred-name "user/handle")

(def bio-pred-id 201)
(def bio-pred-name "user/bio")

(defn pid->keyword
  [pid]
  (-> pid str keyword))

(deftest full-text-index-test
  (testing "full-text index"
    (let [lang :en]
      (with-open [idx (full-text/memory-index lang)]
        (testing "after initialization"
          (doto (full-text/writer idx)
            .commit
            .close)
          (testing "populated with predicates for a subject"
            (let [subj-num  10
                  subj-id   (flake/->sid-checked user-coll-id subj-num)
                  handle    "mfillmore13"
                  bio-1     "I actually was the president"
                  pred-vals {handle-pred-id handle, bio-pred-id bio-1}]
              (with-open [wrtr (full-text/writer idx)]
                (full-text/put-subject idx wrtr subj-id pred-vals)
                (.commit wrtr)
                (let [subject-under-test (full-text/get-subject idx subj-id)]
                  (is (and (= handle (get subject-under-test
                                          (pid->keyword handle-pred-id)))
                           (= bio-1 (get subject-under-test
                                         (pid->keyword bio-pred-id))))
                      "populated subject can be retrieved")))
              (testing "when updating a single predicate"
                (let [bio-update  "No really, I was POTUS"
                      pred-update {bio-pred-id bio-update}]
                  (with-open [wrtr (full-text/writer idx)]
                    (full-text/put-subject idx wrtr subj-id pred-update)
                    (.commit wrtr)
                    (let [subject-under-test (full-text/get-subject idx subj-id)]
                      (is (= bio-update (get subject-under-test
                                              (pid->keyword bio-pred-id)))
                          "the updated predicate can be retrieved")
                      (is (= handle (get subject-under-test
                                         (pid->keyword handle-pred-id)))
                          "unchanged predicates are retained")
                      (is (not (-> subject-under-test vals set (contains? bio-1)))
                          "previous predicate values are not retained"))))))))))))
