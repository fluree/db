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

(def test-db-schema {:coll {user-coll-id   {:id user-coll-id, :name user-coll-name}
                            user-coll-name {:id user-coll-id, :name user-coll-name}}
                     :pred {handle-pred-id   {:id handle-pred-id, :name handle-pred-name}
                            handle-pred-name {:id handle-pred-id, :name handle-pred-name}
                            bio-pred-id      {:id bio-pred-id, :name bio-pred-name}
                            bio-pred-name    {:id bio-pred-id, :name bio-pred-name}}})

(defrecord TestDB [schema]
  IFlureeDb
  (-c-prop [_ prop coll]
    (get-in schema [:coll coll prop]))
  (-p-prop [_ prop pred]
    (get-in schema [:pred pred prop])))

(defn test-db
  []
  (->TestDB test-db-schema))

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
              (testing "search"
                (let [db (test-db)]
                  (let [var                "?msg"
                        search             (str "fullText:" bio-pred-name)
                        param              "president"
                        subject-under-test (full-text/search idx db [var search param])]
                    (is (= [var]
                           (:headers subject-under-test))
                        "returns the correct headers")
                    (is (some (fn [t]
                                (= t [subj-id]))
                              (:tuples subject-under-test))
                        "includes the subject id in the returned tuples list")
                    (testing "with wildcard"
                      (let [param              "pres*"
                            subject-under-test (full-text/search idx db [var search param])]
                        (is (some (fn [t]
                                    (= t [subj-id]))
                                  (:tuples subject-under-test))
                            "includes the subject id in the returned tuples list"))))))
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
