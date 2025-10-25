(ns fluree.db.json-ld.iri-test
  (:require [clojure.test :refer [deftest testing is]]
            [fluree.db.json-ld.iri :as iri]))

(deftest sid-interning-test
  (testing "SID interning when enabled"
    (when (iri/interning-enabled?)
      (testing "returns identical instances for same ns-code and name"
        (let [sid1 (iri/deserialize-sid [8 "test-subject"])
              sid2 (iri/deserialize-sid [8 "test-subject"])]
          (is (identical? sid1 sid2)
              "Same SID should return identical instance")))

      (testing "returns different instances for different SIDs"
        (let [sid1 (iri/deserialize-sid [8 "subject-1"])
              sid2 (iri/deserialize-sid [8 "subject-2"])]
          (is (not (identical? sid1 sid2))
              "Different SIDs should not be identical")))

      (testing "interner cache tracks entries"
        (let [initial-size (iri/interner-size)]
          ;; Create a unique SID
          (iri/deserialize-sid [8 (str "unique-" (System/currentTimeMillis))])
          (let [new-size (iri/interner-size)]
            (is (>= new-size initial-size)
                "Cache size should increase or stay same after creating SID"))))))

  (testing "SID creation when interning disabled"
    (when-not (iri/interning-enabled?)
      (testing "deserialize-sid still works without interning"
        (let [sid (iri/deserialize-sid [8 "test-subject"])]
          (is (some? sid))
          (is (= 8 (iri/get-ns-code sid)))
          (is (= "test-subject" (iri/get-name sid))))))))

(deftest sid-equality-test
  (testing "SID equality is value-based regardless of interning"
    (let [sid1 (iri/deserialize-sid [8 "subject"])
          sid2 (iri/deserialize-sid [8 "subject"])]
      (is (= sid1 sid2) "Equal SIDs should be equal")
      (is (= (hash sid1) (hash sid2)) "Equal SIDs should have same hash"))))
