(ns fluree.db.json-ld.iri-test
  (:require [clojure.test :refer [deftest testing is]]
            [fluree.db.json-ld.iri :as iri]))

(deftest sid-interning-test
  (testing "deserialize-sid returns identical instances for same ns-code and name"
    (let [sid1 (iri/deserialize-sid [8 "test-subject"])
          sid2 (iri/deserialize-sid [8 "test-subject"])]
      (is (identical? sid1 sid2)
          "Interning ensures identical instances for same SID values")))

  (testing "deserialize-sid returns different instances for different SIDs"
    (let [sid1 (iri/deserialize-sid [8 "subject-1"])
          sid2 (iri/deserialize-sid [8 "subject-2"])]
      (is (not (identical? sid1 sid2))
          "Different SID values should not be identical")))

  (testing "iri->sid returns identical instances for same IRI"
    (let [sid1 (iri/iri->sid "https://ns.flur.ee/ledger#test")
          sid2 (iri/iri->sid "https://ns.flur.ee/ledger#test")]
      (is (identical? sid1 sid2)
          "Interning ensures identical instances for same IRI")))

  (testing "interning works across different creation methods"
    (let [sid-from-deserialize (iri/deserialize-sid [8 "example"])
          sid-from-iri (iri/iri->sid "https://ns.flur.ee/ledger#example")]
      (is (identical? sid-from-deserialize sid-from-iri)
          "Same SID created via different methods should be identical"))))

(deftest sid-equality-test
  (testing "SID equality is value-based"
    (let [sid1 (iri/deserialize-sid [8 "subject"])
          sid2 (iri/deserialize-sid [8 "subject"])]
      (is (= sid1 sid2) "Equal SIDs should be equal")
      (is (= (hash sid1) (hash sid2)) "Equal SIDs should have same hash"))))
