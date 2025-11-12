(ns fluree.db.indexer.stats-serializer-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.indexer.hll :as hll]
            [fluree.db.indexer.stats-serializer :as serializer]
            [jsonista.core :as json]))

(deftest serialize-deserialize-test
  (testing "Round-trip serialization"
    (let [;; Create some test sketches
          sketch-1 (reduce (fn [s i] (hll/add-value s (str "val-" i)))
                           (hll/create-sketch)
                           (range 100))
          sketch-2 (reduce (fn [s i] (hll/add-value s (str "subj-" i)))
                           (hll/create-sketch)
                           (range 50))

          ;; SIDs are two-tuples [ns-code local-name]
          property-sketches {[100 "name"] {:values sketch-1
                                           :subjects sketch-2}
                             [100 "age"] {:values sketch-1}}

          ;; Serialize
          json-str (serializer/serialize-stats-sketches
                    "test/ledger"
                    42
                    property-sketches)

          ;; Deserialize
          result (serializer/deserialize-stats-sketches json-str)]

      (is (= "test/ledger" (:ledger-alias result)))
      (is (= 42 (:indexed-t result)))

      ;; Verify property [100 "name"] has both sketches
      (is (contains? (:property-sketches result) [100 "name"]))
      (is (:values (get (:property-sketches result) [100 "name"])))
      (is (:subjects (get (:property-sketches result) [100 "name"])))

      ;; Verify cardinality is preserved
      (let [values-card (hll/cardinality sketch-1)
            deserialized-values-card (hll/cardinality
                                      (:values (get (:property-sketches result) [100 "name"])))]
        (is (= values-card deserialized-values-card)
            "Cardinality should be preserved across serialization")))))

(deftest json-format-test
  (testing "JSON v1 format structure"
    (let [sketch (reduce (fn [s i] (hll/add-value s (str "val-" i)))
                         (hll/create-sketch)
                         (range 10))

          property-sketches {[100 "name"] {:values sketch}}

          json-str (serializer/serialize-stats-sketches
                    "test/ledger"
                    10
                    property-sketches)

          parsed (json/read-value json-str)]

      (is (= 1 (get parsed "v"))
          "Version should be 1")

      (is (= "test/ledger" (get parsed "ledgerAlias")))
      (is (= 10 (get parsed "indexedT")))

      (is (= {"algo" "hll++"
              "p" 8
              "m" 256
              "registerBits" 6}
             (get parsed "hll"))
          "HLL metadata should be correct")

      (is (= {"format" "base64"
              "compression" "none"}
             (get parsed "registerEncoding"))
          "Register encoding should be specified")

      ;; Verify property structure
      (is (contains? (get parsed "properties") "[100 \"name\"]")
          "Property should be keyed by pr-str of SID two-tuple")

      (let [prop-data (get-in parsed ["properties" "[100 \"name\"]" "values"])]
        (is (string? (get prop-data "registersB64"))
            "Registers should be base64 encoded")
        (is (number? (get prop-data "approxNDV"))
            "Approximate NDV should be included")
        (is (= 1 (get prop-data "epoch"))
            "Epoch should default to 1")))))

(deftest merge-sketches-test
  (testing "Merging property sketches"
    (let [;; Create sketches for property 100: first 50 values
          sketch-a-vals (reduce (fn [s i] (hll/add-value s (str "val-" i)))
                                (hll/create-sketch)
                                (range 50))

          ;; Create sketches for property 100: next 50 values (50-99)
          sketch-b-vals (reduce (fn [s i] (hll/add-value s (str "val-" i)))
                                (hll/create-sketch)
                                (range 50 100))

          ;; Create sketches for property 200: different values
          sketch-c-vals (reduce (fn [s i] (hll/add-value s (str "other-" i)))
                                (hll/create-sketch)
                                (range 30))

          old-sketches {[100 "name"] {:values sketch-a-vals}}
          new-sketches {[100 "name"] {:values sketch-b-vals}
                        [100 "age"] {:values sketch-c-vals}}

          merged (serializer/merge-property-sketches old-sketches new-sketches)]

      ;; Property [100 "name"] should have merged sketch estimating ~100
      (is (contains? merged [100 "name"]))
      (let [merged-card (hll/cardinality (:values (get merged [100 "name"])))]
        (is (< 80 merged-card 120)
            "Merged sketch should estimate ~100 distinct values"))

      ;; Property [100 "age"] should exist with original sketch
      (is (contains? merged [100 "age"]))
      (let [prop-age-card (hll/cardinality (:values (get merged [100 "age"])))]
        (is (< 25 prop-age-card 35)
            "Property [100 \"age\"] should have ~30 distinct values")))))

(deftest merge-both-values-and-subjects-test
  (testing "Merging sketches with both values and subjects"
    (let [;; Property has both values and subjects sketches
          old-vals (reduce #(hll/add-value %1 (str "val-" %2))
                           (hll/create-sketch)
                           (range 10))
          old-subjs (reduce #(hll/add-value %1 (str "subj-" %2))
                            (hll/create-sketch)
                            (range 5))

          new-vals (reduce #(hll/add-value %1 (str "val-" %2))
                           (hll/create-sketch)
                           (range 5 15))
          new-subjs (reduce #(hll/add-value %1 (str "subj-" %2))
                            (hll/create-sketch)
                            (range 3 8))

          old-sketches {[100 "friend"] {:values old-vals :subjects old-subjs}}
          new-sketches {[100 "friend"] {:values new-vals :subjects new-subjs}}

          merged (serializer/merge-property-sketches old-sketches new-sketches)]

      (is (contains? merged [100 "friend"]))

      ;; Values should merge to ~15 distinct
      (let [vals-card (hll/cardinality (:values (get merged [100 "friend"])))]
        (is (< 12 vals-card 18)
            "Merged values should estimate ~15 distinct"))

      ;; Subjects should merge to ~8 distinct
      (let [subjs-card (hll/cardinality (:subjects (get merged [100 "friend"])))]
        (is (< 6 subjs-card 10)
            "Merged subjects should estimate ~8 distinct")))))

(deftest unsupported-version-test
  (testing "Error on unsupported version"
    (let [bad-json "{\"v\": 999, \"ledgerAlias\": \"test\", \"properties\": {}}"]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Unsupported stats sketches version"
           (serializer/deserialize-stats-sketches bad-json))))))
