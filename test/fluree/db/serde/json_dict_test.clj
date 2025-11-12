(ns fluree.db.serde.json-dict-test
  "Tests for dictionary-based JSON serialization."
  (:require [clojure.test :refer [deftest testing is]]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.serde :as serde]
            [fluree.db.serde.json :as json-serde]
            [fluree.db.serde.json-dict :as json-dict]
            [fluree.db.util.json :as json-util]))

(defn generate-test-flakes
  "Generate test flakes with variety of subjects, predicates, and datatypes."
  [num-flakes]
  (let [num-subjects (max 1 (/ num-flakes 20))
        predicates ["name" "email" "age" "address" "phone"]
        datatypes ["string" "integer"]]
    (vec
     (for [i (range num-flakes)]
       (let [subj-id (mod i num-subjects)
             pred-id (mod i (count predicates))
             dt-id (mod i (count datatypes))
             dt-name (nth datatypes dt-id)
             value (if (= dt-name "integer") (long i) (str "value-" i))]
         (flake/create
          (iri/deserialize-sid [8 (str "subject-" subj-id)])
          (iri/deserialize-sid [8 (nth predicates pred-id)])
          value
          (iri/deserialize-sid [2 dt-name])
          (inc i)
          true
          nil))))))

(defn generate-reference-flakes
  "Generate test flakes with reference (IRI) objects."
  [num-flakes]
  (vec
   (for [i (range num-flakes)]
     (flake/create
      (iri/deserialize-sid [8 (str "subject-" i)])
      (iri/deserialize-sid [8 "ref"])
      (iri/deserialize-sid [8 (str "target-" (mod i 10))])
      const/$id
      (inc i)
      true
      nil))))

(defn flakes-equal?
  "Compare two flakes for equality."
  [f1 f2]
  (and (= (flake/s f1) (flake/s f2))
       (= (flake/p f1) (flake/p f2))
       (= (flake/o f1) (flake/o f2))
       (= (flake/dt f1) (flake/dt f2))
       (= (flake/t f1) (flake/t f2))
       (= (flake/op f1) (flake/op f2))
       (= (flake/m f1) (flake/m f2))))

(deftest dict-round-trip-test
  (testing "Dictionary format preserves flakes through round-trip serialization"
    (let [test-flakes (generate-test-flakes 100)
          dict-ser (json-dict/json-dict-serde)
          leaf {:flakes test-flakes}

          ;; Serialize, convert to JSON, parse, deserialize
          serialized (serde/-serialize-leaf dict-ser leaf)
          json-string (json-util/stringify serialized)
          parsed (json-util/parse json-string true)
          deserialized (serde/-deserialize-leaf dict-ser parsed)
          result-flakes (:flakes deserialized)]

      (is (= 2 (get serialized "version"))
          "Should use version 2 format")
      (is (contains? serialized "dict")
          "Should have dictionary key")
      (is (pos? (count (get serialized "dict")))
          "Dictionary should not be empty")
      (is (= (count test-flakes) (count result-flakes))
          "Should preserve flake count")
      (is (every? identity (map flakes-equal? test-flakes result-flakes))
          "All flakes should be identical after round-trip"))))

(deftest dict-with-references-test
  (testing "Dictionary format handles IRI references correctly"
    (let [test-flakes (generate-reference-flakes 50)
          dict-ser (json-dict/json-dict-serde)
          leaf {:flakes test-flakes}

          serialized (serde/-serialize-leaf dict-ser leaf)
          json-string (json-util/stringify serialized)
          parsed (json-util/parse json-string true)
          deserialized (serde/-deserialize-leaf dict-ser parsed)
          result-flakes (:flakes deserialized)]

      (is (= (count test-flakes) (count result-flakes))
          "Should preserve flake count")
      (is (every? identity (map flakes-equal? test-flakes result-flakes))
          "Reference flakes should be identical after round-trip"))))

(deftest dict-vs-standard-equivalence-test
  (testing "Dictionary and standard formats produce equivalent results"
    (let [test-flakes (generate-test-flakes 50)
          leaf {:flakes test-flakes}

          ;; Standard format
          std-ser (json-serde/json-serde)
          std-serialized (serde/-serialize-leaf std-ser leaf)
          std-json (json-util/stringify std-serialized)
          std-parsed (json-util/parse std-json true)
          std-deserialized (serde/-deserialize-leaf std-ser std-parsed)
          std-flakes (:flakes std-deserialized)

          ;; Dictionary format
          dict-ser (json-dict/json-dict-serde)
          dict-serialized (serde/-serialize-leaf dict-ser leaf)
          dict-json (json-util/stringify dict-serialized)
          dict-parsed (json-util/parse dict-json true)
          dict-deserialized (serde/-deserialize-leaf dict-ser dict-parsed)
          dict-flakes (:flakes dict-deserialized)]

      (is (= (count std-flakes) (count dict-flakes))
          "Both formats should produce same number of flakes")
      (is (every? identity (map flakes-equal? std-flakes dict-flakes))
          "Both formats should produce identical flakes")
      (is (< (count dict-json) (count std-json))
          "Dictionary format should be more compact"))))

(deftest dict-size-reduction-test
  (testing "Dictionary format provides significant size reduction"
    (let [test-flakes (generate-test-flakes 1000)
          leaf {:flakes test-flakes}

          std-ser (json-serde/json-serde)
          std-json (json-util/stringify (serde/-serialize-leaf std-ser leaf))

          dict-ser (json-dict/json-dict-serde)
          dict-json (json-util/stringify (serde/-serialize-leaf dict-ser leaf))

          reduction (* 100 (- 1 (/ (count dict-json) (double (count std-json)))))]

      (is (> reduction 30)
          (str "Dictionary format should reduce size by at least 30% (actual: "
               (format "%.1f%%" reduction) ")")))))

(deftest legacy-format-support-test
  (testing "DictSerializer can read legacy (non-dict) format"
    (let [test-flakes (generate-test-flakes 50)
          leaf {:flakes test-flakes}

          ;; Serialize with standard format (no version, no dict)
          std-ser (json-serde/json-serde)
          std-serialized (serde/-serialize-leaf std-ser leaf)
          std-json (json-util/stringify std-serialized)
          std-parsed (json-util/parse std-json true)

          ;; Deserialize with dict serializer (should auto-detect)
          dict-ser (json-dict/json-dict-serde)
          deserialized (serde/-deserialize-leaf dict-ser std-parsed)
          result-flakes (:flakes deserialized)]

      (is (= (count test-flakes) (count result-flakes))
          "Should read legacy format")
      (is (every? identity (map flakes-equal? test-flakes result-flakes))
          "Legacy format flakes should be identical"))))

(deftest version-detection-test
  (testing "Serializer detects format version correctly"
    (let [test-flakes (generate-test-flakes 10)
          leaf {:flakes test-flakes}
          dict-ser (json-dict/json-dict-serde)

          ;; Version 2 format
          v2-serialized (serde/-serialize-leaf dict-ser leaf)

          ;; Version 1 format (standard)
          std-ser (json-serde/json-serde)
          v1-serialized (serde/-serialize-leaf std-ser leaf)]

      (is (= 2 (get v2-serialized "version"))
          "New format should be version 2")
      (is (nil? (get v1-serialized "version"))
          "Standard format should have no version")
      (is (contains? v2-serialized "dict")
          "Version 2 should have dict")
      (is (not (contains? v1-serialized "dict"))
          "Version 1 should not have dict"))))
