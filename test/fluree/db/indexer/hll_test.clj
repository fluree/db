(ns fluree.db.indexer.hll-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.indexer.hll :as hll]))

(deftest create-sketch-test
  (testing "Create empty sketch"
    (let [sketch (hll/create-sketch)]
      (is (= 256 (alength ^bytes sketch))
          "Sketch should have 256 registers")
      (is (every? zero? (seq sketch))
          "All registers should be initialized to 0"))))

(deftest add-value-test
  (testing "Adding values updates sketch"
    (let [sketch (hll/create-sketch)
          updated (hll/add-value sketch "test-value")]
      (is (not (every? zero? (seq updated)))
          "Sketch should have non-zero registers after adding value")
      (is (= sketch updated)
          "add-value should mutate sketch in place"))))

(deftest cardinality-accuracy-test
  (testing "Cardinality estimation accuracy"
    ;; Test with known cardinalities
    (let [test-cases [{:n 10 :tolerance 0.3}      ; 30% tolerance for small n
                      {:n 100 :tolerance 0.2}     ; 20% tolerance
                      {:n 1000 :tolerance 0.15}   ; 15% tolerance (HLL variance)
                      {:n 10000 :tolerance 0.1}]] ; 10% tolerance

      (doseq [{:keys [n tolerance]} test-cases]
        (testing (str "Estimating " n " distinct values")
          (let [sketch (reduce (fn [s i]
                                 (hll/add-value s (str "value-" i)))
                               (hll/create-sketch)
                               (range n))
                estimate (hll/cardinality sketch)
                error (Math/abs (- 1.0 (/ estimate (double n))))
                within-tolerance? (<= error tolerance)]

            (is within-tolerance?
                (format "Estimate %d should be within %.0f%% of actual %d (error: %.1f%%)"
                        estimate (* tolerance 100) n (* error 100)))))))))

(deftest cardinality-duplicates-test
  (testing "Cardinality with duplicates"
    (let [sketch (reduce (fn [s v]
                           (hll/add-value s v))
                         (hll/create-sketch)
                         ;; Add 1000 values, each repeated 5 times
                         (mapcat #(repeat 5 (str "value-" %)) (range 1000)))
          estimate (hll/cardinality sketch)]

      (is (< 800 estimate 1200)
          "Should estimate ~1000 distinct values despite 5000 total adds"))))

(deftest merge-sketches-test
  (testing "Merging sketches produces correct cardinality"
    (let [;; Create two sketches with distinct value ranges
          sketch-a (reduce (fn [s i]
                             (hll/add-value s (str "a-" i)))
                           (hll/create-sketch)
                           (range 500))
          sketch-b (reduce (fn [s i]
                             (hll/add-value s (str "b-" i)))
                           (hll/create-sketch)
                           (range 500))
          merged (hll/merge-sketches sketch-a sketch-b)
          estimate (hll/cardinality merged)]

      (is (< 800 estimate 1250)
          "Merged sketch should estimate ~1000 distinct values (with HLL variance)")

      ;; Verify merge is register-wise max
      (dotimes [i 256]
        (is (= (aget ^bytes merged i)
               (max (aget ^bytes sketch-a i) (aget ^bytes sketch-b i)))
            "Each register should be max of both sketches")))))

(deftest merge-idempotence-test
  (testing "Merging a sketch with itself gives same cardinality"
    (let [sketch (reduce (fn [s i]
                           (hll/add-value s (str "value-" i)))
                         (hll/create-sketch)
                         (range 1000))
          card-before (hll/cardinality sketch)
          merged (hll/merge-sketches sketch sketch)
          card-after (hll/cardinality merged)]

      (is (= card-before card-after)
          "Cardinality should not change when merging sketch with itself"))))

(deftest serialization-test
  (testing "Serialize and deserialize sketch"
    (let [original (reduce (fn [s i]
                             (hll/add-value s (str "value-" i)))
                           (hll/create-sketch)
                           (range 1000))
          serialized (hll/serialize original)
          deserialized (hll/deserialize serialized)]

      (is (string? serialized)
          "Serialized form should be a string")

      (is (= (seq original) (seq deserialized))
          "Deserialized sketch should match original registers")

      (is (= (hll/cardinality original)
             (hll/cardinality deserialized))
          "Cardinality should be preserved across serialization"))))

(deftest empty-sketch-test
  (testing "Empty sketch has cardinality 0"
    (let [sketch (hll/create-sketch)
          estimate (hll/cardinality sketch)]
      (is (= 0 estimate)
          "Empty sketch should estimate 0 distinct values"))))

(deftest single-value-test
  (testing "Single value gives cardinality ~1"
    (let [sketch (hll/add-value (hll/create-sketch) "single-value")
          estimate (hll/cardinality sketch)]
      (is (<= 1 estimate 3)
          "Single value should estimate close to 1"))))

(deftest sketch-info-test
  (testing "Sketch info returns debug information"
    (let [sketch (reduce (fn [s i]
                           (hll/add-value s (str "value-" i)))
                         (hll/create-sketch)
                         (range 100))
          info (hll/sketch-info sketch)]

      (is (= 256 (:num-registers info)))
      (is (= 8 (:precision info)))
      (is (pos? (:cardinality info)))
      (is (< (:empty-registers info) 256)
          "Should have some non-empty registers")
      (is (pos? (:max-register info)))
      (is (pos? (:avg-register info))))))

(deftest hash-value-determinism-test
  (testing "Hash values are deterministic"
    (let [value "test-value"
          hash1 (hll/hash-value value)
          hash2 (hll/hash-value value)]
      (is (= hash1 hash2)
          "Same value should always produce same hash"))))

(deftest hash-value-distribution-test
  (testing "Hash values are well-distributed"
    (let [hashes (map hll/hash-value (map str (range 1000)))
          unique-hashes (count (set hashes))]
      (is (= 1000 unique-hashes)
          "1000 different values should produce 1000 different hashes"))))

(deftest serialization-round-trip-preserves-registers-test
  (testing "Serialize/deserialize round-trip preserves every register exactly"
    (let [original (reduce (fn [s i]
                             (hll/add-value s (str "value-" i)))
                           (hll/create-sketch)
                           (range 500))
          serialized (hll/serialize original)
          deserialized (hll/deserialize serialized)]

      ;; Verify every register is preserved
      (dotimes [i 256]
        (is (= (aget ^bytes original i) (aget ^bytes deserialized i))
            (format "Register %d should be preserved" i)))

      ;; Verify cardinality is identical
      (is (= (hll/cardinality original)
             (hll/cardinality deserialized))
          "Cardinality must be identical after round-trip"))))

(deftest merge-is-register-wise-max-test
  (testing "Merge computes register-wise maximum correctly"
    (let [sketch-a (hll/create-sketch)
          sketch-b (hll/create-sketch)

          ;; Add different values to each sketch
          _ (dotimes [i 100]
              (hll/add-value sketch-a (str "a-" i)))
          _ (dotimes [i 100]
              (hll/add-value sketch-b (str "b-" i)))

          merged (hll/merge-sketches sketch-a sketch-b)]

      ;; Verify every register is max of both
      (dotimes [i 256]
        (is (= (aget ^bytes merged i)
               (max (aget ^bytes sketch-a i) (aget ^bytes sketch-b i)))
            (format "Register %d should be max(%d, %d)"
                    i (aget ^bytes sketch-a i) (aget ^bytes sketch-b i)))))))
