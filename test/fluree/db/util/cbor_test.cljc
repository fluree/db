(ns fluree.db.util.cbor-test
  (:require [clojure.test :refer [deftest testing is]]
            [fluree.db.util.cbor :as cbor]))

(deftest cbor-availability-test
  (testing "CBOR is available on this platform"
    (is (true? cbor/cbor-available?)
        "CBOR should be available on JVM and Node.js")))

(deftest cbor-encode-decode-test
  (testing "Basic CBOR encoding and decoding"
    (let [data {:name "Alice"
                :age 30
                :tags ["developer" "clojure"]}
          encoded (cbor/encode data)
          decoded (cbor/decode encoded)]
      (is (some? encoded) "Encoding should produce bytes")
      (is (= data decoded) "Decoded data should match original"))))

(deftest cbor-nested-data-test
  (testing "CBOR handles nested data structures"
    (let [data {:user {:name "Bob"
                       :email "bob@example.com"}
                :prefs {:theme "dark"
                        :notifications true}
                :count 42}
          encoded (cbor/encode data)
          decoded (cbor/decode encoded)]
      (is (= data decoded) "Nested structures should round-trip correctly"))))

(deftest cbor-arrays-test
  (testing "CBOR handles arrays/vectors"
    (let [data {:items [1 2 3 4 5]
                :names ["alice" "bob" "charlie"]}
          encoded (cbor/encode data)
          decoded (cbor/decode encoded)]
      (is (= data decoded) "Arrays should round-trip correctly"))))

(deftest cbor-nil-values-test
  (testing "CBOR handles nil values"
    (let [data {:field1 "value"
                :field2 nil
                :field3 "other"}
          encoded (cbor/encode data)
          decoded (cbor/decode encoded)]
      (is (= data decoded) "Nil values should be preserved"))))

(deftest cbor-empty-collections-test
  (testing "CBOR handles empty collections"
    (let [data {:empty-map {}
                :empty-vec []
                :value 123}
          encoded (cbor/encode data)
          decoded (cbor/decode encoded)]
      (is (= data decoded) "Empty collections should round-trip correctly"))))
