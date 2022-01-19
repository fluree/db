(ns fluree.db.serde.avro-test
  (:require [clojure.test :refer :all]
            [fluree.db.serde.avro :refer :all]
            [fluree.db.flake :refer [->Flake]])
  (:import (java.math BigInteger BigDecimal)))

(defn- rand-long [] (long (rand Long/MAX_VALUE)))

(defn- rand-flake [obj]
  (->Flake (rand-long) (rand-long) obj (rand-long) true "meta"))

(deftest serialize-deserialize-test
  (testing "block with 3 flakes: string, bigdec, & bigint objects"
    (let [test-flakes [(rand-flake "object")
                       (rand-flake (BigDecimal. 42.7))
                       (rand-flake (BigInteger. "3333333333333777777777777"))]
          test-block  {:block  (rand-long)
                       :t      (rand-long)
                       :flakes test-flakes}
          serializer (avro-serde)]
      (is (= test-block
             (->> test-block
                  (.-serialize-block serializer)
                  (.-deserialize-block serializer)))))))
