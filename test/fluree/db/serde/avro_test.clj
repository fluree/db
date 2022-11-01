(ns fluree.db.serde.avro-test
  (:require [clojure.test :refer :all]
            [fluree.db.serde.avro :refer :all]
            [fluree.db.flake :as flake]
            [fluree.db.constants :as const])
  (:import (java.math BigInteger BigDecimal)))

(defn- rand-long [] (long (rand Long/MAX_VALUE)))

(defn- rand-flake [obj datatype]
  (flake/create (rand-long) (rand-long) obj datatype (rand-long) true "meta"))

(deftest serialize-deserialize-test
  (testing "block with 3 flakes: string, bigdec, & bigint objects"
    (let [test-flakes [(rand-flake "object" const/$xsd:string)
                       (rand-flake (BigDecimal. 42.7) const/$xsd:decimal)
                       (rand-flake (BigInteger. "3333333333333777777777777") const/$xsd:integer)]
          test-block  {:block  (rand-long)
                       :t      (rand-long)
                       :flakes test-flakes}
          serializer (avro-serde)]
      (is (= test-block
             (->> test-block
                  (.-serialize-block serializer)
                  (.-deserialize-block serializer)))))))
