(ns fluree.db.util.bytes-test
  (:require [clojure.test :as t :refer [deftest testing is]]
            [clojure.test.check :as check]
            [clojure.test.check.properties :as prop]
            [fluree.db.util.bytes :as bytes]
            [malli.generator :as mgen]))

(defn string->longs
  [^String s]
  (->> (.getBytes s "UTF-8")
       (partition-all 8)
       (mapv bytes/UTF8->long)))

(defn longs->string
  [ls]
  (bytes/UTF8->string (mapcat bytes/long->UTF8 ls)))

(def IRI
  "Very basic IRI regex, broader than RFC 3987."
  [:re #"^[a-zA-Z][a-zA-Z0-9+.-]*:[^\s<>\x00]+$"])

(def string->longs->string
  (prop/for-all [s (mgen/generator IRI)] (= s (longs->string (string->longs s)))))

(deftest iri->longs-roundtrip
  (testing "can handle negative longs"
    (let [s "Permian–Triassic_extinction_event"]
      (is (= s (longs->string (string->longs s))))))
  (testing "roundtrip property"
    (let [result (check/quick-check 100 string->longs->string)]
      (is (true? (:pass? result))
          (str result)))))
