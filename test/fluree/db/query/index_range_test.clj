(ns fluree.db.query.index-range-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.flake :as flake]
            [fluree.db.constants :as const]))

(deftest ^:integration index-range-scans
  (testing "Various index range scans using the API."
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "query/index-range")
          context [test-utils/default-context {:ex "http://example.org/ns/"}]
          db      @(fluree/stage
                     @(fluree/db ledger)
                     {"@context" ["https://ns.flur.ee" context]
                      "insert"
                      [{:id           :ex/brian,
                        :type         :ex/User,
                        :schema/name  "Brian"
                        :schema/email "brian@example.org"
                        :schema/age   50
                        :ex/favNums   7}
                       {:id           :ex/alice,
                        :type         :ex/User,
                        :schema/name  "Alice"
                        :schema/email "alice@example.org"
                        :schema/age   50
                        :ex/favNums   [42, 76, 9]}
                       {:id           :ex/cam,
                        :type         :ex/User,
                        :schema/name  "Cam"
                        :schema/email "cam@example.org"
                        :schema/age   34
                        :ex/favNums   [5, 10]
                        :ex/friend    [:ex/brian :ex/alice]}]})
          cam-iri (fluree/expand-iri context :ex/cam)
          cam-sid (fluree/encode-iri db cam-iri)]

      (is (= "http://example.org/ns/cam"
             cam-iri)
          "Expanding compact IRI is broken, likely other tests will fail.")

      (is (iri/sid? cam-sid)
          "The compact IRI did not resolve to an integer subject id.")

      (testing "Slice operations"
        (testing "Slice for subject id only"
          (let [alice-iri (fluree/expand-iri context :ex/alice)
                alice-sid (fluree/encode-iri db alice-iri)]
            (is (= 7
                   (->> @(fluree/slice db :spot [alice-sid])
                        (filterv #(= alice-sid (flake/s %)))
                        (count)))
                "Slice should return a vector of flakes for only Alice")))

        (testing "Slice for subject + predicate"
          (let [alice-iri   (fluree/expand-iri context :ex/alice)
                alice-sid   (fluree/encode-iri db alice-iri)
                favNums-iri (fluree/expand-iri context :ex/favNums)
                favNums-pid (fluree/encode-iri db favNums-iri)]
            (is (= [[alice-sid favNums-pid 9 const/$xsd:long 1 true nil]
                    [alice-sid favNums-pid 42 const/$xsd:long 1 true nil]
                    [alice-sid favNums-pid 76 const/$xsd:long 1 true nil]]
                   (->> @(fluree/slice db :spot [alice-sid favNums-pid])
                        (mapv flake/Flake->parts)))
                "Slice should only return Alice's favNums (multi-cardinality)")))

        (testing "Slice for subject + predicate + value"
          (let [alice-iri   (fluree/expand-iri context :ex/alice)
                alice-sid   (fluree/encode-iri db alice-iri)
                favNums-iri (fluree/expand-iri context :ex/favNums)
                favNums-pid (fluree/encode-iri db favNums-iri)]
            (is (= [[alice-sid favNums-pid 42 const/$xsd:long 1 true nil]]
                   (->> @(fluree/slice db :spot [alice-sid favNums-pid 42])
                        (mapv flake/Flake->parts)))
                "Slice should only return the specified favNum value")))

        (testing "Slice for subject + predicate + value + datatype"
          (let [alice-iri   (fluree/expand-iri context :ex/alice)
                alice-sid   (fluree/encode-iri db alice-iri)
                favNums-iri (fluree/expand-iri context :ex/favNums)
                favNums-pid (fluree/encode-iri db favNums-iri)]
            (is (= [[alice-sid favNums-pid 42 const/$xsd:long 1 true nil]]
                   (->> @(fluree/slice db :spot [alice-sid favNums-pid [42 const/$xsd:long]])
                        (mapv flake/Flake->parts)))
                "Slice should only return the specified favNum value with matching datatype")))

        (testing "Slice for subject + predicate + value + mismatch datatype"
          (let [alice-iri   (fluree/expand-iri context :ex/alice)
                alice-sid   (fluree/encode-iri db alice-iri)
                favNums-iri (fluree/expand-iri context :ex/favNums)
                favNums-pid (fluree/encode-iri db favNums-iri)]
            (is (= []
                   (->> @(fluree/slice db :spot [alice-sid favNums-pid [42 const/$xsd:string]])
                        (mapv flake/Flake->parts)))
                "We specify a different datatype for the value, nothing should be returned")))))))
