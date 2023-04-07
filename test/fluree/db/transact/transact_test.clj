(ns fluree.db.transact.transact-test
  (:require [clojure.test :refer :all]
            [fluree.db.util.core :as util]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]
            [clojure.string :as str]))

(deftest ^:integration staging-data
  (testing "Disallow staging invalid transactions"
    (let [conn           (test-utils/create-conn )
          ledger         @(fluree/create conn "tx/disallow" {:defaultContext ["" {:ex "http://example.org/ns/"}]})

          stage-id-only     (try
                              @(fluree/stage
                                (fluree/db ledger)
                                {:id :ex/alice})
                              (catch Exception e e))
          stage-empty-txn   (try
                              @(fluree/stage
                                (fluree/db ledger)
                                {})
                              (catch Exception e e))
          stage-empty-node   (try
                               @(fluree/stage
                                 (fluree/db ledger)
                                 [{:id :ex/alice
                                   :schema/age 42}
                                  {}])
                               (catch Exception e e))
          db-ok           @(fluree/stage
                            (fluree/db ledger)
                            {:id :ex/alice
                             :schema/age 42})]
      (is (util/exception? stage-id-only))
      (is (str/starts-with? (ex-message stage-id-only)
                            "Invalid transaction, transaction node contains no properties for @id:" ))
      (is (util/exception? stage-empty-txn))
      (is (= (ex-message stage-empty-txn)
             "Invalid transaction, transaction node contains no properties." ))
      (is (util/exception? stage-empty-node))
      (is (= (ex-message stage-empty-node)
             "Invalid transaction, transaction node contains no properties." ))
      (is (= [[:ex/alice :id "http://example.org/ns/alice"]
              [:ex/alice :schema/age 42]
              [:schema/age :id "http://schema.org/age"]
              [:rdfs/Class :id "http://www.w3.org/2000/01/rdf-schema#Class"]
              [:rdf/type :id "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]
              [:id :id "@id"]]
             @(fluree/query db-ok '{:select [?s ?p ?o]
                                    :where  [[?s ?p ?o]]})))))
  (testing "Allow transacting `false` values"
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "tx/bools" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          db-bool @(fluree/stage
                     (fluree/db ledger)
                     {:id         :ex/alice
                      :ex/isCool   false})]
      (is (= [[:ex/alice :id "http://example.org/ns/alice"]
              [:ex/alice :ex/isCool false]
              [:ex/isCool :id "http://example.org/ns/isCool"]
              [:rdfs/Class :id "http://www.w3.org/2000/01/rdf-schema#Class"]
              [:rdf/type :id "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]
              [:id :id "@id"]]
             @(fluree/query db-bool '{:select  [?s ?p ?o]
                                      :where   [[?s ?p ?o]]}))))))
