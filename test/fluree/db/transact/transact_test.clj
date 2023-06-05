(ns fluree.db.transact.transact-test
  (:require [clojure.test :refer :all]
            [fluree.db.util.core :as util]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]
            [clojure.string :as str]
            [fluree.db.util.json :as json]
            [fluree.json-ld :as json-ld]))

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
                                      :where   [[?s ?p ?o]]})))))
  (testing "Allow transacting `json` values"
    (let [conn    @(fluree/connect {:method :memory})
          ledger  @(fluree/create conn "tx/bools" {:defaultContext {"ex" "http://example.org/ns/"}})
          db0  (fluree/db ledger)
          db1 @(fluree/stage
                 (fluree/db ledger)
                 {"@context" {"ex:json" {"@type" "@json"}}
                  "@graph" [{"@id" "ex:alice"
                             "@type" "ex:Person"
                             "ex:json" {"json" "data"
                                        "is" ["cool" "right?" 1 false 1.0]}}
                            {"@id" "ex:bob"
                             "@type" "ex:Person"
                             "ex:json" {:edn "data"
                                        :is ["cool" "right?" 1 false 1.0]}}]})]
      (is (= [{"@id" "ex:bob",
               "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" ["ex:Person"],
               "ex:json" {"edn" "data", "is" ["cool" "right?" 1 false 1.0M]}}
              {"@id" "ex:alice",
               "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" ["ex:Person"],
               "ex:json" {"json" "data", "is" ["cool" "right?" 1 false 1.0M]}}]
             @(fluree/query db1 {"@context" {"ex" "http://example.org/ns/"}
                                 "select" {"?s" ["*"]}
                                 "where" [["?s" "@type" "ex:Person"]]}))
          "comes out as data from subject crawl")
      (is (= [{"edn" "data", "is" ["cool" "right?" 1 false 1.0M]}
              {"json" "data", "is" ["cool" "right?" 1 false 1.0M]}]
             @(fluree/query db1 {"@context" {"ex" "http://example.org/ns/"}
                                 "select" "?json"
                                 "where" [["?s" "ex:json" "?json"]]}))
          "comes out as data from select clause"))))
