(ns fluree.db.query.misc-queries-test
  (:require
    [clojure.string :as str]
    [clojure.test :refer :all]
    [fluree.db.test-utils :as test-utils]
    [fluree.db.json-ld.api :as fluree]
    [fluree.db.util.log :as log]))

(deftest ^:integration s+p+o-full-db-queries
  (testing "Query that pulls entire database."
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "query/everything" {:context {:ex "http://example.org/ns/"}})
          db     @(fluree/stage
                    ledger
                    {:graph [{:id           :ex/alice,
                              :type         :ex/User,
                              :schema/name  "Alice"
                              :schema/email "alice@flur.ee"
                              :schema/age   42}
                             {:id          :ex/bob,
                              :type        :ex/User,
                              :schema/name "Bob"
                              :schema/age  22}
                             {:id           :ex/jane,
                              :type         :ex/User,
                              :schema/name  "Jane"
                              :schema/email "jane@flur.ee"
                              :schema/age   30}]})]

      (is (= @(fluree/query db {:select ['?s '?p '?o]
                                :where  [['?s '?p '?o]]})
             [[:ex/jane :id "http://example.org/ns/jane"]
              [:ex/jane :rdf/type :ex/User]
              [:ex/jane :schema/name "Jane"]
              [:ex/jane :schema/email "jane@flur.ee"]
              [:ex/jane :schema/age 30]
              [:ex/bob :id "http://example.org/ns/bob"]
              [:ex/bob :rdf/type :ex/User]
              [:ex/bob :schema/name "Bob"]
              [:ex/bob :schema/age 22]
              [:ex/alice :id "http://example.org/ns/alice"]
              [:ex/alice :rdf/type :ex/User]
              [:ex/alice :schema/name "Alice"]
              [:ex/alice :schema/email "alice@flur.ee"]
              [:ex/alice :schema/age 42]
              ["did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6" :id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"]
              ["did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6" :rdf/type :f/DID]
              ["did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6" :f/role "fluree-root-role"]
              ["fluree-fn-false" :id "fluree-fn-false"]
              ["fluree-fn-false" :rdf/type :f/Function]
              ["fluree-fn-false" :skos/definition "Always denies access to any data when attached to a rule."]
              ["fluree-fn-false" :skos/prefLabel "False function"]
              ["fluree-fn-false" :f/code "false"]
              [:f/opsAll :id "https://ns.flur.ee/ledger#opsAll"]
              ["fluree-fn-true" :id "fluree-fn-true"]
              ["fluree-fn-true" :rdf/type :f/Function]
              ["fluree-fn-true" :skos/definition "Always allows full access to any data when attached to a rule."]
              ["fluree-fn-true" :skos/prefLabel "True function"]
              ["fluree-fn-true" :f/code "true"]
              ["fluree-root-rule" :id "fluree-root-rule"]
              ["fluree-root-rule" :rdf/type :f/Rule]
              ["fluree-root-rule" :skos/definition "Default root rule, attached to fluree-root-role."]
              ["fluree-root-rule" :skos/prefLabel "Root rule"]
              ["fluree-root-rule" :f/allTypes true]
              ["fluree-root-rule" :f/function "fluree-fn-true"]
              ["fluree-root-rule" :f/operations :f/opsAll]
              ["fluree-root-role" :id "fluree-root-role"]
              ["fluree-root-role" :rdf/type :f/Role]
              ["fluree-root-role" :skos/definition "Default role that gives full root access to a ledger."]
              ["fluree-root-role" :skos/prefLabel "Root role"]
              ["fluree-root-role" :f/rules "fluree-root-rule"]
              [:schema/age :id "http://schema.org/age"]
              [:schema/email :id "http://schema.org/email"]
              [:schema/name :id "http://schema.org/name"]
              [:ex/User :id "http://example.org/ns/User"]
              [:ex/User :rdf/type :rdfs/Class]
              [:f/role :id "https://ns.flur.ee/ledger#role"]
              [:f/role :rdf/type :id]
              [:f/DID :id "https://ns.flur.ee/ledger#DID"]
              [:f/DID :rdf/type :rdfs/Class]
              [:f/Context :id "https://ns.flur.ee/ledger#Context"]
              [:f/Context :rdf/type :rdfs/Class]
              [:f/code :id "https://ns.flur.ee/ledger#code"]
              [:f/Function :id "https://ns.flur.ee/ledger#Function"]
              [:f/Function :rdf/type :rdfs/Class]
              [:f/operations :id "https://ns.flur.ee/ledger#operations"]
              [:f/operations :rdf/type :id]
              [:f/function :id "https://ns.flur.ee/ledger#function"]
              [:f/function :rdf/type :id]
              [:f/allTypes :id "https://ns.flur.ee/ledger#allTypes"]
              [:f/Rule :id "https://ns.flur.ee/ledger#Rule"]
              [:f/Rule :rdf/type :rdfs/Class]
              [:f/rules :id "https://ns.flur.ee/ledger#rules"]
              [:f/rules :rdf/type :id]
              [:skos/prefLabel :id "http://www.w3.org/2008/05/skos#prefLabel"]
              [:skos/definition :id "http://www.w3.org/2008/05/skos#definition"]
              [:f/Role :id "https://ns.flur.ee/ledger#Role"]
              [:f/Role :rdf/type :rdfs/Class]
              [:f/context :id "https://ns.flur.ee/ledger#context"]
              [:rdfs/Class :id "http://www.w3.org/2000/01/rdf-schema#Class"]
              [:rdf/type :id "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]
              ["fluree-default-context" :id "fluree-default-context"]
              ["fluree-default-context" :rdf/type :f/Context]
              ["fluree-default-context" :f/context "{\"schema\":\"http://schema.org/\",\"wiki\":\"https://www.wikidata.org/wiki/\",\"xsd\":\"http://www.w3.org/2001/XMLSchema#\",\"type\":\"@type\",\"rdfs\":\"http://www.w3.org/2000/01/rdf-schema#\",\"ex\":\"http://example.org/ns/\",\"id\":\"@id\",\"f\":\"https://ns.flur.ee/ledger#\",\"sh\":\"http://www.w3.org/ns/shacl#\",\"skos\":\"http://www.w3.org/2008/05/skos#\",\"rdf\":\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"}"]
              [:id :id "@id"]])
          "Entire database should be pulled."))))
