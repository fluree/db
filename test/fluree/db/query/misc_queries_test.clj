(ns fluree.db.query.misc-queries-test
  (:require
    [clojure.string :as str]
    [clojure.test :refer :all]
    [fluree.db.test-utils :as test-utils]
    [fluree.db.json-ld.api :as fluree]
    [fluree.db.util.log :as log]))

(deftest ^:integration select-sid
  (testing "Select index's subject id in query using special keyword"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "query/subid" {:context {:ex "http://example.org/ns/"}})
          db     @(fluree/stage
                    ledger
                    {:graph [{:id          :ex/alice,
                              :type        :ex/User,
                              :schema/name "Alice"}
                             {:id           :ex/bob,
                              :type         :ex/User,
                              :schema/name  "Bob"
                              :ex/favArtist {:id          :ex/picasso
                                             :schema/name "Picasso"}}]})]
      (is (= @(fluree/query db {:select {'?s [:_id :* {:ex/favArtist [:_id :schema/name]}]}
                                :where  [['?s :type :ex/User]]})
             [{:_id          211106232532993,
               :id           :ex/bob,
               :rdf/type     [:ex/User],
               :schema/name  "Bob",
               :ex/favArtist {:_id         211106232532994
                              :schema/name "Picasso"}}
              {:_id         211106232532992,
               :id          :ex/alice,
               :rdf/type    [:ex/User],
               :schema/name "Alice"}])))))

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
              [:schema/age :id "http://schema.org/age"]
              [:schema/email :id "http://schema.org/email"]
              [:schema/name :id "http://schema.org/name"]
              [:ex/User :id "http://example.org/ns/User"]
              [:ex/User :rdf/type :rdfs/Class]
              [:f/Context :id "https://ns.flur.ee/ledger#Context"]
              [:f/Context :rdf/type :rdfs/Class]
              [:f/context :id "https://ns.flur.ee/ledger#context"]
              [:rdfs/Class :id "http://www.w3.org/2000/01/rdf-schema#Class"]
              [:rdf/type :id "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]
              ["fluree-default-context" :id "fluree-default-context"]
              ["fluree-default-context" :rdf/type :f/Context]
              ["fluree-default-context" :f/context "{\"schema\":\"http://schema.org/\",\"wiki\":\"https://www.wikidata.org/wiki/\",\"xsd\":\"http://www.w3.org/2001/XMLSchema#\",\"type\":\"@type\",\"rdfs\":\"http://www.w3.org/2000/01/rdf-schema#\",\"ex\":\"http://example.org/ns/\",\"id\":\"@id\",\"f\":\"https://ns.flur.ee/ledger#\",\"sh\":\"http://www.w3.org/ns/shacl#\",\"skos\":\"http://www.w3.org/2008/05/skos#\",\"rdf\":\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"}"]
              [:id :id "@id"]])
          "Entire database should be pulled."))))
