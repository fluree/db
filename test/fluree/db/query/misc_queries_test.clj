(ns fluree.db.query.misc-queries-test
  (:require
    [clojure.test :refer :all]
    [fluree.db.test-utils :as test-utils]
    [fluree.db.json-ld.api :as fluree]))

(deftest ^:integration select-sid
  (testing "Select index's subject id in query using special keyword"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "query/subid" {:context {:ex "http://example.org/ns/"}})
          db     @(fluree/stage
                    (fluree/db ledger)
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
    (with-redefs [fluree.db.util.core/current-time-iso (fn [] "1970-01-01T00:12:00.00000Z")]
      (let [conn   (test-utils/create-conn)
            ledger @(fluree/create conn "query/everything" {:context {:ex "http://example.org/ns/"}})
            db     @(fluree/stage
                      (fluree/db ledger)
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
                                :schema/age   30}]})
            db @(fluree/commit! ledger db)]

        (is (= [[:ex/jane :id "http://example.org/ns/jane"]
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
                ["did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"
                 :id
                 "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"]
                ["fluree:db:sha256:bbfcuiy4ja4gmeti55i6nyubvvpocjw4q6mztopxxnykc5w24odem"
                 :id
                 "fluree:db:sha256:bbfcuiy4ja4gmeti55i6nyubvvpocjw4q6mztopxxnykc5w24odem"]
                ["fluree:db:sha256:bbfcuiy4ja4gmeti55i6nyubvvpocjw4q6mztopxxnykc5w24odem"
                 "https://ns.flur.ee/commitdata#address"
                 "fluree:memory://dc2f3160e6ddcf3d271c6873c0a72a2a35519743f7c69a58c07238241561601a"]
                ["fluree:db:sha256:bbfcuiy4ja4gmeti55i6nyubvvpocjw4q6mztopxxnykc5w24odem"
                 "https://ns.flur.ee/commitdata#flakes"
                 62]
                ["fluree:db:sha256:bbfcuiy4ja4gmeti55i6nyubvvpocjw4q6mztopxxnykc5w24odem"
                 "https://ns.flur.ee/commitdata#size"
                 5450]
                ["fluree:db:sha256:bbfcuiy4ja4gmeti55i6nyubvvpocjw4q6mztopxxnykc5w24odem"
                 "https://ns.flur.ee/commitdata#t"
                 1]
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
                ["https://ns.flur.ee/commitdata#v"
                 :id
                 "https://ns.flur.ee/commitdata#v"]
                ["https://ns.flur.ee/commitdata#v" :rdf/type :id]
                ["https://ns.flur.ee/commitdata#t"
                 :id
                 "https://ns.flur.ee/commitdata#t"]
                ["https://ns.flur.ee/commitdata#t" :rdf/type :id]
                ["https://ns.flur.ee/commitdata#size"
                 :id
                 "https://ns.flur.ee/commitdata#size"]
                ["https://ns.flur.ee/commitdata#size" :rdf/type :id]
                ["https://ns.flur.ee/commitdata#flakes"
                 :id
                 "https://ns.flur.ee/commitdata#flakes"]
                ["https://ns.flur.ee/commitdata#flakes" :rdf/type :id]
                ["https://ns.flur.ee/commitdata#address"
                 :id
                 "https://ns.flur.ee/commitdata#address"]
                ["https://ns.flur.ee/commitdata#address" :rdf/type :id]
                [:f/branch :id "https://ns.flur.ee/ledger#branch"]
                [:f/branch :rdf/type :id]
                [:f/alias :id "https://ns.flur.ee/ledger#alias"]
                [:f/alias :rdf/type :id]
                ["https://ns.flur.ee/commit#data"
                 :id
                 "https://ns.flur.ee/commit#data"]
                ["https://ns.flur.ee/commit#data" :rdf/type :id]
                ["fluree-default-context" :id "fluree-default-context"]
                ["fluree-default-context" :rdf/type :f/Context]
                ["fluree-default-context"
                 :f/context
                 "{\"schema\":\"http://schema.org/\",\"wiki\":\"https://www.wikidata.org/wiki/\",\"xsd\":\"http://www.w3.org/2001/XMLSchema#\",\"type\":\"@type\",\"rdfs\":\"http://www.w3.org/2000/01/rdf-schema#\",\"ex\":\"http://example.org/ns/\",\"id\":\"@id\",\"f\":\"https://ns.flur.ee/ledger#\",\"sh\":\"http://www.w3.org/ns/shacl#\",\"skos\":\"http://www.w3.org/2008/05/skos#\",\"rdf\":\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"}"]
                ["https://ns.flur.ee/commit#address"
                 :id
                 "https://ns.flur.ee/commit#address"]
                ["https://ns.flur.ee/commit#address" :rdf/type :id]
                ["https://ns.flur.ee/commit#v" :id "https://ns.flur.ee/commit#v"]
                ["https://ns.flur.ee/commit#v" :rdf/type :id]
                ["https://www.w3.org/2018/credentials#issuer"
                 :id
                 "https://www.w3.org/2018/credentials#issuer"]
                ["https://www.w3.org/2018/credentials#issuer" :rdf/type :id]
                [:f/tag :id "https://ns.flur.ee/ledger#tag"]
                [:f/tag :rdf/type :id]
                ["https://ns.flur.ee/commit#time"
                 :id
                 "https://ns.flur.ee/commit#time"]
                ["https://ns.flur.ee/commit#time" :rdf/type :id]
                [:f/message :id "https://ns.flur.ee/ledger#message"]
                [:f/message :rdf/type :id]
                [:f/commit :id "https://ns.flur.ee/ledger#commit"]
                [:f/commit :rdf/type :id]
                [:f/previous :id "https://ns.flur.ee/ledger#previous"]
                [:f/previous :rdf/type :id]
                [:f/address :id "https://ns.flur.ee/ledger#address"]
                [:f/address :rdf/type :id]
                [:id :id "@id"]
                ["fluree:commit:sha256:bb4qxlsmiryknqoqxlwegfnk4v4gq5qmxhkieds6ccew5utaq2adc"
                 :id
                 "fluree:commit:sha256:bb4qxlsmiryknqoqxlwegfnk4v4gq5qmxhkieds6ccew5utaq2adc"]
                ["fluree:commit:sha256:bb4qxlsmiryknqoqxlwegfnk4v4gq5qmxhkieds6ccew5utaq2adc"
                 "https://ns.flur.ee/commit#time"
                 720000]
                ["fluree:commit:sha256:bb4qxlsmiryknqoqxlwegfnk4v4gq5qmxhkieds6ccew5utaq2adc"
                 "https://www.w3.org/2018/credentials#issuer"
                 "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"]
                ["fluree:commit:sha256:bb4qxlsmiryknqoqxlwegfnk4v4gq5qmxhkieds6ccew5utaq2adc"
                 "https://ns.flur.ee/commit#v"
                 0]
                ["fluree:commit:sha256:bb4qxlsmiryknqoqxlwegfnk4v4gq5qmxhkieds6ccew5utaq2adc"
                 "https://ns.flur.ee/commit#data"
                 "fluree:db:sha256:bbfcuiy4ja4gmeti55i6nyubvvpocjw4q6mztopxxnykc5w24odem"]]

               @(fluree/query db {:select ['?s '?p '?o]
                                  :where  [['?s '?p '?o]]}))
            "Entire database should be pulled.")))))
