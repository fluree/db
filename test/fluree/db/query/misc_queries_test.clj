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
      (is (= [{:_id          211106232532993,
               :id           :ex/bob,
               :rdf/type     [:ex/User],
               :schema/name  "Bob",
               :ex/favArtist {:_id         211106232532994
                              :schema/name "Picasso"}}
              {:_id         211106232532992,
               :id          :ex/alice,
               :rdf/type    [:ex/User],
               :schema/name "Alice"}]
             @(fluree/query db {:select {'?s [:_id :* {:ex/favArtist [:_id :schema/name]}]}
                                :where  [['?s :type :ex/User]]}))))))

(deftest ^:integration s+p+o-full-db-queries
  (testing "Query that pulls entire database."
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
                              :schema/age   30}]})]

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
              [:schema/age :id "http://schema.org/age"]
              [:schema/email :id "http://schema.org/email"]
              [:schema/name :id "http://schema.org/name"]
              [:ex/User :id "http://example.org/ns/User"]
              [:ex/User :rdf/type :rdfs/Class]
              [:rdfs/Class :id "http://www.w3.org/2000/01/rdf-schema#Class"]
              [:rdf/type :id "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]
              [:id :id "@id"]]
             @(fluree/query db {:select ['?s '?p '?o]
                                :where  [['?s '?p '?o]]}))
          "Entire database should be pulled."))))
