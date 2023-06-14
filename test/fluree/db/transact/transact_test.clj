(ns fluree.db.transact.transact-test
  (:require [clojure.test :refer :all]
            [fluree.db.did :as did]
            [fluree.db.util.core :as util]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]
            [clojure.string :as str]))

(deftest ^:integration staging-data
  (testing "Disallow staging invalid transactions"
    (let [conn             (test-utils/create-conn)
          ledger           @(fluree/create conn "tx/disallow" {:defaultContext ["" {:ex "http://example.org/ns/"}]})

          stage-id-only    (try
                             @(fluree/stage
                               (fluree/db ledger)
                               {:id :ex/alice})
                             (catch Exception e e))
          stage-empty-txn  (try
                             @(fluree/stage
                               (fluree/db ledger)
                               {})
                             (catch Exception e e))
          stage-empty-node (try
                             @(fluree/stage
                               (fluree/db ledger)
                               [{:id         :ex/alice
                                 :schema/age 42}
                                {}])
                             (catch Exception e e))
          db-ok            @(fluree/stage
                             (fluree/db ledger)
                             {:id         :ex/alice
                              :schema/age 42})]
      (is (util/exception? stage-id-only))
      (is (str/starts-with? (ex-message stage-id-only)
                            "Invalid transaction, transaction node contains no properties for @id:"))
      (is (util/exception? stage-empty-txn))
      (is (= (ex-message stage-empty-txn)
             "Invalid transaction, transaction node contains no properties."))
      (is (util/exception? stage-empty-node))
      (is (= (ex-message stage-empty-node)
             "Invalid transaction, transaction node contains no properties."))
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
                    {:id        :ex/alice
                     :ex/isCool false})]
      (is (= [[:ex/alice :id "http://example.org/ns/alice"]
              [:ex/alice :ex/isCool false]
              [:ex/isCool :id "http://example.org/ns/isCool"]
              [:rdfs/Class :id "http://www.w3.org/2000/01/rdf-schema#Class"]
              [:rdf/type :id "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]
              [:id :id "@id"]]
             @(fluree/query db-bool '{:select [?s ?p ?o]
                                      :where  [[?s ?p ?o]]})))))

  (testing "mixed data types (ref & string) are handled correctly"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "tx/mixed-dts"
                                 {:defaultContext
                                  ["" {:ex "http://example.org/ns/"}]})
          db     @(fluree/stage (fluree/db ledger)
                                {:id               :ex/brian
                                 :ex/favCoffeeShop [:wiki/Q37158
                                                    "Clemmons Coffee"]})
          _db    @(fluree/commit! ledger db)
          loaded (test-utils/retry-load conn "tx/mixed-dts" 100)
          db     (fluree/db loaded)
          query  '{:select {?b [:*]}
                   :where  [[?b :id :ex/brian]]}]
      (is (= [{:id               :ex/brian
               :ex/favCoffeeShop [{:id :wiki/Q37158} "Clemmons Coffee"]}]
             @(fluree/query db query)))))

  (testing "mixed data types (num & string) are handled correctly"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "tx/mixed-dts"
                                 {:defaultContext
                                  ["" {:ex "http://example.org/ns/"}]})
          db     @(fluree/stage (fluree/db ledger)
                                {:id :ex/wes
                                 :ex/aFewOfMyFavoriteThings
                                 {"@list" [2011 "jabalí"]}})
          _db    @(fluree/commit! ledger db)
          loaded (test-utils/retry-load conn "tx/mixed-dts" 100)
          db     (fluree/db loaded)
          query  '{:select {?b [:*]}
                   :where  [[?b :id :ex/wes]]}]
      (is (= [{:id                        :ex/wes
               :ex/aFewOfMyFavoriteThings [2011 "jabalí"]}]
             @(fluree/query db query))))))

(deftest policy-ordering-test
  (testing "transaction order does not affect query results"
    (let [conn            (test-utils/create-conn)
          ledger          @(fluree/create conn "tx/policy-order" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          alice-did       (:id (did/private->did-map "c0459840c334ca9f20c257bed971da88bd9b1b5d4fca69d4e3f4b8504f981c07"))
          data            [{:id          :ex/alice,
                            :type        :ex/User,
                            :schema/name "Alice"}
                           {:id          :ex/john,
                            :type        :ex/User,
                            :schema/name "John"}
                           {:id      alice-did
                            :ex/user :ex/alice
                            :f/role  :ex/userRole}]
          policy          [{:id            :ex/UserPolicy,
                            :type          [:f/Policy],
                            :f/targetClass :ex/User
                            :f/allow       [{:id           :ex/globalViewAllow
                                             :f/targetRole :ex/userRole
                                             :f/action     [:f/view]}]}]
          db-data-first   @(fluree/stage
                             (fluree/db ledger)
                             (into data policy))
          db-policy-first @(fluree/stage
                             (fluree/db ledger)
                             (into policy data))
          user-query      '{:select {?s [:*]}
                            :where  [[?s :rdf/type :ex/User]]}]
      (let [users [{:id :ex/john, :rdf/type [:ex/User], :schema/name "John"}
                   {:id :ex/alice, :rdf/type [:ex/User], :schema/name "Alice"}]]
        (is (= users
               @(fluree/query db-data-first user-query)))
        (is (= users
               @(fluree/query db-policy-first user-query)))))))
