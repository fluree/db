(ns fluree.db.transact.retraction-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]))

(deftest ^:integration retracting-data
  (testing "Retractions of individual properties and entire subjects."
    (let [conn           (test-utils/create-conn)
          ledger         @(fluree/create conn "tx/retract")
          db             @(fluree/stage
                            (fluree/db ledger)
                            {:context {:ex "http://example.org/ns/"}
                             :graph   [{:id          :ex/alice,
                                        :type        :ex/User,
                                        :schema/name "Alice"
                                        :schema/age  42}
                                       {:id          :ex/bob,
                                        :type        :ex/User,
                                        :schema/name "Bob"
                                        :schema/age  22}
                                       {:id          :ex/jane,
                                        :type        :ex/User,
                                        :schema/name "Jane"
                                        :schema/age  30}]})
          ;; retract Alice's age attribute by using nil
          db-age-retract @(fluree/stage
                            db
                            {:context    {:ex "http://example.org/ns/"}
                             :id         :ex/alice,
                             :schema/age nil})]
      (is (= @(fluree/query db-age-retract
                            '{:context {:ex "http://example.org/ns/"},
                              :select {?s [:*]},
                              :where [[?s :id :ex/alice]]})
             [{:id           :ex/alice,
               :rdf/type     [:ex/User],
               :schema/name  "Alice"}])
          "Alice should no longer have an age property"))))
