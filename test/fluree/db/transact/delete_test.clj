(ns fluree.db.transact.delete-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-fixtures :as test]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.log :as log]))

(use-fixtures :once test/test-system)

(deftest deleting-data
  (testing "Deletions of entire subjects."
    (let [conn             test/memory-conn
          ledger           @(fluree/create conn "tx/delete" {:context {:ex "http://example.org/ns/"}})
          db               @(fluree/stage
                              ledger
                              {:graph [{:id          :ex/alice,
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

          ;; delete everything for :ex/alice
          db-subj-delete   @(fluree/stage db
                                          {:delete [:ex/alice '?p '?o]
                                           :where  [[:ex/alice '?p '?o]]})

          ;; delete any :schema/age values for :ex/bob
          db-subj-pred-del @(fluree/stage db
                                          {:delete [:ex/bob :schema/age '?o]
                                           :where  [[:ex/bob :schema/age '?o]]})

          ;; delete all subjects where :schema/age = 30
          db-age-delete    @(fluree/stage db
                                          {:delete ['?s '?p '?o]
                                           :where  [['?s :schema/age 30]
                                                    ['?s '?p '?o]]})]

      (is (= @(fluree/query db-subj-delete
                            {:select '?name
                             :where  [['?s :schema/name '?name]]})
             ["Jane" "Bob"])
          "Only Jane and Cam should be left in the db.")

      (is (= @(fluree/query db-subj-pred-del
                            {:selectOne [:*]
                             :from      :ex/bob})
             {:id          :ex/bob,
              :rdf/type    [:ex/User],
              :schema/name "Bob"})
          "Only Jane and Cam should be left in the db.")

      (is (= @(fluree/query db-age-delete
                            {:select '?name
                             :where  [['?s :schema/name '?name]]})
             ["Bob" "Alice"])
          "Only Bob and Alice should be left in the db."))))
