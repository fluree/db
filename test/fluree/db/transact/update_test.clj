(ns fluree.db.transact.update-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.log :as log]))

(deftest ^:integration deleting-data
  (testing "Deletions of entire subjects."
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "tx/delete" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
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

          ;; delete everything for :ex/alice
          db-subj-delete @(fluree/stage db
                                        '{:delete [:ex/alice ?p ?o]
                                          :where  [[:ex/alice ?p ?o]]})

          ;; delete any :schema/age values for :ex/bob
          db-subj-pred-del @(fluree/stage db
                                          '{:delete [:ex/bob :schema/age ?o]
                                            :where  [[:ex/bob :schema/age ?o]]})

          ;; delete all subjects with a :schema/email predicate
          db-all-preds @(fluree/stage db
                                      '{:delete [?s ?p ?o]
                                        :where  [[?s :schema/email ?x]
                                                 [?s ?p ?o]]})

          ;; delete all subjects where :schema/age = 30
          db-age-delete @(fluree/stage db
                                       '{:delete [?s ?p ?o]
                                         :where  [[?s :schema/age 30]
                                                  [?s ?p ?o]]})

          ;; Change Bob's age - but only if his age is still 22
          db-update-bob @(fluree/stage db
                                       '{:delete [:ex/bob :schema/age 22]
                                         :insert [:ex/bob :schema/age 23]
                                         :where  [[:ex/bob :schema/age 22]]})

          ;; Shouldn't change Bob's age as the current age is not a match
          db-update-bob2 @(fluree/stage db
                                        '{:delete [:ex/bob :schema/age 99]
                                          :insert [:ex/bob :schema/age 23]
                                          :where  [[:ex/bob :schema/age 99]]})

          ;; change Jane's age regardless of its current value
          db-update-jane @(fluree/stage db
                                        '{:delete [:ex/jane :schema/age ?current-age]
                                          :insert [:ex/jane :schema/age 31]
                                          :where  [[:ex/jane :schema/age ?current-age]]})]

      (is (= @(fluree/query db-subj-delete
                            '{:select ?name
                              :where  [[?s :schema/name ?name]]})
             ["Jane" "Bob"])
          "Only Jane and Bob should be left in the db.")

      (is (= @(fluree/query db-subj-pred-del
                            '{:selectOne {?s [:*]}
                              :where     [[?s :id :ex/bob]]})
             {:id          :ex/bob,
              :rdf/type    [:ex/User],
              :schema/name "Bob"})
          "Bob should no longer have an age property.")

      (is (= @(fluree/query db-all-preds
                            '{:select ?name
                              :where  [[?s :schema/name ?name]]})
             ["Bob"])
          "Only Bob should be left, as he is the only one without an email.")

      (is (= @(fluree/query db-age-delete
                            '{:select ?name
                              :where  [[?s :schema/name ?name]]})
             ["Bob" "Alice"])
          "Only Bob and Alice should be left in the db.")

    (testing "Updating property value only if it's current value is a match."
      (is (= [{:id          :ex/bob,
               :rdf/type    [:ex/User],
               :schema/name "Bob"
               :schema/age  23}]
             @(fluree/query db-update-bob
                            '{:select {?s [:*]}
                              :where  [[?s :id :ex/bob]]}))
          "Bob's age should now be updated to 23 (from 22)."))

    (testing "No update should happen if there is no match."
      (is (= [{:id          :ex/bob,
               :rdf/type    [:ex/User],
               :schema/name "Bob"
               :schema/age  22}]
             @(fluree/query db-update-bob2
                            '{:select {?s [:*]}
                              :where  [[?s :id :ex/bob]]}))
          "Bob's age should have not been changed and still be 22."))

    (testing "Replacing existing property value with new property value."
      (is (= [{:id           :ex/jane,
               :rdf/type     [:ex/User],
               :schema/name  "Jane"
               :schema/email "jane@flur.ee"
               :schema/age   31}]
             @(fluree/query db-update-jane
                            '{:select {?s [:*]}
                              :where  [[?s :id :ex/jane]]}))
          "Jane's age should now be updated to 31 (from 30).")))))
