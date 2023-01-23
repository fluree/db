(ns fluree.db.query.union-query-test
  (:require
    [clojure.test :refer :all]
    [fluree.db.test-utils :as test-utils]
    [fluree.db.json-ld.api :as fluree]))

(deftest ^:integration union-queries
  (testing "Testing various 'union' query clauses."
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "query/union" {:context {:ex "http://example.org/ns/"}})
          db     @(fluree/stage
                    (fluree/db ledger)
                    [{:id           :ex/brian,
                      :type         :ex/User,
                      :schema/name  "Brian"
                      :ex/last      "Smith"
                      :schema/email "brian@example.org"
                      :schema/age   50
                      :ex/favNums   7
                      :ex/scores    [76 80 15]}
                     {:id           :ex/alice,
                      :type         :ex/User,
                      :schema/name  "Alice"
                      :ex/last      "Smith"
                      :schema/email "alice@example.org"
                      :ex/favColor  "Green"
                      :schema/age   42
                      :ex/favNums   [42, 76, 9]
                      :ex/scores    [102 92.5 90]}
                     {:id          :ex/cam,
                      :type        :ex/User,
                      :schema/name "Cam"
                      :ex/last     "Jones"
                      :ex/email    "cam@example.org"
                      :schema/age  34
                      :ex/favNums  [5, 10]
                      :ex/scores   [97.2 100 80]
                      :ex/friend   [:ex/brian :ex/alice]}])]

      ;; basic combine :schema/email and :ex/email into same result variable
      (is (= @(fluree/query db {:select ['?name '?email]
                                :where  [['?s :rdf/type :ex/User]
                                         ['?s :schema/name '?name]
                                         {:union [[['?s :ex/email '?email]]
                                                  [['?s :schema/email '?email]]]}]})
             [["Cam" "cam@example.org"]
              ["Alice" "alice@example.org"]
              ["Brian" "brian@example.org"]])
          "Emails for all 3 users should return, even though some are :schema/email and others :ex/email")

      ;; basic union that uses different variables for output
      (is (= @(fluree/query db {:select ['?s '?email1 '?email2]
                                :where  [['?s :rdf/type :ex/User]
                                         {:union [[['?s :ex/email '?email1]]
                                                  [['?s :schema/email '?email2]]]}]})
             [[:ex/cam "cam@example.org" nil]
              [:ex/alice nil "alice@example.org"]
              [:ex/brian nil "brian@example.org"]])
          "Emails for all 3 users should return, but wil each using their own var and nils for others")

      ;; basic union that uses different variables for output and has a passthrough variable
      (is (= @(fluree/query db {:select ['?name '?email1 '?email2]
                                :where  [['?s :rdf/type :ex/User]
                                         ['?s :schema/name '?name]
                                         {:union [[['?s :ex/email '?email1]]
                                                  [['?s :schema/email '?email2]]]}]})
             [["Cam" "cam@example.org" nil]
              ["Alice" nil "alice@example.org"]
              ["Brian" nil "brian@example.org"]])
          "Emails for all 3 users should return using different vars, but also passing through a variable"))))

