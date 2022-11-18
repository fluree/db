(ns fluree.db.query.filter-query-test
  (:require
    [clojure.test :refer :all]
    [fluree.db.test-utils :as test-utils]
    [fluree.db.json-ld.api :as fluree]
    #_[fluree.db.util.log :as log]))


(deftest ^:integration filter-test
  (let [conn   (test-utils/create-conn)
        ledger @(fluree/create conn "query/filter" {:context {:ex "http://example.org/ns/"}})
        db     @(fluree/stage
                 ledger
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
                   :schema/email    "cam@example.org"
                   :schema/age  34
                   :ex/favNums  [5, 10]
                   :ex/scores   [97.2 100 80]
                   :ex/friend   [:ex/brian :ex/alice]}])]

    (testing "single filter"
      (is (= [["Brian" 50]]
             @(fluree/query db {:select ['?name '?age]
                                :where  [['?s :rdf/type :ex/User]
                                         ['?s :schema/age '?age]
                                         ['?s :schema/name '?name]
                                         {:filter ["(> ?age 45)"]}]})))
      (is (= [["Alice" 42]
              ["Brian" 50]]
             @(fluree/query db {:select ['?name '?age]
                                :where  [['?s :rdf/type :ex/User]
                                         ['?s :schema/age '?age]
                                         ['?s :schema/name '?name]
                                         {:filter ["(> ?age 40)"]}]}))))
    (testing "multiple filters on same var"
      (is (= [["Alice" 42]]
             @(fluree/query db {:select ['?name '?age]
                                :where  [['?s :rdf/type :ex/User]
                                         ['?s :schema/age '?age]
                                         ['?s :schema/name '?name]
                                         {:filter ["(> ?age 40)", "(< ?age 50)"]}]}))))
    (testing "multiple filters, different vars"
      (is (= [["Brian" "Smith"]]
             @(fluree/query db {:select ['?name '?last]
                                :where  [['?s :rdf/type :ex/User]
                                         ['?s :schema/age '?age]
                                         ['?s :schema/name '?name]
                                         ['?s :ex/last '?last]
                                         {:filter ["(> ?age 45)", "(strEnds ?last \"ith\") "]}]}))))))

