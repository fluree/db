(ns fluree.db.query.filter-query-test
  (:require
    [clojure.test :refer :all]
    [fluree.db.test-utils :as test-utils]
    [fluree.db.json-ld.api :as fluree]))

(deftest ^:integration filter-test
  (let [conn   (test-utils/create-conn)
        ledger @(fluree/create conn "query/filter" {:context {:ex "http://example.org/ns/"}})
        db     @(fluree/stage
                  (fluree/db ledger)
                 [{:id           :ex/brian,
                   :type         :ex/User,
                   :schema/name  "Brian"
                   :ex/last      "Smith"
                   :schema/email "brian@example.org"
                   :schema/age   50
                   :ex/favNums   7}
                  {:id           :ex/alice,
                   :type         :ex/User,
                   :schema/name  "Alice"
                   :ex/last      "Smith"
                   :schema/email "alice@example.org"
                   :ex/favColor  "Green"
                   :schema/age   42
                   :ex/favNums   [42, 76, 9]}
                  {:id          :ex/cam,
                   :type        :ex/User,
                   :schema/name "Cam"
                   :ex/last     "Jones"
                   :schema/email    "cam@example.org"
                   :schema/age  34
                   :ex/favColor "Blue"
                   :ex/favNums  [5, 10]
                   :ex/friend   [:ex/brian :ex/alice]}
                  {:id          :ex/david,
                   :type        :ex/User,
                   :schema/name "David"
                   :ex/last     "Jones"
                   :schema/email    "david@example.org"
                   :schema/age  46
                   :ex/favNums  [15 70]
                   :ex/friend   [:ex/cam]}])]

    (testing "single filter"
      (is (= [["David" 46]
              ["Brian" 50]]
             @(fluree/query db {:select ['?name '?age]
                                :where  [['?s :rdf/type :ex/User]
                                         ['?s :schema/age '?age]
                                         ['?s :schema/name '?name]
                                         {:filter ["(> ?age 45)"]}]}))))
    (testing "multiple filters on same var"
      (is (= [["David" 46]]
             @(fluree/query db {:select ['?name '?age]
                                :where  [['?s :rdf/type :ex/User]
                                         ['?s :schema/age '?age]
                                         ['?s :schema/name '?name]
                                         {:filter ["(> ?age 45)", "(< ?age 50)"]}]}))))
    (testing "multiple filters, different vars"
      (is (= [["Brian" "Smith"]]
             @(fluree/query db {:select ['?name '?last]
                                :where  [['?s :rdf/type :ex/User]
                                         ['?s :schema/age '?age]
                                         ['?s :schema/name '?name]
                                         ['?s :ex/last '?last]
                                         {:filter ["(> ?age 45)", "(strEnds ?last \"ith\")"]}]}))))

    (testing "nested filters"
      (is (= [["Brian" 50]]
             @(fluree/query db '{:context {:ex "http://example.org/ns/"}
                                 :select [?name ?age]
                                 :where  [[?s :rdf/type :ex/User]
                                          [?s :schema/age ?age]
                                          [?s :schema/name ?name]
                                          {:filter ["(> ?age (/ (+ ?age 47) 2))"]}]}))))

    ;;TODO: simple-subject-crawl does not yet support filters.
    ;;these are being run as regular analytial queries
    (testing "simple-subject-crawl"
      (is (= [{:id :ex/david,
               :rdf/type [:ex/User],
               :schema/name "David",
               :ex/last "Jones",
               :schema/email "david@example.org",
               :schema/age 46,
               :ex/favNums [15 70],
               :ex/friend {:id :ex/cam}}
              {:id :ex/brian,
               :rdf/type [:ex/User],
               :schema/name "Brian",
               :ex/last "Smith",
               :schema/email "brian@example.org",
               :schema/age 50,
               :ex/favNums 7}]
             @(fluree/query db {:select {"?s" ["*"]}
                                :where  [["?s" :schema/age "?age"]
                                         {:filter ["(> ?age 45)"]}]})))
      (is (= [{:id :ex/david,
               :rdf/type [:ex/User],
               :schema/name "David",
               :ex/last "Jones",
               :schema/email "david@example.org",
               :schema/age 46,
               :ex/favNums [15 70],
               :ex/friend {:id :ex/cam}}]
             @(fluree/query db {:select {"?s" ["*"]}
                                :where  [["?s" :schema/age "?age"]
                                         {:filter ["(> ?age 45)", "(< ?age 50)"]}]})))
      (is (= [{:rdf/type [:ex/User]
               :schema/email "cam@example.org"
               :ex/favNums [5 10]
               :schema/age 34
               :ex/last "Jones"
               :schema/name "Cam"
               :id :ex/cam
               :ex/friend [{:id :ex/brian} {:id :ex/alice}]
               :ex/favColor "Blue"}]
             @(fluree/query db {:select {"?s" ["*"]}
                                :where  [["?s" :ex/favColor "?color"]
                                         {:filter ["(strStarts ?color \"B\")"]}]}))))))
