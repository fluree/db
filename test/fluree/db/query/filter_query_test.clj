(ns fluree.db.query.filter-query-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.api :as fluree]))

(deftest ^:integration filter-test
  (let [conn   (test-utils/create-conn)
        ledger @(fluree/create conn "query/filter")
        db     @(fluree/stage
                  (fluree/db ledger)
                  {"@context" ["https://ns.flur.ee"
                               test-utils/default-context
                               {:ex "http://example.org/ns/"}]
                   "insert"
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
                    {:id           :ex/cam,
                     :type         :ex/User,
                     :schema/name  "Cam"
                     :ex/last      "Jones"
                     :schema/email "cam@example.org"
                     :schema/age   34
                     :ex/favColor  "Blue"
                     :ex/favNums   [5, 10]
                     :ex/friend    [:ex/brian :ex/alice]}
                    {:id           :ex/david,
                     :type         :ex/User,
                     :schema/name  "David"
                     :ex/last      "Jones"
                     :schema/email "david@example.org"
                     :schema/age   46
                     :ex/favNums   [15 70]
                     :ex/friend    [:ex/cam]}]})]

    (testing "single filter"
      (is (= [["Brian" 50]
              ["David" 46]]
             @(fluree/query db {:context [test-utils/default-context
                                          {:ex "http://example.org/ns/"}]
                                :select  '[?name ?age]
                                :where   '[{:type        :ex/User
                                            :schema/age  ?age
                                            :schema/name ?name}
                                           [:filter "(> ?age 45)"]]}))))
    (testing "single filter, different vars"
      (is (= [["Brian" "Smith"]]
             @(fluree/query db {:context [test-utils/default-context
                                          {:ex "http://example.org/ns/"}]
                                :select  '[?name ?last]
                                :where   '[{:type        :ex/User
                                            :schema/age  ?age
                                            :schema/name ?name
                                            :ex/last     ?last}
                                           [:filter "(and (> ?age 45) (strEnds ?last \"ith\"))"]]}))))
    (testing "multiple filters on same var"
      (is (= [["David" 46]]
             @(fluree/query db {:context [test-utils/default-context
                                          {:ex "http://example.org/ns/"}]
                                :select  '[?name ?age]
                                :where   '[{:type        :ex/User
                                            :schema/age  ?age
                                            :schema/name ?name}
                                           [:filter "(> ?age 45)" "(< ?age 50)"]]}))))
    (testing "multiple filters, different vars"
      (is (= [["Brian" "Smith"]]
             @(fluree/query db {:context [test-utils/default-context
                                          {:ex "http://example.org/ns/"}]
                                :select  '[?name ?last]
                                :where   '[{:type        :ex/User
                                            :schema/age  ?age
                                            :schema/name ?name
                                            :ex/last     ?last}
                                           [:filter "(> ?age 45)" "(strEnds ?last \"ith\")"]]}))))

    (testing "nested filters"
      (is (= [["Brian" 50]]
             @(fluree/query db {:context [test-utils/default-context
                                          {:ex "http://example.org/ns/"}]
                                :select  '[?name ?age]
                                :where   '[{:type        :ex/User
                                            :schema/age  ?age
                                            :schema/name ?name}
                                           [:filter "(> ?age (/ (+ ?age 47) 2))"]]}))))

    (testing "filtering for absence"
      (is (= ["Brian" "David"]
             @(fluree/query db {:context [test-utils/default-context
                                          {:ex "http://example.org/ns/"}]
                                :select  '?name
                                :where   '[{:id          ?s
                                            :type        :ex/User
                                            :schema/name ?name}
                                           [:optional {:id          ?s
                                                       :ex/favColor ?color}]
                                           [:filter "(not (bound ?color))"]]}))))

    (testing "filtering bound variables"
      (is (= ["Cam"]
             @(fluree/query db {:context [test-utils/default-context
                                          {:ex "http://example.org/ns/"}]
                                :select  '?name
                                :where   '[{:type        :ex/User
                                            :schema/name ?name}
                                           [:bind ?nameLength "(strLen ?name)"]
                                           [:filter "(> 4 ?nameLength)"]]}))))

    (testing "filtering literal value-maps"
      (is (= ["Cam"]
             @(fluree/query db {:context [test-utils/default-context
                                          {:ex "http://example.org/ns/"}]
                                :select  '?name
                                :where   '[{:type        :ex/User
                                            :schema/name ?name}
                                           [:bind ?nameLength "(strLen ?name)"]
                                           [:filter "(> {\"@value\" 4 :type :xsd/int} ?nameLength)"]]})))
      (is (= ["Cam"]
             @(fluree/query db {:context [test-utils/default-context
                                          {:ex "http://example.org/ns/"}]
                                :select  '?name
                                :where   '[{:type        :ex/User
                                            :schema/name ?name}
                                           [:bind ?nameLength "(strLen ?name)"]
                                           [:filter "(in ?nameLength [2 3 {\"@value\" 4 :type :xsd/int}])"]]}))))

    (testing "filtering variables bound to iris"
      (let [db-dads @(fluree/stage
                       db
                       {"@context" {"ex" "http://example.org/"}
                        "insert"   {"@id"       "ex:bob"
                                    "ex:father" [{"@id" "ex:alex-jr"}, {"@id" "ex:aj"}]}})]
        (is (= [["ex:bob" "ex:aj" "ex:alex-jr"] ["ex:bob" "ex:alex-jr" "ex:aj"]]
               @(fluree/query db-dads {:context {"ex" "http://example.org/"}
                                       :select  '[?s ?f1 ?f2]
                                       :where   '[{"@id"       ?s
                                                   "ex:father" ?f1}
                                                  {"@id"       ?s
                                                   "ex:father" ?f2}
                                                  ["filter" "(not= ?f1 ?f2)"]]})))))

    (testing "value map filters"
      (is (= [["Brian" "Smith"]]
             @(fluree/query db {:context [test-utils/default-context
                                          {:ex     "http://example.org/ns/"
                                           :value  "@value"
                                           :filter "@filter"}]
                                :select  '[?name ?last]
                                :where   '[{:type        :ex/User
                                            :schema/age  {:value  ?age
                                                          :filter "(> ?age 45)"}
                                            :schema/name ?name
                                            :ex/last     {:value  ?last
                                                          :filter "(strEnds ?last \"ith\")"}}]}))))

    ;;TODO: simple-subject-crawl does not yet support filters.
    ;;these are being run as regular analytial queries
    (testing "simple-subject-crawl"
      (is (= [{:id           :ex/david,
               :type         :ex/User,
               :schema/name  "David",
               :ex/last      "Jones",
               :schema/email "david@example.org",
               :schema/age   46,
               :ex/favNums   [15 70],
               :ex/friend    {:id :ex/cam}}
              {:id           :ex/brian,
               :type         :ex/User,
               :schema/name  "Brian",
               :ex/last      "Smith",
               :schema/email "brian@example.org",
               :schema/age   50,
               :ex/favNums   7}]
             @(fluree/query db {:context [test-utils/default-context
                                          {:ex "http://example.org/ns/"}]
                                :select  {"?s" ["*"]}
                                :where   [{:id "?s", :schema/age "?age"}
                                          [:filter "(> ?age 45)"]]})))
      (is (= [{:id           :ex/david,
               :type         :ex/User,
               :schema/name  "David",
               :ex/last      "Jones",
               :schema/email "david@example.org",
               :schema/age   46,
               :ex/favNums   [15 70],
               :ex/friend    {:id :ex/cam}}]
             @(fluree/query db {:context [test-utils/default-context
                                          {:ex "http://example.org/ns/"}]
                                :select  {"?s" ["*"]}
                                :where   [{:id "?s", :schema/age "?age"}
                                          [:filter "(> ?age 45)" "(< ?age 50)"]]})))
      (is (= [{:type         :ex/User
               :schema/email "cam@example.org"
               :ex/favNums   [5 10]
               :schema/age   34
               :ex/last      "Jones"
               :schema/name  "Cam"
               :id           :ex/cam
               :ex/friend    [{:id :ex/alice} {:id :ex/brian}]
               :ex/favColor  "Blue"}]
             @(fluree/query db {:context [test-utils/default-context
                                          {:ex "http://example.org/ns/"}]
                                :select  {"?s" ["*"]}
                                :where   [{:id "?s", :ex/favColor "?color"}
                                          [:filter "(strStarts ?color \"B\")"]]}))))))
