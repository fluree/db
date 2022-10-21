(ns fluree.db.query.json-ld-compound-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-fixtures :as test]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.log :as log]))


(use-fixtures :once test/test-system)

(deftest simple-compound-queries
  (testing "Simple compound queries."
    (let [conn   test/memory-conn
          ledger @(fluree/create conn "query/compounda")
          db     @(fluree/stage
                    ledger
                    [{:context      {:ex "http://example.org/ns/"}
                      :id           :ex/brian,
                      :type         :ex/User,
                      :schema/name  "Brian"
                      :schema/email "brian@example.org"
                      :schema/age   50
                      :ex/favNums   7}
                     {:context      {:ex "http://example.org/ns/"}
                      :id           :ex/alice,
                      :type         :ex/User,
                      :schema/name  "Alice"
                      :schema/email "alice@example.org"
                      :schema/age   42
                      :ex/favNums   [42, 76, 9]}
                     {:context      {:ex "http://example.org/ns/"}
                      :id           :ex/cam,
                      :type         :ex/User,
                      :schema/name  "Cam"
                      :schema/email "cam@example.org"
                      :schema/age   34
                      :ex/favNums   [5, 10]
                      :ex/friend    [:ex/brian :ex/alice]}])

          two-tuple-select-with-crawl
                 @(fluree/query db {:context {:ex "http://example.org/ns/"}
                                    :select  ['?age {'?f [:*]}]
                                    :where   [['?s :schema/name "Cam"]
                                              ['?s :ex/friend '?f]
                                              ['?f :schema/age '?age]]})

          two-tuple-select-with-crawl+var
                 @(fluree/query db {:context {:ex "http://example.org/ns/"}
                                    :select  ['?age {'?f [:*]}]
                                    :where   [['?s :schema/name '?name]
                                              ['?s :ex/friend '?f]
                                              ['?f :schema/age '?age]]
                                    :vars    {'?name "Cam"}})]

      (is (= two-tuple-select-with-crawl
             two-tuple-select-with-crawl+var
             [[50 {:id           :ex/brian,
                   :rdf/type     [:ex/User],
                   :schema/name  "Brian",
                   :schema/email "brian@example.org",
                   :schema/age   50,
                   :ex/favNums   7}]
              [42 {:id           :ex/alice,
                   :rdf/type     [:ex/User],
                   :schema/name  "Alice",
                   :schema/email "alice@example.org",
                   :schema/age   42,
                   :ex/favNums   [9, 42, 76]}]]))

      ;; here we have pass-through variables (?name and ?age) which must get "passed through"
      ;; the last where statements into the select statement
      (is (= @(fluree/query db {:context {:ex "http://example.org/ns/"}
                                :select  ['?name '?age '?email]
                                :where   [['?s :schema/name "Cam"]
                                          ['?s :ex/friend '?f]
                                          ['?f :schema/name '?name]
                                          ['?f :schema/age '?age]
                                          ['?f :schema/email '?email]]})
             [["Brian" 50 "brian@example.org"]
              ["Alice" 42 "alice@example.org"]])
          "Prior where statement variables may not be passing through to select results")

      ;; same as prior query, but using selectOne
      (is (= @(fluree/query db {:context   {:ex "http://example.org/ns/"}
                                :selectOne ['?name '?age '?email]
                                :where     [['?s :schema/name "Cam"]
                                            ['?s :ex/friend '?f]
                                            ['?f :schema/name '?name]
                                            ['?f :schema/age '?age]
                                            ['?f :schema/email '?email]]})
             ["Brian" 50 "brian@example.org"])
          "selectOne should only return a single result, like (first ...)")

      ;; if mixing multi-cardinality results along with single cardinality, there
      ;; should be a result output for every multi-cardinality value and the single
      ;; cardinality values should duplicate
      (is (= @(fluree/query db {:context {:ex "http://example.org/ns/"}
                                :select  ['?name '?favNums]
                                :where   [['?s :schema/name '?name]
                                          ['?s :ex/favNums '?favNums]]})
             [["Cam" 5] ["Cam" 10]
              ["Alice" 9] ["Alice" 42] ["Alice" 76]
              ["Brian" 7]])
          "Multi-cardinality values should duplicate non-multicardinality values ")

      ;; ordering by a single variable
      (is (= @(fluree/query db {:context {:ex "http://example.org/ns/"}
                                :select  ['?name '?favNums]
                                :where   [['?s :schema/name '?name]
                                          ['?s :ex/favNums '?favNums]]
                                :orderBy '?favNums})
             [["Cam" 5] ["Brian" 7] ["Alice" 9] ["Cam" 10] ["Alice" 42] ["Alice" 76]])
          "Ordering of favNums not in ascending order.")

      ;; ordering by a single variable descending
      (is (= @(fluree/query db {:context {:ex "http://example.org/ns/"}
                                :select  ['?name '?favNums]
                                :where   [['?s :schema/name '?name]
                                          ['?s :ex/favNums '?favNums]]
                                :orderBy '(desc ?favNums)})
             [["Alice" 76] ["Alice" 42] ["Cam" 10] ["Alice" 9] ["Brian" 7] ["Cam" 5]])
          "Ordering of favNums not in descending order.")

      ;; ordering by multiple variables
      (is (= @(fluree/query db {:context {:ex "http://example.org/ns/"}
                                :select  ['?name '?favNums]
                                :where   [['?s :schema/name '?name]
                                          ['?s :ex/favNums '?favNums]]
                                :orderBy ['?name '(desc ?favNums)]})
             [["Alice" 76] ["Alice" 42] ["Alice" 9] ["Brian" 7] ["Cam" 10] ["Cam" 5]])
          "Ordering of multiple variables not working.")
      )))
