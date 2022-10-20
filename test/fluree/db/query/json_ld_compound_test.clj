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
                      :schema/age   50}
                     {:context      {:ex "http://example.org/ns/"}
                      :id           :ex/alice,
                      :type         :ex/User,
                      :schema/name  "Alice"
                      :schema/email "alice@example.org"
                      :schema/age   42}
                     {:context      {:ex "http://example.org/ns/"}
                      :id           :ex/cam,
                      :type         :ex/User,
                      :schema/name  "Cam"
                      :schema/email "cam@example.org"
                      :schema/age   34
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
                                    :vars    {'?name "Cam"}})


          ]
      (is (= two-tuple-select-with-crawl
             two-tuple-select-with-crawl+var
             [[50 {:id :ex/brian, :rdf/type [:ex/User], :schema/name "Brian", :schema/email "brian@example.org", :schema/age 50}]
              [42 {:id :ex/alice, :rdf/type [:ex/User], :schema/name "Alice", :schema/email "alice@example.org", :schema/age 42}]]))

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
          "Prior where statement variables may not be passing through to select results"))))
