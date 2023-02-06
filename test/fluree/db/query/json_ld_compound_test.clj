(ns fluree.db.query.json-ld-compound-test
  (:require
    [clojure.test :refer :all]
    [fluree.db.test-utils :as test-utils]
    [fluree.db.json-ld.api :as fluree]))

(deftest ^:integration simple-compound-queries
  (testing "Simple compound queries."
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "query/compounda")
          db     @(fluree/stage
                   (fluree/db ledger)
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
                     :schema/age   50
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
          @(fluree/query db '{:context {:ex "http://example.org/ns/"}
                              :select  [?age {?f [:*]}]
                              :where   [[?s :schema/name "Cam"]
                                        [?s :ex/friend ?f]
                                        [?f :schema/age ?age]]})

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
              [50 {:id           :ex/alice,
                   :rdf/type     [:ex/User],
                   :schema/name  "Alice",
                   :schema/email "alice@example.org",
                   :schema/age   50,
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
              ["Alice" 50 "alice@example.org"]])
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

      ;; ordering by multiple variables where some are equal, and not all carried to 'select'
      (is (= @(fluree/query db {:context {:ex "http://example.org/ns/"}
                                :select  ['?name '?favNums]
                                :where   [['?s :schema/name '?name]
                                          ['?s :schema/age '?age]
                                          ['?s :ex/favNums '?favNums]]
                                :orderBy ['?age '?name '(desc ?favNums)]})
             [["Cam" 10] ["Cam" 5] ["Alice" 76] ["Alice" 42] ["Alice" 9] ["Brian" 7]])
          "Ordering of multiple variables where some are equal working.")

      ;; group-by with a multicardinality value, but not using any aggregate function
      (is (= @(fluree/query db {:context  {:ex "http://example.org/ns/"}
                                :select   ['?name '?favNums]
                                :where    [['?s :schema/name '?name]
                                           ['?s :ex/favNums '?favNums]]
                                :group-by '?name
                                :order-by '?name})
             [["Alice" [9 42 76]] ["Brian" [7]] ["Cam" [5 10]]])
          "Sums of favNums by person are not accurate.")



      ;; checking s, p, o values all pulled correctly and all IRIs are resolved from sid integer & compacted
      (is (= @(fluree/query db
                            {:context  {:ex "http://example.org/ns/"}
                             :select  ['?s '?p '?o]
                             :where   [['?s :schema/age 34]
                                       ['?s '?p '?o]]})
             [[:ex/cam :id "http://example.org/ns/cam"]
              [:ex/cam :rdf/type :ex/User]
              [:ex/cam :schema/name "Cam"]
              [:ex/cam :schema/email "cam@example.org"]
              [:ex/cam :schema/age 34]
              [:ex/cam :ex/favNums 5]
              [:ex/cam :ex/favNums 10]
              [:ex/cam :ex/friend :ex/brian]
              [:ex/cam :ex/friend :ex/alice]])
          "IRIs are resolved from subj ids, whether s, p, or o vals.")

      ;; checking object-subject joins
      (is (= @(fluree/query db
                            '{:context {:ex "http://example.org/ns/"}
                              :select  {?s ["*" {:ex/friend ["*"]}]}
                              :where   [[?s :ex/friend ?o]
                                        [?o :schema/name "Alice"]]})
             [{:id :ex/cam,
               :rdf/type [:ex/User],
               :schema/name "Cam",
               :schema/email "cam@example.org",
               :schema/age 34,
               :ex/favNums [5 10],
               :ex/friend
               [{:id :ex/brian,
                 :rdf/type [:ex/User],
                 :schema/name "Brian",
                 :schema/email "brian@example.org",
                 :schema/age 50,
                 :ex/favNums 7}
                {:id :ex/alice,
                 :rdf/type [:ex/User],
                 :schema/name "Alice",
                 :schema/email "alice@example.org",
                 :schema/age 50,
                 :ex/favNums [9 42 76]}]}])
          "Subjects appearing as objects should be referenceable."))))
