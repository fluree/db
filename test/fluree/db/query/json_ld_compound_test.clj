(ns fluree.db.query.json-ld-compound-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(deftest ^:integration simple-compound-queries
  (testing "Simple compound queries."
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "query/compounda")
          db     @(fluree/stage
                   (fluree/db ledger)
                   {"@context" ["https://ns.flur.ee"
                                test-utils/default-context
                                {:ex "http://example.org/ns/"}]
                    "insert"
                    [{:id           :ex/brian,
                      :type         :ex/User,
                      :schema/name  "Brian"
                      :schema/email "brian@example.org"
                      :schema/age   50
                      :ex/favNums   7}
                     {:id           :ex/alice,
                      :type         :ex/User,
                      :schema/name  "Alice"
                      :schema/email "alice@example.org"
                      :schema/age   50
                      :ex/favNums   [42, 76, 9]}
                     {:id           :ex/cam,
                      :type         :ex/User,
                      :schema/name  "Cam"
                      :schema/email "cam@example.org"
                      :schema/age   34
                      :ex/favNums   [5, 10]
                      :ex/friend    [:ex/brian :ex/alice]}]})

          two-tuple-select-with-crawl
          @(fluree/query db {:context [test-utils/default-context
                                       {:ex "http://example.org/ns/"}]
                             :select '[?age {?f [:*]}]
                             :where  '{:schema/name "Cam"
                                       :ex/friend   {:id         ?f
                                                     :schema/age ?age}}})

          two-tuple-select-with-crawl+var
          @(fluree/query db {:context [test-utils/default-context
                                       {:ex "http://example.org/ns/"}]
                             :select  '[?age {?f [:*]}]
                             :where   '{:schema/name ?name
                                        :ex/friend   {:id         ?f
                                                      :schema/age ?age}}
                             :values  '[?name ["Cam"]]})]

      (is (= [[50 {:id           :ex/alice,
                   :type     :ex/User,
                   :schema/name  "Alice",
                   :schema/email "alice@example.org",
                   :schema/age   50,
                   :ex/favNums   [9, 42, 76]}]
              [50 {:id           :ex/brian,
                   :type     :ex/User,
                   :schema/name  "Brian",
                   :schema/email "brian@example.org",
                   :schema/age   50,
                   :ex/favNums   7}]]
             two-tuple-select-with-crawl
             two-tuple-select-with-crawl+var))

      ;; here we have pass-through variables (?name and ?age) which must get "passed through"
      ;; the last where statements into the select statement
      (is (= [["Alice" 50 "alice@example.org"]
              ["Brian" 50 "brian@example.org"]]
             @(fluree/query db '{:context {:schema "http://schema.org/"
                                           :ex "http://example.org/ns/"}
                                 :select  [?name ?age ?email]
                                 :where   {:schema/name "Cam"
                                           :ex/friend   {:schema/name  ?name
                                                         :schema/age   ?age
                                                         :schema/email ?email}}}))
          "Prior where statement variables may not be passing through to select results")

      ;; same as prior query, but using selectOne
      (is (= ["Alice" 50 "alice@example.org"]
             @(fluree/query db '{:context   {:schema "http://schema.org/"
                                             :ex     "http://example.org/ns/"}
                                 :selectOne [?name ?age ?email]
                                 :where     {:schema/name "Cam"
                                             :ex/friend   {:schema/name  ?name
                                                           :schema/age   ?age
                                                           :schema/email ?email}}}))
          "selectOne should only return a single result, like (first ...)")

      ;; if mixing multi-cardinality results along with single cardinality, there
      ;; should be a result output for every multi-cardinality value and the single
      ;; cardinality values should duplicate
      (is (= [["Alice" 9] ["Alice" 42] ["Alice" 76]
              ["Brian" 7]
              ["Cam" 5] ["Cam" 10]]
             @(fluree/query db '{:context {:schema "http://schema.org/"
                                           :ex     "http://example.org/ns/"}
                                 :select  [?name ?favNums]
                                 :where   {:schema/name ?name
                                           :ex/favNums  ?favNums}}))
          "Multi-cardinality values should duplicate non-multicardinality values ")

      ;; ordering by a single variable
      (is (= @(fluree/query db '{:context {:schema "http://schema.org/"
                                           :ex     "http://example.org/ns/"}
                                 :select  [?name ?favNums]
                                 :where   {:schema/name ?name
                                           :ex/favNums  ?favNums}
                                 :orderBy ?favNums})
             [["Cam" 5] ["Brian" 7] ["Alice" 9] ["Cam" 10] ["Alice" 42] ["Alice" 76]])
          "Ordering of favNums not in ascending order.")

      ;; ordering by a single variable descending
      (is (= @(fluree/query db '{:context {:schema "http://schema.org/"
                                           :ex     "http://example.org/ns/"}
                                 :select  [?name ?favNums]
                                 :where   {:schema/name ?name
                                           :ex/favNums  ?favNums}
                                 :orderBy (desc ?favNums)})
             [["Alice" 76] ["Alice" 42] ["Cam" 10] ["Alice" 9] ["Brian" 7] ["Cam" 5]])
          "Ordering of favNums not in descending order.")

      ;; ordering by multiple variables
      (is (= @(fluree/query db '{:context {:schema "http://schema.org/"
                                           :ex     "http://example.org/ns/"}
                                 :select  [?name ?favNums]
                                 :where   {:schema/name ?name
                                           :ex/favNums  ?favNums}
                                 :orderBy [?name (desc ?favNums)]})
             [["Alice" 76] ["Alice" 42] ["Alice" 9] ["Brian" 7] ["Cam" 10] ["Cam" 5]])
          "Ordering of multiple variables not working.")

      ;; ordering by multiple variables where some are equal, and not all carried to select
      (is (= @(fluree/query db '{:context {:schema "http://schema.org/"
                                           :ex     "http://example.org/ns/"}
                                 :select  [?name ?favNums]
                                 :where   {:schema/name ?name
                                           :schema/age  ?age
                                           :ex/favNums  ?favNums}
                                 :orderBy [?age ?name (desc ?favNums)]})
             [["Cam" 10] ["Cam" 5] ["Alice" 76] ["Alice" 42] ["Alice" 9] ["Brian" 7]])
          "Ordering of multiple variables where some are equal working.")

      ;; group-by with a multicardinality value, but not using any aggregate function
      (is (= @(fluree/query db '{:context  {:schema "http://schema.org/"
                                            :ex     "http://example.org/ns/"}
                                 :select   [?name ?favNums]
                                 :where    {:schema/name ?name
                                            :ex/favNums  ?favNums}
                                 :group-by ?name
                                 :order-by ?name})
             [["Alice" [9 42 76]] ["Brian" [7]] ["Cam" [5 10]]])
          "Sums of favNums by person are not accurate.")

;; checking s, p, o values all pulled correctly and all IRIs are resolved from sid integer & compacted
      (is (= [[:ex/cam :type :ex/User]
              [:ex/cam :schema/age 34]
              [:ex/cam :schema/email "cam@example.org"]
              [:ex/cam :schema/name "Cam"]
              [:ex/cam :ex/favNums 5]
              [:ex/cam :ex/favNums 10]
              [:ex/cam :ex/friend :ex/alice]
              [:ex/cam :ex/friend :ex/brian]]
             @(fluree/query db {:context [test-utils/default-context
                                          {:ex "http://example.org/ns/"}]
                                :select '[?s ?p ?o]
                                :where  '{:id         ?s
                                          :schema/age 34
                                          ?p          ?o}}))
          "IRIs are resolved from subj ids, whether s, p, or o vals.")

      ;; checking object-subject joins
      (is (= [{:id :ex/cam,
               :type :ex/User,
               :schema/name "Cam",
               :schema/email "cam@example.org",
               :schema/age 34,
               :ex/favNums [5 10],
               :ex/friend
               [{:id :ex/alice,
                 :type :ex/User,
                 :schema/name "Alice",
                 :schema/email "alice@example.org",
                 :schema/age 50,
                 :ex/favNums [9 42 76]}
                {:id :ex/brian,
                 :type :ex/User,
                 :schema/name "Brian",
                 :schema/email "brian@example.org",
                 :schema/age 50,
                 :ex/favNums 7}]}]
             @(fluree/query db {:context [test-utils/default-context
                                          {:ex "http://example.org/ns/"}]
                                :select '{?s ["*" {:ex/friend ["*"]}]}
                                :where  '{:id        ?s
                                          :ex/friend {:schema/name "Alice"}}}))
          "Subjects appearing as objects should be referenceable."))))
