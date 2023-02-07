(ns fluree.db.query.fql-parse-test
  (:require
    [clojure.test :refer :all]
    [fluree.db.query.exec.where :as where]
    [fluree.db.test-utils :as test-utils]
    [fluree.db.json-ld.api :as fluree]
    [fluree.db.query.fql.parse :as parse]))

(defn de-recordify-select
  "Select statements are parsed into records.
  This fn turns them into raw maps/vectors for ease of testing "
  [select]
  (if (sequential? select)
    (mapv #(into {} %) select)
    (into {} select)))

(deftest test-parse-query
  (let [conn   (test-utils/create-conn)
        ledger @(fluree/create conn "query/parse"
                               {:context {:ex "http://example.org/ns/"}})
        db     @(fluree/stage
                  (fluree/db ledger)
                  [{:id           :ex/brian,
                    :type         :ex/User,
                    :schema/name  "Brian"
                    :schema/email "brian@example.org"
                    :schema/age   50
                    :ex/favNums   7}
                   {:id           :ex/alice,
                    :type         :ex/User,
                    :ex/favColor  "Green"
                    :schema/name  "Alice"
                    :schema/email "alice@example.org"
                    :schema/age   50
                    :ex/favNums   [42, 76, 9]}
                   {:id          :ex/cam,
                    :type        :ex/User,
                    :schema/name "Cam"
                    :ex/email    "cam@example.org"
                    :schema/age  34
                    :ex/favNums  [5, 10]
                    :ex/friend   [:ex/brian :ex/alice]}])]
    (testing "parse-analytical-query"
      (let [ssc {:select {"?s" ["*"]}
                 :where  [["?s" :schema/name "Alice"]]}
            {:keys [select where] :as parsed} (parse/parse-analytical-query* ssc db)
            {::where/keys [patterns]} where]
        (is (= {:var       '?s
                :selection ["*"]
                :depth     0
                :spec      {:depth 0 :wildcard? true}}
               (de-recordify-select select)))
        (is (= [[{::where/var '?s}
                 {::where/val 1002 ::where/datatype 7}
                 {::where/val "Alice" ::where/datatype 1}]]
               patterns)))
      (let [vars-query {:select {"?s" ["*"]}
                        :where  [["?s" :schema/name '?name]]
                        :vars   {'?name "Alice"}}
            {:keys [select where vars] :as parsed} (parse/parse-analytical-query vars-query db)
            {::where/keys [patterns]} where]
        (is (= {'?name
                {::where/var '?name
                 ::where/val "Alice"}}
               vars))
        (is (= {:var       '?s
                :selection ["*"]
                :depth     0
                :spec      {:depth 0 :wildcard? true}}
               (de-recordify-select select)))
        (is (= [[{::where/var '?s}
                 {::where/val      1002
                  ::where/datatype 7}
                 {::where/var '?name}]]
               patterns)))
      (let [query {:context {:ex "http://example.org/ns/"}
                   :select  ['?name '?age '?email]
                   :where   [['?s :schema/name "Cam"]
                             ['?s :ex/friend '?f]
                             ['?f :schema/name '?name]
                             ['?f :schema/age '?age]
                             ['?f :ex/email '?email]]}
            {:keys [select where] :as parsed} (parse/parse-analytical-query query db)
            {::where/keys [patterns]} where]
        (is (= [{:var '?name}
                {:var '?age}
                {:var '?email}]
               (de-recordify-select select)))
        (is (= [[{::where/var '?s}
                 {::where/val      1002
                  ::where/datatype 7}
                 {::where/val      "Cam"
                  ::where/datatype 1}]
                [{::where/var '?s}
                 {::where/val      1008
                  ::where/datatype 7}
                 {::where/var '?f}]
                [{::where/var '?f}
                 {::where/val      1002
                  ::where/datatype 7}
                 {::where/var '?name}]
                [{::where/var '?f}
                 {::where/val      1004
                  ::where/datatype 7}
                 {::where/var '?age}]
                [{::where/var '?f}
                 {::where/val      1007
                  ::where/datatype 7}
                 {::where/var '?email}]]
               patterns)))
      (testing "class, optional"
        (let [optional-q {:select ['?name '?favColor]
                          :where  [['?s :rdf/type :ex/User]
                                   ['?s :schema/name '?name]
                                   {:optional ['?s :ex/favColor '?favColor]}]}
              {:keys [select where] :as parsed} (parse/parse-analytical-query optional-q db)
              {::where/keys [patterns]} where]
          (is (= [{:var '?name} {:var '?favColor}]
                 (mapv #(into {} %) select)))
          (is (= [[:class
                   [{::where/var '?s}
                    {::where/val      200
                     ::where/datatype 7}
                    {::where/val      1001
                     ::where/datatype 0}]]
                  [{::where/var '?s}
                   {::where/val      1002
                    ::where/datatype 7}
                   {::where/var '?name}]
                  [:optional
                   {::where/patterns
                    [[{::where/var '?s}
                      {::where/val      1006
                       ::where/datatype 7}
                      {::where/var '?favColor}]]
                    ::where/filters {}}]]
                 patterns))))
      (testing "class, union"
        (let [union-q {:select ['?s '?email1 '?email2]
                       :where  [['?s :rdf/type :ex/User]
                                {:union [[['?s :ex/email '?email1]]
                                         [['?s :schema/email '?email2]]]}]}
              {:keys [select where] :as parsed} (parse/parse-analytical-query union-q db)
              {::where/keys [patterns]} where]
          (is (= [{:var '?s} {:var '?email1} {:var '?email2}]
                 (de-recordify-select select)))
          (is (= [[:class
                   [{::where/var '?s}
                    {::where/val      200
                     ::where/datatype 7}
                    {::where/val      1001
                     ::where/datatype 0}]]
                  [:union
                   [{::where/patterns
                     [[{::where/var '?s}
                       {::where/val      1007
                        ::where/datatype 7}
                       {::where/var '?email1}]]
                     ::where/filters {}}
                    {::where/patterns
                     [[{::where/var '?s}
                       {::where/val      1003
                        ::where/datatype 7}
                       {::where/var '?email2}]]
                     ::where/filters {}}]]]
                 patterns))))
      (testing "class, filters"
        (let [filter-q {:select ['?name '?age]
                        :where  [['?s :rdf/type :ex/User]
                                 ['?s :schema/age '?age]
                                 ['?s :schema/name '?name]
                                 {:filter ["(> ?age 45)", "(< ?age 50)"]}]}
              {:keys [select where] :as parsed} (parse/parse-analytical-query filter-q db)
              {::where/keys [patterns filters]} where]
          (is (= [{:var '?name} {:var '?age}]
                 (de-recordify-select select)))
          (is (= [[:class
                   [{::where/var '?s}
                    {::where/val      200
                     ::where/datatype 7}
                    {::where/val      1001
                     ::where/datatype 0}]]
                  [{::where/var '?s}
                   {::where/val      1004
                    ::where/datatype 7}
                   {::where/var '?age}]
                  [{::where/var '?s}
                   {::where/val      1002
                    ::where/datatype 7}
                   {::where/var '?name}]]
                 patterns))))
      (testing "group-by, order-by"
        (let [query {:select   ['?name '?favNums]
                     :where    [['?s :schema/name '?name]
                                ['?s :ex/favNums '?favNums]]
                     :group-by '?name
                     :order-by '?name}
              {:keys [select where group-by order-by] :as parsed} (parse/parse-analytical-query query db)]
          (is (= ['?name]
                 group-by))
          (is (= [['?name :asc]]
                 order-by)))))))
