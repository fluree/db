(ns fluree.db.shacl.shacl-basic-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.core :as util]))

(use-fixtures :each test-utils/deterministic-blank-node-fixture)

(deftest ^:integration using-pre-defined-types-as-classes
  (testing "Class not used as class initially can still be used as one."
    (let [conn      (test-utils/create-conn)
          ledger    @(fluree/create conn "class/testing")
          context   [test-utils/default-context {:ex "http://example.org/ns/"}]
          db1       @(fluree/stage
                      (fluree/db ledger)
                      {"@context" ["https://ns.flur.ee" context]
                       "insert"   {:id                 :ex/MyClass
                                   :schema/description "Just a basic object not used as a class"}})
          db2       @(fluree/stage
                      db1
                      {:context ["https://ns.flur.ee" context]
                       "insert" {:id                 :ex/myClassInstance
                                 :type               :ex/MyClass
                                 :schema/description "Now a new subject uses MyClass as a Class"}})
          query-res @(fluree/query db2 {:context context
                                        :select  {:ex/myClassInstance [:*]}})]
      (is (= query-res
             [{:id                 :ex/myClassInstance
               :type               :ex/MyClass
               :schema/description "Now a new subject uses MyClass as a Class"}])))))

(deftest ^:integration shacl-cardinality-constraints
  (testing "shacl minimum and maximum cardinality"
    (let [conn         (test-utils/create-conn)
          ledger       @(fluree/create conn "shacl/a")
          context      [test-utils/default-context {:ex "http://example.org/ns/"}]
          user-query   {:context context
                        :select  {'?s [:*]}
                        :where   {:id '?s, :type :ex/User}}
          db           @(fluree/stage
                         (fluree/db ledger)
                         {"@context" ["https://ns.flur.ee" context]
                          "insert"
                          {:id             :ex/UserShape
                           :type           [:sh/NodeShape]
                           :sh/targetClass :ex/User
                           :sh/property    [{:sh/path     :schema/name
                                             :sh/minCount 1
                                             :sh/maxCount 1
                                             :sh/datatype :xsd/string}]}})
          db-ok        @(fluree/stage
                         db
                         {"@context" ["https://ns.flur.ee" context]
                          "insert"
                          {:id              :ex/john
                           :type            :ex/User
                           :schema/name     "John"
                           :schema/callSign "j-rock"}})
          ; no :schema/name
          db-no-names  @(fluree/stage
                          db
                          {"@context" ["https://ns.flur.ee" context]
                           "insert"
                           {:id              :ex/john
                            :type            :ex/User
                            :schema/callSign "j-rock"}})
          db-two-names @(fluree/stage
                          db
                          {"@context" ["https://ns.flur.ee" context]
                           "insert"
                           {:id              :ex/john
                            :type            :ex/User
                            :schema/name     ["John", "Johnny"]
                            :schema/callSign "j-rock"}})]
      (is (= {:status 400,
              :error :shacl/violation
              :report
              [{:subject :ex/john
                :path [:schema/name]
                :value 0
                :expect 1
                :constraint :sh/minCount
                :message "count 0 is less than minimum count of 1"
                :shape "_:fdb-2"}]}
             (ex-data db-no-names)))
      (is (= "Subject :ex/john path [:schema/name] violates constraint :sh/minCount of shape _:fdb-2 - count 0 is less than minimum count of 1."
             (ex-message db-no-names)))
      (is (= {:status 400,
              :error :shacl/violation
              :report
              [{:subject :ex/john
                :path [:schema/name]
                :value 2
                :expect 1
                :message "count 2 is greater than maximum count of 1"
                :constraint :sh/maxCount
                :shape "_:fdb-2"}]}
            (ex-data db-two-names)))
      (is (= "Subject :ex/john path [:schema/name] violates constraint :sh/maxCount of shape _:fdb-2 - count 2 is greater than maximum count of 1."
             (ex-message db-two-names)))
      (is (= [{:id              :ex/john,
               :type            :ex/User,
               :schema/name     "John",
               :schema/callSign "j-rock"}]
             @(fluree/query db-ok user-query))
          "basic rdf:type query response not correct"))))

(deftest ^:integration shacl-datatype-constraints
  (testing "shacl datatype errors"
    (let [conn         (test-utils/create-conn)
          ledger       @(fluree/create conn "shacl/b")
          context      [test-utils/default-context {:ex "http://example.org/ns/"}]
          user-query   {:context context
                        :select  {'?s [:*]}
                        :where   {:id '?s, :type :ex/User}}
          db           @(fluree/stage
                         (fluree/db ledger)
                         {"@context" ["https://ns.flur.ee" context]
                          "insert"
                          {:id             :ex/UserShape
                           :type           :sh/NodeShape
                           :sh/targetClass :ex/User
                           :sh/property    [{:sh/path     :schema/name
                                             :sh/datatype :xsd/string}]}})
          db-ok        @(fluree/stage
                         db
                         {"@context" ["https://ns.flur.ee" context]
                          "insert"
                          {:id          :ex/john
                           :type        :ex/User
                           :schema/name "John"}})
          ;; need to specify type inline in order to avoid coercion
          db-int-name  @(fluree/stage
                         db
                         {"@context" ["https://ns.flur.ee" context]
                          "insert"
                          {:id          :ex/john
                           :type        :ex/User
                           :schema/name {:type :xsd/integer :value 42}}})
          db-bool-name @(fluree/stage
                         db
                         {"@context" ["https://ns.flur.ee" context]
                          "insert"
                          {:id          :ex/john
                           :type        :ex/User
                           :schema/name {:type :xsd/boolean :value true}}})]
      (is (= {:status 400,
           :error :shacl/violation,
           :report
           [{:subject :ex/john,
             :constraint :sh/datatype,
             :shape "_:fdb-2",
             :expect :xsd/string,
             :path [:schema/name],
             :value [:xsd/integer],
             :message "the following values do not have expected datatype :xsd/string: 42"}]}
             (ex-data db-int-name)))
      (is (= "Subject :ex/john path [:schema/name] violates constraint :sh/datatype of shape _:fdb-2 - the following values do not have expected datatype :xsd/string: 42."
             (ex-message db-int-name)))
      (is (= {:status 400,
              :error :shacl/violation,
              :report
              [{:subject :ex/john,
                :constraint :sh/datatype,
                :shape "_:fdb-2",
                :expect :xsd/string,
                :path [:schema/name],
                :value [:xsd/boolean],
                :message "the following values do not have expected datatype :xsd/string: true"}]}
             (ex-data db-bool-name))
          "Exception, because :schema/name is a boolean and not a string.")
      (is (= "Subject :ex/john path [:schema/name] violates constraint :sh/datatype of shape _:fdb-2 - the following values do not have expected datatype :xsd/string: true."
             (ex-message db-bool-name)))
      (is (= @(fluree/query db-ok user-query)
             [{:id          :ex/john
               :type        :ex/User
               :schema/name "John"}])
          "basic rdf:type query response not correct"))))

(deftest ^:integration shacl-closed-shape
  (testing "shacl closed shape"
    (let [conn       (test-utils/create-conn)
          ledger     @(fluree/create conn "shacl/c")
          context    [test-utils/default-context {:ex "http://example.org/ns/"}]
          user-query {:context context
                      :select  {'?s [:*]}
                      :where   {:id '?s, :type :ex/User}}
          db         @(fluree/stage
                       (fluree/db ledger)
                       {"@context" ["https://ns.flur.ee" context]
                        "insert"
                        {:id                   :ex/UserShape
                         :type                 :sh/NodeShape
                         :sh/targetClass       :ex/User
                         :sh/property          [{:sh/path     :schema/name
                                                 :sh/datatype :xsd/string}]
                         :sh/closed            true
                         :sh/ignoredProperties [:type]}})

          db-ok         @(fluree/stage
                          db
                          {"@context" ["https://ns.flur.ee" context]
                           "insert"
                           {:id          :ex/john
                            :type        :ex/User
                            :schema/name "John"}})
          ; no :schema/name
          db-extra-prop @(fluree/stage
                           db
                           {"@context" ["https://ns.flur.ee" context]
                            "insert"
                            {:id           :ex/john
                             :type         :ex/User
                             :schema/name  "John"
                             :schema/email "john@flur.ee"}})]
      (is (= {:status 400,
              :error :shacl/violation,
              :report
              [{:subject :ex/john,
                :constraint :sh/closed,
                :shape :ex/UserShape,
                :value ["john@flur.ee"],
                :expect [:type :schema/name],
                :message "disallowed path :schema/email with values john@flur.ee"}]}
             (ex-data db-extra-prop)))
      (is (= "Subject :ex/john violates constraint :sh/closed of shape :ex/UserShape - disallowed path :schema/email with values john@flur.ee."
             (ex-message db-extra-prop)))

      (is (= [{:id          :ex/john
               :type        :ex/User
               :schema/name "John"}]
             @(fluree/query db-ok user-query))
          "basic type query response not correct"))))

(deftest ^:integration shacl-property-pairs
  (testing "shacl property pairs"
    (let [conn       (test-utils/create-conn)
          ledger     @(fluree/create conn "shacl/pairs")
          context    [test-utils/default-context {:ex "http://example.org/ns/"}]
          user-query {:context context
                      :select  {'?s [:*]}
                      :where   {:id '?s, :type :ex/User}}]
      (testing "single-cardinality equals"
        (let [db    @(fluree/stage
                       (fluree/db ledger)
                       {"@context" ["https://ns.flur.ee" context]
                        "insert"
                        {:id             :ex/EqualNamesShape
                         :type           :sh/NodeShape
                         :sh/targetClass :ex/User
                         :sh/property    [{:sh/path   :schema/name
                                           :sh/equals :ex/firstName}]}})


              db-not-equal @(fluree/stage
                              db
                              {"@context" ["https://ns.flur.ee" context]
                               "insert"
                               {:id           :ex/john
                                :type         :ex/User
                                :schema/name  "John"
                                :ex/firstName "Jack"}})]
          (is (= {:status 400,
                  :error  :shacl/violation,
                  :report
                  [{:subject    :ex/john,
                    :constraint :sh/equals,
                    :shape      "_:fdb-2",
                    :path       [:schema/name],
                    :value      ["John"],
                    :expect     ["Jack"],
                    :message    "path [:schema/name] values John do not equal :ex/firstName values Jack"}]}
                 (ex-data db-not-equal)))
          (is (= "Subject :ex/john path [:schema/name] violates constraint :sh/equals of shape _:fdb-2 - path [:schema/name] values John do not equal :ex/firstName values Jack."
                 (ex-message db-not-equal)))
          (let [db-ok @(fluree/stage
                         db
                         {"@context" ["https://ns.flur.ee" context]
                          "insert"
                          {:id           :ex/alice
                           :type         :ex/User
                           :schema/name  "Alice"
                           :ex/firstName "Alice"}})]
            (is (= [{:id           :ex/alice
                     :type         :ex/User
                     :schema/name  "Alice"
                     :ex/firstName "Alice"}]
                   @(fluree/query db-ok user-query))))))
      (testing "multi-cardinality equals"
        (let [db @(fluree/stage
                    (fluree/db ledger)
                    {"@context" ["https://ns.flur.ee" context]
                     "insert"
                     {:id :ex/EqualNamesShape
                      :type :sh/NodeShape
                      :sh/targetClass :ex/User
                      :sh/property [{:sh/path :ex/favNums
                                     :sh/equals :ex/luckyNums}]}})]
          (let [db-not-equal1 @(fluree/stage
                                 db
                                 {"@context" ["https://ns.flur.ee" context]
                                  "insert"
                                  {:id           :ex/brian
                                   :type         :ex/User
                                   :schema/name  "Brian"
                                   :ex/favNums   [11 17]
                                   :ex/luckyNums [13 18]}})]
            (is (= {:status 400,
                    :error  :shacl/violation,
                    :report
                    [{:subject    :ex/brian,
                      :constraint :sh/equals,
                      :shape      "_:fdb-6",
                      :path       [:ex/favNums],
                      :value      [11 17],
                      :expect     [13 18],
                      :message    "path [:ex/favNums] values 11, 17 do not equal :ex/luckyNums values 13, 18"}]}
                   (ex-data db-not-equal1)))
            (is (= "Subject :ex/brian path [:ex/favNums] violates constraint :sh/equals of shape _:fdb-6 - path [:ex/favNums] values 11, 17 do not equal :ex/luckyNums values 13, 18."
                   (ex-message db-not-equal1))))
          (let [db-not-equal2 @(fluree/stage
                                 db
                                 {"@context" ["https://ns.flur.ee" context]
                                  "insert"
                                  {:id           :ex/brian
                                   :type         :ex/User
                                   :schema/name  "Brian"
                                   :ex/favNums   [11 17]
                                   :ex/luckyNums [11]}})]
            (is (= {:status 400,
                    :error  :shacl/violation,
                    :report
                    [{:subject    :ex/brian,
                      :constraint :sh/equals,
                      :shape      "_:fdb-6",
                      :path       [:ex/favNums],
                      :value      [11 17],
                      :expect     [11],
                      :message "path [:ex/favNums] values 11, 17 do not equal :ex/luckyNums values 11"}]}
                   (ex-data db-not-equal2)))
            (is (= "Subject :ex/brian path [:ex/favNums] violates constraint :sh/equals of shape _:fdb-6 - path [:ex/favNums] values 11, 17 do not equal :ex/luckyNums values 11."
                   (ex-message db-not-equal2))))
          (let [db-not-equal3 @(fluree/stage
                                 db
                                 {"@context" ["https://ns.flur.ee" context]
                                  "insert"
                                  {:id           :ex/brian
                                   :type         :ex/User
                                   :schema/name  "Brian"
                                   :ex/favNums   [11 17]
                                   :ex/luckyNums [11 17 18]}})]
            (is (= {:status 400,
                    :error  :shacl/violation,
                    :report
                    [{:subject    :ex/brian,
                      :constraint :sh/equals,
                      :shape      "_:fdb-6",
                      :path       [:ex/favNums],
                      :value      [11 17],
                      :expect     [11 17 18],
                      :message    "path [:ex/favNums] values 11, 17 do not equal :ex/luckyNums values 11, 17, 18"}]}
                   (ex-data db-not-equal3)))
            (is (= "Subject :ex/brian path [:ex/favNums] violates constraint :sh/equals of shape _:fdb-6 - path [:ex/favNums] values 11, 17 do not equal :ex/luckyNums values 11, 17, 18."
                   (ex-message db-not-equal3))))
          (let [db-not-equal4 @(fluree/stage
                                 db
                                 {"@context" ["https://ns.flur.ee" context]
                                  "insert"
                                  {:id           :ex/brian
                                   :type         :ex/User
                                   :schema/name  "Brian"
                                   :ex/favNums   [11 17]
                                   :ex/luckyNums ["11" "17"]}})]
            (is (= {:status 400,
                    :error  :shacl/violation,
                    :report
                    [{:subject    :ex/brian,
                      :constraint :sh/equals,
                      :shape      "_:fdb-6",
                      :path       [:ex/favNums],
                      :value      [11 17],
                      :expect     ["11" "17"],
                      :message    "path [:ex/favNums] values 11, 17 do not equal :ex/luckyNums values 11, 17"}]}
                   (ex-data db-not-equal4)))
            (is (= "Subject :ex/brian path [:ex/favNums] violates constraint :sh/equals of shape _:fdb-6 - path [:ex/favNums] values 11, 17 do not equal :ex/luckyNums values 11, 17."
                   (ex-message db-not-equal4))))
          (let [db-ok @(fluree/stage
                         db
                         {"@context" ["https://ns.flur.ee" context]
                          "insert"
                          {:id           :ex/alice
                           :type         :ex/User
                           :schema/name  "Alice"
                           :ex/favNums   [11 17]
                           :ex/luckyNums [11 17]}})]
            (is (= [{:id           :ex/alice
                     :type         :ex/User
                     :schema/name  "Alice"
                     :ex/favNums   [11 17]
                     :ex/luckyNums [11 17]}]
                   @(fluree/query db-ok user-query))))
          (let [db-ok2 @(fluree/stage
                          db
                          {"@context" ["https://ns.flur.ee" context]
                           "insert"
                           {:id           :ex/alice
                            :type         :ex/User
                            :schema/name  "Alice"
                            :ex/favNums   [11 17]
                            :ex/luckyNums [17 11]}})]
            (is (= [{:id           :ex/alice
                     :type         :ex/User
                     :schema/name  "Alice"
                     :ex/favNums   [11 17]
                     :ex/luckyNums [11 17]}]
                   @(fluree/query db-ok2 user-query))))))
      (testing "disjoint"
        (let [db    @(fluree/stage
                       (fluree/db ledger)
                       {"@context" ["https://ns.flur.ee" context]
                        "insert"
                        {:id             :ex/DisjointShape
                         :type           :sh/NodeShape
                         :sh/targetClass :ex/User
                         :sh/property    [{:sh/path     :ex/favNums
                                           :sh/disjoint :ex/luckyNums}]}})
              db-ok @(fluree/stage
                       db
                       {"@context" ["https://ns.flur.ee" context]
                        "insert"
                        {:id           :ex/alice
                         :type         :ex/User
                         :schema/name  "Alice"
                         :ex/favNums   [11 17]
                         :ex/luckyNums 1}})

              db-not-disjoint1 @(fluree/stage
                                  db
                                  {"@context" ["https://ns.flur.ee" context]
                                   "insert"
                                   {:id           :ex/brian
                                    :type         :ex/User
                                    :schema/name  "Brian"
                                    :ex/favNums   11
                                    :ex/luckyNums 11}})
              db-not-disjoint2 @(fluree/stage
                                  db
                                  {"@context" ["https://ns.flur.ee" context]
                                   "insert"
                                   {:id           :ex/brian
                                    :type         :ex/User
                                    :schema/name  "Brian"
                                    :ex/favNums   [11 17 31]
                                    :ex/luckyNums 11}})

              db-not-disjoint3 @(fluree/stage
                                  db
                                  {"@context" ["https://ns.flur.ee" context]
                                   "insert"
                                   {:id           :ex/brian
                                    :type         :ex/User
                                    :schema/name  "Brian"
                                    :ex/favNums   [11 17 31]
                                    :ex/luckyNums [13 18 11]}})]
          (is (= {:status 400,
                  :error  :shacl/violation,
                  :report
                  [{:subject    :ex/brian,
                    :constraint :sh/disjoint,
                    :shape      "_:fdb-14",
                    :path       [:ex/favNums],
                    :value      [11],
                    :expect     [11],
                    :message    "path [:ex/favNums] values 11 are not disjoint with :ex/luckyNums values 11"}]}
                 (ex-data db-not-disjoint1)))
          (is (= "Subject :ex/brian path [:ex/favNums] violates constraint :sh/disjoint of shape _:fdb-14 - path [:ex/favNums] values 11 are not disjoint with :ex/luckyNums values 11."
                 (ex-message db-not-disjoint1)))

          (is (= {:status 400,
                  :error  :shacl/violation,
                  :report
                  [{:subject    :ex/brian,
                    :constraint :sh/disjoint,
                    :shape      "_:fdb-14",
                    :path       [:ex/favNums],
                    :value      [11 17 31],
                    :expect     [11],
                    :message    "path [:ex/favNums] values 11, 17, 31 are not disjoint with :ex/luckyNums values 11"}]}
                 (ex-data db-not-disjoint2))
              "Exception, because :ex/favNums is not disjoint from :ex/luckyNums")
          (is (= "Subject :ex/brian path [:ex/favNums] violates constraint :sh/disjoint of shape _:fdb-14 - path [:ex/favNums] values 11, 17, 31 are not disjoint with :ex/luckyNums values 11."
                 (ex-message db-not-disjoint2)))

          (is (= {:status 400,
                  :error  :shacl/violation,
                  :report
                  [{:subject    :ex/brian,
                    :constraint :sh/disjoint,
                    :shape      "_:fdb-14",
                    :path       [:ex/favNums],
                    :value      [11 17 31],
                    :expect     [11 13 18],
                    :message    "path [:ex/favNums] values 11, 17, 31 are not disjoint with :ex/luckyNums values 11, 13, 18"}]}
                 (ex-data db-not-disjoint3))
              "Exception, because :ex/favNums is not disjoint from :ex/luckyNums")
          (is (= "Subject :ex/brian path [:ex/favNums] violates constraint :sh/disjoint of shape _:fdb-14 - path [:ex/favNums] values 11, 17, 31 are not disjoint with :ex/luckyNums values 11, 13, 18."
                 (ex-message db-not-disjoint3)))

          (is (= [{:id           :ex/alice
                   :type         :ex/User
                   :schema/name  "Alice"
                   :ex/favNums   [11 17]
                   :ex/luckyNums 1}]
                 @(fluree/query db-ok user-query)))))
      (testing "lessThan"
        (let [db     @(fluree/stage
                        (fluree/db ledger)
                        {"@context" ["https://ns.flur.ee" context]
                         "insert"
                         {:id             :ex/LessThanShape
                          :type           :sh/NodeShape
                          :sh/targetClass :ex/User
                          :sh/property    [{:sh/path     :ex/p1
                                            :sh/lessThan :ex/p2}]}})
              db-ok1 @(fluree/stage
                        db
                        {"@context" ["https://ns.flur.ee" context]
                         "insert"
                         {:id          :ex/alice
                          :type        :ex/User
                          :schema/name "Alice"
                          :ex/p1       [11 17]
                          :ex/p2       [18 19]}})

              db-ok2 @(fluree/stage
                        db
                        {"@context" ["https://ns.flur.ee" context]
                         "insert"
                         {:id          :ex/alice
                          :type        :ex/User
                          :schema/name "Alice"
                          :ex/p1       [11 17]
                          :ex/p2       [18]}})]
          (is (= [{:id          :ex/alice
                   :type        :ex/User
                   :schema/name "Alice"
                   :ex/p1       [11 17]
                   :ex/p2       [18 19]}]
                 @(fluree/query db-ok1 user-query)))
          (is (= [{:id          :ex/alice
                   :type        :ex/User
                   :schema/name "Alice"
                   :ex/p1       [11 17]
                   :ex/p2       18}]
                 @(fluree/query db-ok2 user-query)))

          (let [db-fail1 @(fluree/stage
                            db
                            {"@context" ["https://ns.flur.ee" context]
                             "insert"
                             {:id          :ex/alice
                              :type        :ex/User
                              :schema/name "Alice"
                              :ex/p1       [11 17]
                              :ex/p2       17}})]
            (is (= {:status 400,
                    :error  :shacl/violation,
                    :report
                    [{:subject    :ex/alice,
                      :constraint :sh/lessThan,
                      :shape      "_:fdb-20",
                      :path       [:ex/p1],
                      :value      [11 17],
                      :expect     [17],
                      :message    "path [:ex/p1] values 11, 17 are not all less than :ex/p2 values 17"}]}
                   (ex-data db-fail1)))
            (is (= "Subject :ex/alice path [:ex/p1] violates constraint :sh/lessThan of shape _:fdb-20 - path [:ex/p1] values 11, 17 are not all less than :ex/p2 values 17."
                   (ex-message db-fail1))))
          (let [db-fail2 @(fluree/stage
                            db
                            {"@context" ["https://ns.flur.ee" context]
                             "insert"
                             {:id          :ex/alice
                              :type        :ex/User
                              :schema/name "Alice"
                              :ex/p1       [11 17]
                              :ex/p2       ["18" "19"]}})]
            (is (= {:status 400,
                    :error  :shacl/violation,
                    :report
                    [{:subject    :ex/alice,
                      :constraint :sh/lessThan,
                      :shape      "_:fdb-20",
                      :path       [:ex/p1],
                      :value      [11 17],
                      :expect     ["18" "19"],
                      :message    "path [:ex/p1] values 11, 17 are not all comparable with :ex/p2 values 18, 19"}]}
                   (ex-data db-fail2)))
            (is (= "Subject :ex/alice path [:ex/p1] violates constraint :sh/lessThan of shape _:fdb-20 - path [:ex/p1] values 11, 17 are not all comparable with :ex/p2 values 18, 19."
                   (ex-message db-fail2))))
          (let [db-fail3 @(fluree/stage
                            db
                            {"@context" ["https://ns.flur.ee" context]
                             "insert"
                             {:id          :ex/alice
                              :type        :ex/User
                              :schema/name "Alice"
                              :ex/p1       [12 17]
                              :ex/p2       [10 18]}})]
            (is (= {:status 400,
                    :error  :shacl/violation,
                    :report
                    [{:subject    :ex/alice,
                      :constraint :sh/lessThan,
                      :shape      "_:fdb-20",
                      :path       [:ex/p1],
                      :value      [12 17],
                      :expect     [10 18],
                      :message    "path [:ex/p1] values 12, 17 are not all less than :ex/p2 values 10, 18"}]}
                   (ex-data db-fail3)))
            (is (= "Subject :ex/alice path [:ex/p1] violates constraint :sh/lessThan of shape _:fdb-20 - path [:ex/p1] values 12, 17 are not all less than :ex/p2 values 10, 18."
                   (ex-message db-fail3))))
          (let [db-fail4 @(fluree/stage
                            db
                            {"@context" ["https://ns.flur.ee" context]
                             "insert"
                             {:id          :ex/alice
                              :type        :ex/User
                              :schema/name "Alice"
                              :ex/p1       [11 17]
                              :ex/p2       [12 16]}})]
            (is (= {:status 400,
                    :error  :shacl/violation,
                    :report
                    [{:subject    :ex/alice,
                      :constraint :sh/lessThan,
                      :shape      "_:fdb-20",
                      :path       [:ex/p1],
                      :value      [11 17],
                      :expect     [12 16],
                      :message    "path [:ex/p1] values 11, 17 are not all less than :ex/p2 values 12, 16"}]}
                   (ex-data db-fail4)))
            (is (= "Subject :ex/alice path [:ex/p1] violates constraint :sh/lessThan of shape _:fdb-20 - path [:ex/p1] values 11, 17 are not all less than :ex/p2 values 12, 16."
                   (ex-message db-fail4))))
          (let [db-iris  @(fluree/stage
                            db
                            {"@context" ["https://ns.flur.ee" context]
                             "insert"
                             {:id          :ex/alice
                              :type        :ex/User
                              :schema/name "Alice"
                              :ex/p1       :ex/brian
                              :ex/p2       :ex/john}})]
            (is (= {:status 400,
                    :error  :shacl/violation,
                    :report
                    [{:subject    :ex/alice,
                      :constraint :sh/lessThan,
                      :shape      "_:fdb-20",
                      :path       [:ex/p1],
                      :value      [:ex/brian],
                      :expect     [:ex/john],
                      :message    "path [:ex/p1] values :ex/brian are not all comparable with :ex/p2 values :ex/john"}]}
                   (ex-data db-iris)))
            (is (= "Subject :ex/alice path [:ex/p1] violates constraint :sh/lessThan of shape _:fdb-20 - path [:ex/p1] values :ex/brian are not all comparable with :ex/p2 values :ex/john."
                   (ex-message db-iris))))))
      (testing "lessThanOrEquals"
        (let [db     @(fluree/stage
                        (fluree/db ledger)
                        {"@context" ["https://ns.flur.ee" context]
                         "insert"
                         {:id             :ex/LessThanOrEqualsShape
                          :type           :sh/NodeShape
                          :sh/targetClass :ex/User
                          :sh/property    [{:sh/path             :ex/p1
                                            :sh/lessThanOrEquals :ex/p2}]}})
              db-ok1 @(fluree/stage
                        db
                        {"@context" ["https://ns.flur.ee" context]
                         "insert"
                         {:id          :ex/alice
                          :type        :ex/User
                          :schema/name "Alice"
                          :ex/p1       [11 17]
                          :ex/p2       [17 19]}})

              db-ok2 @(fluree/stage
                        db
                        {"@context" ["https://ns.flur.ee" context]
                         "insert"
                         {:id          :ex/alice
                          :type        :ex/User
                          :schema/name "Alice"
                          :ex/p1       [11 17]
                          :ex/p2       17}})]
          (is (= [{:id          :ex/alice
                   :type        :ex/User
                   :schema/name "Alice"
                   :ex/p1       [11 17]
                   :ex/p2       [17 19]}]
                 @(fluree/query db-ok1 user-query)))
          (is (= [{:id          :ex/alice
                   :type        :ex/User
                   :schema/name "Alice"
                   :ex/p1       [11 17]
                   :ex/p2       17}]
                 @(fluree/query db-ok2 user-query)))
          (let [db-fail1 @(fluree/stage
                            db
                            {"@context" ["https://ns.flur.ee" context]
                             "insert"
                             {:id          :ex/alice
                              :type        :ex/User
                              :schema/name "Alice"
                              :ex/p1       [11 17]
                              :ex/p2       10}})]
            (is (= {:status 400,
                    :error  :shacl/violation,
                    :report
                    [{:subject    :ex/alice,
                      :constraint :sh/lessThanOrEquals,
                      :shape      "_:fdb-29",
                      :path       [:ex/p1],
                      :value      [11 17],
                      :expect     [10],
                      :message    "path [:ex/p1] values 11, 17 are not all less than :ex/p2 values 10"}]}
                   (ex-data db-fail1)))
            (is (= "Subject :ex/alice path [:ex/p1] violates constraint :sh/lessThanOrEquals of shape _:fdb-29 - path [:ex/p1] values 11, 17 are not all less than :ex/p2 values 10."
                   (ex-message db-fail1))))

          (let [db-fail2 @(fluree/stage
                            db
                            {"@context" ["https://ns.flur.ee" context]
                             "insert"
                             {:id          :ex/alice
                              :type        :ex/User
                              :schema/name "Alice"
                              :ex/p1       [11 17]
                              :ex/p2       ["17" "19"]}})]
            (is (= {:status 400,
                    :error  :shacl/violation,
                    :report
                    [{:subject    :ex/alice,
                      :constraint :sh/lessThanOrEquals,
                      :shape      "_:fdb-29",
                      :path       [:ex/p1],
                      :value      [11 17],
                      :expect     ["17" "19"],
                      :message    "path [:ex/p1] values 11, 17 are not all comparable with :ex/p2 values 17, 19"}]}
                   (ex-data db-fail2)))
            (is (= "Subject :ex/alice path [:ex/p1] violates constraint :sh/lessThanOrEquals of shape _:fdb-29 - path [:ex/p1] values 11, 17 are not all comparable with :ex/p2 values 17, 19."
                   (ex-message db-fail2))))

          (let [db-fail3 @(fluree/stage
                            db
                            {"@context" ["https://ns.flur.ee" context]
                             "insert"
                             {:id          :ex/alice
                              :type        :ex/User
                              :schema/name "Alice"
                              :ex/p1       [12 17]
                              :ex/p2       [10 17]}})]
            (is (= {:status 400,
                    :error  :shacl/violation,
                    :report
                    [{:subject    :ex/alice,
                      :constraint :sh/lessThanOrEquals,
                      :shape      "_:fdb-29",
                      :path       [:ex/p1],
                      :value      [12 17],
                      :expect     [10 17],
                      :message    "path [:ex/p1] values 12, 17 are not all less than :ex/p2 values 10, 17"}]}
                   (ex-data db-fail3)))
            (is (= "Subject :ex/alice path [:ex/p1] violates constraint :sh/lessThanOrEquals of shape _:fdb-29 - path [:ex/p1] values 12, 17 are not all less than :ex/p2 values 10, 17."
                   (ex-message db-fail3))))

          (let [db-fail4 @(fluree/stage
                            db
                            {"@context" ["https://ns.flur.ee" context]
                             "insert"
                             {:id          :ex/alice
                              :type        :ex/User
                              :schema/name "Alice"
                              :ex/p1       [11 17]
                              :ex/p2       [12 16]}})]
            (is (= {:status 400,
                    :error  :shacl/violation,
                    :report
                    [{:subject    :ex/alice,
                      :constraint :sh/lessThanOrEquals,
                      :shape      "_:fdb-29",
                      :path       [:ex/p1],
                      :value      [11 17],
                      :expect     [12 16],
                      :message    "path [:ex/p1] values 11, 17 are not all less than :ex/p2 values 12, 16"}]}
                   (ex-data db-fail4)))
            (is (= "Subject :ex/alice path [:ex/p1] violates constraint :sh/lessThanOrEquals of shape _:fdb-29 - path [:ex/p1] values 11, 17 are not all less than :ex/p2 values 12, 16."
                   (ex-message db-fail4))))
          )))))

(deftest ^:integration shacl-value-range
  (testing "shacl value range constraints"
    (let [conn       (test-utils/create-conn)
          ledger     @(fluree/create conn "shacl/value-range")
          context    [test-utils/default-context {:ex "http://example.org/ns/"}]
          user-query {:context context
                      :select  {'?s [:*]}
                      :where   {:id '?s, :type :ex/User}}]
      (testing "exclusive constraints"
        (let [db          @(fluree/stage
                            (fluree/db ledger)
                            {"@context" ["https://ns.flur.ee" context]
                             "insert"
                             {:id             :ex/ExclusiveNumRangeShape
                              :type           :sh/NodeShape
                              :sh/targetClass :ex/User
                              :sh/property    [{:sh/path         :schema/age
                                                :sh/minExclusive 1
                                                :sh/maxExclusive 100}]}})
              db-ok       @(fluree/stage
                            db
                            {"@context" ["https://ns.flur.ee" context]
                             "insert"
                             {:id         :ex/john
                              :type       :ex/User
                              :schema/age 2}})
              db-too-low  @(fluree/stage
                             db
                             {"@context" ["https://ns.flur.ee" context]
                              "insert"
                              {:id         :ex/john
                               :type       :ex/User
                               :schema/age 1}})
              db-too-high @(fluree/stage
                             db
                             {"@context" ["https://ns.flur.ee" context]
                              "insert"
                              {:id         :ex/john
                               :type       :ex/User
                               :schema/age 100}})]
          (is (= {:status 400,
                  :error :shacl/violation,
                  :report
                  [{:subject :ex/john,
                    :constraint :sh/minExclusive,
                    :shape "_:fdb-2",
                    :path [:schema/age],
                    :expect 1,
                    :value 1,
                    :message "value 1 is less than exclusive minimum 1"}]}
                 (ex-data db-too-low)))
          (is (= "Subject :ex/john path [:schema/age] violates constraint :sh/minExclusive of shape _:fdb-2 - value 1 is less than exclusive minimum 1."
                 (ex-message db-too-low)))

          (is (= {:status 400,
                  :error :shacl/violation,
                  :report
                  [{:subject :ex/john,
                    :constraint :sh/maxExclusive,
                    :shape "_:fdb-2",
                    :path [:schema/age],
                    :expect 100,
                    :value 100,
                    :message "value 100 is greater than exclusive maximum 100"}]}
                 (ex-data db-too-high)))
          (is (= "Subject :ex/john path [:schema/age] violates constraint :sh/maxExclusive of shape _:fdb-2 - value 100 is greater than exclusive maximum 100."
                 (ex-message db-too-high)))

          (is (= [{:id         :ex/john
                   :type       :ex/User
                   :schema/age 2}]
                 @(fluree/query db-ok user-query)))))
      (testing "inclusive constraints"
        (let [db          @(fluree/stage
                             (fluree/db ledger)
                             {"@context" ["https://ns.flur.ee" context]
                              "insert"
                              {:id             :ex/InclusiveNumRangeShape
                               :type           :sh/NodeShape
                               :sh/targetClass :ex/User
                               :sh/property    [{:sh/path         :schema/age
                                                 :sh/minInclusive 1
                                                 :sh/maxInclusive 100}]}})
              db-ok       @(fluree/stage
                             db
                             {"@context" ["https://ns.flur.ee" context]
                              "insert"
                              {:id         :ex/brian
                               :type       :ex/User
                               :schema/age 1}})
              db-ok2      @(fluree/stage
                             db-ok
                             {"@context" ["https://ns.flur.ee" context]
                              "insert"
                              {:id         :ex/alice
                               :type       :ex/User
                               :schema/age 100}})
              db-too-low  @(fluree/stage
                             db
                             {"@context" ["https://ns.flur.ee" context]
                              "insert"
                              {:id         :ex/alice
                               :type       :ex/User
                               :schema/age 0}})
              db-too-high @(fluree/stage
                             db
                             {"@context" ["https://ns.flur.ee" context]
                              "insert"
                              {:id         :ex/alice
                               :type       :ex/User
                               :schema/age 101}})]
          (is (= {:status 400,
                  :error :shacl/violation,
                  :report
                  [{:subject :ex/alice,
                    :constraint :sh/minInclusive,
                    :shape "_:fdb-7",
                    :path [:schema/age],
                    :expect 1,
                    :value 0,
                    :message "value 0 is less than inclusive minimum 1"}]}
                 (ex-data db-too-low)))
          (is (= "Subject :ex/alice path [:schema/age] violates constraint :sh/minInclusive of shape _:fdb-7 - value 0 is less than inclusive minimum 1."
                 (ex-message db-too-low)))

          (is (= {:status 400,
                  :error :shacl/violation,
                  :report
                  [{:subject :ex/alice,
                    :constraint :sh/maxInclusive,
                    :shape "_:fdb-7",
                    :path [:schema/age],
                    :expect 100,
                    :value 101,
                    :message "value 101 is greater than inclusive maximum 100"}]}
                 (ex-data db-too-high)))
          (is (= "Subject :ex/alice path [:schema/age] violates constraint :sh/maxInclusive of shape _:fdb-7 - value 101 is greater than inclusive maximum 100."
                 (ex-message db-too-high)))

          (is (= [{:id         :ex/alice
                   :type       :ex/User
                   :schema/age 100}
                  {:id         :ex/brian
                   :type       :ex/User
                   :schema/age 1}]
                 @(fluree/query db-ok2 user-query)))))
      (testing "non-numeric values"
        (let [db         @(fluree/stage
                           (fluree/db ledger)
                           {"@context" ["https://ns.flur.ee" context]
                            "insert"
                            {:id             :ex/NumRangeShape
                             :type           :sh/NodeShape
                             :sh/targetClass :ex/User
                             :sh/property    [{:sh/path         :schema/age
                                               :sh/minExclusive 0}]}})
              db-subj-id @(fluree/stage
                            db
                            {"@context" ["https://ns.flur.ee" context]
                             "insert"
                             {:id         :ex/alice
                              :type       :ex/User
                              :schema/age :ex/brian}})
              db-string  @(fluree/stage
                            db
                            {"@context" ["https://ns.flur.ee" context]
                             "insert"
                             {:id         :ex/alice
                              :type       :ex/User
                              :schema/age "10"}})]
          (is (= {:status 400,
                  :error :shacl/violation,
                  :report
                  [{:subject :ex/alice,
                    :constraint :sh/minExclusive,
                    :shape "_:fdb-13",
                    :path [:schema/age],
                    :expect 0,
                    :value :ex/brian,
                    :message "value :ex/brian is less than exclusive minimum 0"}]}
                 (ex-data db-subj-id)))
          (is (= "Subject :ex/alice path [:schema/age] violates constraint :sh/minExclusive of shape _:fdb-13 - value :ex/brian is less than exclusive minimum 0."
                 (ex-message db-subj-id)))

          (is (= {:status 400,
                  :error :shacl/violation,
                  :report
                  [{:subject :ex/alice,
                    :constraint :sh/minExclusive,
                    :shape "_:fdb-13",
                    :path [:schema/age],
                    :expect 0,
                    :value "10",
                    :message "value 10 is less than exclusive minimum 0"}]}
                 (ex-data db-string)))
          (is (= "Subject :ex/alice path [:schema/age] violates constraint :sh/minExclusive of shape _:fdb-13 - value 10 is less than exclusive minimum 0."
                 (ex-message db-string))))))))

(deftest ^:integration shacl-string-length-constraints
  (testing "shacl string length constraint errors"
    (let [conn       (test-utils/create-conn)
          ledger     @(fluree/create conn "shacl/str")
          context    [test-utils/default-context {:ex "http://example.org/ns/"}]
          user-query {:context context
                      :select  {'?s [:*]}
                      :where   {:id '?s, :type :ex/User}}
          db         @(fluree/stage
                       (fluree/db ledger)
                       {"@context" ["https://ns.flur.ee" context]
                        "insert"
                        {:id             :ex/UserShape
                         :type           :sh/NodeShape
                         :sh/targetClass :ex/User
                         :sh/property    [{:sh/path      :schema/name
                                           :sh/minLength 4
                                           :sh/maxLength 10}]}})
          db-ok-str  @(fluree/stage
                       db
                       {"@context" ["https://ns.flur.ee" context]
                        "insert"
                        {:id          :ex/john
                         :type        :ex/User
                         :schema/name "John"}})

          db-ok-non-str @(fluree/stage
                          db
                          {"@context" ["https://ns.flur.ee" context]
                           "insert"
                           {:id          :ex/john
                            :type        :ex/User
                            :schema/name 12345}})

          db-too-short-str    @(fluree/stage
                                 db
                                 {"@context" ["https://ns.flur.ee" context]
                                  "insert"
                                  {:id          :ex/al
                                   :type        :ex/User
                                   :schema/name "Al"}})
          db-too-long-str     @(fluree/stage
                                 db
                                 {"@context" ["https://ns.flur.ee" context]
                                  "insert"
                                  {:id          :ex/jean-claude
                                   :type        :ex/User
                                   :schema/name "Jean-Claude"}})
          db-too-long-non-str @(fluree/stage
                                 db
                                 {"@context" ["https://ns.flur.ee" context]
                                  "insert"
                                  {:id          :ex/john
                                   :type        :ex/User
                                   :schema/name 12345678910}})
          db-ref-value        @(fluree/stage
                                 db
                                 {"@context" ["https://ns.flur.ee" context]
                                  "insert"
                                  {:id          :ex/john
                                   :type        :ex/User
                                   :schema/name :ex/ref}})]
      (is (= {:status 400,
              :error :shacl/violation,
              :report
              [{:subject :ex/al,
                :constraint :sh/minLength,
                :shape "_:fdb-2",
                :path [:schema/name],
                :expect 4,
                :value "Al",
                :message "value \"Al\" has string length less than minimum length 4"}]}
             (ex-data db-too-short-str)))
      (is (= "Subject :ex/al path [:schema/name] violates constraint :sh/minLength of shape _:fdb-2 - value \"Al\" has string length less than minimum length 4."
             (ex-message db-too-short-str)))
      (is (= {:status 400,
              :error :shacl/violation,
              :report
              [{:subject :ex/jean-claude,
                :constraint :sh/maxLength,
                :shape "_:fdb-2",
                :path [:schema/name],
                :expect 10,
                :value "Jean-Claude",
                :message "value \"Jean-Claude\" has string length greater than maximum length 10"}]}
             (ex-data db-too-long-str)))
      (is (= "Subject :ex/jean-claude path [:schema/name] violates constraint :sh/maxLength of shape _:fdb-2 - value \"Jean-Claude\" has string length greater than maximum length 10."
             (ex-message db-too-long-str)))
      (is (= {:status 400,
              :error :shacl/violation,
              :report
              [{:subject :ex/john,
                :constraint :sh/maxLength,
                :shape "_:fdb-2",
                :path [:schema/name],
                :expect 10,
                :value 12345678910,
                :message "value \"12345678910\" has string length greater than maximum length 10"}]}
             (ex-data db-too-long-non-str)))
      (is (= "Subject :ex/john path [:schema/name] violates constraint :sh/maxLength of shape _:fdb-2 - value \"12345678910\" has string length greater than maximum length 10."
             (ex-message db-too-long-non-str)))
      (is (= {:status 400,
              :error :shacl/violation,
              :report
              [{:subject :ex/john,
                :constraint :sh/maxLength,
                :shape "_:fdb-2",
                :path [:schema/name],
                :expect 10,
                :value #fluree/SID [101 "ref"],
                :message "value :ex/ref is not a literal value"}]}
             (ex-data db-ref-value)))
      (is (= "Subject :ex/john path [:schema/name] violates constraint :sh/maxLength of shape _:fdb-2 - value :ex/ref is not a literal value."
             (ex-message db-ref-value)))
      (is (= [{:id          :ex/john
               :type        :ex/User
               :schema/name "John"}]
             @(fluree/query db-ok-str user-query)))
      (is (= [{:id          :ex/john
               :type        :ex/User
               :schema/name 12345}]
             @(fluree/query db-ok-non-str user-query))))))

(deftest ^:integration shacl-string-pattern-constraints
  (testing "shacl string regex constraint errors"
    (let [conn           (test-utils/create-conn)
          ledger         @(fluree/create conn "shacl/str")
          context        [test-utils/default-context {:ex "http://example.org/ns/"}]
          user-query     {:context context
                          :select  {'?s [:*]}
                          :where   {:id '?s, :type :ex/User}}
          db             @(fluree/stage
                           (fluree/db ledger)
                           {"@context" ["https://ns.flur.ee" context]
                            "insert"
                            {:id             :ex/UserShape
                             :type           [:sh/NodeShape]
                             :sh/targetClass :ex/User
                             :sh/property    [{:sh/path    :ex/greeting
                                               :sh/pattern "hello   (.*?)world"
                                               :sh/flags   ["x" "s"]}
                                              {:sh/path    :ex/birthYear
                                               :sh/pattern "(19|20)[0-9][0-9]"}]}})
          db-ok-greeting @(fluree/stage
                           db
                           {"@context" ["https://ns.flur.ee" context]
                            "insert"
                            {:id          :ex/brian
                             :type        :ex/User
                             :ex/greeting "hello\nworld!"}})

          db-ok-birthyear        @(fluree/stage
                                   db
                                   {"@context" ["https://ns.flur.ee" context]
                                    "insert"
                                    {:id           :ex/john
                                     :type         :ex/User
                                     :ex/birthYear 1984}})
          db-wrong-case-greeting @(fluree/stage
                                    db
                                    {"@context" ["https://ns.flur.ee" context]
                                     "insert"
                                     {:id          :ex/alice
                                      :type        :ex/User
                                      :ex/greeting "HELLO\nWORLD!"}})
          db-wrong-birth-year    @(fluree/stage
                                    db
                                    {"@context" ["https://ns.flur.ee" context]
                                     "insert"
                                     {:id           :ex/alice
                                      :type         :ex/User
                                      :ex/birthYear 1776}})
          db-ref-value           @(fluree/stage
                                    db
                                    {"@context" ["https://ns.flur.ee" context]
                                     "insert"
                                     {:id           :ex/john
                                      :type         :ex/User
                                      :ex/birthYear :ex/ref}})]
      (is (= {:status 400,
              :error :shacl/violation,
              :report
              [{:subject :ex/alice,
                :constraint :sh/pattern,
                :shape "_:fdb-2",
                :path [:ex/greeting],
                :expect "hello   (.*?)world",
                :value "HELLO
WORLD!",
                :message (str "value "
                              (pr-str "HELLO
WORLD!")
                              " does not match pattern \"hello   (.*?)world\" with :sh/flags s, x")}]}
             (ex-data db-wrong-case-greeting)))
      (is (= (str "Subject :ex/alice path [:ex/greeting] violates constraint :sh/pattern of shape _:fdb-2 - value "
                  (pr-str "HELLO
WORLD!")
                  " does not match pattern \"hello   (.*?)world\" with :sh/flags s, x.")
             (ex-message db-wrong-case-greeting)))

      (is (= {:status 400,
              :error :shacl/violation,
              :report
              [{:subject :ex/alice,
                :constraint :sh/pattern,
                :shape "_:fdb-3",
                :path [:ex/birthYear],
                :expect "(19|20)[0-9][0-9]",
                :value 1776,
                :message "value \"1776\" does not match pattern \"(19|20)[0-9][0-9]\""}]}
             (ex-data db-wrong-birth-year)))
      (is (= "Subject :ex/alice path [:ex/birthYear] violates constraint :sh/pattern of shape _:fdb-3 - value \"1776\" does not match pattern \"(19|20)[0-9][0-9]\"."
             (ex-message db-wrong-birth-year)))
      (is (= {:status 400,
              :error :shacl/violation,
              :report
              [{:subject :ex/john,
                :constraint :sh/pattern,
                :shape "_:fdb-3",
                :expect "(19|20)[0-9][0-9]",
                :path [:ex/birthYear],
                :value #fluree/SID [101 "ref"],
                :message "value \":ex/ref\" does not match pattern \"(19|20)[0-9][0-9]\""}]}
             (ex-data db-ref-value)))
      (is (= "Subject :ex/john path [:ex/birthYear] violates constraint :sh/pattern of shape _:fdb-3 - value \":ex/ref\" does not match pattern \"(19|20)[0-9][0-9]\"."
             (ex-message db-ref-value)))
      (is (= [{:id          :ex/brian
               :type        :ex/User
               :ex/greeting "hello\nworld!"}]
             @(fluree/query db-ok-greeting user-query)))
      (is (= [{:id           :ex/john
               :type         :ex/User
               :ex/birthYear 1984}]
             @(fluree/query db-ok-birthyear user-query))))))

(deftest ^:integration shacl-multiple-properties-test
  (testing "multiple properties works"
    (let [conn         (test-utils/create-conn)
          ledger       @(fluree/create conn "shacl/b")
          context      [test-utils/default-context {:ex "http://example.org/ns/"}]
          user-query   {:context context
                        :select  {'?s [:*]}
                        :where   {:id '?s, :type :ex/User}}
          db           @(fluree/stage
                          (fluree/db ledger)
                          {"@context" ["https://ns.flur.ee" context]
                           "insert"
                           {:id             :ex/UserShape
                            :type           :sh/NodeShape
                            :sh/targetClass :ex/User
                            :sh/property    [{:sh/path     :schema/name
                                              :sh/datatype :xsd/string
                                              :sh/minCount 1
                                              :sh/maxCount 1}
                                             {:sh/path         :schema/age
                                              :sh/minCount     1
                                              :sh/maxCount     1
                                              :sh/minInclusive 0
                                              :sh/maxInclusive 130}
                                             {:sh/path     :schema/email
                                              :sh/datatype :xsd/string}]}})]
      (let [db-ok @(fluree/stage
                     db
                     {"@context" ["https://ns.flur.ee" context]
                      "insert"
                      {:id :ex/john
                       :type :ex/User
                       :schema/name "John"
                       :schema/age 40
                       :schema/email "john@example.org"}})]
        (is (= [{:id           :ex/john
                 :type         :ex/User
                 :schema/age   40
                 :schema/email "john@example.org"
                 :schema/name  "John"}]
               @(fluree/query db-ok user-query))))

      (let [db-no-name   @(fluree/stage
                            db
                            {"@context" ["https://ns.flur.ee" context]
                             "insert"
                             {:id           :ex/john
                              :type         :ex/User
                              :schema/age   40
                              :schema/email "john@example.org"}})]
        (is (= {:status 400,
                :error :shacl/violation,
                :report
                [{:subject :ex/john,
                  :constraint :sh/minCount,
                  :shape "_:fdb-2",
                  :path [:schema/name],
                  :value 0,
                  :expect 1,
                  :message "count 0 is less than minimum count of 1"}]}
               (ex-data db-no-name)))
        (is (= "Subject :ex/john path [:schema/name] violates constraint :sh/minCount of shape _:fdb-2 - count 0 is less than minimum count of 1."
               (ex-message db-no-name))))
      (let [db-two-names @(fluree/stage
                            db
                            {"@context" ["https://ns.flur.ee" context]
                             "insert"
                             {:id           :ex/john
                              :type         :ex/User
                              :schema/name  ["John" "Billy"]
                              :schema/age   40
                              :schema/email "john@example.org"}})]
        (is (= {:status 400,
                :error :shacl/violation,
                :report
                [{:subject :ex/john,
                  :constraint :sh/maxCount,
                  :shape "_:fdb-2",
                  :path [:schema/name],
                  :value 2,
                  :expect 1,
                  :message "count 2 is greater than maximum count of 1"}]}
               (ex-data db-two-names)))
        (is (= "Subject :ex/john path [:schema/name] violates constraint :sh/maxCount of shape _:fdb-2 - count 2 is greater than maximum count of 1."
               (ex-message db-two-names))))
      (let [db-too-old @(fluree/stage
                          db
                          {"@context" ["https://ns.flur.ee" context]
                           "insert"
                           {:id :ex/john
                            :type :ex/User
                            :schema/name "John"
                            :schema/age 140
                            :schema/email "john@example.org"}})]
        (is (= {:status 400,
                :error :shacl/violation,
                :report
                [{:subject :ex/john,
                  :constraint :sh/maxInclusive,
                  :shape "_:fdb-3",
                  :path [:schema/age],
                  :expect 130,
                  :value 140,
                  :message "value 140 is greater than inclusive maximum 130"}]}
               (ex-data db-too-old)))
        (is (= "Subject :ex/john path [:schema/age] violates constraint :sh/maxInclusive of shape _:fdb-3 - value 140 is greater than inclusive maximum 130."
               (ex-message db-too-old))))
      (let [db-two-ages  @(fluree/stage
                            db
                            {"@context" ["https://ns.flur.ee" context]
                             "insert"
                             {:id :ex/john
                              :type :ex/User
                              :schema/name "John"
                              :schema/age [40 21]
                              :schema/email "john@example.org"}})]
        (is (= {:status 400,
                :error :shacl/violation,
                :report
                [{:subject :ex/john,
                  :constraint :sh/maxCount,
                  :shape "_:fdb-3",
                  :path [:schema/age],
                  :value 2,
                  :expect 1,
                  :message "count 2 is greater than maximum count of 1"}]}
               (ex-data db-two-ages)))
        (is (= "Subject :ex/john path [:schema/age] violates constraint :sh/maxCount of shape _:fdb-3 - count 2 is greater than maximum count of 1."
               (ex-message db-two-ages))))
      (let [db-num-email @(fluree/stage
                            db
                            {"@context" ["https://ns.flur.ee" context]
                             "insert"
                             {:id           :ex/john
                              :type         :ex/User
                              :schema/name  "John"
                              :schema/age   40
                              :schema/email 42}})]
        (is (= {:status 400, :error :db/value-coercion}
               (ex-data db-num-email)))
        (is (= "Value 42 cannot be coerced to provided datatype: http://www.w3.org/2001/XMLSchema#string."
               (ex-message db-num-email)))))))

(deftest ^:integration property-paths
  (let [conn    @(fluree/connect {:method :memory})
        ledger  @(fluree/create conn "propertypathstest")
        context [test-utils/default-str-context {"ex" "http://example.com/"}]
        db0     (fluree/db ledger)]
    (testing "inverse path"
      (let [;; a valid Parent is anybody who is the object of a parent predicate
            db1          @(fluree/stage db0 {"@context" ["https://ns.flur.ee" context]
                                             "insert"   {"@type"          "sh:NodeShape"
                                                         "id"             "ex:ParentShape"
                                                         "sh:targetClass" {"@id" "ex:Parent"}
                                                         "sh:property"    [{"sh:path"     {"sh:inversePath" {"id" "ex:parent"}}
                                                                            "sh:minCount" 1}]}})
            valid-parent @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                             "insert"   {"id"          "ex:Luke"
                                                         "schema:name" "Luke"
                                                         "ex:parent"   {"id"          "ex:Anakin"
                                                                        "type"        "ex:Parent"
                                                                        "schema:name" "Anakin"}}})
            invalid-pal  @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                             "insert"   {"id"          "ex:bad-parent"
                                                         "type"        "ex:Parent"
                                                         "schema:name" "Darth Vader"}})]
        (is (= [{"id"          "ex:Luke",
                 "schema:name" "Luke",
                 "ex:parent"   {"id"          "ex:Anakin"
                                "type"        "ex:Parent"
                                "schema:name" "Anakin"}}]
               @(fluree/query valid-parent {"@context" context
                                            "select"   {"ex:Luke" ["*" {"ex:parent" ["*"]}]}})))

        (is (= {:status 400,
                :error  :shacl/violation,
                :report
                [{:subject    "ex:bad-parent",
                  :constraint "sh:minCount",
                  :shape      "_:fdb-2",
                  :expect     1,
                  :path       [{"sh:inversePath" "ex:parent"}],
                  :value      0,
                  :message    "count 0 is less than minimum count of 1"}]}
               (ex-data invalid-pal)))
        (is (= "Subject ex:bad-parent path [{\"sh:inversePath\" \"ex:parent\"}] violates constraint sh:minCount of shape _:fdb-2 - count 0 is less than minimum count of 1."
               (ex-message invalid-pal)))))
    (testing "sequence paths"
      (let [ ;; a valid Pal is anybody who has a pal with a name
            db1         @(fluree/stage db0 {"@context" ["https://ns.flur.ee" context]
                                            "insert"   {"@type"          "sh:NodeShape"
                                                        "sh:targetClass" {"@id" "ex:Pal"}
                                                        "sh:property"
                                                        [{"sh:path"
                                                          {"@list" [{"id" "ex:pal"} {"id" "schema:name"}]}
                                                          "sh:minCount" 1}]}})
            valid-pal   @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                            "insert"   {"id"          "ex:good-pal"
                                                        "type"        "ex:Pal"
                                                        "schema:name" "J.D."
                                                        "ex:pal"      [{"schema:name" "Turk"}
                                                                       {"schema:name" "Rowdy"}]}})
            invalid-pal @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                            "insert"   {"id"          "ex:bad-pal"
                                                        "type"        "ex:Pal"
                                                        "schema:name" "Darth Vader"
                                                        "ex:pal"      {"ex:evil" "has no name"}}})]
        (is (= {"id"          "ex:good-pal"
                "type"        "ex:Pal"
                "schema:name" "J.D."
                "ex:pal"      #{{"schema:name" "Rowdy"}
                                {"schema:name" "Turk"}}}
               (-> @(fluree/query valid-pal {"@context" context
                                             "select"   {"ex:good-pal" ["*" {"ex:pal" ["schema:name"]}]}})
                   first
                   (update "ex:pal" set))))
        (is (= {:status 400,
                :error :shacl/violation,
                :report
                [{:subject "ex:bad-pal",
                  :constraint "sh:minCount",
                  :shape "_:fdb-8",
                  :expect 1,
                  :path ["ex:pal" "schema:name"],
                  :value 0,
                  :message "count 0 is less than minimum count of 1"}]}
               (ex-data invalid-pal)))
        (is (= "Subject ex:bad-pal path [\"ex:pal\" \"schema:name\"] violates constraint sh:minCount of shape _:fdb-8 - count 0 is less than minimum count of 1."
               (ex-message invalid-pal)))))
    (testing "sequence paths"
      (let [db1         @(fluree/stage db0 {"@context" ["https://ns.flur.ee" context]
                                            "insert"   [{"@type"          "sh:NodeShape"
                                                         "sh:targetClass" {"@id" "ex:Pal"}
                                                         "sh:property"
                                                         [{"sh:path"
                                                           {"@list" [{"id" "ex:pal"} {"id" "ex:name"}]}
                                                           "sh:minCount" 1}]}]})
            valid-pal   @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                            "insert"   {"id"      "ex:jd"
                                                        "type"    "ex:Pal"
                                                        "ex:name" "J.D."
                                                        "ex:pal"  [{"ex:name" "Turk"}
                                                                   {"ex:name" "Rowdy"}]}})


            invalid-pal @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                            "insert"   {"id"      "ex:jd"
                                                        "type"    "ex:Pal"
                                                        "ex:name" "J.D."
                                                        "ex:pal"  [{"id" "ex:not-pal"
                                                                    "ex:not-name" "noname"}
                                                                   {"id" "ex:turk"
                                                                    "ex:name" "Turk"}
                                                                   {"id" "ex:rowdy"
                                                                    "ex:name" "Rowdy"}]}})]

        (is (= [{"id" "ex:jd",
                 "type" "ex:Pal",
                 "ex:name" "J.D.",
                 "ex:pal" [{"ex:name" "Turk"} {"ex:name" "Rowdy"}]}]
               @(fluree/query valid-pal {"@context" context
                                         "select"   {"ex:jd" ["*" {"ex:pal" ["ex:name"]}]}})))
        (is (= {:status 400,
                :error :shacl/violation,
                :report
                [{:subject "ex:jd",
                  :constraint "sh:minCount",
                  :shape "_:fdb-16",
                  :expect 1,
                  :path ["ex:pal" "ex:name"],
                  :value 0,
                  :message "count 0 is less than minimum count of 1"}]}
               (ex-data invalid-pal)))
        (is (= "Subject ex:jd path [\"ex:pal\" \"ex:name\"] violates constraint sh:minCount of shape _:fdb-16 - count 0 is less than minimum count of 1."
               (ex-message invalid-pal)))))

    (testing "predicate-path"
      (let [db1 @(fluree/stage db0 {"@context" ["https://ns.flur.ee" context]
                                    "insert" [{"@type" "sh:NodeShape"
                                               "sh:targetClass" {"@id" "ex:Named"}
                                               "sh:property"
                                               [{"sh:path"
                                                 {"@list" [{"id" "ex:name"}]}
                                                 "sh:datatype" {"id" "xsd:string"}}]}]})
            valid-named   @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                              "insert"   {"id"      "ex:good-pal"
                                                          "type"    "ex:Named"
                                                          "ex:name" {"@value" 123
                                                                     "@type" "xsd:integer"}}})]
        (is (= {:status 400,
                :error :shacl/violation,
                :report
                [{:subject "ex:good-pal",
                  :constraint "sh:datatype",
                  :shape "_:fdb-23",
                  :expect "xsd:string",
                  :path ["ex:name"],
                  :value ["xsd:integer"],
                  :message "the following values do not have expected datatype xsd:string: 123"}]}
               (ex-data valid-named)))))
    (testing "inverse sequence path"
      (let [ ;; a valid Princess is anybody who is the child of someone's queen
            db1              @(fluree/stage db0 {"@context" ["https://ns.flur.ee" context]
                                                 "insert"   {"@type"          "sh:NodeShape"
                                                             "id"             "ex:PrincessShape"
                                                             "sh:targetClass" {"@id" "ex:Princess"}
                                                             "sh:property"    [{"sh:path"
                                                                                {"@list"
                                                                                 [{"sh:inversePath"
                                                                                   {"id" "ex:child"}}
                                                                                  {"sh:inversePath"
                                                                                   {"id" "ex:queen"}}]}
                                                                                "sh:minCount" 1}]}})
            valid-princess   @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                                 "insert"   {"id"          "ex:Pleb"
                                                             "schema:name" "Pleb"
                                                             "ex:queen"    {"id"          "ex:Buttercup"
                                                                            "schema:name" "Buttercup"
                                                                            "ex:child"    {"id"          "ex:Mork"
                                                                                           "type"        "ex:Princess"
                                                                                           "schema:name" "Mork"}}}})
            invalid-princess @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                                 "insert"   {"id"          "ex:Pleb"
                                                             "schema:name" "Pleb"
                                                             "ex:child"    {"id"          "ex:Gerb"
                                                                            "type"        "ex:Princess"
                                                                            "schema:name" "Gerb"}}})]
        (is (= [{"id" "ex:Mork", "type" "ex:Princess", "schema:name" "Mork"}]
               @(fluree/query valid-princess {"@context" context
                                              "select"   {"ex:Mork" ["*"]}})))

        (is (= {:status 400,
                :error :shacl/violation,
                :report
                [{:subject "ex:Gerb",
                  :constraint "sh:minCount",
                  :shape "_:fdb-26",
                  :expect 1,
                  :path [{"sh:inversePath" "ex:child"} {"sh:inversePath" "ex:queen"}],
                  :value 0,
                  :message "count 0 is less than minimum count of 1"}]}
               (ex-data invalid-princess)))
        (is (= "Subject ex:Gerb path [{\"sh:inversePath\" \"ex:child\"} {\"sh:inversePath\" \"ex:queen\"}] violates constraint sh:minCount of shape _:fdb-26 - count 0 is less than minimum count of 1."
               (ex-message invalid-princess)))))))

(deftest ^:integration shacl-class-test
  (let [conn    @(fluree/connect {:method :memory})
        ledger  @(fluree/create conn "classtest")
        context test-utils/default-str-context
        db0     (fluree/db ledger)
        db1     @(fluree/stage db0 {"@context" ["https://ns.flur.ee" context]
                                    "insert"   [{"@type"          "sh:NodeShape"
                                                 "sh:targetClass" {"@id" "https://example.com/Country"}
                                                 "sh:property"
                                                 [{"sh:path"     {"@id" "https://example.com/name"}
                                                   "sh:datatype" {"@id" "xsd:string"}
                                                   "sh:minCount" 1
                                                   "sh:maxCount" 1}]}
                                                {"@type"          "sh:NodeShape"
                                                 "sh:targetClass" {"@id" "https://example.com/Actor"}
                                                 "sh:property"
                                                 [{"sh:path"        {"@id" "https://example.com/country"}
                                                   "sh:class"       {"@id" "https://example.com/Country"}
                                                   "sh:maxCount"    1
                                                   "sh:description" "Birth country"}
                                                  {"sh:path"     {"@id" "https://example.com/name"}
                                                   "sh:minCount" 1
                                                   "sh:maxCount" 1
                                                   "sh:datatype" {"@id" "xsd:string"}}]}]})
        ;; valid inline type
        db2     @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                    "insert"   {"@id"                           "https://example.com/Actor/65731"
                                                "https://example.com/country"   {"@id"                      "https://example.com/Country/AU"
                                                                                 "@type"                    "https://example.com/Country"
                                                                                 "https://example.com/name" "Oz"}
                                                "https://example.com/gender"    "Male"
                                                "https://example.com/character" ["Jake Sully" "Marcus Wright"]
                                                "https://example.com/movie"     [{"@id" "https://example.com/Movie/19995"}
                                                                                 {"@id" "https://example.com/Movie/534"}]
                                                "@type"                         "https://example.com/Actor"
                                                "https://example.com/name"      "Sam Worthington"}})
        ;; valid node ref
        db3     @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                    "insert"   [{"@id"                      "https://example.com/Country/US"
                                                 "@type"                    "https://example.com/Country"
                                                 "https://example.com/name" "United States of America"}
                                                {"@id"                         "https://example.com/Actor/4242"
                                                 "https://example.com/country" {"@id" "https://example.com/Country/US"}
                                                 "https://example.com/gender"  "Female"
                                                 "@type"                       "https://example.com/Actor"
                                                 "https://example.com/name"    "Rindsey Rohan"}]})
        ;; invalid inline type
        db4     @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                    "insert"   {"@id"                         "https://example.com/Actor/1001"
                                                "https://example.com/country" {"@id"                      "https://example.com/Country/Absurdistan"
                                                                               "@type"                    "https://example.com/FakeCountry"
                                                                               "https://example.com/name" "Absurdistan"}
                                                "https://example.com/gender"  "Male"
                                                "@type"                       "https://example.com/Actor"
                                                "https://example.com/name"    "Not Real"}})
        ;; invalid node ref type
        db5     @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                    "insert"   [{"@id"                      "https://example.com/Country/Absurdistan"
                                                 "@type"                    "https://example.com/FakeCountry"
                                                 "https://example.com/name" "Absurdistan"}
                                                {"@id"                         "https://example.com/Actor/8675309"
                                                 "https://example.com/country" {"@id" "https://example.com/Country/Absurdistan"}
                                                 "https://example.com/gender"  "Female"
                                                 "@type"                       "https://example.com/Actor"
                                                 "https://example.com/name"    "Jenny Tutone"}]})]
    (is (not (ex-data db2)))
    (is (not (ex-data db3)))
    (is (= {:status 400,
            :error  :shacl/violation,
            :report
            [{:subject    "https://example.com/Actor/1001",
              :constraint "sh:class",
              :shape      "_:fdb-5",
              :path       ["https://example.com/country"],
              :expect     "https://example.com/Country",
              :value      ["https://example.com/FakeCountry"],
              :message    "missing required class https://example.com/Country"}]}
           (ex-data db4)))
    (is (= "Subject https://example.com/Actor/1001 path [\"https://example.com/country\"] violates constraint sh:class of shape _:fdb-5 - missing required class https://example.com/Country."
                          (ex-message db4)))
    (is (= {:status 400,
            :error  :shacl/violation,
            :report
            [{:subject    "https://example.com/Actor/8675309",
              :constraint "sh:class",
              :shape      "_:fdb-5",
              :path       ["https://example.com/country"],
              :expect     "https://example.com/Country",
              :value      ["https://example.com/FakeCountry"],
              :message    "missing required class https://example.com/Country"}]}
           (ex-data db5)))
    (is (= "Subject https://example.com/Actor/8675309 path [\"https://example.com/country\"] violates constraint sh:class of shape _:fdb-5 - missing required class https://example.com/Country."
           (ex-message db5)))))

(deftest ^:integration shacl-in-test
  (testing "value nodes"
    (let [conn    @(fluree/connect {:method :memory})
          ledger  @(fluree/create conn "shacl-in-test")
          context [test-utils/default-str-context {"ex" "http://example.com/ns/"}]
          db0     (fluree/db ledger)
          db1     @(fluree/stage db0 {"@context" ["https://ns.flur.ee" context]
                                      "insert"   [{"type"           ["sh:NodeShape"]
                                                   "sh:targetClass" {"id" "ex:Pony"}
                                                   "sh:property"    [{"sh:path" {"id" "ex:color"}
                                                                      "sh:in"   '("cyan" "magenta")}]}]})
          db2     @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                      "insert"   {"id"       "ex:YellowPony"
                                                  "type"     "ex:Pony"
                                                  "ex:color" "yellow"}})]
      (is (= {:status 400,
              :error :shacl/violation,
              :report
              [{:subject "ex:YellowPony",
                :constraint "sh:in",
                :shape "_:fdb-3",
                :expect ["cyan" "magenta"],
                :path ["ex:color"],
                :value "yellow",
                :message "value \"yellow\" is not in [\"cyan\" \"magenta\"]"}]}
             (ex-data db2)))
      (is (= "Subject ex:YellowPony path [\"ex:color\"] violates constraint sh:in of shape _:fdb-3 - value \"yellow\" is not in [\"cyan\" \"magenta\"]."
             (ex-message db2)))))
  (testing "node refs"
    (let [conn    @(fluree/connect {:method :memory})
          ledger  @(fluree/create conn "shacl-in-test")
          db0     (fluree/db ledger)
          context [test-utils/default-str-context {"ex" "http://example.com/ns/"}]
          db1     @(fluree/stage db0 {"@context" ["https://ns.flur.ee" context]
                                      "insert"   [{"type"           ["sh:NodeShape"]
                                                   "sh:targetClass" {"id" "ex:Pony"}
                                                   "sh:property"    [{"sh:path" {"id" "ex:color"}
                                                                      "sh:in"   '({"id" "ex:Pink"}
                                                                                  {"id" "ex:Purple"})}]}]})
          db2     @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                      "insert"   [{"id"   "ex:Pink"
                                                   "type" "ex:color"}
                                                  {"id"   "ex:Purple"
                                                   "type" "ex:color"}
                                                  {"id"   "ex:Green"
                                                   "type" "ex:color"}
                                                  {"id"       "ex:RainbowPony"
                                                   "type"     "ex:Pony"
                                                   "ex:color" [{"id" "ex:Pink"}
                                                               {"id" "ex:Green"}]}]})
          db3     @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                      "insert"   [{"id"       "ex:PastelPony"
                                                   "type"     "ex:Pony"
                                                   "ex:color" [{"id" "ex:Pink"}
                                                               {"id" "ex:Purple"}]}]})]
      (is (= {:status 400,
              :error :shacl/violation,
              :report
              [{:subject "ex:RainbowPony",
                :constraint "sh:in",
                :shape "_:fdb-7",
                :expect ["ex:Pink" "ex:Purple"],
                :path ["ex:color"],
                :value #fluree/SID [101 "Green"],
                :message "value \"ex:Green\" is not in [\"ex:Pink\" \"ex:Purple\"]"}]}
             (ex-data db2)))
      (is (= "Subject ex:RainbowPony path [\"ex:color\"] violates constraint sh:in of shape _:fdb-7 - value \"ex:Green\" is not in [\"ex:Pink\" \"ex:Purple\"]."
            (ex-message db2)))

      (is (not (ex-data db3)))
      (is (= {"id"       "ex:PastelPony"
              "type"     "ex:Pony"
              "ex:color" [{"id" "ex:Pink"} {"id" "ex:Purple"}]}
             (-> @(fluree/query db3 {"@context" context
                                     "select"   {"?p" ["*"]}
                                     "where"    {"id"   "?p"
                                                 "type" "ex:Pony"}})
                 first
                 (update "ex:color" (partial sort-by #(get % "id"))))))))
  (testing "mixed values and refs"
    (let [conn    @(fluree/connect {:method :memory})
          ledger  @(fluree/create conn "shacl-in-test")
          db0     (fluree/db ledger)
          context [test-utils/default-str-context {"ex" "http://example.com/ns/"}]
          db1     @(fluree/stage db0 {"@context" ["https://ns.flur.ee" context]
                                      "insert"   [{"type"           ["sh:NodeShape"]
                                                   "sh:targetClass" {"id" "ex:Pony"}
                                                   "sh:property"    [{"sh:path" {"id" "ex:color"}
                                                                      "sh:in"   '({"id" "ex:Pink"}
                                                                                  {"id" "ex:Purple"}
                                                                                  "green")}]}]})
          db2     @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                      "insert"   {"id"       "ex:RainbowPony"
                                                  "type"     "ex:Pony"
                                                  "ex:color" [{"id" "ex:Pink"}
                                                              {"id" "ex:Green"}]}})]
      (is (= {:status 400,
              :error :shacl/violation,
              :report
              [{:subject "ex:RainbowPony",
                :constraint "sh:in",
                :shape "_:fdb-12",
                :expect ["ex:Pink" "ex:Purple" "green"],
                :path ["ex:color"],
                :value #fluree/SID [101 "Green"],
                :message "value \"ex:Green\" is not in [\"ex:Pink\" \"ex:Purple\" \"green\"]"}]}
             (ex-data db2)))
      (is (= "Subject ex:RainbowPony path [\"ex:color\"] violates constraint sh:in of shape _:fdb-12 - value \"ex:Green\" is not in [\"ex:Pink\" \"ex:Purple\" \"green\"]."
             (ex-message db2))))))

(deftest ^:integration shacl-targetobjectsof-test
  (let [conn    @(fluree/connect {:method :memory})
        ledger  @(fluree/create conn "shacl-target-objects-of-test")
        context [test-utils/default-str-context {"ex" "http://example.com/ns/"}]
        db0     (fluree/db ledger)]
    (testing "subject and object of constrained predicate in the same txn"
      (testing "datatype constraint"
        (let [db1                @(fluree/stage db0
                                                {"@context" ["https://ns.flur.ee" context]
                                                 "insert"
                                                 {"@id"                "ex:friendShape"
                                                  "type"               ["sh:NodeShape"]
                                                  "sh:targetObjectsOf" {"@id" "ex:friend"}
                                                  "sh:property"        [{"sh:path"     {"@id" "ex:name"}
                                                                         "sh:datatype" {"@id" "xsd:string"}}]}})
              db-bad-friend-name @(fluree/stage db1
                                                {"@context" ["https://ns.flur.ee" context]
                                                 "insert"
                                                 [{"id"        "ex:Alice"
                                                   "ex:name"   "Alice"
                                                   "type"      "ex:User"
                                                   "ex:friend" {"@id" "ex:Bob"}}
                                                  {"id"      "ex:Bob"
                                                   "ex:name" 123
                                                   "type"    "ex:User"}]})]
          (is (= "Value 123 cannot be coerced to provided datatype: http://www.w3.org/2001/XMLSchema#string."
                 (ex-message db-bad-friend-name)))))
      (testing "maxCount"
        (let [db1           @(fluree/stage db0
                                           {"@context" ["https://ns.flur.ee" context]
                                            "insert"
                                            {"@id"                "ex:friendShape"
                                             "type"               ["sh:NodeShape"]
                                             "sh:targetObjectsOf" {"@id" "ex:friend"}
                                             "sh:property"        [{"sh:path"     {"@id" "ex:ssn"}
                                                                    "sh:maxCount" 1}]}})
              db-excess-ssn @(fluree/stage db1
                                           {"@context" ["https://ns.flur.ee" context]
                                            "insert"
                                            [{"id"        "ex:Alice"
                                              "ex:name"   "Alice"
                                              "type"      "ex:User"
                                              "ex:friend" {"@id" "ex:Bob"}}
                                             {"id"     "ex:Bob"
                                              "ex:ssn" ["111-11-1111"
                                                        "222-22-2222"]
                                              "type"   "ex:User"}]})]
          (is (= {:status 400,
                  :error  :shacl/violation,
                  :report
                  [{:subject    "ex:Bob",
                    :constraint "sh:maxCount",
                    :shape      "_:fdb-5",
                    :path       ["ex:ssn"],
                    :value      2,
                    :expect     1,
                    :message    "count 2 is greater than maximum count of 1"}]}
                 (ex-data db-excess-ssn)))
          (is (= "Subject ex:Bob path [\"ex:ssn\"] violates constraint sh:maxCount of shape _:fdb-5 - count 2 is greater than maximum count of 1."
                 (ex-message db-excess-ssn)))))
      (testing "required properties"
        (let [db1           @(fluree/stage db0
                                           {"@context" ["https://ns.flur.ee" context]
                                            "insert"
                                            [{"@id"                "ex:friendShape"
                                              "type"               ["sh:NodeShape"]
                                              "sh:targetObjectsOf" {"@id" "ex:friend"}
                                              "sh:property"        [{"sh:path"     {"@id" "ex:ssn"}
                                                                     "sh:minCount" 1}]}]})
              db-just-alice @(fluree/stage db1
                                           {"@context" ["https://ns.flur.ee" context]
                                            "insert"
                                            [{"id"        "ex:Alice"
                                              "ex:name"   "Alice"
                                              "type"      "ex:User"
                                              "ex:friend" {"@id" "ex:Bob"}}]})]
          (is (= {:status 400,
                  :error  :shacl/violation,
                  :report
                  [{:subject    "ex:Bob",
                    :constraint "sh:minCount",
                    :shape      "_:fdb-8",
                    :path       ["ex:ssn"],
                    :value      0,
                    :expect     1,
                    :message    "count 0 is less than minimum count of 1"}]}
                 (ex-data db-just-alice)))
          (is (= "Subject ex:Bob path [\"ex:ssn\"] violates constraint sh:minCount of shape _:fdb-8 - count 0 is less than minimum count of 1."
                 (ex-message db-just-alice)))))
      (testing "combined with `sh:targetClass`"
        (let [db1           @(fluree/stage db0
                                           {"@context" ["https://ns.flur.ee" context]
                                            "insert"
                                            [{"@id"            "ex:UserShape"
                                              "type"           ["sh:NodeShape"]
                                              "sh:targetClass" {"@id" "ex:User"}
                                              "sh:property"    [{"sh:path"     {"@id" "ex:ssn"}
                                                                 "sh:maxCount" 1}]}
                                             {"@id"                "ex:friendShape"
                                              "type"               ["sh:NodeShape"]
                                              "sh:targetObjectsOf" {"@id" "ex:friend"}
                                              "sh:property"        [{"sh:path"     {"@id" "ex:name"}
                                                                     "sh:maxCount" 1}]}]})
              db-bad-friend @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                                "insert"   [{"id"        "ex:Alice"
                                                             "ex:name"   "Alice"
                                                             "type"      "ex:User"
                                                             "ex:friend" {"@id" "ex:Bob"}}
                                                            {"id"      "ex:Bob"
                                                             "ex:name" ["Bob" "Robert"]
                                                             "ex:ssn"  "111-11-1111"
                                                             "type"    "ex:User"}]})]
          (is (= {:status 400,
                  :error  :shacl/violation,
                  :report
                  [{:subject    "ex:Bob",
                    :constraint "sh:maxCount",
                    :shape      "_:fdb-12",
                    :path       ["ex:name"],
                    :value      2,
                    :expect     1,
                    :message    "count 2 is greater than maximum count of 1"}]}
                 (ex-data db-bad-friend)))
          (is (= "Subject ex:Bob path [\"ex:name\"] violates constraint sh:maxCount of shape _:fdb-12 - count 2 is greater than maximum count of 1."
                 (ex-message db-bad-friend))))))
    (testing "separate txns"
      (testing "maxCount"
        (let [db1                    @(fluree/stage db0
                                                    {"@context" ["https://ns.flur.ee" context]
                                                     "insert"
                                                     [{"@id"                "ex:friendShape"
                                                       "type"               ["sh:NodeShape"]
                                                       "sh:targetObjectsOf" {"@id" "ex:friend"}
                                                       "sh:property"        [{"sh:path"     {"@id" "ex:ssn"}
                                                                              "sh:maxCount" 1}]}]})
              db2                    @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                                         "insert"   [{"id"     "ex:Bob"
                                                                      "ex:ssn" ["111-11-1111" "222-22-2222"]
                                                                      "type"   "ex:User"}]})
              db-db-forbidden-friend @(fluree/stage db2
                                                    {"@context" ["https://ns.flur.ee" context]
                                                     "insert"
                                                     {"id"        "ex:Alice"
                                                      "type"      "ex:User"
                                                      "ex:friend" {"@id" "ex:Bob"}}})]

          (is (= {:status 400,
                  :error :shacl/violation,
                  :report
                  [{:subject "ex:Bob",
                    :constraint "sh:maxCount",
                    :shape "_:fdb-15",
                    :expect 1,
                    :path ["ex:ssn"],
                    :value 2,
                    :message "count 2 is greater than maximum count of 1"}]}
                 (ex-data db-db-forbidden-friend)))
          (is (= "Subject ex:Bob path [\"ex:ssn\"] violates constraint sh:maxCount of shape _:fdb-15 - count 2 is greater than maximum count of 1."
                 (ex-message db-db-forbidden-friend))))
        (let [db1           @(fluree/stage db0
                                           {"@context" ["https://ns.flur.ee" context]
                                            "insert"
                                            [{"@id"                "ex:friendShape"
                                              "type"               ["sh:NodeShape"]
                                              "sh:targetObjectsOf" {"@id" "ex:friend"}
                                              "sh:property"        [{"sh:path"     {"@id" "ex:ssn"}
                                                                     "sh:maxCount" 1}]}]})
              db2           @(fluree/stage db1
                                           {"@context" ["https://ns.flur.ee" context]
                                            "insert"
                                            [{"id"        "ex:Alice"
                                              "ex:name"   "Alice"
                                              "type"      "ex:User"
                                              "ex:friend" {"@id" "ex:Bob"}}
                                             {"id"      "ex:Bob"
                                              "ex:name" "Bob"
                                              "type"    "ex:User"}]})
              db-excess-ssn @(fluree/stage db2
                                           {"@context" ["https://ns.flur.ee" context]
                                            "insert"
                                            {"id"     "ex:Bob"
                                             "ex:ssn" ["111-11-1111"
                                                       "222-22-2222"]}})]
          (is (= {:status 400,
                  :error :shacl/violation,
                  :report
                  [{:subject "ex:Bob",
                    :constraint "sh:maxCount",
                    :shape "_:fdb-19",
                    :expect 1,
                    :path ["ex:ssn"],
                    :value 2,
                    :message "count 2 is greater than maximum count of 1"}]}
                 (ex-data db-excess-ssn)))
          (is (= "Subject ex:Bob path [\"ex:ssn\"] violates constraint sh:maxCount of shape _:fdb-19 - count 2 is greater than maximum count of 1."
                 (ex-message db-excess-ssn)))))
      (testing "datatype"
        (let [db1     @(fluree/stage db0
                                     {"@context" ["https://ns.flur.ee" context]
                                      "insert"   {"@id"                "ex:friendShape"
                                                  "type"               ["sh:NodeShape"]
                                                  "sh:targetObjectsOf" {"@id" "ex:friend"}
                                                  "sh:property"        [{"sh:path"     {"@id" "ex:name"}
                                                                         "sh:datatype" {"@id" "xsd:string"}}]}})

              ;; need to specify type in order to avoid sh:datatype coercion
              db2                 @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                                      "insert"   {"id"      "ex:Bob"
                                                                  "ex:name" {"@type" "xsd:integer" "@value" 123}
                                                                  "type"    "ex:User"}})
              db-forbidden-friend @(fluree/stage db2
                                                 {"@context" ["https://ns.flur.ee" context]
                                                  "insert"
                                                  {"id"        "ex:Alice"
                                                   "type"      "ex:User"
                                                   "ex:friend" {"@id" "ex:Bob"}}})]
          (is (= {:status 400,
                  :error :shacl/violation,
                  :report
                  [{:subject "ex:Bob",
                    :constraint "sh:datatype",
                    :shape "_:fdb-23",
                    :expect "xsd:string",
                    :path ["ex:name"],
                    :value ["xsd:integer"],
                    :message "the following values do not have expected datatype xsd:string: 123"}]}
                 (ex-data db-forbidden-friend)))
          (is (= "Subject ex:Bob path [\"ex:name\"] violates constraint sh:datatype of shape _:fdb-23 - the following values do not have expected datatype xsd:string: 123."
                 (ex-message db-forbidden-friend))))))))

(deftest ^:integration shape-based-constraints
  (testing "sh:node"
    (let [conn    @(fluree/connect {:method :memory})
          ledger  @(fluree/create conn "shape-constaints")
          db0     (fluree/db ledger)
          context [test-utils/default-str-context {"ex" "http://example.com/ns/"}]

          db1            @(fluree/stage db0 {"@context" ["https://ns.flur.ee" context]
                                             "insert"   [{"id"          "ex:AddressShape"
                                                          "type"        "sh:NodeShape"
                                                          "sh:property" [{"sh:path"     {"id" "ex:postalCode"}
                                                                          "sh:maxCount" 1}]}
                                                         {"id"             "ex:PersonShape"
                                                          "type"           "sh:NodeShape"
                                                          "sh:targetClass" {"id" "ex:Person"}
                                                          "sh:property"    [{"sh:path"     {"id" "ex:address"}
                                                                             "sh:node"     {"id" "ex:AddressShape"}
                                                                             "sh:minCount" 1}]}]})
          valid-person   @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                             "insert"   {"id"         "ex:Bob"
                                                         "type"       "ex:Person"
                                                         "ex:address" {"ex:postalCode" "12345"}}})
          invalid-person @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                             "insert"   {"id"         "ex:Reto"
                                                         "type"       "ex:Person"
                                                         "ex:address" {"ex:postalCode" ["12345" "45678"]}}})]
      (is (= [{"id"         "ex:Bob",
               "type"       "ex:Person",
               "ex:address" {"ex:postalCode" "12345"}}]
             @(fluree/query valid-person {"@context" context
                                          "select"   {"ex:Bob" ["*" {"ex:address" ["ex:postalCode"]}]}})))
      (is (= {:status 400,
              :error  :shacl/violation,
              :report
              [{:subject    "ex:Reto",
                :constraint "sh:node",
                :shape      "_:fdb-3",
                :path       ["ex:address"],
                :expect     ["ex:AddressShape"],
                :value      "_:fdb-7",
                :message    "node _:fdb-7 does not conform to shapes [\"ex:AddressShape\"]"}]}
             (ex-data invalid-person)))
      (is (= "Subject ex:Reto path [\"ex:address\"] violates constraint sh:node of shape _:fdb-3 - node _:fdb-7 does not conform to shapes [\"ex:AddressShape\"]."
             (ex-message invalid-person)))))

  (testing "sh:qualifiedValueShape property shape"
    (let [conn        @(fluree/connect {:method :memory})
          ledger      @(fluree/create conn "shape-constaints")
          db0         (fluree/db ledger)
          context     [test-utils/default-str-context {"ex" "http://example.com/ns/"}]
          db1         @(fluree/stage db0 {"@context" ["https://ns.flur.ee" context]
                                          "insert"   [{"id"             "ex:KidShape"
                                                       "type"           "sh:NodeShape"
                                                       "sh:targetClass" {"id" "ex:Kid"}
                                                       "sh:property"
                                                       [{"sh:path"                {"id" "ex:parent"}
                                                         "sh:minCount"            2
                                                         "sh:maxCount"            2
                                                         "sh:qualifiedValueShape" {"sh:path"    {"id" "ex:gender"}
                                                                                   "sh:pattern" "female"}
                                                         "sh:qualifiedMinCount"   1}]}
                                                      {"id"        "ex:Bob"
                                                       "ex:gender" "male"}
                                                      {"id"        "ex:Jane"
                                                       "ex:gender" "female"}]})
          valid-kid   @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                          "insert"   {"id"        "ex:ValidKid"
                                                      "type"      "ex:Kid"
                                                      "ex:parent" [{"id" "ex:Bob"} {"id" "ex:Jane"}]}})
          invalid-kid @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                          "insert"   {"id"        "ex:InvalidKid"
                                                      "type"      "ex:Kid"
                                                      "ex:parent" [{"id" "ex:Bob"}
                                                                   {"id"        "ex:Zorba"
                                                                    "ex:gender" "alien"}]}})]
      (is (= {"id"        "ex:ValidKid"
              "type"      "ex:Kid"
              "ex:parent" [{"id" "ex:Bob"}
                           {"id" "ex:Jane"}]}
             (-> @(fluree/query valid-kid {"@context" context
                                           "select"   {"ex:ValidKid" ["*"]}})
                 first
                 (update "ex:parent" (partial sort-by #(get % "id"))))))
      (is (= {:status 400,
              :error  :shacl/violation,
              :report
              [{:subject    "ex:InvalidKid",
                :constraint "sh:qualifiedValueShape",
                :shape      "_:fdb-9",
                :path       ["ex:parent"],
                :expect     "_:fdb-10",
                :value      ["ex:Bob" "ex:Zorba"],
                :message    "values [\"ex:Bob\" \"ex:Zorba\"] conformed to _:fdb-10 less than sh:qualifiedMinCount 1 times"}]}
             (ex-data invalid-kid)))
      (is (= "Subject ex:InvalidKid path [\"ex:parent\"] violates constraint sh:qualifiedValueShape of shape _:fdb-9 - values [\"ex:Bob\" \"ex:Zorba\"] conformed to _:fdb-10 less than sh:qualifiedMinCount 1 times."
             (ex-message invalid-kid)))))
  (testing "sh:qualifiedValueShape node shape"
    (let [conn   @(fluree/connect {:method :memory})
          ledger @(fluree/create conn "shape-constaints")
          db0    (fluree/db ledger)

          context     [test-utils/default-str-context {"ex" "http://example.com/ns/"}]
          db1         @(fluree/stage db0 {"@context" ["https://ns.flur.ee" context]
                                          "insert"   [{"id"             "ex:KidShape"
                                                       "type"           "sh:NodeShape"
                                                       "sh:targetClass" {"id" "ex:Kid"}
                                                       "sh:property"
                                                       [{"sh:path"              {"id" "ex:parent"}
                                                         "sh:minCount"          2
                                                         "sh:maxCount"          2
                                                         "sh:qualifiedValueShape"
                                                         {"id"          "ex:ParentShape"
                                                          "type"        "sh:NodeShape"
                                                          "sh:property" {"sh:path"    {"id" "ex:gender"}
                                                                         "sh:pattern" "female"}}
                                                         "sh:qualifiedMinCount" 1}]}
                                                      {"id"        "ex:Mom"
                                                       "type"      "ex:Parent"
                                                       "ex:gender" "female"}
                                                      {"id"        "ex:Dad"
                                                       "type"      "ex:Parent"
                                                       "ex:gender" "male"}]})
          valid-kid   @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                          "insert"   {"id"        "ex:ValidKid"
                                                      "type"      "ex:Kid"
                                                      "ex:parent" [{"id" "ex:Mom"} {"id" "ex:Dad"}]}})
          invalid-kid @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                          "insert"   {"id"        "ex:InvalidKid"
                                                      "type"      "ex:Kid"
                                                      "ex:parent" [{"id"        "ex:Bob"
                                                                    "ex:gender" "male"}
                                                                   {"id"        "ex:Zorba"
                                                                    "type"      "ex:Parent"
                                                                    "ex:gender" "alien"}]}})]
      (is (= [{"id"        "ex:ValidKid"
               "type"      "ex:Kid"
               "ex:parent" [{"id" "ex:Dad"}
                            {"id" "ex:Mom"}]}]
             @(fluree/query valid-kid {"@context" context
                                       "select"   {"ex:ValidKid" ["*"]}})))
      (is (= {:status 400,
              :error :shacl/violation,
              :report
              [{:subject "ex:InvalidKid",
                :constraint "sh:qualifiedValueShape",
                :shape "_:fdb-14",
                :expect "ex:ParentShape",
                :path ["ex:parent"],
                :value ["ex:Bob" "ex:Zorba"],
                :message "values [\"ex:Bob\" \"ex:Zorba\"] conformed to ex:ParentShape less than sh:qualifiedMinCount 1 times"}]}
             (ex-data invalid-kid)))
      (is (= "Subject ex:InvalidKid path [\"ex:parent\"] violates constraint sh:qualifiedValueShape of shape _:fdb-14 - values [\"ex:Bob\" \"ex:Zorba\"] conformed to ex:ParentShape less than sh:qualifiedMinCount 1 times."
             (ex-message invalid-kid)))))
  (testing "sh:qualifiedValueShapesDisjoint"
    (let [conn   @(fluree/connect {:method :memory})
          ledger @(fluree/create conn "shape-constraints")
          db0    (fluree/db ledger)

          context [test-utils/default-str-context {"ex" "http://example.com/ns/"}]
          db1     @(fluree/stage db0 {"@context" ["https://ns.flur.ee" context]
                                      "insert"
                                      [{"id"      "ex:Digit"
                                        "ex:name" "Toe"}
                                       {"id"             "ex:HandShape"
                                        "type"           "sh:NodeShape"
                                        "sh:targetClass" {"id" "ex:Hand"}
                                        "sh:property"
                                        [{"sh:path"     {"id" "ex:digit"}
                                          "sh:maxCount" 5}
                                         {"sh:path"                         {"id" "ex:digit"}
                                          "sh:qualifiedValueShape"          {"id"        "ex:thumbshape"
                                                                             "sh:path"   {"id" "ex:name"}
                                                                             "sh:hasValue" "Thumb"}
                                          "sh:qualifiedMinCount"            1
                                          "sh:qualifiedMaxCount"            1
                                          "sh:qualifiedValueShapesDisjoint" true}
                                         {"sh:path"                         {"id" "ex:digit"}
                                          "sh:qualifiedValueShape"          {"id"        "ex:fingershape"
                                                                             "sh:path"   {"id" "ex:name"}
                                                                             "sh:hasValue" "Finger"}
                                          "sh:qualifiedMinCount"            4
                                          "sh:qualifiedMaxCount"            4
                                          "sh:qualifiedValueShapesDisjoint" true}]}]})

          valid-hand @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                         "insert"   {"id"       "ex:ValidHand"
                                                     "type"     "ex:Hand"
                                                     "ex:digit" [{"id" "ex:thumb" "ex:name" "Thumb"}
                                                                 {"id" "ex:finger1" "ex:name" "Finger"}
                                                                 {"id" "ex:finger2" "ex:name" "Finger"}
                                                                 {"id" "ex:finger3" "ex:name" "Finger"}
                                                                 {"id" "ex:finger4" "ex:name" "Finger"}]}})
          invalid-hand @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                           "insert"   {"id"       "ex:InvalidHand"
                                                       "type"     "ex:Hand"
                                                       "ex:digit" [{"id" "ex:thumb" "ex:name" "Thumb"}
                                                                   {"id" "ex:finger1" "ex:name" "Finger"}
                                                                   {"id" "ex:finger2" "ex:name" "Finger"}
                                                                   {"id" "ex:finger3" "ex:name" "Finger"}
                                                                   {"id" "ex:finger4andthumb"
                                                                    "ex:name" ["Finger" "Thumb"]}]}})
          ]
      (is (= [{"id"   "ex:ValidHand",
               "type" "ex:Hand",
               "ex:digit"
               [{"ex:name" "Finger"}
                {"ex:name" "Finger"}
                {"ex:name" "Finger"}
                {"ex:name" "Finger"}
                {"ex:name" "Thumb"}]}]
             @(fluree/query valid-hand {"@context" context
                                        "select"   {"ex:ValidHand" ["*" {"ex:digit" ["ex:name"]}]}})))
      (is (= {:status 400,
              :error :shacl/violation,
              :report
              [{:subject "ex:InvalidHand",
                :constraint "sh:qualifiedValueShape",
                :shape "_:fdb-20",
                :path ["ex:digit"],
                :expect "ex:thumbshape",
                :value "ex:finger4andthumb",
                :message "value ex:finger4andthumb conformed to a sibling qualified value shape [\"ex:fingershape\"] in violation of the sh:qualifiedValueShapesDisjoint constraint"}
               {:subject "ex:InvalidHand",
                :constraint "sh:qualifiedValueShape",
                :shape "_:fdb-21",
                :path ["ex:digit"],
                :expect "ex:fingershape",
                :value "ex:finger4andthumb",
                :message "value ex:finger4andthumb conformed to a sibling qualified value shape [\"ex:thumbshape\"] in violation of the sh:qualifiedValueShapesDisjoint constraint"}]}
             (ex-data invalid-hand)))
      (is (= "Subject ex:InvalidHand path [\"ex:digit\"] violates constraint sh:qualifiedValueShape of shape _:fdb-20 - value ex:finger4andthumb conformed to a sibling qualified value shape [\"ex:fingershape\"] in violation of the sh:qualifiedValueShapesDisjoint constraint.
Subject ex:InvalidHand path [\"ex:digit\"] violates constraint sh:qualifiedValueShape of shape _:fdb-21 - value ex:finger4andthumb conformed to a sibling qualified value shape [\"ex:thumbshape\"] in violation of the sh:qualifiedValueShapesDisjoint constraint."
             (ex-message invalid-hand))))))

(deftest ^:integration post-processing-validation
  (let [conn    @(fluree/connect {:method :memory})
        ledger  @(fluree/create conn "post-processing")
        context [test-utils/default-str-context {"ex" "http://example.com/ns/"}]
        db0     (fluree/db ledger)]
    (testing "shacl-objects-of-test"
      (let [db1                 @(fluree/stage db0
                                               {"@context" ["https://ns.flur.ee" context]
                                                "insert"
                                                {"@id"                "ex:friendShape"
                                                 "type"               ["sh:NodeShape"]
                                                 "sh:targetObjectsOf" {"@id" "ex:friend"}
                                                 "sh:property"        [{"sh:path"     {"@id" "ex:name"}
                                                                        "sh:datatype" {"@id" "xsd:string"}}]}})
            ;; need to specify type in order to avoid sh:datatype coercion
            db2                 @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                                    "insert"   {"id"      "ex:Bob"
                                                                "ex:name" {"@type" "xsd:integer" "@value" 123}
                                                                "type"    "ex:User"}})
            db-forbidden-friend @(fluree/stage db2
                                               {"@context" ["https://ns.flur.ee" context]
                                                "insert"
                                                {"id"        "ex:Alice"
                                                 "type"      "ex:User"
                                                 "ex:friend" {"@id" "ex:Bob"}}})]
        (is (= {:status 400,
                :error :shacl/violation,
                :report
                [{:subject "ex:Bob",
                  :constraint "sh:datatype",
                  :shape "_:fdb-2",
                  :expect "xsd:string",
                  :path ["ex:name"],
                  :value ["xsd:integer"],
                  :message "the following values do not have expected datatype xsd:string: 123"}]}
               (ex-data db-forbidden-friend)))
        (is (= "Subject ex:Bob path [\"ex:name\"] violates constraint sh:datatype of shape _:fdb-2 - the following values do not have expected datatype xsd:string: 123."
               (ex-message db-forbidden-friend)))))
    (testing "shape constraints"
      (let [db1            @(fluree/stage db0 {"@context" ["https://ns.flur.ee" context]
                                               "insert"
                                               [{"id"          "ex:CoolShape"
                                                 "type"        "sh:NodeShape"
                                                 "sh:property" [{"sh:path"     {"id" "ex:isCool"}
                                                                 "sh:hasValue" true
                                                                 "sh:minCount" 1}]}
                                                {"id"             "ex:PersonShape"
                                                 "type"           "sh:NodeShape"
                                                 "sh:targetClass" {"id" "ex:Person"}
                                                 "sh:property"    [{"sh:path"     {"id" "ex:cool"}
                                                                    "sh:node"     {"id" "ex:CoolShape"}
                                                                    "sh:minCount" 1}]}]})
            valid-person   @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                               "insert"   {"id"      "ex:Bob"
                                                           "type"    "ex:Person"
                                                           "ex:cool" {"ex:isCool" true}}})
            invalid-person @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                               "insert"   {"id"      "ex:Reto"
                                                           "type"    "ex:Person"
                                                           "ex:cool" {"ex:isCool" false}}})]
        (is (= [{"id"      "ex:Bob",
                 "type"    "ex:Person",
                 "ex:cool" {"ex:isCool" true}}]
               @(fluree/query valid-person {"@context" context
                                            "select"   {"ex:Bob" ["*" {"ex:cool" ["ex:isCool"]}]}})))
        (is (= {:status 400,
                :error :shacl/violation,
                :report
                [{:subject "ex:Reto",
                  :constraint "sh:node",
                  :shape "_:fdb-7",
                  :expect ["ex:CoolShape"],
                  :path ["ex:cool"],
                  :value "_:fdb-11",
                  :message "node _:fdb-11 does not conform to shapes [\"ex:CoolShape\"]"}]}
               (ex-data invalid-person)))
        (is (= "Subject ex:Reto path [\"ex:cool\"] violates constraint sh:node of shape _:fdb-7 - node _:fdb-11 does not conform to shapes [\"ex:CoolShape\"]."
               (ex-message invalid-person)))))
    (testing "extended path constraints"
      (let [db1            @(fluree/stage db0 {"@context" ["https://ns.flur.ee" context]
                                               "insert"   {"id"             "ex:PersonShape"
                                                           "type"           "sh:NodeShape"
                                                           "sh:targetClass" {"id" "ex:Person"}
                                                           "sh:property"    [{"sh:path"
                                                                              {"@list" [{"id" "ex:cool"}
                                                                                        {"id" "ex:dude"}]}
                                                                              "sh:nodeKind" {"id" "sh:BlankNode"}
                                                                              "sh:minCount" 1}]}})
            valid-person   @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                               "insert"   {"id"      "ex:Bob"
                                                           "type"    "ex:Person"
                                                           "ex:cool" {"ex:dude" {"ex:isBlank" true}}}})
            invalid-person @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                               "insert"   {"id"      "ex:Reto"
                                                           "type"    "ex:Person"
                                                           "ex:cool" {"ex:dude" {"id"         "ex:Dude"
                                                                                 "ex:isBlank" false}}}})]
        (is (= [{"id"      "ex:Bob",
                 "type"    "ex:Person",
                 "ex:cool" {"ex:dude" {"ex:isBlank" true}}}]
               @(fluree/query valid-person {"@context" context
                                            "select"   {"ex:Bob" ["*" {"ex:cool" [{"ex:dude" ["ex:isBlank"]}]}]}})))
        (is (= {:status 400,
                :error :shacl/violation,
                :report
                [{:subject "ex:Reto",
                  :constraint "sh:nodeKind",
                  :shape "_:fdb-13",
                  :expect "sh:BlankNode",
                  :path ["ex:cool" "ex:dude"],
                  :value "ex:Dude",
                  :message "value ex:Dude is is not of kind sh:BlankNode"}]}
               (ex-data invalid-person)))
        (is (= "Subject ex:Reto path [\"ex:cool\" \"ex:dude\"] violates constraint sh:nodeKind of shape _:fdb-13 - value ex:Dude is is not of kind sh:BlankNode."
               (ex-message invalid-person)))))))
