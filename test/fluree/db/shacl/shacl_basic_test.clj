(ns fluree.db.shacl.shacl-basic-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.core :as util]))

(deftest ^:integration using-pre-defined-types-as-classes
  (testing "Class not used as class initially can still be used as one."
    (let [conn      (test-utils/create-conn)
          ledger    @(fluree/create conn "class/testing")
          context   [test-utils/default-context {:ex "http://example.org/ns/"}]
          db1       @(fluree/stage
                      (fluree/db ledger)
                      {"@context" context
                       "insert"   {:id                 :ex/MyClass
                                   :schema/description "Just a basic object not used as a class"}})
          db2       @(fluree/stage
                      db1
                      {:context context
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
    (let [conn       (test-utils/create-conn)
          ledger     @(fluree/create conn "shacl/a")
          context    [test-utils/default-context {:ex "http://example.org/ns/"}]
          user-query {:context context
                      :select  {'?s [:*]}
                      :where   {:id '?s, :type :ex/User}}
          db         @(fluree/stage
                       (fluree/db ledger)
                       {"@context" context
                        "insert"
                        {:id             :ex/UserShape
                         :type           [:sh/NodeShape]
                         :sh/targetClass :ex/User
                         :sh/property    [{:id :ex/pshape1
                                           :sh/path     :schema/name
                                           :sh/minCount 1
                                           :sh/maxCount 1
                                           :sh/datatype :xsd/string}]}})]
      (testing "cardinality ok"
        (let [db-ok @(fluree/stage
                      db
                      {"@context" context
                       "insert"
                       {:id              :ex/john
                        :type            :ex/User
                        :schema/name     "John"
                        :schema/callSign "j-rock"}})]
          (is (= [{:id              :ex/john,
                   :type            :ex/User,
                   :schema/name     "John",
                   :schema/callSign "j-rock"}]
                 @(fluree/query db-ok user-query))
              "basic rdf:type query response not correct")))
      (testing "cardinality less than"
        (let [db-no-names @(fluree/stage
                            db
                            {"@context" context
                             "insert"
                             {:id              :ex/john
                              :type            :ex/User
                              :schema/callSign "j-rock"}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/minCount,
                     :sh/sourceShape         :ex/pshape1
                     :sh/value               0,
                     :f/expectation          1,
                     :sh/resultMessage       "count 0 is less than minimum count of 1",
                     :sh/resultPath          [:schema/name]}]}}
                 (ex-data db-no-names)))
          (is (= "Subject :ex/john path [:schema/name] violates constraint :sh/minCount of shape :ex/pshape1 - count 0 is less than minimum count of 1."
                 (ex-message db-no-names)))))
      (testing "cardinality greater than"
        (let [db-two-names @(fluree/stage
                             db
                             {"@context" context
                              "insert"
                              {:id              :ex/john
                               :type            :ex/User
                               :schema/name     ["John", "Johnny"]
                               :schema/callSign "j-rock"}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/maxCount,
                     :sh/sourceShape         :ex/pshape1,
                     :sh/value               2,
                     :f/expectation          1,
                     :sh/resultMessage       "count 2 is greater than maximum count of 1",
                     :sh/resultPath          [:schema/name]}]}}
                 (ex-data db-two-names)))
          (is (= "Subject :ex/john path [:schema/name] violates constraint :sh/maxCount of shape :ex/pshape1 - count 2 is greater than maximum count of 1."
                 (ex-message db-two-names))))))))

(deftest ^:integration shacl-datatype-constraints
  (testing "shacl datatype errors"
    (let [conn       (test-utils/create-conn)
          ledger     @(fluree/create conn "shacl/b")
          context    [test-utils/default-context {:ex "http://example.org/ns/"}]
          user-query {:context context
                      :select  {'?s [:*]}
                      :where   {:id '?s, :type :ex/User}}
          db         @(fluree/stage
                       (fluree/db ledger)
                       {"@context" context
                        "insert"
                        {:id             :ex/UserShape
                         :type           :sh/NodeShape
                         :sh/targetClass :ex/User
                         :sh/property    [{:id          :ex/pshape1
                                           :sh/path     :schema/name
                                           :sh/datatype :xsd/string}]}})]
      (testing "datatype ok"
        (let [db-ok @(fluree/stage
                      db
                      {"@context" context
                       "insert"
                       {:id          :ex/john
                        :type        :ex/User
                        :schema/name "John"}})]
          (is (= @(fluree/query db-ok user-query)
                 [{:id          :ex/john
                   :type        :ex/User
                   :schema/name "John"}])
              "basic rdf:type query response not correct")))
      (testing "incorrect literal type"
        (let [db-int-name @(fluree/stage
                            db
                            {"@context" context
                             "insert"
                             {:id          :ex/john
                              :type        :ex/User
                               ;; need to specify type inline in order to avoid coercion
                              :schema/name {:type :xsd/integer :value 42}}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/datatype,
                     :sh/sourceShape         :ex/pshape1,
                     :sh/value               [:xsd/integer],
                     :f/expectation          :xsd/string,
                     :sh/resultMessage       "the following values do not have expected datatype :xsd/string: 42",
                     :sh/resultPath          [:schema/name]}]}}
                 (ex-data db-int-name)))
          (is (= "Subject :ex/john path [:schema/name] violates constraint :sh/datatype of shape :ex/pshape1 - the following values do not have expected datatype :xsd/string: 42."
                 (ex-message db-int-name)))))
      (testing "incorrect ref type"
        (let [db-bool-name @(fluree/stage
                             db
                             {"@context" context
                              "insert"
                              {:id          :ex/john
                               :type        :ex/User
                               :schema/name {:id :ex/john}}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/datatype,
                     :sh/sourceShape         :ex/pshape1,
                     :sh/value               [:id],
                     :f/expectation          :xsd/string,
                     :sh/resultMessage       "the following values do not have expected datatype :xsd/string: :ex/john",
                     :sh/resultPath          [:schema/name]}]}}
                 (ex-data db-bool-name))
              "Exception, because :schema/name is a boolean and not a string.")
          (is (= "Subject :ex/john path [:schema/name] violates constraint :sh/datatype of shape :ex/pshape1 - the following values do not have expected datatype :xsd/string: :ex/john."
                 (ex-message db-bool-name))))))))

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
                       {"@context" context
                        "insert"
                        {:id                   :ex/UserShape
                         :type                 :sh/NodeShape
                         :sh/targetClass       :ex/User
                         :sh/property          [{:sh/path     :schema/name
                                                 :sh/datatype :xsd/string}]
                         :sh/closed            true
                         :sh/ignoredProperties [:type]}})]
      (testing "no extra properties"
        (let [db-ok @(fluree/stage
                      db
                      {"@context" context
                       "insert"
                       {:id          :ex/john
                        :type        :ex/User
                        :schema/name "John"}})]
          (is (= [{:id          :ex/john
                   :type        :ex/User
                   :schema/name "John"}]
                 @(fluree/query db-ok user-query))
              "basic type query response not correct")))
      (testing "extra properties"
        (let [db-extra-prop @(fluree/stage
                              db
                              {"@context" context
                               "insert"
                               {:id           :ex/john
                                :type         :ex/User
                                :schema/name  "John"
                                :schema/email "john@flur.ee"}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/closed,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               ["john@flur.ee"],
                     :f/expectation          [:type :schema/name],
                     :sh/resultMessage       "disallowed path :schema/email with values john@flur.ee"}]}}
                 (ex-data db-extra-prop)))
          (is (= "Subject :ex/john violates constraint :sh/closed of shape :ex/UserShape - disallowed path :schema/email with values john@flur.ee."
                 (ex-message db-extra-prop))))))))

(deftest ^:integration shacl-property-pairs
  (testing "shacl property pairs"
    (let [conn       (test-utils/create-conn)
          ledger     @(fluree/create conn "shacl/pairs")
          context    [test-utils/default-context {:ex "http://example.org/ns/"}]
          user-query {:context context
                      :select  {'?s [:*]}
                      :where   {:id '?s, :type :ex/User}}]
      (testing "single-cardinality equals"
        (let [db @(fluree/stage
                   (fluree/db ledger)
                   {"@context" context
                    "insert"
                    {:id             :ex/EqualNamesShape
                     :type           :sh/NodeShape
                     :sh/targetClass :ex/User
                     :sh/property    [{:id        :ex/pshape1
                                       :sh/path   :schema/name
                                       :sh/equals :ex/firstName}]}})

              db-not-equal @(fluree/stage
                             db
                             {"@context" context
                              "insert"
                              {:id           :ex/john
                               :type         :ex/User
                               :schema/name  "John"
                               :ex/firstName "Jack"}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/equals,
                     :sh/sourceShape         :ex/pshape1,
                     :sh/value               ["John"],
                     :f/expectation          ["Jack"],
                     :sh/resultMessage       "path [:schema/name] values John do not equal :ex/firstName values Jack",
                     :sh/resultPath          [:schema/name]}]}}
                 (ex-data db-not-equal)))
          (is (= "Subject :ex/john path [:schema/name] violates constraint :sh/equals of shape :ex/pshape1 - path [:schema/name] values John do not equal :ex/firstName values Jack."
                 (ex-message db-not-equal)))
          (let [db-ok @(fluree/stage
                        db
                        {"@context" context
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
                   {"@context" context
                    "insert"
                    {:id             :ex/EqualNamesShape
                     :type           :sh/NodeShape
                     :sh/targetClass :ex/User
                     :sh/property    [{:id        :ex/pshape1
                                       :sh/path   :ex/favNums
                                       :sh/equals :ex/luckyNums}]}})]
          (let [db-not-equal1 @(fluree/stage
                                db
                                {"@context" context
                                 "insert"
                                 {:id           :ex/brian
                                  :type         :ex/User
                                  :schema/name  "Brian"
                                  :ex/favNums   [11 17]
                                  :ex/luckyNums [13 18]}})]
            (is (= {:status 422,
                    :error  :shacl/violation,
                    :report
                    {:type        :sh/ValidationReport,
                     :sh/conforms false,
                     :sh/result
                     [{:type                   :sh/ValidationResult,
                       :sh/resultSeverity      :sh/Violation
                       :sh/focusNode           :ex/brian,
                       :sh/constraintComponent :sh/equals,
                       :sh/sourceShape         :ex/pshape1
                       :sh/value               [11 17],
                       :f/expectation          [13 18],
                       :sh/resultMessage       "path [:ex/favNums] values 11, 17 do not equal :ex/luckyNums values 13, 18",
                       :sh/resultPath          [:ex/favNums]}]}}
                   (ex-data db-not-equal1)))
            (is (= "Subject :ex/brian path [:ex/favNums] violates constraint :sh/equals of shape :ex/pshape1 - path [:ex/favNums] values 11, 17 do not equal :ex/luckyNums values 13, 18."
                   (ex-message db-not-equal1))))
          (let [db-not-equal2 @(fluree/stage
                                db
                                {"@context" context
                                 "insert"
                                 {:id           :ex/brian
                                  :type         :ex/User
                                  :schema/name  "Brian"
                                  :ex/favNums   [11 17]
                                  :ex/luckyNums [11]}})]
            (is (= {:status 422,
                    :error  :shacl/violation,
                    :report
                    {:type        :sh/ValidationReport,
                     :sh/conforms false,
                     :sh/result
                     [{:type                   :sh/ValidationResult,
                       :sh/resultSeverity      :sh/Violation
                       :sh/focusNode           :ex/brian,
                       :sh/constraintComponent :sh/equals,
                       :sh/sourceShape         :ex/pshape1,
                       :sh/value               [11 17],
                       :f/expectation          [11],
                       :sh/resultMessage       "path [:ex/favNums] values 11, 17 do not equal :ex/luckyNums values 11",
                       :sh/resultPath          [:ex/favNums]}]}}
                   (ex-data db-not-equal2)))
            (is (= "Subject :ex/brian path [:ex/favNums] violates constraint :sh/equals of shape :ex/pshape1 - path [:ex/favNums] values 11, 17 do not equal :ex/luckyNums values 11."
                   (ex-message db-not-equal2))))
          (let [db-not-equal3 @(fluree/stage
                                db
                                {"@context" context
                                 "insert"
                                 {:id           :ex/brian
                                  :type         :ex/User
                                  :schema/name  "Brian"
                                  :ex/favNums   [11 17]
                                  :ex/luckyNums [11 17 18]}})]
            (is (= {:status 422,
                    :error  :shacl/violation,
                    :report
                    {:type        :sh/ValidationReport,
                     :sh/conforms false,
                     :sh/result
                     [{:type                   :sh/ValidationResult,
                       :sh/resultSeverity      :sh/Violation
                       :sh/focusNode           :ex/brian,
                       :sh/constraintComponent :sh/equals,
                       :sh/sourceShape         :ex/pshape1,
                       :sh/value               [11 17],
                       :f/expectation          [17 11 18],
                       :sh/resultMessage       "path [:ex/favNums] values 11, 17 do not equal :ex/luckyNums values 11, 17, 18",
                       :sh/resultPath          [:ex/favNums]}]}}
                   (ex-data db-not-equal3)))
            (is (= "Subject :ex/brian path [:ex/favNums] violates constraint :sh/equals of shape :ex/pshape1 - path [:ex/favNums] values 11, 17 do not equal :ex/luckyNums values 11, 17, 18."
                   (ex-message db-not-equal3))))
          (let [db-not-equal4 @(fluree/stage
                                db
                                {"@context" context
                                 "insert"
                                 {:id           :ex/brian
                                  :type         :ex/User
                                  :schema/name  "Brian"
                                  :ex/favNums   [11 17]
                                  :ex/luckyNums ["11" "17"]}})]
            (is (= {:status 422,
                    :error  :shacl/violation,
                    :report
                    {:type        :sh/ValidationReport,
                     :sh/conforms false,
                     :sh/result
                     [{:type                   :sh/ValidationResult,
                       :sh/resultSeverity      :sh/Violation
                       :sh/focusNode           :ex/brian,
                       :sh/constraintComponent :sh/equals,
                       :sh/sourceShape         :ex/pshape1
                       :sh/value               [11 17],
                       :f/expectation          ["17" "11"],
                       :sh/resultMessage       "path [:ex/favNums] values 11, 17 do not equal :ex/luckyNums values 11, 17",
                       :sh/resultPath          [:ex/favNums]}]}}
                   (ex-data db-not-equal4)))
            (is (= "Subject :ex/brian path [:ex/favNums] violates constraint :sh/equals of shape :ex/pshape1 - path [:ex/favNums] values 11, 17 do not equal :ex/luckyNums values 11, 17."
                   (ex-message db-not-equal4))))
          (let [db-ok @(fluree/stage
                        db
                        {"@context" context
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
                         {"@context" context
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
        (let [db @(fluree/stage
                   (fluree/db ledger)
                   {"@context" context
                    "insert"
                    {:id             :ex/DisjointShape
                     :type           :sh/NodeShape
                     :sh/targetClass :ex/User
                     :sh/property    [{:id          :ex/pshape1
                                       :sh/path     :ex/favNums
                                       :sh/disjoint :ex/luckyNums}]}})]
          (testing "disjoint values"
            (let [db-ok @(fluree/stage
                          db
                          {"@context" context
                           "insert"
                           {:id           :ex/alice
                            :type         :ex/User
                            :schema/name  "Alice"
                            :ex/favNums   [11 17]
                            :ex/luckyNums 1}})]
              (is (= [{:id           :ex/alice
                       :type         :ex/User
                       :schema/name  "Alice"
                       :ex/favNums   [11 17]
                       :ex/luckyNums 1}]
                     @(fluree/query db-ok user-query)))))
          (testing "single not disjoint value"
            (let [db-not-disjoint1 @(fluree/stage
                                     db
                                     {"@context" context
                                      "insert"
                                      {:id           :ex/brian
                                       :type         :ex/User
                                       :schema/name  "Brian"
                                       :ex/favNums   11
                                       :ex/luckyNums 11}})]
              (is (= {:status 422,
                      :error  :shacl/violation,
                      :report
                      {:type        :sh/ValidationReport,
                       :sh/conforms false,
                       :sh/result
                       [{:type                   :sh/ValidationResult,
                         :sh/resultSeverity      :sh/Violation
                         :sh/focusNode           :ex/brian,
                         :sh/constraintComponent :sh/disjoint,
                         :sh/sourceShape         :ex/pshape1
                         :sh/value               [11],
                         :f/expectation          [11],
                         :sh/resultMessage       "path [:ex/favNums] values 11 are not disjoint with :ex/luckyNums values 11",
                         :sh/resultPath          [:ex/favNums]}]}}
                     (ex-data db-not-disjoint1)))
              (is (= "Subject :ex/brian path [:ex/favNums] violates constraint :sh/disjoint of shape :ex/pshape1 - path [:ex/favNums] values 11 are not disjoint with :ex/luckyNums values 11."
                     (ex-message db-not-disjoint1)))))
          (testing "multiple disjoint tests"
            (let [db-not-disjoint2 @(fluree/stage
                                     db
                                     {"@context" context
                                      "insert"
                                      {:id           :ex/brian
                                       :type         :ex/User
                                       :schema/name  "Brian"
                                       :ex/favNums   [11 17 31]
                                       :ex/luckyNums 11}})]
              (is (= {:status 422,
                      :error  :shacl/violation,
                      :report
                      {:type        :sh/ValidationReport,
                       :sh/conforms false,
                       :sh/result
                       [{:type                   :sh/ValidationResult,
                         :sh/resultSeverity      :sh/Violation
                         :sh/focusNode           :ex/brian,
                         :sh/constraintComponent :sh/disjoint,
                         :sh/sourceShape         :ex/pshape1
                         :sh/value               [11 17 31],
                         :f/expectation          [11],
                         :sh/resultMessage       "path [:ex/favNums] values 11, 17, 31 are not disjoint with :ex/luckyNums values 11",
                         :sh/resultPath          [:ex/favNums]}]}}
                     (ex-data db-not-disjoint2))
                  "Exception, because :ex/favNums is not disjoint from :ex/luckyNums")
              (is (= "Subject :ex/brian path [:ex/favNums] violates constraint :sh/disjoint of shape :ex/pshape1 - path [:ex/favNums] values 11, 17, 31 are not disjoint with :ex/luckyNums values 11."
                     (ex-message db-not-disjoint2)))))
          (testing "multiple non disjoint values"
            (let [db-not-disjoint3 @(fluree/stage
                                     db
                                     {"@context" context
                                      "insert"
                                      {:id           :ex/brian
                                       :type         :ex/User
                                       :schema/name  "Brian"
                                       :ex/favNums   [11 17 31]
                                       :ex/luckyNums [13 18 11]}})]
              (is (= {:status 422,
                      :error  :shacl/violation,
                      :report
                      {:type        :sh/ValidationReport,
                       :sh/conforms false,
                       :sh/result
                       [{:type                   :sh/ValidationResult,
                         :sh/resultSeverity      :sh/Violation
                         :sh/focusNode           :ex/brian,
                         :sh/constraintComponent :sh/disjoint,
                         :sh/sourceShape         :ex/pshape1
                         :sh/value               [11 17 31],
                         :f/expectation          [13 11 18],
                         :sh/resultMessage       "path [:ex/favNums] values 11, 17, 31 are not disjoint with :ex/luckyNums values 11, 13, 18",
                         :sh/resultPath          [:ex/favNums]}]}}
                     (ex-data db-not-disjoint3))
                  "Exception, because :ex/favNums is not disjoint from :ex/luckyNums")
              (is (= "Subject :ex/brian path [:ex/favNums] violates constraint :sh/disjoint of shape :ex/pshape1 - path [:ex/favNums] values 11, 17, 31 are not disjoint with :ex/luckyNums values 11, 13, 18."
                     (ex-message db-not-disjoint3)))))))
      (testing "lessThan"
        (let [db     @(fluree/stage
                       (fluree/db ledger)
                       {"@context" context
                        "insert"
                        {:id             :ex/LessThanShape
                         :type           :sh/NodeShape
                         :sh/targetClass :ex/User
                         :sh/property    [{:id          :ex/pshape1
                                           :sh/path     :ex/p1
                                           :sh/lessThan :ex/p2}]}})
              db-ok1 @(fluree/stage
                       db
                       {"@context" context
                        "insert"
                        {:id          :ex/alice
                         :type        :ex/User
                         :schema/name "Alice"
                         :ex/p1       [11 17]
                         :ex/p2       [18 19]}})

              db-ok2 @(fluree/stage
                       db
                       {"@context" context
                        "insert"
                        {:id          :ex/alice
                         :type        :ex/User
                         :schema/name "Alice"
                         :ex/p1       [11 17]
                         :ex/p2       [18]}})]
          (testing "values less than"
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
                   @(fluree/query db-ok2 user-query))))
          (testing "values not less than other value"
            (let [db-fail1 @(fluree/stage
                             db
                             {"@context" context
                              "insert"
                              {:id          :ex/alice
                               :type        :ex/User
                               :schema/name "Alice"
                               :ex/p1       [11 17]
                               :ex/p2       17}})]
              (is (= {:status 422,
                      :error  :shacl/violation,
                      :report
                      {:type        :sh/ValidationReport,
                       :sh/conforms false,
                       :sh/result
                       [{:type                   :sh/ValidationResult,
                         :sh/resultSeverity      :sh/Violation
                         :sh/focusNode           :ex/alice,
                         :sh/constraintComponent :sh/lessThan,
                         :sh/sourceShape         :ex/pshape1
                         :sh/value               [11 17],
                         :f/expectation          [17],
                         :sh/resultMessage       "path [:ex/p1] values 11, 17 are not all less than :ex/p2 values 17",
                         :sh/resultPath          [:ex/p1]}]}}
                     (ex-data db-fail1)))
              (is (= "Subject :ex/alice path [:ex/p1] violates constraint :sh/lessThan of shape :ex/pshape1 - path [:ex/p1] values 11, 17 are not all less than :ex/p2 values 17."
                     (ex-message db-fail1)))))
          (testing "values not comparable to other values"
            (let [db-fail2 @(fluree/stage
                             db
                             {"@context" context
                              "insert"
                              {:id          :ex/alice
                               :type        :ex/User
                               :schema/name "Alice"
                               :ex/p1       [11 17]
                               :ex/p2       ["18" "19"]}})]
              (is (= {:status 422,
                      :error  :shacl/violation,
                      :report
                      {:type        :sh/ValidationReport,
                       :sh/conforms false,
                       :sh/result
                       [{:type                   :sh/ValidationResult,
                         :sh/resultSeverity      :sh/Violation
                         :sh/focusNode           :ex/alice,
                         :sh/constraintComponent :sh/lessThan,
                         :sh/sourceShape         :ex/pshape1
                         :sh/value               [11 17],
                         :f/expectation          ["19" "18"],
                         :sh/resultMessage       "path [:ex/p1] values 11, 17 are not all comparable with :ex/p2 values 18, 19",
                         :sh/resultPath          [:ex/p1]}]}}
                     (ex-data db-fail2)))
              (is (= "Subject :ex/alice path [:ex/p1] violates constraint :sh/lessThan of shape :ex/pshape1 - path [:ex/p1] values 11, 17 are not all comparable with :ex/p2 values 18, 19."
                     (ex-message db-fail2)))))
          (testing "values not less than all other values"
            (let [db-fail3 @(fluree/stage
                             db
                             {"@context" context
                              "insert"
                              {:id          :ex/alice
                               :type        :ex/User
                               :schema/name "Alice"
                               :ex/p1       [12 17]
                               :ex/p2       [10 18]}})]
              (is (= {:status 422,
                      :error  :shacl/violation,
                      :report
                      {:type        :sh/ValidationReport,
                       :sh/conforms false,
                       :sh/result
                       [{:type                   :sh/ValidationResult,
                         :sh/resultSeverity      :sh/Violation
                         :sh/focusNode           :ex/alice,
                         :sh/constraintComponent :sh/lessThan,
                         :sh/sourceShape         :ex/pshape1
                         :sh/value               [12 17],
                         :f/expectation          [10 18],
                         :sh/resultMessage       "path [:ex/p1] values 12, 17 are not all less than :ex/p2 values 10, 18",
                         :sh/resultPath          [:ex/p1]}]}}
                     (ex-data db-fail3)))
              (is (= "Subject :ex/alice path [:ex/p1] violates constraint :sh/lessThan of shape :ex/pshape1 - path [:ex/p1] values 12, 17 are not all less than :ex/p2 values 10, 18."
                     (ex-message db-fail3)))))
          (testing "values not less than all"
            (let [db-fail4 @(fluree/stage
                             db
                             {"@context" context
                              "insert"
                              {:id          :ex/alice
                               :type        :ex/User
                               :schema/name "Alice"
                               :ex/p1       [11 17]
                               :ex/p2       [12 16]}})]
              (is (= {:status 422,
                      :error  :shacl/violation,
                      :report
                      {:type        :sh/ValidationReport,
                       :sh/conforms false,
                       :sh/result
                       [{:type                   :sh/ValidationResult,
                         :sh/resultSeverity      :sh/Violation
                         :sh/focusNode           :ex/alice,
                         :sh/constraintComponent :sh/lessThan,
                         :sh/sourceShape         :ex/pshape1
                         :sh/value               [11 17],
                         :f/expectation          [12 16],
                         :sh/resultMessage       "path [:ex/p1] values 11, 17 are not all less than :ex/p2 values 12, 16",
                         :sh/resultPath          [:ex/p1]}]}}
                     (ex-data db-fail4)))
              (is (= "Subject :ex/alice path [:ex/p1] violates constraint :sh/lessThan of shape :ex/pshape1 - path [:ex/p1] values 11, 17 are not all less than :ex/p2 values 12, 16."
                     (ex-message db-fail4)))))
          (testing "not comparable with iris"
            (let [db-iris @(fluree/stage
                            db
                            {"@context" context
                             "insert"
                             {:id          :ex/alice
                              :type        :ex/User
                              :schema/name "Alice"
                              :ex/p1       :ex/brian
                              :ex/p2       :ex/john}})]
              (is (= {:status 422,
                      :error  :shacl/violation,
                      :report
                      {:type        :sh/ValidationReport,
                       :sh/conforms false,
                       :sh/result
                       [{:type                   :sh/ValidationResult,
                         :sh/resultSeverity      :sh/Violation
                         :sh/focusNode           :ex/alice,
                         :sh/constraintComponent :sh/lessThan,
                         :sh/sourceShape         :ex/pshape1
                         :sh/value               [:ex/brian],
                         :f/expectation          [:ex/john],
                         :sh/resultMessage       "path [:ex/p1] values :ex/brian are not all comparable with :ex/p2 values :ex/john",
                         :sh/resultPath          [:ex/p1]}]}}
                     (ex-data db-iris)))
              (is (= "Subject :ex/alice path [:ex/p1] violates constraint :sh/lessThan of shape :ex/pshape1 - path [:ex/p1] values :ex/brian are not all comparable with :ex/p2 values :ex/john."
                     (ex-message db-iris)))))))
      (testing "lessThanOrEquals"
        (let [db @(fluree/stage
                   (fluree/db ledger)
                   {"@context" context
                    "insert"
                    {:id             :ex/LessThanOrEqualsShape
                     :type           :sh/NodeShape
                     :sh/targetClass :ex/User
                     :sh/property    [{:id                  :ex/pshape1
                                       :sh/path             :ex/p1
                                       :sh/lessThanOrEquals :ex/p2}]}})]
          (testing "all values less than or equal"
            (let [db-ok1 @(fluree/stage
                           db
                           {"@context" context
                            "insert"
                            {:id          :ex/alice
                             :type        :ex/User
                             :schema/name "Alice"
                             :ex/p1       [11 17]
                             :ex/p2       [17 19]}})

                  db-ok2 @(fluree/stage
                           db
                           {"@context" context
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
                     @(fluree/query db-ok2 user-query)))))
          (testing "all values not less than other value"
            (let [db-fail1 @(fluree/stage
                             db
                             {"@context" context
                              "insert"
                              {:id          :ex/alice
                               :type        :ex/User
                               :schema/name "Alice"
                               :ex/p1       [11 17]
                               :ex/p2       10}})]
              (is (= {:status 422,
                      :error  :shacl/violation,
                      :report
                      {:type        :sh/ValidationReport,
                       :sh/conforms false,
                       :sh/result
                       [{:type                   :sh/ValidationResult,
                         :sh/resultSeverity      :sh/Violation
                         :sh/focusNode           :ex/alice,
                         :sh/constraintComponent :sh/lessThanOrEquals,
                         :sh/sourceShape         :ex/pshape1
                         :sh/value               [11 17],
                         :f/expectation          [10],
                         :sh/resultMessage       "path [:ex/p1] values 11, 17 are not all less than :ex/p2 values 10",
                         :sh/resultPath          [:ex/p1]}]}}
                     (ex-data db-fail1)))
              (is (= "Subject :ex/alice path [:ex/p1] violates constraint :sh/lessThanOrEquals of shape :ex/pshape1 - path [:ex/p1] values 11, 17 are not all less than :ex/p2 values 10."
                     (ex-message db-fail1)))))
          (testing "all values not comparable with other values"
            (let [db-fail2 @(fluree/stage
                             db
                             {"@context" context
                              "insert"
                              {:id          :ex/alice
                               :type        :ex/User
                               :schema/name "Alice"
                               :ex/p1       [11 17]
                               :ex/p2       ["17" "19"]}})]
              (is (= {:status 422,
                      :error  :shacl/violation,
                      :report
                      {:type        :sh/ValidationReport,
                       :sh/conforms false,
                       :sh/result
                       [{:type                   :sh/ValidationResult,
                         :sh/resultSeverity      :sh/Violation
                         :sh/focusNode           :ex/alice,
                         :sh/constraintComponent :sh/lessThanOrEquals,
                         :sh/sourceShape         :ex/pshape1
                         :sh/value               [11 17],
                         :f/expectation          ["19" "17"],
                         :sh/resultMessage       "path [:ex/p1] values 11, 17 are not all comparable with :ex/p2 values 17, 19",
                         :sh/resultPath          [:ex/p1]}]}}
                     (ex-data db-fail2)))
              (is (= "Subject :ex/alice path [:ex/p1] violates constraint :sh/lessThanOrEquals of shape :ex/pshape1 - path [:ex/p1] values 11, 17 are not all comparable with :ex/p2 values 17, 19."
                     (ex-message db-fail2)))))
          (testing "all values not less than other values"
            (let [db-fail3 @(fluree/stage
                             db
                             {"@context" context
                              "insert"
                              {:id          :ex/alice
                               :type        :ex/User
                               :schema/name "Alice"
                               :ex/p1       [12 17]
                               :ex/p2       [10 17]}})]
              (is (= {:status 422,
                      :error  :shacl/violation,
                      :report
                      {:type        :sh/ValidationReport,
                       :sh/conforms false,
                       :sh/result
                       [{:type                   :sh/ValidationResult,
                         :sh/resultSeverity      :sh/Violation
                         :sh/focusNode           :ex/alice,
                         :sh/constraintComponent :sh/lessThanOrEquals,
                         :sh/sourceShape         :ex/pshape1
                         :sh/value               [12 17],
                         :f/expectation          [17 10],
                         :sh/resultMessage       "path [:ex/p1] values 12, 17 are not all less than :ex/p2 values 10, 17",
                         :sh/resultPath          [:ex/p1]}]}}
                     (ex-data db-fail3)))
              (is (= "Subject :ex/alice path [:ex/p1] violates constraint :sh/lessThanOrEquals of shape :ex/pshape1 - path [:ex/p1] values 12, 17 are not all less than :ex/p2 values 10, 17."
                     (ex-message db-fail3)))))
          (testing "all values not less than all other values"
            (let [db-fail4 @(fluree/stage
                             db
                             {"@context" context
                              "insert"
                              {:id          :ex/alice
                               :type        :ex/User
                               :schema/name "Alice"
                               :ex/p1       [11 17]
                               :ex/p2       [12 16]}})]
              (is (= {:status 422,
                      :error  :shacl/violation,
                      :report
                      {:type        :sh/ValidationReport,
                       :sh/conforms false,
                       :sh/result
                       [{:type                   :sh/ValidationResult,
                         :sh/resultSeverity      :sh/Violation
                         :sh/focusNode           :ex/alice,
                         :sh/constraintComponent :sh/lessThanOrEquals,
                         :sh/sourceShape         :ex/pshape1
                         :sh/value               [11 17],
                         :f/expectation          [12 16],
                         :sh/resultMessage       "path [:ex/p1] values 11, 17 are not all less than :ex/p2 values 12, 16",
                         :sh/resultPath          [:ex/p1]}]}}
                     (ex-data db-fail4)))
              (is (= "Subject :ex/alice path [:ex/p1] violates constraint :sh/lessThanOrEquals of shape :ex/pshape1 - path [:ex/p1] values 11, 17 are not all less than :ex/p2 values 12, 16."
                     (ex-message db-fail4))))))))))

(deftest ^:integration shacl-value-range
  (testing "shacl value range constraints"
    (let [conn       (test-utils/create-conn)
          ledger     @(fluree/create conn "shacl/value-range")
          context    [test-utils/default-context {:ex "http://example.org/ns/"}]
          user-query {:context context
                      :select  {'?s [:*]}
                      :where   {:id '?s, :type :ex/User}}]
      (testing "exclusive constraints"
        (let [db @(fluree/stage
                   (fluree/db ledger)
                   {"@context" context
                    "insert"
                    {:id             :ex/ExclusiveNumRangeShape
                     :type           :sh/NodeShape
                     :sh/targetClass :ex/User
                     :sh/property    [{:id              :ex/pshape1
                                       :sh/path         :schema/age
                                       :sh/minExclusive 1
                                       :sh/maxExclusive 100}]}})]
          (testing "values in range"
            (let [db-ok @(fluree/stage
                          db
                          {"@context" context
                           "insert"
                           {:id         :ex/john
                            :type       :ex/User
                            :schema/age 2}})]
              (is (= [{:id         :ex/john
                       :type       :ex/User
                       :schema/age 2}]
                     @(fluree/query db-ok user-query)))))
          (testing "values too low"
            (let [db-too-low @(fluree/stage
                               db
                               {"@context" context
                                "insert"
                                {:id         :ex/john
                                 :type       :ex/User
                                 :schema/age 1}})]
              (is (= {:status 422,
                      :error  :shacl/violation,
                      :report
                      {:type        :sh/ValidationReport,
                       :sh/conforms false,
                       :sh/result
                       [{:type                   :sh/ValidationResult,
                         :sh/resultSeverity      :sh/Violation
                         :sh/focusNode           :ex/john,
                         :sh/constraintComponent :sh/minExclusive,
                         :sh/sourceShape         :ex/pshape1,
                         :sh/value               1,
                         :f/expectation          1,
                         :sh/resultMessage       "value 1 is less than exclusive minimum 1",
                         :sh/resultPath          [:schema/age]}]}}
                     (ex-data db-too-low)))
              (is (= "Subject :ex/john path [:schema/age] violates constraint :sh/minExclusive of shape :ex/pshape1 - value 1 is less than exclusive minimum 1."
                     (ex-message db-too-low)))))
          (testing "values too high"
            (let [db-too-high @(fluree/stage
                                db
                                {"@context" context
                                 "insert"
                                 {:id         :ex/john
                                  :type       :ex/User
                                  :schema/age 100}})]
              (is (= {:status 422,
                      :error  :shacl/violation,
                      :report
                      {:type        :sh/ValidationReport,
                       :sh/conforms false,
                       :sh/result
                       [{:type                   :sh/ValidationResult,
                         :sh/resultSeverity      :sh/Violation
                         :sh/focusNode           :ex/john,
                         :sh/constraintComponent :sh/maxExclusive,
                         :sh/sourceShape         :ex/pshape1,
                         :sh/value               100,
                         :f/expectation          100,
                         :sh/resultMessage       "value 100 is greater than exclusive maximum 100",
                         :sh/resultPath          [:schema/age]}]}}
                     (ex-data db-too-high)))
              (is (= "Subject :ex/john path [:schema/age] violates constraint :sh/maxExclusive of shape :ex/pshape1 - value 100 is greater than exclusive maximum 100."
                     (ex-message db-too-high)))))))
      (testing "inclusive constraints"
        (let [db @(fluree/stage
                   (fluree/db ledger)
                   {"@context" context
                    "insert"
                    {:id             :ex/InclusiveNumRangeShape
                     :type           :sh/NodeShape
                     :sh/targetClass :ex/User
                     :sh/property    [{:id              :ex/pshape1
                                       :sh/path         :schema/age
                                       :sh/minInclusive 1
                                       :sh/maxInclusive 100}]}})]
          (testing "values at limit"
            (let [db-ok  @(fluree/stage
                           db
                           {"@context" context
                            "insert"
                            {:id         :ex/brian
                             :type       :ex/User
                             :schema/age 1}})
                  db-ok2 @(fluree/stage
                           db-ok
                           {"@context" context
                            "insert"
                            {:id         :ex/alice
                             :type       :ex/User
                             :schema/age 100}})]
              (is (= [{:id         :ex/alice
                       :type       :ex/User
                       :schema/age 100}
                      {:id         :ex/brian
                       :type       :ex/User
                       :schema/age 1}]
                     @(fluree/query db-ok2 user-query)))))
          (testing "values below min"
            (let [db-too-low @(fluree/stage
                               db
                               {"@context" context
                                "insert"
                                {:id         :ex/alice
                                 :type       :ex/User
                                 :schema/age 0}})]
              (is (= {:status 422,
                      :error  :shacl/violation,
                      :report
                      {:type        :sh/ValidationReport,
                       :sh/conforms false,
                       :sh/result
                       [{:type                   :sh/ValidationResult,
                         :sh/resultSeverity      :sh/Violation
                         :sh/focusNode           :ex/alice,
                         :sh/constraintComponent :sh/minInclusive,
                         :sh/sourceShape         :ex/pshape1
                         :sh/value               0,
                         :f/expectation          1,
                         :sh/resultMessage       "value 0 is less than inclusive minimum 1",
                         :sh/resultPath          [:schema/age]}]}}
                     (ex-data db-too-low)))
              (is (= "Subject :ex/alice path [:schema/age] violates constraint :sh/minInclusive of shape :ex/pshape1 - value 0 is less than inclusive minimum 1."
                     (ex-message db-too-low)))))
          (testing "values above max"
            (let [db-too-high @(fluree/stage
                                db
                                {"@context" context
                                 "insert"
                                 {:id         :ex/alice
                                  :type       :ex/User
                                  :schema/age 101}})]
              (is (= {:status 422,
                      :error  :shacl/violation,
                      :report
                      {:type        :sh/ValidationReport,
                       :sh/conforms false,
                       :sh/result
                       [{:type                   :sh/ValidationResult,
                         :sh/resultSeverity      :sh/Violation
                         :sh/focusNode           :ex/alice,
                         :sh/constraintComponent :sh/maxInclusive,
                         :sh/sourceShape         :ex/pshape1
                         :sh/value               101,
                         :f/expectation          100,
                         :sh/resultMessage       "value 101 is greater than inclusive maximum 100",
                         :sh/resultPath          [:schema/age]}]}}
                     (ex-data db-too-high)))
              (is (= "Subject :ex/alice path [:schema/age] violates constraint :sh/maxInclusive of shape :ex/pshape1 - value 101 is greater than inclusive maximum 100."
                     (ex-message db-too-high)))))))
      (testing "non-numeric values"
        (let [db         @(fluree/stage
                           (fluree/db ledger)
                           {"@context" context
                            "insert"
                            {:id             :ex/NumRangeShape
                             :type           :sh/NodeShape
                             :sh/targetClass :ex/User
                             :sh/property    [{:id              :ex/pshape1
                                               :sh/path         :schema/age
                                               :sh/minExclusive 0}]}})
              db-subj-id @(fluree/stage
                           db
                           {"@context" context
                            "insert"
                            {:id         :ex/alice
                             :type       :ex/User
                             :schema/age :ex/brian}})
              db-string  @(fluree/stage
                           db
                           {"@context" context
                            "insert"
                            {:id         :ex/alice
                             :type       :ex/User
                             :schema/age "10"}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation
                     :sh/focusNode           :ex/alice,
                     :sh/constraintComponent :sh/minExclusive,
                     :sh/sourceShape         :ex/pshape1
                     :sh/value               :ex/brian,
                     :f/expectation          0,
                     :sh/resultMessage       "value :ex/brian is less than exclusive minimum 0",
                     :sh/resultPath          [:schema/age]}]}}
                 (ex-data db-subj-id)))
          (is (= "Subject :ex/alice path [:schema/age] violates constraint :sh/minExclusive of shape :ex/pshape1 - value :ex/brian is less than exclusive minimum 0."
                 (ex-message db-subj-id)))

          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation
                     :sh/focusNode           :ex/alice,
                     :sh/constraintComponent :sh/minExclusive,
                     :sh/sourceShape         :ex/pshape1
                     :sh/value               "10",
                     :f/expectation          0,
                     :sh/resultMessage       "value 10 is less than exclusive minimum 0",
                     :sh/resultPath          [:schema/age]}]}}
                 (ex-data db-string)))
          (is (= "Subject :ex/alice path [:schema/age] violates constraint :sh/minExclusive of shape :ex/pshape1 - value 10 is less than exclusive minimum 0."
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
                       {"@context" context
                        "insert"
                        {:id             :ex/UserShape
                         :type           :sh/NodeShape
                         :sh/targetClass :ex/User
                         :sh/property    [{:id           :ex/pshape1
                                           :sh/path      :schema/name
                                           :sh/minLength 4
                                           :sh/maxLength 10}]}})]
      (testing "string is correct length"
        (let [db-ok-str @(fluree/stage
                          db
                          {"@context" context
                           "insert"
                           {:id          :ex/john
                            :type        :ex/User
                            :schema/name "John"}})]
          (is (= [{:id          :ex/john
                   :type        :ex/User
                   :schema/name "John"}]
                 @(fluree/query db-ok-str user-query)))))
      (testing "non-string literals are stringified and checked"
        (let [db-ok-non-str @(fluree/stage
                              db
                              {"@context" context
                               "insert"
                               {:id          :ex/john
                                :type        :ex/User
                                :schema/name 12345}})]
          (is (= [{:id          :ex/john
                   :type        :ex/User
                   :schema/name 12345}]
                 @(fluree/query db-ok-non-str user-query)))))
      (testing "string is too short"
        (let [db-too-short-str @(fluree/stage
                                 db
                                 {"@context" context
                                  "insert"
                                  {:id          :ex/al
                                   :type        :ex/User
                                   :schema/name "Al"}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation
                     :sh/focusNode           :ex/al,
                     :sh/constraintComponent :sh/minLength,
                     :sh/sourceShape         :ex/pshape1,
                     :sh/value               "Al",
                     :f/expectation          4,
                     :sh/resultMessage       "value \"Al\" has string length less than minimum length 4",
                     :sh/resultPath          [:schema/name]}]}}
                 (ex-data db-too-short-str)))
          (is (= "Subject :ex/al path [:schema/name] violates constraint :sh/minLength of shape :ex/pshape1 - value \"Al\" has string length less than minimum length 4."
                 (ex-message db-too-short-str)))))
      (testing "string is too long"
        (let [db-too-long-str @(fluree/stage
                                db
                                {"@context" context
                                 "insert"
                                 {:id          :ex/jean-claude
                                  :type        :ex/User
                                  :schema/name "Jean-Claude"}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation
                     :sh/focusNode           :ex/jean-claude,
                     :sh/constraintComponent :sh/maxLength,
                     :sh/sourceShape         :ex/pshape1,
                     :sh/value               "Jean-Claude",
                     :f/expectation          10,
                     :sh/resultMessage       "value \"Jean-Claude\" has string length greater than maximum length 10",
                     :sh/resultPath          [:schema/name]}]}}
                 (ex-data db-too-long-str)))
          (is (= "Subject :ex/jean-claude path [:schema/name] violates constraint :sh/maxLength of shape :ex/pshape1 - value \"Jean-Claude\" has string length greater than maximum length 10."
                 (ex-message db-too-long-str)))))
      (testing "non-string literals are stringified"
        (let [db-too-long-non-str @(fluree/stage
                                    db
                                    {"@context" context
                                     "insert"
                                     {:id          :ex/john
                                      :type        :ex/User
                                      :schema/name 12345678910}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/maxLength,
                     :sh/sourceShape         :ex/pshape1,
                     :sh/value               12345678910,
                     :f/expectation          10,
                     :sh/resultMessage       "value \"12345678910\" has string length greater than maximum length 10",
                     :sh/resultPath          [:schema/name]}]}}
                 (ex-data db-too-long-non-str)))
          (is (= "Subject :ex/john path [:schema/name] violates constraint :sh/maxLength of shape :ex/pshape1 - value \"12345678910\" has string length greater than maximum length 10."
                 (ex-message db-too-long-non-str)))))
      (testing "non-literal values violate"
        (let [db-ref-value @(fluree/stage
                             db
                             {"@context" context
                              "insert"
                              {:id          :ex/john
                               :type        :ex/User
                               :schema/name :ex/ref}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/maxLength,
                     :sh/sourceShape         :ex/pshape1,
                     :sh/value               #fluree/SID [101 "ref"],
                     :f/expectation          10,
                     :sh/resultMessage       "value :ex/ref is not a literal value",
                     :sh/resultPath          [:schema/name]}]}}
                 (ex-data db-ref-value)))
          (is (= "Subject :ex/john path [:schema/name] violates constraint :sh/maxLength of shape :ex/pshape1 - value :ex/ref is not a literal value."
                 (ex-message db-ref-value))))))))

(deftest ^:integration shacl-string-pattern-constraints
  (testing "shacl string regex constraint errors"
    (let [conn       (test-utils/create-conn)
          ledger     @(fluree/create conn "shacl/str")
          context    [test-utils/default-context {:ex "http://example.org/ns/"}]
          user-query {:context context
                      :select  {'?s [:*]}
                      :where   {:id '?s, :type :ex/User}}
          db         @(fluree/stage
                       (fluree/db ledger)
                       {"@context" context
                        "insert"
                        {:id             :ex/UserShape
                         :type           [:sh/NodeShape]
                         :sh/targetClass :ex/User
                         :sh/property    [{:id         :ex/pshape1
                                           :sh/path    :ex/greeting
                                           :sh/pattern "hello   (.*?)world"
                                           :sh/flags   ["x" "s"]}
                                          {:id         :ex/pshape2
                                           :sh/path    :ex/birthYear
                                           :sh/pattern "(19|20)[0-9][0-9]"}]}})]
      (testing "string matches pattern"
        (let [db-ok-greeting @(fluree/stage
                               db
                               {"@context" context
                                "insert"
                                {:id          :ex/brian
                                 :type        :ex/User
                                 :ex/greeting "hello\nworld!"}})]
          (is (= [{:id          :ex/brian
                   :type        :ex/User
                   :ex/greeting "hello\nworld!"}]
                 @(fluree/query db-ok-greeting user-query)))))
      (testing "stringified literal matches pattern"
        (let [db-ok-birthyear @(fluree/stage
                                db
                                {"@context" context
                                 "insert"
                                 {:id           :ex/john
                                  :type         :ex/User
                                  :ex/birthYear 1984}})]
          (is (= [{:id           :ex/john
                   :type         :ex/User
                   :ex/birthYear 1984}]
                 @(fluree/query db-ok-birthyear user-query)))))
      (testing "string does not match pattern"
        (let [db-wrong-case-greeting @(fluree/stage
                                       db
                                       {"@context" context
                                        "insert"
                                        {:id          :ex/alice
                                         :type        :ex/User
                                         :ex/greeting "HELLO\nWORLD!"}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation
                     :sh/focusNode           :ex/alice,
                     :sh/constraintComponent :sh/pattern,
                     :sh/sourceShape         :ex/pshape1,
                     :sh/value               "HELLO
WORLD!",
                     :f/expectation          "hello   (.*?)world",
                     :sh/resultMessage       (str "value "
                                                  (pr-str "HELLO
WORLD!")
                                                  " does not match pattern \"hello   (.*?)world\" with :sh/flags s, x")
                     :sh/resultPath          [:ex/greeting]}]}}
                 (ex-data db-wrong-case-greeting)))
          (is (= (str "Subject :ex/alice path [:ex/greeting] violates constraint :sh/pattern of shape :ex/pshape1 - value "
                      (pr-str "HELLO
WORLD!")
                      " does not match pattern \"hello   (.*?)world\" with :sh/flags s, x.")
                 (ex-message db-wrong-case-greeting)))))
      (testing "stringified literal does not match pattern"
        (let [db-wrong-birth-year @(fluree/stage
                                    db
                                    {"@context" context
                                     "insert"
                                     {:id           :ex/alice
                                      :type         :ex/User
                                      :ex/birthYear 1776}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:type                   :sh/ValidationResult,
                     :sh/focusNode           :ex/alice,
                     :sh/resultSeverity      :sh/Violation
                     :sh/constraintComponent :sh/pattern,
                     :sh/sourceShape         :ex/pshape2
                     :sh/value               1776,
                     :f/expectation          "(19|20)[0-9][0-9]",
                     :sh/resultMessage       "value \"1776\" does not match pattern \"(19|20)[0-9][0-9]\"",
                     :sh/resultPath          [:ex/birthYear]}]}}
                 (ex-data db-wrong-birth-year)))
          (is (= "Subject :ex/alice path [:ex/birthYear] violates constraint :sh/pattern of shape :ex/pshape2 - value \"1776\" does not match pattern \"(19|20)[0-9][0-9]\"."
                 (ex-message db-wrong-birth-year)))))
      (testing "non-literal values automatically produce violation"
        (let [db-ref-value @(fluree/stage
                             db
                             {"@context" context
                              "insert"
                              {:id           :ex/john
                               :type         :ex/User
                               :ex/birthYear :ex/ref}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/pattern,
                     :sh/sourceShape         :ex/pshape2
                     :sh/value               #fluree/SID [101 "ref"],
                     :f/expectation          "(19|20)[0-9][0-9]",
                     :sh/resultMessage       "value \":ex/ref\" does not match pattern \"(19|20)[0-9][0-9]\"",
                     :sh/resultPath          [:ex/birthYear]}]}}
                 (ex-data db-ref-value)))
          (is (= "Subject :ex/john path [:ex/birthYear] violates constraint :sh/pattern of shape :ex/pshape2 - value \":ex/ref\" does not match pattern \"(19|20)[0-9][0-9]\"."
                 (ex-message db-ref-value))))))))

(deftest language-constraints
  (let [conn    @(fluree/connect-memory)
        ledger  @(fluree/create conn "validation-report")
        context [test-utils/default-str-context
                 {"ex" "http://example.com/ns/"}]
        db0     (fluree/db ledger)]
    (testing "language-in"
      (let [db1 @(fluree/stage db0 {"@context" context
                                    "insert"
                                    {"@id"           "ex:langShape"
                                     "type"          "sh:NodeShape"
                                     "sh:targetNode" {"@id" "ex:a"}
                                     "sh:property"   [{"id"            "ex:pshape1"
                                                       "sh:path"       {"@id" "ex:label"}
                                                       "sh:languageIn" ["en" "fr"]}]}})]
        (testing "no error when language conforms"
          (let [db2 @(fluree/stage db1 {"@context" context
                                        "insert"
                                        {"@id"      "ex:a"
                                         "ex:label" {"@value" "foo" "@language" "en"}}})]
            (is (nil? (ex-data db2)))))
        (testing "error when disallowed language transacted"
          (let [db2 @(fluree/stage db1 {"@context" context
                                        "insert"
                                        {"@id"      "ex:a"
                                         "ex:label" {"@value" "foo" "@language" "cz"}}})]
            (is (= {:status 422,
                    :error  :shacl/violation,
                    :report
                    {"type"        "sh:ValidationReport",
                     "sh:conforms" false,
                     "sh:result"
                     [{"sh:constraintComponent" "sh:languageIn",
                       "sh:focusNode"           "ex:a",
                       "sh:resultSeverity"      "sh:Violation",
                       "sh:value"               "foo",
                       "sh:resultPath"          ["ex:label"],
                       "type"                   "sh:ValidationResult",
                       "sh:resultMessage"
                       "value \"foo\" does not have language tag in [\"en\" \"fr\"]",
                       "sh:sourceShape"         "ex:pshape1"
                       "f:expectation"          ["en" "fr"]}]}}
                   (ex-data db2)))
            (is (= "Subject ex:a path [\"ex:label\"] violates constraint sh:languageIn of shape ex:pshape1 - value \"foo\" does not have language tag in [\"en\" \"fr\"]."
                   (ex-message db2)))))))
    (testing "unique-lang"
      (let [db1 @(fluree/stage db0 {"@context" context
                                    "insert"
                                    {"@id"           "ex:langShape"
                                     "type"          "sh:NodeShape"
                                     "sh:targetNode" {"@id" "ex:a"}
                                     "sh:property"   [{"id"            "ex:pshape1"
                                                       "sh:path"       {"@id" "ex:label"}
                                                       "sh:uniqueLang" true}]}})]
        (testing "no error when all langs unique"
          (let [db2 @(fluree/stage db1 {"@context" context
                                        "insert"
                                        {"@id"      "ex:a"
                                         "ex:label" [{"@value" "foo" "@language" "en"}
                                                     {"@value" "feuou" "@language" "fr"}]}})]
            (is (nil? (ex-data db2)))))
        (testing "error when a lang is repeated"
          (let [db2 @(fluree/stage db1 {"@context" context
                                        "insert"
                                        {"@id"      "ex:a"
                                         "ex:label" [{"@value" "foo" "@language" "en"}
                                                     {"@value" "bar" "@language" "en"}]}})]
            (is (= {:status 422,
                    :error  :shacl/violation,
                    :report
                    {"type"        "sh:ValidationReport",
                     "sh:conforms" false,
                     "sh:result"
                     [{"sh:constraintComponent" "sh:uniqueLang",
                       "sh:focusNode"           "ex:a",
                       "sh:resultSeverity"      "sh:Violation",
                       "sh:value"               false,
                       "sh:resultPath"          ["ex:label"],
                       "type"                   "sh:ValidationResult",
                       "sh:resultMessage"       "values [\"bar\" \"foo\"] do not have unique language tags",
                       "sh:sourceShape"         "ex:pshape1",
                       "f:expectation"          true}]}}
                   (ex-data db2)))
            (is (= "Subject ex:a path [\"ex:label\"] violates constraint sh:uniqueLang of shape ex:pshape1 - values [\"bar\" \"foo\"] do not have unique language tags."
                   (ex-message db2)))))))))

(deftest ^:integration shacl-multiple-properties-test
  (testing "multiple properties works"
    (let [conn       (test-utils/create-conn)
          ledger     @(fluree/create conn "shacl/b")
          context    [test-utils/default-context {:ex "http://example.org/ns/"}]
          user-query {:context context
                      :select  {'?s [:*]}
                      :where   {:id '?s, :type :ex/User}}
          db         @(fluree/stage
                       (fluree/db ledger)
                       {"@context" context
                        "insert"
                        {:id             :ex/UserShape
                         :type           :sh/NodeShape
                         :sh/targetClass :ex/User
                         :sh/property    [{:id          :ex/pshape1
                                           :sh/path     :schema/name
                                           :sh/datatype :xsd/string
                                           :sh/minCount 1
                                           :sh/maxCount 1}
                                          {:id              :ex/pshape2
                                           :sh/path         :schema/age
                                           :sh/minCount     1
                                           :sh/maxCount     1
                                           :sh/minInclusive 0
                                           :sh/maxInclusive 130}
                                          {:id          :ex/pshape3
                                           :sh/path     :schema/email
                                           :sh/datatype :xsd/string}]}})]
      (testing "all constraints satisfied"
        (let [db-ok @(fluree/stage
                      db
                      {"@context" context
                       "insert"
                       {:id           :ex/john
                        :type         :ex/User
                        :schema/name  "John"
                        :schema/age   40
                        :schema/email "john@example.org"}})]
          (is (= [{:id           :ex/john
                   :type         :ex/User
                   :schema/age   40
                   :schema/email "john@example.org"
                   :schema/name  "John"}]
                 @(fluree/query db-ok user-query)))))
      (testing "one constraint violated"
        (let [db-no-name @(fluree/stage
                           db
                           {"@context" context
                            "insert"
                            {:id           :ex/john
                             :type         :ex/User
                             :schema/age   40
                             :schema/email "john@example.org"}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:sh/constraintComponent :sh/minCount,
                     :type                   :sh/ValidationResult,
                     :sh/resultMessage       "count 0 is less than minimum count of 1",
                     :sh/resultPath          [:schema/name],
                     :f/expectation          1,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/value               0,
                     :sh/sourceShape         :ex/pshape1,
                     :sh/focusNode           :ex/john}]}}
                 (ex-data db-no-name)))
          (is (= "Subject :ex/john path [:schema/name] violates constraint :sh/minCount of shape :ex/pshape1 - count 0 is less than minimum count of 1."
                 (ex-message db-no-name)))))
      (testing "cardinality constraint violated"
        (let [db-two-names @(fluree/stage
                             db
                             {"@context" context
                              "insert"
                              {:id           :ex/john
                               :type         :ex/User
                               :schema/name  ["John" "Billy"]
                               :schema/age   40
                               :schema/email "john@example.org"}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:sh/constraintComponent :sh/maxCount,
                     :type                   :sh/ValidationResult,
                     :sh/resultMessage       "count 2 is greater than maximum count of 1",
                     :sh/resultPath          [:schema/name],
                     :f/expectation          1,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/value               2,
                     :sh/sourceShape         :ex/pshape1,
                     :sh/focusNode           :ex/john}]}}
                 (ex-data db-two-names)))
          (is (= "Subject :ex/john path [:schema/name] violates constraint :sh/maxCount of shape :ex/pshape1 - count 2 is greater than maximum count of 1."
                 (ex-message db-two-names)))))
      (testing "max constraint violated"
        (let [db-too-old @(fluree/stage
                           db
                           {"@context" context
                            "insert"
                            {:id           :ex/john
                             :type         :ex/User
                             :schema/name  "John"
                             :schema/age   140
                             :schema/email "john@example.org"}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:sh/constraintComponent :sh/maxInclusive,
                     :type                   :sh/ValidationResult,
                     :sh/resultMessage       "value 140 is greater than inclusive maximum 130",
                     :sh/resultPath          [:schema/age],
                     :f/expectation          130,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/value               140,
                     :sh/sourceShape         :ex/pshape2
                     :sh/focusNode           :ex/john}]}}
                 (ex-data db-too-old)))
          (is (= "Subject :ex/john path [:schema/age] violates constraint :sh/maxInclusive of shape :ex/pshape2 - value 140 is greater than inclusive maximum 130."
                 (ex-message db-too-old)))))
      (testing "second cardinality constraint violated"
        (let [db-two-ages @(fluree/stage
                            db
                            {"@context" context
                             "insert"
                             {:id           :ex/john
                              :type         :ex/User
                              :schema/name  "John"
                              :schema/age   [40 21]
                              :schema/email "john@example.org"}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:sh/constraintComponent :sh/maxCount,
                     :type                   :sh/ValidationResult,
                     :sh/resultMessage       "count 2 is greater than maximum count of 1",
                     :sh/resultPath          [:schema/age],
                     :f/expectation          1,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/value               2,
                     :sh/sourceShape         :ex/pshape2
                     :sh/focusNode           :ex/john}]}}
                 (ex-data db-two-ages)))
          (is (= "Subject :ex/john path [:schema/age] violates constraint :sh/maxCount of shape :ex/pshape2 - count 2 is greater than maximum count of 1."
                 (ex-message db-two-ages)))))
      (testing "datatype constraint violated"
        (let [db-num-email @(fluree/stage
                             db
                             {"@context" context
                              "insert"
                              {:id           :ex/john
                               :type         :ex/User
                               :schema/name  "John"
                               :schema/age   40
                               :schema/email 42}})]
          (is (= {:error  :shacl/violation
                  :report {:sh/conforms false
                           :sh/result   [{:f/expectation          :xsd/string
                                          :sh/constraintComponent :sh/datatype
                                          :sh/focusNode           :ex/john
                                          :sh/resultMessage       "the following values do not have expected datatype :xsd/string: 42"
                                          :sh/resultPath          [:schema/email]
                                          :sh/resultSeverity      :sh/Violation
                                          :sh/sourceShape         :ex/pshape3
                                          :sh/value               [:xsd/integer]
                                          :type                   :sh/ValidationResult}]
                           :type        :sh/ValidationReport}
                  :status 422}
                 (ex-data db-num-email)))
          (is (= "Subject :ex/john path [:schema/email] violates constraint :sh/datatype of shape :ex/pshape3 - the following values do not have expected datatype :xsd/string: 42."
                 (ex-message db-num-email))))))))

(deftest ^:integration property-paths
  (let [conn    @(fluree/connect-memory)
        ledger  @(fluree/create conn "propertypathstest")
        context [test-utils/default-str-context {"ex" "http://example.com/"}]
        db0     (fluree/db ledger)]
    (testing "inverse path"
      (let [;; a valid Parent is anybody who is the object of a parent predicate
            db1          @(fluree/stage db0 {"@context" context
                                             "insert"   {"@type"          "sh:NodeShape"
                                                         "id"             "ex:ParentShape"
                                                         "sh:targetClass" {"@id" "ex:Parent"}
                                                         "sh:property"    [{"id" "ex:pshape1"
                                                                            "sh:path"     {"sh:inversePath" {"id" "ex:parent"}}
                                                                            "sh:minCount" 1}]}})
            valid-parent @(fluree/stage db1 {"@context" context
                                             "insert"   {"id"          "ex:Luke"
                                                         "schema:name" "Luke"
                                                         "ex:parent"   {"id"          "ex:Anakin"
                                                                        "type"        "ex:Parent"
                                                                        "schema:name" "Anakin"}}})
            invalid-pal  @(fluree/stage db1 {"@context" context
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

        (is (= {:status 422,
                :error :shacl/violation,
                :report
                {"type" "sh:ValidationReport",
                 "sh:conforms" false,
                 "sh:result"
                 [{"sh:constraintComponent" "sh:minCount",
                   "sh:focusNode" "ex:bad-parent",
                   "sh:resultSeverity" "sh:Violation",
                   "sh:value" 0,
                   "sh:resultPath" [{"sh:inversePath" "ex:parent"}],
                   "type" "sh:ValidationResult",
                   "sh:resultMessage" "count 0 is less than minimum count of 1",
                   "sh:sourceShape" "ex:pshape1"
                   "f:expectation" 1}]}}
               (ex-data invalid-pal)))
        (is (= "Subject ex:bad-parent path [{\"sh:inversePath\" \"ex:parent\"}] violates constraint sh:minCount of shape ex:pshape1 - count 0 is less than minimum count of 1."
               (ex-message invalid-pal)))))
    (testing "sequence paths"
      (let [;; a valid Pal is anybody who has a pal with a name
            db1         @(fluree/stage db0 {"@context" context
                                            "insert"   {"@type"          "sh:NodeShape"
                                                        "sh:targetClass" {"@id" "ex:Pal"}
                                                        "sh:property"
                                                        [{"id" "ex:pshape1"
                                                          "sh:path"
                                                          {"@list" [{"id" "ex:pal"} {"id" "schema:name"}]}
                                                          "sh:minCount" 1}]}})
            valid-pal   @(fluree/stage db1 {"@context" context
                                            "insert"   {"id"          "ex:good-pal"
                                                        "type"        "ex:Pal"
                                                        "schema:name" "J.D."
                                                        "ex:pal"      [{"schema:name" "Turk"}
                                                                       {"schema:name" "Rowdy"}]}})
            invalid-pal @(fluree/stage db1 {"@context" context
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
        (is (= {:status 422,
                :error :shacl/violation,
                :report
                {"type" "sh:ValidationReport",
                 "sh:conforms" false,
                 "sh:result"
                 [{"sh:constraintComponent" "sh:minCount",
                   "sh:focusNode" "ex:bad-pal",
                   "sh:resultSeverity" "sh:Violation",
                   "sh:value" 0,
                   "sh:resultPath" ["ex:pal" "schema:name"],
                   "type" "sh:ValidationResult",
                   "sh:resultMessage" "count 0 is less than minimum count of 1",
                   "sh:sourceShape" "ex:pshape1",
                   "f:expectation" 1}]}}
               (ex-data invalid-pal)))
        (is (= "Subject ex:bad-pal path [\"ex:pal\" \"schema:name\"] violates constraint sh:minCount of shape ex:pshape1 - count 0 is less than minimum count of 1."
               (ex-message invalid-pal)))))
    (testing "sequence paths"
      (let [db1         @(fluree/stage db0 {"@context" context
                                            "insert"   [{"@type"          "sh:NodeShape"
                                                         "sh:targetClass" {"@id" "ex:Pal"}
                                                         "sh:property"
                                                         [{"id" "ex:pshape1"
                                                           "sh:path"
                                                           {"@list" [{"id" "ex:pal"} {"id" "ex:name"}]}
                                                           "sh:minCount" 1}]}]})
            valid-pal   @(fluree/stage db1 {"@context" context
                                            "insert"   {"id"      "ex:jd"
                                                        "type"    "ex:Pal"
                                                        "ex:name" "J.D."
                                                        "ex:pal"  [{"ex:name" "Turk"}
                                                                   {"ex:name" "Rowdy"}]}})

            invalid-pal @(fluree/stage db1 {"@context" context
                                            "insert"   {"id"      "ex:jd"
                                                        "type"    "ex:Pal"
                                                        "ex:name" "J.D."
                                                        "ex:pal"  [{"id" "ex:not-pal"
                                                                    "ex:not-name" "noname"}
                                                                   {"id" "ex:turk"
                                                                    "ex:name" "Turk"}
                                                                   {"id" "ex:rowdy"
                                                                    "ex:name" "Rowdy"}]}})]

        (is (= {"id" "ex:jd",
                "type" "ex:Pal",
                "ex:name" "J.D.",
                "ex:pal" [{"ex:name" "Rowdy"} {"ex:name" "Turk"}]}
               (-> @(fluree/query valid-pal {"@context" context
                                             "select"   {"ex:jd" ["*" {"ex:pal" ["ex:name"]}]}})
                   (first)
                   (update "ex:pal" #(sort-by first %)))))
        (is (= {:status 422,
                :error :shacl/violation,
                :report
                {"type" "sh:ValidationReport",
                 "sh:conforms" false,
                 "sh:result"
                 [{"sh:constraintComponent" "sh:minCount",
                   "sh:focusNode" "ex:jd",
                   "sh:resultSeverity" "sh:Violation",
                   "sh:value" 0,
                   "sh:resultPath" ["ex:pal" "ex:name"],
                   "type" "sh:ValidationResult",
                   "sh:resultMessage" "count 0 is less than minimum count of 1",
                   "sh:sourceShape" "ex:pshape1",
                   "f:expectation" 1}]}}
               (ex-data invalid-pal)))
        (is (= "Subject ex:jd path [\"ex:pal\" \"ex:name\"] violates constraint sh:minCount of shape ex:pshape1 - count 0 is less than minimum count of 1."
               (ex-message invalid-pal)))))

    (testing "predicate-path"
      (let [db1 @(fluree/stage db0 {"@context" context
                                    "insert" [{"@type" "sh:NodeShape"
                                               "sh:targetClass" {"@id" "ex:Named"}
                                               "sh:property"
                                               [{"id" "ex:pshape1"
                                                 "sh:path"
                                                 {"@list" [{"id" "ex:name"}]}
                                                 "sh:datatype" {"id" "xsd:string"}}]}]})
            valid-named   @(fluree/stage db1 {"@context" context
                                              "insert"   {"id"      "ex:good-pal"
                                                          "type"    "ex:Named"
                                                          "ex:name" {"@value" 123
                                                                     "@type" "xsd:integer"}}})]
        (is (= {:status 422,
                :error :shacl/violation,
                :report
                {"type" "sh:ValidationReport",
                 "sh:conforms" false,
                 "sh:result"
                 [{"sh:constraintComponent" "sh:datatype",
                   "sh:focusNode" "ex:good-pal",
                   "sh:resultSeverity" "sh:Violation",
                   "sh:value" ["xsd:integer"],
                   "sh:resultPath" ["ex:name"],
                   "type" "sh:ValidationResult",
                   "sh:resultMessage"
                   "the following values do not have expected datatype xsd:string: 123",
                   "sh:sourceShape" "ex:pshape1",
                   "f:expectation" "xsd:string"}]}}
               (ex-data valid-named)))))
    (testing "inverse sequence path"
      (let [;; a valid Princess is anybody who is the child of someone's queen
            db1              @(fluree/stage db0 {"@context" context
                                                 "insert"   {"@type"          "sh:NodeShape"
                                                             "id"             "ex:PrincessShape"
                                                             "sh:targetClass" {"@id" "ex:Princess"}
                                                             "sh:property"    [{"id" "ex:pshape1"
                                                                                "sh:path"
                                                                                {"@list"
                                                                                 [{"sh:inversePath"
                                                                                   {"id" "ex:child"}}
                                                                                  {"sh:inversePath"
                                                                                   {"id" "ex:queen"}}]}
                                                                                "sh:minCount" 1}]}})
            valid-princess   @(fluree/stage db1 {"@context" context
                                                 "insert"   {"id"          "ex:Pleb"
                                                             "schema:name" "Pleb"
                                                             "ex:queen"    {"id"          "ex:Buttercup"
                                                                            "schema:name" "Buttercup"
                                                                            "ex:child"    {"id"          "ex:Mork"
                                                                                           "type"        "ex:Princess"
                                                                                           "schema:name" "Mork"}}}})
            invalid-princess @(fluree/stage db1 {"@context" context
                                                 "insert"   {"id"          "ex:Pleb"
                                                             "schema:name" "Pleb"
                                                             "ex:child"    {"id"          "ex:Gerb"
                                                                            "type"        "ex:Princess"
                                                                            "schema:name" "Gerb"}}})]
        (is (= [{"id" "ex:Mork", "type" "ex:Princess", "schema:name" "Mork"}]
               @(fluree/query valid-princess {"@context" context
                                              "select"   {"ex:Mork" ["*"]}})))

        (is (= {:status 422,
                :error :shacl/violation,
                :report
                {"type" "sh:ValidationReport",
                 "sh:conforms" false,
                 "sh:result"
                 [{"sh:constraintComponent" "sh:minCount",
                   "sh:focusNode" "ex:Gerb",
                   "sh:resultSeverity" "sh:Violation",
                   "sh:value" 0,
                   "sh:resultPath"
                   [{"sh:inversePath" "ex:child"} {"sh:inversePath" "ex:queen"}],
                   "type" "sh:ValidationResult",
                   "sh:resultMessage" "count 0 is less than minimum count of 1",
                   "sh:sourceShape" "ex:pshape1",
                   "f:expectation" 1}]}}
               (ex-data invalid-princess)))
        (is (= "Subject ex:Gerb path [{\"sh:inversePath\" \"ex:child\"} {\"sh:inversePath\" \"ex:queen\"}] violates constraint sh:minCount of shape ex:pshape1 - count 0 is less than minimum count of 1."
               (ex-message invalid-princess)))))))

(deftest ^:integration shacl-class-test
  (let [conn    @(fluree/connect-memory)
        ledger  @(fluree/create conn "classtest")
        context test-utils/default-str-context
        db0     (fluree/db ledger)
        db1     @(fluree/stage db0 {"@context" context
                                    "insert"   [{"@type"          "sh:NodeShape"
                                                 "sh:targetClass" {"@id" "https://example.com/Country"}
                                                 "sh:property"
                                                 [{"id"          "ex:pshape1"
                                                   "sh:path"     {"@id" "https://example.com/name"}
                                                   "sh:datatype" {"@id" "xsd:string"}
                                                   "sh:minCount" 1
                                                   "sh:maxCount" 1}]}
                                                {"@type"          "sh:NodeShape"
                                                 "sh:targetClass" {"@id" "https://example.com/Actor"}
                                                 "sh:property"
                                                 [{"id"             "ex:pshape2"
                                                   "sh:path"        {"@id" "https://example.com/country"}
                                                   "sh:class"       {"@id" "https://example.com/Country"}
                                                   "sh:maxCount"    1
                                                   "sh:description" "Birth country"}
                                                  {"id"          "ex:pshape3"
                                                   "sh:path"     {"@id" "https://example.com/name"}
                                                   "sh:minCount" 1
                                                   "sh:maxCount" 1
                                                   "sh:datatype" {"@id" "xsd:string"}}]}]})
        ;; valid inline type

        ;; valid node ref

        ;; invalid inline type
        db4 @(fluree/stage db1 {"@context" context
                                "insert"   {"@id"                         "https://example.com/Actor/1001"
                                            "https://example.com/country" {"@id"                      "https://example.com/Country/Absurdistan"
                                                                           "@type"                    "https://example.com/FakeCountry"
                                                                           "https://example.com/name" "Absurdistan"}
                                            "https://example.com/gender"  "Male"
                                            "@type"                       "https://example.com/Actor"
                                            "https://example.com/name"    "Not Real"}})
        ;; invalid node ref type
        db5 @(fluree/stage db1 {"@context" context
                                "insert"   [{"@id"                      "https://example.com/Country/Absurdistan"
                                             "@type"                    "https://example.com/FakeCountry"
                                             "https://example.com/name" "Absurdistan"}
                                            {"@id"                         "https://example.com/Actor/8675309"
                                             "https://example.com/country" {"@id" "https://example.com/Country/Absurdistan"}
                                             "https://example.com/gender"  "Female"
                                             "@type"                       "https://example.com/Actor"
                                             "https://example.com/name"    "Jenny Tutone"}]})]
    (testing "valid inline type"
      (let [db2 @(fluree/stage db1 {"@context" context
                                    "insert"   {"@id"                           "https://example.com/Actor/65731"
                                                "https://example.com/country"   {"@id"                      "https://example.com/Country/AU"
                                                                                 "@type"                    "https://example.com/Country"
                                                                                 "https://example.com/name" "Oz"}
                                                "https://example.com/gender"    "Male"
                                                "https://example.com/character" ["Jake Sully" "Marcus Wright"]
                                                "https://example.com/movie"     [{"@id" "https://example.com/Movie/19995"}
                                                                                 {"@id" "https://example.com/Movie/534"}]
                                                "@type"                         "https://example.com/Actor"
                                                "https://example.com/name"      "Sam Worthington"}})]
        (is (not (ex-data db2)))))
    (testing "valid node ref"
      (let [db3 @(fluree/stage db1 {"@context" context
                                    "insert"   [{"@id"                      "https://example.com/Country/US"
                                                 "@type"                    "https://example.com/Country"
                                                 "https://example.com/name" "United States of America"}
                                                {"@id"                         "https://example.com/Actor/4242"
                                                 "https://example.com/country" {"@id" "https://example.com/Country/US"}
                                                 "https://example.com/gender"  "Female"
                                                 "@type"                       "https://example.com/Actor"
                                                 "https://example.com/name"    "Rindsey Rohan"}]})]
        (is (not (ex-data db3)))))
    (is (= {:status 422,
            :error  :shacl/violation,
            :report
            {"type"        "sh:ValidationReport",
             "sh:conforms" false,
             "sh:result"
             [{"sh:constraintComponent" "sh:class",
               "sh:focusNode"           "https://example.com/Actor/1001",
               "sh:resultSeverity"      "sh:Violation",
               "sh:value"               ["https://example.com/FakeCountry"],
               "sh:resultPath"          ["https://example.com/country"],
               "type"                   "sh:ValidationResult",
               "sh:resultMessage"
               "missing required class https://example.com/Country",
               "sh:sourceShape"         "ex:pshape2",
               "f:expectation"          "https://example.com/Country"}]}}
           (ex-data db4)))
    (is (= "Subject https://example.com/Actor/1001 path [\"https://example.com/country\"] violates constraint sh:class of shape ex:pshape2 - missing required class https://example.com/Country."
           (ex-message db4)))
    (is (= {:status 422,
            :error  :shacl/violation,
            :report
            {"type" "sh:ValidationReport",
             "sh:conforms" false,
             "sh:result"
             [{"sh:constraintComponent" "sh:class",
               "sh:focusNode" "https://example.com/Actor/8675309",
               "sh:resultSeverity" "sh:Violation",
               "sh:value" ["https://example.com/FakeCountry"],
               "sh:resultPath" ["https://example.com/country"],
               "type" "sh:ValidationResult",
               "sh:resultMessage"
               "missing required class https://example.com/Country",
               "sh:sourceShape" "ex:pshape2",
               "f:expectation" "https://example.com/Country"}]}}
           (ex-data db5)))
    (is (= "Subject https://example.com/Actor/8675309 path [\"https://example.com/country\"] violates constraint sh:class of shape ex:pshape2 - missing required class https://example.com/Country."
           (ex-message db5)))))

(deftest ^:integration shacl-in-test
  (testing "value nodes"
    (let [conn    @(fluree/connect-memory)
          ledger  @(fluree/create conn "shacl-in-test")
          context [test-utils/default-str-context {"ex" "http://example.com/ns/"}]
          db0     (fluree/db ledger)
          db1     @(fluree/stage db0 {"@context" context
                                      "insert"   [{"type"           ["sh:NodeShape"]
                                                   "sh:targetClass" {"id" "ex:Pony"}
                                                   "sh:property"    [{"id"      "ex:pshape1"
                                                                      "sh:path" {"id" "ex:color"}
                                                                      "sh:in"   '("cyan" "magenta")}]}]})
          db2     @(fluree/stage db1 {"@context" context
                                      "insert"   {"id"       "ex:YellowPony"
                                                  "type"     "ex:Pony"
                                                  "ex:color" "yellow"}})]
      (is (= {:status 422,
              :error  :shacl/violation,
              :report
              {"type"        "sh:ValidationReport",
               "sh:conforms" false,
               "sh:result"
               [{"sh:constraintComponent" "sh:in",
                 "sh:focusNode"           "ex:YellowPony",
                 "sh:resultSeverity"      "sh:Violation",
                 "sh:value"               "yellow",
                 "sh:resultPath"          ["ex:color"],
                 "type"                   "sh:ValidationResult",
                 "sh:resultMessage"
                 "value \"yellow\" is not in [\"cyan\" \"magenta\"]",
                 "sh:sourceShape"         "ex:pshape1",
                 "f:expectation"          ["cyan" "magenta"]}]}}
             (ex-data db2)))
      (is (= "Subject ex:YellowPony path [\"ex:color\"] violates constraint sh:in of shape ex:pshape1 - value \"yellow\" is not in [\"cyan\" \"magenta\"]."
             (ex-message db2)))))
  (testing "node refs"
    (let [conn    @(fluree/connect-memory)
          ledger  @(fluree/create conn "shacl-in-test")
          db0     (fluree/db ledger)
          context [test-utils/default-str-context {"ex" "http://example.com/ns/"}]
          db1     @(fluree/stage db0 {"@context" context
                                      "insert"   [{"type"           ["sh:NodeShape"]
                                                   "sh:targetClass" {"id" "ex:Pony"}
                                                   "sh:property"    [{"id"      "ex:pshape1"
                                                                      "sh:path" {"id" "ex:color"}
                                                                      "sh:in"   '({"id" "ex:Pink"}
                                                                                  {"id" "ex:Purple"})}]}]})
          db2     @(fluree/stage db1 {"@context" context
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
          db3     @(fluree/stage db1 {"@context" context
                                      "insert"   [{"id"       "ex:PastelPony"
                                                   "type"     "ex:Pony"
                                                   "ex:color" [{"id" "ex:Pink"}
                                                               {"id" "ex:Purple"}]}]})]
      (is (= {:status 422,
              :error  :shacl/violation,
              :report
              {"type"        "sh:ValidationReport",
               "sh:conforms" false,
               "sh:result"
               [{"sh:constraintComponent" "sh:in",
                 "sh:focusNode"           "ex:RainbowPony",
                 "sh:resultSeverity"      "sh:Violation",
                 "sh:value"               "ex:Green"
                 "sh:resultPath"          ["ex:color"],
                 "type"                   "sh:ValidationResult",
                 "sh:resultMessage"
                 "value \"ex:Green\" is not in [\"ex:Pink\" \"ex:Purple\"]",
                 "sh:sourceShape"         "ex:pshape1",
                 "f:expectation"          ["ex:Pink" "ex:Purple"]}]}}
             (ex-data db2)))
      (is (= "Subject ex:RainbowPony path [\"ex:color\"] violates constraint sh:in of shape ex:pshape1 - value \"ex:Green\" is not in [\"ex:Pink\" \"ex:Purple\"]."
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
    (let [conn    @(fluree/connect-memory)
          ledger  @(fluree/create conn "shacl-in-test")
          db0     (fluree/db ledger)
          context [test-utils/default-str-context {"ex" "http://example.com/ns/"}]
          db1     @(fluree/stage db0 {"@context" context
                                      "insert"   [{"type"           ["sh:NodeShape"]
                                                   "sh:targetClass" {"id" "ex:Pony"}
                                                   "sh:property"    [{"id"      "ex:pshape1"
                                                                      "sh:path" {"id" "ex:color"}
                                                                      "sh:in"   '({"id" "ex:Pink"}
                                                                                  {"id" "ex:Purple"}
                                                                                  "green")}]}]})
          db2     @(fluree/stage db1 {"@context" context
                                      "insert"   {"id"       "ex:RainbowPony"
                                                  "type"     "ex:Pony"
                                                  "ex:color" [{"id" "ex:Pink"}
                                                              {"id" "ex:Green"}]}})]
      (is (= {:status 422,
              :error  :shacl/violation,
              :report
              {"type"        "sh:ValidationReport",
               "sh:conforms" false,
               "sh:result"
               [{"sh:constraintComponent" "sh:in",
                 "sh:focusNode"           "ex:RainbowPony",
                 "sh:resultSeverity"      "sh:Violation",
                 "sh:value"               "ex:Green",
                 "sh:resultPath"          ["ex:color"],
                 "type"                   "sh:ValidationResult",
                 "sh:resultMessage"
                 "value \"ex:Green\" is not in [\"ex:Pink\" \"ex:Purple\" \"green\"]",
                 "sh:sourceShape"         "ex:pshape1",
                 "f:expectation"          ["ex:Pink" "ex:Purple" "green"]}]}}
             (ex-data db2)))
      (is (= "Subject ex:RainbowPony path [\"ex:color\"] violates constraint sh:in of shape ex:pshape1 - value \"ex:Green\" is not in [\"ex:Pink\" \"ex:Purple\" \"green\"]."
             (ex-message db2))))))

(deftest ^:integration shacl-targetobjectsof-test
  (let [conn    @(fluree/connect-memory)
        ledger  @(fluree/create conn "shacl-target-objects-of-test")
        context [test-utils/default-str-context {"ex" "http://example.com/ns/"}]
        db0     (fluree/db ledger)]
    (testing "subject and object of constrained predicate in the same txn"
      (testing "datatype constraint"
        (let [db1                @(fluree/stage db0
                                                {"@context" context
                                                 "insert"
                                                 {"@id"                "ex:friendShape"
                                                  "type"               ["sh:NodeShape"]
                                                  "sh:targetObjectsOf" {"@id" "ex:friend"}
                                                  "sh:property"        [{"id"          "ex:pshape1"
                                                                         "sh:path"     {"@id" "ex:name"}
                                                                         "sh:datatype" {"@id" "xsd:string"}}]}})
              db-bad-friend-name @(fluree/stage db1
                                                {"@context" context
                                                 "insert"
                                                 [{"id"        "ex:Alice"
                                                   "ex:name"   "Alice"
                                                   "type"      "ex:User"
                                                   "ex:friend" {"@id" "ex:Bob"}}
                                                  {"id"      "ex:Bob"
                                                   "ex:name" 123
                                                   "type"    "ex:User"}]})]

          (is (test-utils/shacl-error? db-bad-friend-name))))
      (testing "maxCount"
        (let [db1           @(fluree/stage db0
                                           {"@context" context
                                            "insert"
                                            {"@id"                "ex:friendShape"
                                             "type"               ["sh:NodeShape"]
                                             "sh:targetObjectsOf" {"@id" "ex:friend"}
                                             "sh:property"        [{"id"          "ex:pshape1"
                                                                    "sh:path"     {"@id" "ex:ssn"}
                                                                    "sh:maxCount" 1}]}})
              db-excess-ssn @(fluree/stage db1
                                           {"@context" context
                                            "insert"
                                            [{"id"        "ex:Alice"
                                              "ex:name"   "Alice"
                                              "type"      "ex:User"
                                              "ex:friend" {"@id" "ex:Bob"}}
                                             {"id"     "ex:Bob"
                                              "ex:ssn" ["111-11-1111"
                                                        "222-22-2222"]
                                              "type"   "ex:User"}]})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {"type"        "sh:ValidationReport",
                   "sh:conforms" false,
                   "sh:result"
                   [{"sh:constraintComponent" "sh:maxCount",
                     "sh:focusNode"           "ex:Bob",
                     "sh:resultSeverity"      "sh:Violation",
                     "sh:value"               2,
                     "sh:resultPath"          ["ex:ssn"],
                     "type"                   "sh:ValidationResult",
                     "sh:resultMessage"       "count 2 is greater than maximum count of 1",
                     "sh:sourceShape"         "ex:pshape1",
                     "f:expectation"          1}]}}
                 (ex-data db-excess-ssn)))
          (is (= "Subject ex:Bob path [\"ex:ssn\"] violates constraint sh:maxCount of shape ex:pshape1 - count 2 is greater than maximum count of 1."
                 (ex-message db-excess-ssn)))))
      (testing "required properties"
        (let [db1           @(fluree/stage db0
                                           {"@context" context
                                            "insert"
                                            [{"@id"                "ex:friendShape"
                                              "type"               ["sh:NodeShape"]
                                              "sh:targetObjectsOf" {"@id" "ex:friend"}
                                              "sh:property"        [{"id"          "ex:pshape1"
                                                                     "sh:path"     {"@id" "ex:ssn"}
                                                                     "sh:minCount" 1}]}]})
              db-just-alice @(fluree/stage db1
                                           {"@context" context
                                            "insert"
                                            [{"id"        "ex:Alice"
                                              "ex:name"   "Alice"
                                              "type"      "ex:User"
                                              "ex:friend" {"@id" "ex:Bob"}}]})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {"type"        "sh:ValidationReport",
                   "sh:conforms" false,
                   "sh:result"
                   [{"sh:constraintComponent" "sh:minCount",
                     "sh:focusNode"           "ex:Bob",
                     "sh:resultSeverity"      "sh:Violation",
                     "sh:value"               0,
                     "sh:resultPath"          ["ex:ssn"],
                     "type"                   "sh:ValidationResult",
                     "sh:resultMessage" "count 0 is less than minimum count of 1",
                     "sh:sourceShape"   "ex:pshape1",
                     "f:expectation"    1}]}}
                 (ex-data db-just-alice)))
          (is (= "Subject ex:Bob path [\"ex:ssn\"] violates constraint sh:minCount of shape ex:pshape1 - count 0 is less than minimum count of 1."
                 (ex-message db-just-alice)))))
      (testing "combined with `sh:targetClass`"
        (let [db1           @(fluree/stage db0
                                           {"@context" context
                                            "insert"
                                            [{"@id"            "ex:UserShape"
                                              "type"           ["sh:NodeShape"]
                                              "sh:targetClass" {"@id" "ex:User"}
                                              "sh:property"    [{"id"          "ex:pshape1"
                                                                 "sh:path"     {"@id" "ex:ssn"}
                                                                 "sh:maxCount" 1}]}
                                             {"@id"                "ex:friendShape"
                                              "type"               ["sh:NodeShape"]
                                              "sh:targetObjectsOf" {"@id" "ex:friend"}
                                              "sh:property"        [{"id"          "ex:pshape2"
                                                                     "sh:path"     {"@id" "ex:name"}
                                                                     "sh:maxCount" 1}]}]})
              db-bad-friend @(fluree/stage db1 {"@context" context
                                                "insert"   [{"id"        "ex:Alice"
                                                             "ex:name"   "Alice"
                                                             "type"      "ex:User"
                                                             "ex:friend" {"@id" "ex:Bob"}}
                                                            {"id"      "ex:Bob"
                                                             "ex:name" ["Bob" "Robert"]
                                                             "ex:ssn"  "111-11-1111"
                                                             "type"    "ex:User"}]})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {"type"        "sh:ValidationReport",
                   "sh:conforms" false,
                   "sh:result"
                   [{"sh:constraintComponent" "sh:maxCount",
                     "sh:focusNode"           "ex:Bob",
                     "sh:resultSeverity"      "sh:Violation",
                     "sh:value"               2,
                     "sh:resultPath"          ["ex:name"],
                     "type"                   "sh:ValidationResult",
                     "sh:resultMessage"       "count 2 is greater than maximum count of 1",
                     "sh:sourceShape"         "ex:pshape2",
                     "f:expectation"          1}]}}
                 (ex-data db-bad-friend)))
          (is (= "Subject ex:Bob path [\"ex:name\"] violates constraint sh:maxCount of shape ex:pshape2 - count 2 is greater than maximum count of 1."
                 (ex-message db-bad-friend)))))
      (testing "separate txns"
        (testing "maxCount"
          (let [db1                    @(fluree/stage db0
                                                      {"@context" context
                                                       "insert"
                                                       [{"@id"                "ex:friendShape"
                                                         "type"               ["sh:NodeShape"]
                                                         "sh:targetObjectsOf" {"@id" "ex:friend"}
                                                         "sh:property"        [{"id"          "ex:pshape1"
                                                                                "sh:path"     {"@id" "ex:ssn"}
                                                                                "sh:maxCount" 1}]}]})
                db2                    @(fluree/stage db1 {"@context" context
                                                           "insert"   [{"id"     "ex:Bob"
                                                                        "ex:ssn" ["111-11-1111" "222-22-2222"]
                                                                        "type"   "ex:User"}]})
                db-db-forbidden-friend @(fluree/stage db2
                                                      {"@context" context
                                                       "insert"
                                                       {"id"        "ex:Alice"
                                                        "type"      "ex:User"
                                                        "ex:friend" {"@id" "ex:Bob"}}})]

            (is (= {:status 422,
                    :error  :shacl/violation,
                    :report
                    {"type"        "sh:ValidationReport",
                     "sh:conforms" false,
                     "sh:result"
                     [{"sh:constraintComponent" "sh:maxCount",
                       "sh:focusNode"           "ex:Bob",
                       "sh:resultSeverity"      "sh:Violation",
                       "sh:value"               2,
                       "sh:resultPath"          ["ex:ssn"],
                       "type"                   "sh:ValidationResult",
                       "sh:resultMessage"       "count 2 is greater than maximum count of 1",
                       "sh:sourceShape"         "ex:pshape1",
                       "f:expectation"          1}]}}
                   (ex-data db-db-forbidden-friend)))
            (is (= "Subject ex:Bob path [\"ex:ssn\"] violates constraint sh:maxCount of shape ex:pshape1 - count 2 is greater than maximum count of 1."
                   (ex-message db-db-forbidden-friend))))
          (let [db1           @(fluree/stage db0
                                             {"@context" context
                                              "insert"
                                              [{"@id"                "ex:friendShape"
                                                "type"               ["sh:NodeShape"]
                                                "sh:targetObjectsOf" {"@id" "ex:friend"}
                                                "sh:property"        [{"id"          "ex:pshape1"
                                                                       "sh:path"     {"@id" "ex:ssn"}
                                                                       "sh:maxCount" 1}]}]})
                db2           @(fluree/stage db1
                                             {"@context" context
                                              "insert"
                                              [{"id"        "ex:Alice"
                                                "ex:name"   "Alice"
                                                "type"      "ex:User"
                                                "ex:friend" {"@id" "ex:Bob"}}
                                               {"id"      "ex:Bob"
                                                "ex:name" "Bob"
                                                "type"    "ex:User"}]})
                db-excess-ssn @(fluree/stage db2
                                             {"@context" context
                                              "insert"
                                              {"id"     "ex:Bob"
                                               "ex:ssn" ["111-11-1111"
                                                         "222-22-2222"]}})]
            (is (= {:status 422,
                    :error  :shacl/violation,
                    :report
                    {"type"        "sh:ValidationReport",
                     "sh:conforms" false,
                     "sh:result"
                     [{"sh:constraintComponent" "sh:maxCount",
                       "sh:focusNode"           "ex:Bob",
                       "sh:resultSeverity"      "sh:Violation",
                       "sh:value"               2,
                       "sh:resultPath"          ["ex:ssn"],
                       "type"                   "sh:ValidationResult",
                       "sh:resultMessage"       "count 2 is greater than maximum count of 1",
                       "sh:sourceShape"         "ex:pshape1",
                       "f:expectation"          1}]}}
                   (ex-data db-excess-ssn)))
            (is (= "Subject ex:Bob path [\"ex:ssn\"] violates constraint sh:maxCount of shape ex:pshape1 - count 2 is greater than maximum count of 1."
                   (ex-message db-excess-ssn)))))
        (testing "datatype"
          (let [db1 @(fluree/stage db0
                                   {"@context" context
                                    "insert"   {"@id"                "ex:friendShape"
                                                "type"               ["sh:NodeShape"]
                                                "sh:targetObjectsOf" {"@id" "ex:friend"}
                                                "sh:property"        [{"id"          "ex:pshape1"
                                                                       "sh:path"     {"@id" "ex:name"}
                                                                       "sh:datatype" {"@id" "xsd:string"}}]}})

            ;; need to specify type in order to avoid sh:datatype coercion
                db2                 @(fluree/stage db1 {"@context" context
                                                        "insert"   {"id"      "ex:Bob"
                                                                    "ex:name" {"@type" "xsd:integer" "@value" 123}
                                                                    "type"    "ex:User"}})
                db-forbidden-friend @(fluree/stage db2
                                                   {"@context" context
                                                    "insert"
                                                    {"id"        "ex:Alice"
                                                     "type"      "ex:User"
                                                     "ex:friend" {"@id" "ex:Bob"}}})]
            (is (= {:status 422,
                    :error  :shacl/violation,
                    :report
                    {"type"        "sh:ValidationReport",
                     "sh:conforms" false,
                     "sh:result"
                     [{"sh:constraintComponent" "sh:datatype",
                       "sh:focusNode"           "ex:Bob",
                       "sh:resultSeverity"      "sh:Violation",
                       "sh:value"               ["xsd:integer"],
                       "sh:resultPath"          ["ex:name"],
                       "type"                   "sh:ValidationResult",
                       "sh:resultMessage"
                       "the following values do not have expected datatype xsd:string: 123",
                       "sh:sourceShape"         "ex:pshape1",
                       "f:expectation"          "xsd:string"}]}}
                   (ex-data db-forbidden-friend)))
            (is (= "Subject ex:Bob path [\"ex:name\"] violates constraint sh:datatype of shape ex:pshape1 - the following values do not have expected datatype xsd:string: 123."
                   (ex-message db-forbidden-friend)))))))))

(deftest ^:integration shape-based-constraints
  (testing "sh:node"
    (let [conn    @(fluree/connect-memory)
          ledger  @(fluree/create conn "shape-constaints")
          db0     (fluree/db ledger)
          context [test-utils/default-str-context {"ex" "http://example.com/ns/"}]

          db1            @(fluree/stage db0 {"@context" context
                                             "insert"   [{"id"          "ex:AddressShape"
                                                          "type"        "sh:NodeShape"
                                                          "sh:property" [{"id"          "ex:pshape1"
                                                                          "sh:path"     {"id" "ex:postalCode"}
                                                                          "sh:maxCount" 1}]}
                                                         {"id"             "ex:PersonShape"
                                                          "type"           "sh:NodeShape"
                                                          "sh:targetClass" {"id" "ex:Person"}
                                                          "sh:property"    [{"id"          "ex:pshape2"
                                                                             "sh:path"     {"id" "ex:address"}
                                                                             "sh:node"     {"id" "ex:AddressShape"}
                                                                             "sh:minCount" 1}]}]})
          valid-person   @(fluree/stage db1 {"@context" context
                                             "insert"   {"id"         "ex:Bob"
                                                         "type"       "ex:Person"
                                                         "ex:address" {"ex:postalCode" "12345"}}})
          invalid-person @(fluree/stage db1 {"@context" context
                                             "insert"   {"id"         "ex:Reto"
                                                         "type"       "ex:Person"
                                                         "ex:address" {"id"            "ex:1"
                                                                       "ex:postalCode" ["12345" "45678"]}}})]
      (is (= [{"id"         "ex:Bob",
               "type"       "ex:Person",
               "ex:address" {"ex:postalCode" "12345"}}]
             @(fluree/query valid-person {"@context" context
                                          "select"   {"ex:Bob" ["*" {"ex:address" ["ex:postalCode"]}]}})))
      (is (= {:status 422,
              :error  :shacl/violation,
              :report
              {"type"        "sh:ValidationReport",
               "sh:conforms" false,
               "sh:result"
               [{"sh:constraintComponent" "sh:node",
                 "sh:focusNode"           "ex:Reto",
                 "sh:resultSeverity"      "sh:Violation",
                 "sh:value"               "ex:1",
                 "sh:resultPath"          ["ex:address"],
                 "type"                   "sh:ValidationResult",
                 "sh:resultMessage"
                 "node ex:1 does not conform to shapes [\"ex:AddressShape\"]",
                 "sh:sourceShape"         "ex:pshape2",
                 "f:expectation"          ["ex:AddressShape"]}]}}
             (ex-data invalid-person)))
      (is (= "Subject ex:Reto path [\"ex:address\"] violates constraint sh:node of shape ex:pshape2 - node ex:1 does not conform to shapes [\"ex:AddressShape\"]."
             (ex-message invalid-person)))))

  (testing "sh:qualifiedValueShape property shape"
    (let [conn        @(fluree/connect-memory)
          ledger      @(fluree/create conn "shape-constaints")
          db0         (fluree/db ledger)
          context     [test-utils/default-str-context {"ex" "http://example.com/ns/"}]
          db1         @(fluree/stage db0 {"@context" context
                                          "insert"   [{"id"             "ex:KidShape"
                                                       "type"           "sh:NodeShape"
                                                       "sh:targetClass" {"id" "ex:Kid"}
                                                       "sh:property"
                                                       [{"id"                     "ex:pshape1"
                                                         "sh:path"                {"id" "ex:parent"}
                                                         "sh:minCount"            2
                                                         "sh:maxCount"            2
                                                         "sh:qualifiedValueShape" {"id"         "ex:qshape1"
                                                                                   "sh:path"    {"id" "ex:gender"}
                                                                                   "sh:pattern" "female"}
                                                         "sh:qualifiedMinCount"   1}]}
                                                      {"id"        "ex:Bob"
                                                       "ex:gender" "male"}
                                                      {"id"        "ex:Jane"
                                                       "ex:gender" "female"}]})
          valid-kid   @(fluree/stage db1 {"@context" context
                                          "insert"   {"id"        "ex:ValidKid"
                                                      "type"      "ex:Kid"
                                                      "ex:parent" [{"id" "ex:Bob"} {"id" "ex:Jane"}]}})
          invalid-kid @(fluree/stage db1 {"@context" context
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
      (is (= {:status 422,
              :error  :shacl/violation,
              :report
              {"type"        "sh:ValidationReport",
               "sh:conforms" false,
               "sh:result"
               [{"sh:constraintComponent" "sh:qualifiedValueShape",
                 "sh:focusNode"           "ex:InvalidKid",
                 "sh:resultSeverity"      "sh:Violation",
                 "sh:value"               ["ex:Bob" "ex:Zorba"],
                 "sh:resultPath"          ["ex:parent"],
                 "type"                   "sh:ValidationResult",
                 "sh:resultMessage"
                 "values [\"ex:Bob\" \"ex:Zorba\"] conformed to ex:qshape1 less than sh:qualifiedMinCount 1 times",
                 "sh:sourceShape"         "ex:pshape1",
                 "f:expectation"          "ex:qshape1"}]}}
             (ex-data invalid-kid)))
      (is (= "Subject ex:InvalidKid path [\"ex:parent\"] violates constraint sh:qualifiedValueShape of shape ex:pshape1 - values [\"ex:Bob\" \"ex:Zorba\"] conformed to ex:qshape1 less than sh:qualifiedMinCount 1 times."
             (ex-message invalid-kid)))))
  (testing "sh:qualifiedValueShape node shape"
    (let [conn   @(fluree/connect-memory)
          ledger @(fluree/create conn "shape-constaints")
          db0    (fluree/db ledger)

          context     [test-utils/default-str-context {"ex" "http://example.com/ns/"}]
          db1         @(fluree/stage db0 {"@context" context
                                          "insert"   [{"id"             "ex:KidShape"
                                                       "type"           "sh:NodeShape"
                                                       "sh:targetClass" {"id" "ex:Kid"}
                                                       "sh:property"
                                                       [{"id"                   "ex:pshape1"
                                                         "sh:path"              {"id" "ex:parent"}
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
          valid-kid   @(fluree/stage db1 {"@context" context
                                          "insert"   {"id"        "ex:ValidKid"
                                                      "type"      "ex:Kid"
                                                      "ex:parent" [{"id" "ex:Mom"} {"id" "ex:Dad"}]}})
          invalid-kid @(fluree/stage db1 {"@context" context
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
      (is (= {:status 422,
              :error  :shacl/violation,
              :report
              {"type"        "sh:ValidationReport",
               "sh:conforms" false,
               "sh:result"
               [{"sh:constraintComponent" "sh:qualifiedValueShape",
                 "sh:focusNode"           "ex:InvalidKid",
                 "sh:resultSeverity"      "sh:Violation",
                 "sh:value"               ["ex:Bob" "ex:Zorba"],
                 "sh:resultPath"          ["ex:parent"],
                 "type"                   "sh:ValidationResult",
                 "sh:resultMessage"
                 "values [\"ex:Bob\" \"ex:Zorba\"] conformed to ex:ParentShape less than sh:qualifiedMinCount 1 times",
                 "sh:sourceShape"         "ex:pshape1",
                 "f:expectation"          "ex:ParentShape"}]}}
             (ex-data invalid-kid)))
      (is (= "Subject ex:InvalidKid path [\"ex:parent\"] violates constraint sh:qualifiedValueShape of shape ex:pshape1 - values [\"ex:Bob\" \"ex:Zorba\"] conformed to ex:ParentShape less than sh:qualifiedMinCount 1 times."
             (ex-message invalid-kid)))))
  (testing "sh:qualifiedValueShapesDisjoint"
    (let [conn   @(fluree/connect-memory)
          ledger @(fluree/create conn "shape-constraints")
          db0    (fluree/db ledger)

          context [test-utils/default-str-context {"ex" "http://example.com/ns/"}]
          db1     @(fluree/stage db0 {"@context" context
                                      "insert"
                                      [{"id"      "ex:Digit"
                                        "ex:name" "Toe"}
                                       {"id"             "ex:HandShape"
                                        "type"           "sh:NodeShape"
                                        "sh:targetClass" {"id" "ex:Hand"}
                                        "sh:property"
                                        [{"id"          "ex:pshape1"
                                          "sh:path"     {"id" "ex:digit"}
                                          "sh:maxCount" 5}
                                         {"id"                              "ex:pshape2"
                                          "sh:path"                         {"id" "ex:digit"}
                                          "sh:qualifiedValueShape"          {"id"          "ex:thumbshape"
                                                                             "sh:path"     {"id" "ex:name"}
                                                                             "sh:hasValue" "Thumb"}
                                          "sh:qualifiedMinCount"            1
                                          "sh:qualifiedMaxCount"            1
                                          "sh:qualifiedValueShapesDisjoint" true}
                                         {"id"                              "ex:pshape3"
                                          "sh:path"                         {"id" "ex:digit"}
                                          "sh:qualifiedValueShape"          {"id"          "ex:fingershape"
                                                                             "sh:path"     {"id" "ex:name"}
                                                                             "sh:hasValue" "Finger"}
                                          "sh:qualifiedMinCount"            4
                                          "sh:qualifiedMaxCount"            4
                                          "sh:qualifiedValueShapesDisjoint" true}]}]})

          valid-hand   @(fluree/stage db1 {"@context" context
                                           "insert"   {"id"       "ex:ValidHand"
                                                       "type"     "ex:Hand"
                                                       "ex:digit" [{"id" "ex:thumb" "ex:name" "Thumb"}
                                                                   {"id" "ex:finger1" "ex:name" "Finger"}
                                                                   {"id" "ex:finger2" "ex:name" "Finger"}
                                                                   {"id" "ex:finger3" "ex:name" "Finger"}
                                                                   {"id" "ex:finger4" "ex:name" "Finger"}]}})
          invalid-hand @(fluree/stage db1 {"@context" context
                                           "insert"   {"id"       "ex:InvalidHand"
                                                       "type"     "ex:Hand"
                                                       "ex:digit" [{"id" "ex:thumb" "ex:name" "Thumb"}
                                                                   {"id" "ex:finger1" "ex:name" "Finger"}
                                                                   {"id" "ex:finger2" "ex:name" "Finger"}
                                                                   {"id" "ex:finger3" "ex:name" "Finger"}
                                                                   {"id"      "ex:finger4andthumb"
                                                                    "ex:name" ["Finger" "Thumb"]}]}})]
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
      (is (= {:status 422,
              :error  :shacl/violation,
              :report
              {"type"        "sh:ValidationReport",
               "sh:conforms" false,
               "sh:result"
               [{"sh:constraintComponent" "sh:qualifiedValueShape",
                 "sh:focusNode"           "ex:InvalidHand",
                 "sh:resultSeverity"      "sh:Violation",
                 "sh:value"               "ex:finger4andthumb",
                 "sh:resultPath"          ["ex:digit"],
                 "type"                   "sh:ValidationResult",
                 "sh:resultMessage"
                 "value ex:finger4andthumb conformed to a sibling qualified value shape [\"ex:fingershape\"] in violation of the sh:qualifiedValueShapesDisjoint constraint",
                 "sh:sourceShape"         "ex:pshape2",
                 "f:expectation"          "ex:thumbshape"}
                {"sh:constraintComponent" "sh:qualifiedValueShape",
                 "sh:focusNode"           "ex:InvalidHand",
                 "sh:resultSeverity"      "sh:Violation",
                 "sh:value"               "ex:finger4andthumb",
                 "sh:resultPath"          ["ex:digit"],
                 "type"                   "sh:ValidationResult",
                 "sh:resultMessage"
                 "value ex:finger4andthumb conformed to a sibling qualified value shape [\"ex:thumbshape\"] in violation of the sh:qualifiedValueShapesDisjoint constraint",
                 "sh:sourceShape"         "ex:pshape3",
                 "f:expectation"          "ex:fingershape"}]}}
             (ex-data invalid-hand)))
      (is (= "Subject ex:InvalidHand path [\"ex:digit\"] violates constraint sh:qualifiedValueShape of shape ex:pshape2 - value ex:finger4andthumb conformed to a sibling qualified value shape [\"ex:fingershape\"] in violation of the sh:qualifiedValueShapesDisjoint constraint.
Subject ex:InvalidHand path [\"ex:digit\"] violates constraint sh:qualifiedValueShape of shape ex:pshape3 - value ex:finger4andthumb conformed to a sibling qualified value shape [\"ex:thumbshape\"] in violation of the sh:qualifiedValueShapesDisjoint constraint."
             (ex-message invalid-hand))))))

(deftest ^:integration post-processing-validation
  (let [conn    @(fluree/connect-memory)
        ledger  @(fluree/create conn "post-processing")
        context [test-utils/default-str-context {"ex" "http://example.com/ns/"}]
        db0     (fluree/db ledger)]
    (testing "shacl-objects-of-test"
      (let [db1                 @(fluree/stage db0
                                               {"@context" context
                                                "insert"
                                                {"@id"                "ex:friendShape"
                                                 "type"               ["sh:NodeShape"]
                                                 "sh:targetObjectsOf" {"@id" "ex:friend"}
                                                 "sh:property"        [{"id"          "ex:pshape1"
                                                                        "sh:path"     {"@id" "ex:name"}
                                                                        "sh:datatype" {"@id" "xsd:string"}}]}})
            ;; need to specify type in order to avoid sh:datatype coercion
            db2                 @(fluree/stage db1 {"@context" context
                                                    "insert"   {"id"      "ex:Bob"
                                                                "ex:name" {"@type" "xsd:integer" "@value" 123}
                                                                "type"    "ex:User"}})
            db-forbidden-friend @(fluree/stage db2
                                               {"@context" context
                                                "insert"
                                                {"id"        "ex:Alice"
                                                 "type"      "ex:User"
                                                 "ex:friend" {"@id" "ex:Bob"}}})]
        (is (= {:status 422,
                :error  :shacl/violation,
                :report
                {"type"        "sh:ValidationReport",
                 "sh:conforms" false,
                 "sh:result"
                 [{"sh:constraintComponent" "sh:datatype",
                   "sh:focusNode"           "ex:Bob",
                   "sh:resultSeverity"      "sh:Violation",
                   "sh:value"               ["xsd:integer"],
                   "sh:resultPath"          ["ex:name"],
                   "type"                   "sh:ValidationResult",
                   "sh:resultMessage"
                   "the following values do not have expected datatype xsd:string: 123",
                   "sh:sourceShape"         "ex:pshape1"
                   "f:expectation"          "xsd:string"}]}}
               (ex-data db-forbidden-friend)))
        (is (= "Subject ex:Bob path [\"ex:name\"] violates constraint sh:datatype of shape ex:pshape1 - the following values do not have expected datatype xsd:string: 123."
               (ex-message db-forbidden-friend)))))
    (testing "shape constraints"
      (let [db1            @(fluree/stage db0 {"@context" context
                                               "insert"
                                               [{"id"          "ex:CoolShape"
                                                 "type"        "sh:NodeShape"
                                                 "sh:property" [{"id"          "ex:pshape1"
                                                                 "sh:path"     {"id" "ex:isCool"}
                                                                 "sh:hasValue" true
                                                                 "sh:minCount" 1}]}
                                                {"id"             "ex:PersonShape"
                                                 "type"           "sh:NodeShape"
                                                 "sh:targetClass" {"id" "ex:Person"}
                                                 "sh:property"    [{"id"          "ex:pshape2"
                                                                    "sh:path"     {"id" "ex:cool"}
                                                                    "sh:node"     {"id" "ex:CoolShape"}
                                                                    "sh:minCount" 1}]}]})
            valid-person   @(fluree/stage db1 {"@context" context
                                               "insert"   {"id"      "ex:Bob"
                                                           "type"    "ex:Person"
                                                           "ex:cool" {"ex:isCool" true}}})
            invalid-person @(fluree/stage db1 {"@context" context
                                               "insert"   {"id"      "ex:Reto"
                                                           "type"    "ex:Person"
                                                           "ex:cool" {"id"        "ex:1"
                                                                      "ex:isCool" false}}})]
        (is (= [{"id"      "ex:Bob",
                 "type"    "ex:Person",
                 "ex:cool" {"ex:isCool" true}}]
               @(fluree/query valid-person {"@context" context
                                            "select"   {"ex:Bob" ["*" {"ex:cool" ["ex:isCool"]}]}})))
        (is (= {:status 422,
                :error  :shacl/violation,
                :report
                {"type"        "sh:ValidationReport",
                 "sh:conforms" false,
                 "sh:result"
                 [{"sh:constraintComponent" "sh:node",
                   "sh:focusNode"           "ex:Reto",
                   "sh:resultSeverity"      "sh:Violation",
                   "sh:value"               "ex:1",
                   "sh:resultPath"          ["ex:cool"],
                   "type"                   "sh:ValidationResult",
                   "sh:resultMessage"
                   "node ex:1 does not conform to shapes [\"ex:CoolShape\"]",
                   "sh:sourceShape"         "ex:pshape2",
                   "f:expectation"          ["ex:CoolShape"]}]}}
               (ex-data invalid-person)))
        (is (= "Subject ex:Reto path [\"ex:cool\"] violates constraint sh:node of shape ex:pshape2 - node ex:1 does not conform to shapes [\"ex:CoolShape\"]."
               (ex-message invalid-person)))))
    (testing "extended path constraints"
      (let [db1            @(fluree/stage db0 {"@context" context
                                               "insert"   {"id"             "ex:PersonShape"
                                                           "type"           "sh:NodeShape"
                                                           "sh:targetClass" {"id" "ex:Person"}
                                                           "sh:property"    [{"id"          "ex:pshape1"
                                                                              "sh:path"
                                                                              {"@list" [{"id" "ex:cool"}
                                                                                        {"id" "ex:dude"}]}
                                                                              "sh:nodeKind" {"id" "sh:BlankNode"}
                                                                              "sh:minCount" 1}]}})
            valid-person   @(fluree/stage db1 {"@context" context
                                               "insert"   {"id"      "ex:Bob"
                                                           "type"    "ex:Person"
                                                           "ex:cool" {"ex:dude" {"ex:isBlank" true}}}})
            invalid-person @(fluree/stage db1 {"@context" context
                                               "insert"   {"id"      "ex:Reto"
                                                           "type"    "ex:Person"
                                                           "ex:cool" {"ex:dude" {"id"         "ex:Dude"
                                                                                 "ex:isBlank" false}}}})]
        (is (= [{"id"      "ex:Bob",
                 "type"    "ex:Person",
                 "ex:cool" {"ex:dude" {"ex:isBlank" true}}}]
               @(fluree/query valid-person {"@context" context
                                            "select"   {"ex:Bob" ["*" {"ex:cool" [{"ex:dude" ["ex:isBlank"]}]}]}})))
        (is (= {:status 422,
                :error  :shacl/violation,
                :report
                {"type"        "sh:ValidationReport",
                 "sh:conforms" false,
                 "sh:result"
                 [{"sh:constraintComponent" "sh:nodeKind",
                   "sh:focusNode"           "ex:Reto",
                   "sh:resultSeverity"      "sh:Violation",
                   "sh:value"               "ex:Dude",
                   "sh:resultPath"          ["ex:cool" "ex:dude"],
                   "type"                   "sh:ValidationResult",
                   "sh:resultMessage"       "value ex:Dude is is not of kind sh:BlankNode",
                   "sh:sourceShape"         "ex:pshape1",
                   "f:expectation"          "sh:BlankNode"}]}}
               (ex-data invalid-person)))
        (is (= "Subject ex:Reto path [\"ex:cool\" \"ex:dude\"] violates constraint sh:nodeKind of shape ex:pshape1 - value ex:Dude is is not of kind sh:BlankNode."
               (ex-message invalid-person)))))))

(deftest validation-report
  (let [conn    @(fluree/connect-memory)
        ledger  @(fluree/create conn "validation-report")
        context [test-utils/default-str-context {"ex" "http://example.com/ns/"}]
        db0     (fluree/db ledger)]
    (testing "severity"
      (testing "no severity specified defaults to sh:Violation"
        (let [db1 @(fluree/stage db0 {"@context" context
                                      "insert"
                                      {"@id"           "ex:friendShape"
                                       "type"          ["sh:NodeShape"]
                                       "sh:targetNode" {"@id" "ex:a"}
                                       "sh:property"   [{"sh:path"      {"@id" "ex:name"}
                                                         "sh:maxLength" 3}]}})
              db2 @(fluree/stage db1 {"@context" context
                                      "insert"
                                      {"@id"     "ex:a"
                                       "ex:name" "John"}})]
          (is (= "sh:Violation"
                 (-> (ex-data db2)
                     :report
                     (get "sh:result")
                     (get 0)
                     (get "sh:resultSeverity"))))))
      (testing "severity specified on shape overrides default severity"
        (let [db1 @(fluree/stage db0 {"@context" context
                                      "insert"
                                      {"@id"           "ex:friendShape"
                                       "type"          ["sh:NodeShape"]
                                       "sh:targetNode" {"@id" "ex:a"}
                                       "sh:property"   [{"sh:path"      {"@id" "ex:name"}
                                                         "sh:severity"  {"@id" "ex:EXTREME"}
                                                         "sh:maxLength" 3}]}})
              db2 @(fluree/stage db1 {"@context" context
                                      "insert"
                                      {"@id"     "ex:a"
                                       "ex:name" "John"}})]
          (is (= "ex:EXTREME"
                 (-> (ex-data db2)
                     :report
                     (get "sh:result")
                     (get 0)
                     (get "sh:resultSeverity")))))))
    (testing "message"
      (testing "no sh:message specified on shape defaults to implementation-specific message"
        (let [db1 @(fluree/stage db0 {"@context" context
                                      "insert"
                                      {"@id"           "ex:friendShape"
                                       "type"          ["sh:NodeShape"]
                                       "sh:targetNode" {"@id" "ex:a"}
                                       "sh:property"   [{"sh:path"      {"@id" "ex:name"}
                                                         "sh:maxLength" 3}]}})
              db2 @(fluree/stage db1 {"@context" context
                                      "insert" {"@id" "ex:a"
                                                "ex:name" "John"}})]
          ;; implementation-specific default resultMessage
          (is (= "value \"John\" has string length greater than maximum length 3"
                 (-> (ex-data db2)
                     :report
                     (get "sh:result")
                     (get 0)
                     (get "sh:resultMessage"))))))
      (testing "custom sh:message on shape overrides implmentation-specific message"
        (let [db1 @(fluree/stage db0 {"@context" context
                                      "insert"
                                      {"@id"           "ex:friendShape"
                                       "type"          ["sh:NodeShape"]
                                       "sh:targetNode" {"@id" "ex:a"}
                                       "sh:property"   [{"sh:path"      {"@id" "ex:name"}
                                                         "sh:message"  "THIS NAME IS TOO LONG"
                                                         "sh:maxLength" 3}]}})
              db2 @(fluree/stage db1 {"@context" context
                                      "insert"
                                      {"@id"     "ex:a"
                                       "ex:name" "John"}})]
          (is (= "THIS NAME IS TOO LONG"
                 (-> (ex-data db2)
                     :report
                     (get "sh:result")
                     (get 0)
                     (get "sh:resultMessage")))))))))

(deftest target-subjects-of
  (let [conn    @(fluree/connect-memory)
        ledger  @(fluree/create conn "validation-report")
        context [test-utils/default-str-context {"ex" "http://example.com/ns/"}]
        db0     (fluree/db ledger)

        db1 @(fluree/stage db0 {"@context" context
                                "insert"
                                {"type"                "sh:NodeShape"
                                 "sh:targetSubjectsOf" {"@id" "ex:myProperty"}
                                 "sh:property"         [{"sh:path"     {"@id" "ex:myProperty"}
                                                         "sh:maxCount" 1}]}})]
    (testing "valid target with no violations passes validation"
      (let [db2 @(fluree/stage db1 {"@context" context
                                    "insert" {"id" "ex:valid"
                                              "ex:myProperty" "A"}})]
        (is (nil? (ex-data db2)))))
    (testing "invalid target produces validation errors"
      (let [db2 @(fluree/stage db1 {"@context" context
                                    "insert" {"id" "ex:invalid"
                                              "ex:myProperty" ["A" "B"]}})]
        (is (= 422
               (:status (ex-data db2))))))))

(deftest target-node
  (let [conn    @(fluree/connect-memory)
        ledger  @(fluree/create conn "validation-report")
        context [test-utils/default-str-context {"ex" "http://example.com/ns/"}]
        db0     (fluree/db ledger)

        db1 @(fluree/stage db0 {"@context" context
                                "insert"
                                {"type"          "sh:NodeShape"
                                 "sh:targetNode" [{"@id" "ex:nodeA"} {"@id" "ex:nodeB"}]
                                 "sh:property"   [{"sh:path"     {"@id" "ex:letter"}
                                                   "sh:maxCount" 1}]}})]
    (testing "valid target with no violations passes validation"
      (let [db2 @(fluree/stage db1 {"@context" context
                                    "insert"   {"id"        "ex:nodeA"
                                                "ex:letter" "A"}})]
        (is (nil? (ex-data db2)))))
    (testing "invalid target produces validation erros"
      (let [db2 @(fluree/stage db1 {"@context" context
                                    "insert"   {"id"        "ex:nodeB"
                                                "ex:letter" ["A" "B"]}})]
        (is (= 422
               (:status (ex-data db2))))))))

(deftest alternative-path
  (let [conn @(fluree/connect-memory)
        ledger @(fluree/create conn "validation-report")
        context [test-utils/default-str-context {"ex" "http://example.com/ns/"}]
        db0 (fluree/db ledger)

        db1 @(fluree/stage db0 {"@context" context
                                "insert"
                                {"type" "sh:NodeShape"
                                 "sh:targetClass" {"@id" "ex:Alt"}
                                 "sh:property" [{"id" "ex:pshape1"
                                                 "sh:path" {"sh:alternativePath"
                                                            [{"@id" "ex:property1"} {"@id" "ex:property2"}]}
                                                 "sh:minCount" 2}]}})]
    (testing "constraint is satisfied when constrained by sh:alternativePath"
      (let [db2 @(fluree/stage db1 {"@context" context
                                    "insert"
                                    {"id" "ex:valid"
                                     "type" "ex:Alt"
                                     "ex:property1" "One"
                                     "ex:property2" "Two"}})]
        (is (nil? (ex-data db2)))))
    (testing "constraint is violated when constrained by sh:alternativePath"
      (let [db2 @(fluree/stage db1 {"@context" context
                                    "insert"
                                    {"id" "ex:invalid"
                                     "type" "ex:Alt"
                                     "ex:property1" "One"
                                     "ex:property3" "Three"}})]
        (is (= {:status 422,
                :error :shacl/violation,
                :report
                {"type" "sh:ValidationReport",
                 "sh:conforms" false,
                 "sh:result"
                 [{"sh:constraintComponent" "sh:minCount",
                   "sh:focusNode" "ex:invalid",
                   "sh:resultSeverity" "sh:Violation",
                   "sh:value" 1,
                   "sh:resultPath" [{"sh:alternativePath" "ex:property1"}],
                   "type" "sh:ValidationResult",
                   "sh:resultMessage" "count 1 is less than minimum count of 2",
                   "sh:sourceShape" "ex:pshape1",
                   "f:expectation" 2}]}}

               (ex-data db2)))
        (is (= "Subject ex:invalid path [{\"sh:alternativePath\" \"ex:property1\"}] violates constraint sh:minCount of shape ex:pshape1 - count 1 is less than minimum count of 2."
               (ex-message db2)))))))

(deftest multiple-and-encumbered-targets
  (let [conn @(fluree/connect-memory)
        ledger @(fluree/create conn "encumbered-targets")
        db0 (fluree/db ledger)]
    (testing "targetClass"
      (testing "with multiple targets"
        (let [db1 @(fluree/stage db0 {"@context" test-utils/default-str-context
                                      "insert"
                                      [{"@type" "sh:NodeShape",
                                        "sh:property" [{"sh:datatype" {"@id" "xsd:string"},
                                                        "sh:maxCount" 1,
                                                        "sh:path" {"@id" "ex:term"}}],
                                        "sh:targetClass" [{"@id" "ex:Assertion"} {"@id" "ex:Fact"}]}]})]
          (is (test-utils/shacl-error? @(fluree/stage db1 {"insert" {"@type" "ex:Assertion" "ex:term" 123}})))
          (is (test-utils/shacl-error? @(fluree/stage db1 {"insert" {"@type" "ex:Fact" "ex:term" 123}})))))
      (testing "with extra data"
        (let [db1 @(fluree/stage db0 {"@context" test-utils/default-str-context
                                      "insert"
                                      [{"@type" "sh:NodeShape",
                                        "sh:property" [{"sh:datatype" {"@id" "xsd:string"},
                                                        "sh:maxCount" 1,
                                                        "sh:path" {"@id" "ex:term"}}],
                                        "sh:targetClass" {"@id" "ex:Assertion" "ex:extra" "data"}}]})]
          (is (test-utils/shacl-error? @(fluree/stage db1 {"insert" {"@type" "ex:Assertion" "ex:term" 123}}))))))
    (testing "targetNode"
      (testing "with multiple targets"
        (let [db1 @(fluree/stage db0 {"@context" test-utils/default-str-context
                                      "insert"
                                      [{"@type" "sh:NodeShape",
                                        "sh:property" [{"sh:datatype" {"@id" "xsd:string"},
                                                        "sh:maxCount" 1,
                                                        "sh:path" {"@id" "ex:term"}}],
                                        "sh:targetNode" [{"@id" "ex:foo"}
                                                         {"@id" "ex:bar"}]}]})]
          (is (test-utils/shacl-error? @(fluree/stage db1 {"insert" {"@id" "ex:foo" "ex:term" 123}})))
          (is (test-utils/shacl-error? @(fluree/stage db1 {"insert" {"@id" "ex:bar" "ex:term" 123}})))))
      (testing "with extra data"
        (let [db1 @(fluree/stage db0 {"@context" test-utils/default-str-context
                                      "insert"
                                      [{"@type" "sh:NodeShape",
                                        "sh:property" [{"sh:datatype" {"@id" "xsd:string"},
                                                        "sh:maxCount" 1,
                                                        "sh:path" {"@id" "ex:term"}}],
                                        "sh:targetNode" {"@id" "ex:foo" "ex:extra" "data"}}]})]
          (is (test-utils/shacl-error? @(fluree/stage db1 {"insert" {"@id" "ex:foo" "ex:term" 123}}))))))
    (testing "targetObjectsOf"
      (testing "with multiple targets"
        (let [db1 @(fluree/stage db0 {"@context" test-utils/default-str-context
                                      "insert"
                                      [{"@type" "sh:NodeShape",
                                        "sh:datatype" {"@id" "xsd:string"},
                                        "sh:targetObjectsOf" [{"@id" "ex:foo"}
                                                              {"@id" "ex:bar"}]}]})]
          (is (test-utils/shacl-error? @(fluree/stage db1 {"insert" {"@id" "ex:me" "ex:foo" 123}})))
          (is (test-utils/shacl-error? @(fluree/stage db1 {"insert" {"@id" "ex:me" "ex:bar" 123}})))))
      (testing "with extra data"
        (let [db1 @(fluree/stage db0 {"@context" test-utils/default-str-context
                                      "insert"
                                      [{"@type" "sh:NodeShape",
                                        "sh:property" [{"sh:datatype" {"@id" "xsd:string"},
                                                        "sh:maxCount" 1,
                                                        "sh:path" {"@id" "ex:term"}}],
                                        "sh:targetObjectsOf" {"@id" "ex:foo" "@type" "rdf:Property"}}]})]
          (is (test-utils/shacl-error? @(fluree/stage db1 {"insert" {"@context" test-utils/default-str-context
                                                                     "@graph"   [{"@id" "ex:me"
                                                                                  "ex:foo" {"@id" "ex:you"}}
                                                                                 {"@id" "ex:you"
                                                                                  "ex:term" ["hello", "there"]}]}}))))))
    (testing "targetSubjectsof"
      (testing "with multiple targets"
        (let [db1 @(fluree/stage db0 {"@context" test-utils/default-str-context
                                      "insert"
                                      [{"@type" "sh:NodeShape",
                                        "sh:property" [{"sh:datatype" {"@id" "xsd:string"},
                                                        "sh:maxCount" 1,
                                                        "sh:path" {"@id" "ex:term"}}],
                                        "sh:targetSubjectsOf" [{"@id" "ex:foo"}
                                                               {"@id" "ex:bar"}]}]})]
          (is (test-utils/shacl-error? @(fluree/stage db1 {"insert" {"@id" "ex:me" "ex:foo" "foo" "ex:term" 123}})))
          (is (test-utils/shacl-error? @(fluree/stage db1 {"insert" {"@id" "ex:me" "ex:bar" "bar" "ex:term" 123}})))))
      (testing "with extra data"
        (let [db1 @(fluree/stage db0 {"@context" test-utils/default-str-context
                                      "insert"
                                      [{"@type" "sh:NodeShape",
                                        "sh:property" [{"sh:datatype" {"@id" "xsd:string"},
                                                        "sh:maxCount" 1,
                                                        "sh:path" {"@id" "ex:term"}}],
                                        "sh:targetSubjectsOf" {"@id" "ex:foo" "ex:extra" "data"}}]})]
          (is (test-utils/shacl-error? @(fluree/stage db1 {"insert" {"@id" "ex:me" "ex:foo" "foo" "ex:term" 123}}))))))))
