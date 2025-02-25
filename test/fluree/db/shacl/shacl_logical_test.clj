(ns fluree.db.shacl.shacl-logical-test
  (:require [clojure.test :refer :all]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(deftest ^:integration shacl-not-test
  (testing "shacl basic not constraint works"
    (let [conn       (test-utils/create-conn)
          ledger     @(fluree/create conn "shacl/a")
          context    [test-utils/default-context {:ex "http://example.org/ns/"}]
          user-query {:context context
                      :select  {'?s [:*]}
                      :where   {:id '?s, :type :ex/User}}
          db         @(fluree/stage
                        (fluree/db ledger)
                        {"@context" ["https://ns.flur.ee" context]
                         "insert"
                         {:id             :ex/UserShape
                          :type           [:sh/NodeShape]
                          :sh/targetClass :ex/User
                          :sh/not         [{:id          :ex/pshape1
                                            :sh/path     :schema/companyName
                                            :sh/minCount 1}
                                           {:id        :ex/pshape2
                                            :sh/path   :schema/name
                                            :sh/equals :schema/callSign}]
                          :sh/property    [{:id          :ex/pshape3
                                            :sh/path     :schema/callSign
                                            :sh/minCount 1
                                            :sh/maxCount 1
                                            :sh/datatype :xsd/string}]}})]
      (testing "no violations"
        (let [db-ok      @(fluree/stage
                            db
                            {"@context" ["https://ns.flur.ee" context]
                             "insert"
                             {:id              :ex/john,
                              :type            [:ex/User],
                              :schema/name     "John"
                              :schema/callSign "j-rock"}})
              ok-results @(fluree/query db-ok user-query)]
          (is (= [{:id              :ex/john,
                   :type            :ex/User,
                   :schema/name     "John",
                   :schema/callSign "j-rock"}]
                 ok-results)
              (str "unexpected query result: " (pr-str ok-results)))))
      (testing "not equal"
        (let [db-company-name @(fluree/stage
                                 db
                                 {"@context" ["https://ns.flur.ee" context]
                                  "insert"
                                  {:id                 :ex/john,
                                   :type               [:ex/User],
                                   :schema/companyName "WrongCo"
                                   :schema/callSign    "j-rock"}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape :ex/pshape1"}]}}
                 (ex-data db-company-name)))
          (is (= "Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape :ex/pshape1."
                 (ex-message db-company-name)))))
      (testing "conforms to minCount"
        (let [db-two-names @(fluree/stage
                              db
                              {"@context" ["https://ns.flur.ee" context]
                               "insert"
                               {:id                 :ex/john,
                                :type               [:ex/User],
                                :schema/companyName ["John", "Johnny"]
                                :schema/callSign    "j-rock"}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape :ex/pshape1"}]}}
                 (ex-data db-two-names)))
          (is (= "Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape :ex/pshape1."
                 (ex-message db-two-names)))))
      (testing "conforms to equals"
        (let [db-callsign-name @(fluree/stage
                                  db
                                  {"@context" ["https://ns.flur.ee" context]
                                   "insert"
                                   {:id              :ex/john
                                    :type            [:ex/User]
                                    :schema/name     "Johnny Boy"
                                    :schema/callSign "Johnny Boy"}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape :ex/pshape2"}]}}
                 (ex-data db-callsign-name)))
          (is (= "Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape :ex/pshape2."
                 (ex-message db-callsign-name)))))))

  (testing "shacl not w/ value ranges works"
    (let [conn       (test-utils/create-conn)
          ledger     @(fluree/create conn "shacl/a")
          context    [test-utils/default-context {:ex "http://example.org/ns/"}]
          user-query {:context context
                      :select  {'?s [:*]}
                      :where   {:id '?s, :type :ex/User}}
          db         @(fluree/stage
                        (fluree/db ledger)
                        {"@context" ["https://ns.flur.ee" context]
                         "insert"
                         {:id             :ex/UserShape
                          :type           [:sh/NodeShape]
                          :sh/targetClass :ex/User
                          :sh/not         [{:id              :ex/pshape1
                                            :sh/path         :schema/age
                                            :sh/minInclusive 130}
                                           {:id              :ex/pshape2
                                            :sh/path         :schema/favNums
                                            :sh/maxExclusive 9000}]
                          :sh/property    [{:id          :ex/pshape3
                                            :sh/path     :schema/age
                                            :sh/minCount 1
                                            :sh/maxCount 1
                                            :sh/datatype :xsd/integer}]}})

          db-two-probs @(fluree/stage
                          db
                          {"@context" ["https://ns.flur.ee" context]
                           "insert"
                           {:id              :ex/john
                            :type            [:ex/User]
                            :schema/name     "Johnny Boy"
                            :schema/callSign "Johnny Boy"
                            :schema/age      900
                            :schema/favNums  [4 8 15 16 23 42]}})]
      (testing "no violations"
        (let [db-ok      @(fluree/stage
                            db
                            {"@context" ["https://ns.flur.ee" context]
                             "insert"
                             {:id              :ex/john,
                              :type            [:ex/User],
                              :schema/name     "John"
                              :schema/callSign "j-rock"
                              :schema/age      42
                              :schema/favNums  [9004 9008 9015 9016 9023 9042]}})
              ok-results @(fluree/query db-ok user-query)]
          (is (= [{:id              :ex/john,
                   :type            :ex/User,
                   :schema/name     "John",
                   :schema/callSign "j-rock"
                   :schema/age      42
                   :schema/favNums  [9004 9008 9015 9016 9023 9042]}]
                 ok-results)
              (str "unexpected query result: " (pr-str ok-results)))))
      (testing "conforms to min and max"
        (let [db-too-old @(fluree/stage
                            db
                            {"@context" ["https://ns.flur.ee" context]
                             "insert"
                             {:id                 :ex/john,
                              :type               [:ex/User],
                              :schema/companyName "WrongCo"
                              :schema/callSign    "j-rock"
                              :schema/age         131}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape :ex/pshape1"}
                    {:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape :ex/pshape2"}]}}
                 (ex-data db-too-old)))
          (is (= "Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape :ex/pshape1.
Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape :ex/pshape2."
                 (ex-message db-too-old)))))
      (testing "conforms to max exclusive"
        (let [db-too-low @(fluree/stage
                            db
                            {"@context" ["https://ns.flur.ee" context]
                             "insert"
                             {:id                 :ex/john,
                              :type               [:ex/User],
                              :schema/companyName ["John", "Johnny"]
                              :schema/callSign    "j-rock"
                              :schema/age         27
                              :schema/favNums     [4 8 15 16 23 42]}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape :ex/pshape2"}]}}
                 (ex-data db-too-low)))
          (is (= "Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape :ex/pshape2."
                 (ex-message db-too-low)))))
      (testing "conforms to min and max exclusive"
        (let []
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape :ex/pshape1"}
                    {:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape :ex/pshape2"}]}}
                 (ex-data db-two-probs)))
          (is (= "Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape :ex/pshape1.
Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape :ex/pshape2."
                 (ex-message db-two-probs)))))))

  (testing "shacl not w/ string constraints works"
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
                          :type           [:sh/NodeShape]
                          :sh/targetClass :ex/User
                          :sh/not         [{:id           :ex/pshape1
                                            :sh/path      :ex/tag
                                            :sh/minLength 4}
                                           {:id           :ex/pshape2
                                            :sh/path      :schema/name
                                            :sh/maxLength 10}
                                           {:id         :ex/pshape3
                                            :sh/path    :ex/greeting
                                            :sh/pattern "hello.*"}]}})]
      (testing "no constraint violations"
        (let [db-ok @(fluree/stage
                       db
                       {"@context" ["https://ns.flur.ee" context]
                        "insert"
                        {:id          :ex/jean-claude
                         :type        :ex/User,
                         :schema/name "Jean-Claude"
                         :ex/tag      1
                         :ex/greeting "HOWDY"}})]
          (is (= [{:id          :ex/jean-claude
                   :type        :ex/User,
                   :schema/name "Jean-Claude"
                   :ex/greeting "HOWDY"
                   :ex/tag      1}]
                 @(fluree/query db-ok user-query)))))
      (testing "name conforms"
        (let [db-name-too-short @(fluree/stage
                                   db
                                   {"@context" ["https://ns.flur.ee" context]
                                    "insert"
                                    {:id          :ex/john,
                                     :type        [:ex/User],
                                     :schema/name "John"}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape :ex/pshape1"}
                    {:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape :ex/pshape2"}
                    {:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape :ex/pshape3"}]}}
                 (ex-data db-name-too-short)))
          (is (= "Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape :ex/pshape1.
Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape :ex/pshape2.
Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape :ex/pshape3."
                 (ex-message db-name-too-short)))))
      (testing "tag conforms"
        (let [db-tag-too-long @(fluree/stage
                                 db
                                 {"@context" ["https://ns.flur.ee" context]
                                  "insert"
                                  {:id     :ex/john,
                                   :type   [:ex/User],
                                   :ex/tag 12345}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape :ex/pshape1"}
                    {:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape :ex/pshape2"}
                    {:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape :ex/pshape3"}]}}
                 (ex-data db-tag-too-long)))
          (is (= "Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape :ex/pshape1.
Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape :ex/pshape2.
Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape :ex/pshape3."
                 (ex-message db-tag-too-long)))))
      (testing "greeting conforms"
        (let [db-greeting-incorrect @(fluree/stage
                                       db
                                       {"@context" ["https://ns.flur.ee" context]
                                        "insert"
                                        {:id          :ex/john,
                                         :type        [:ex/User],
                                         :ex/greeting "hello!"}})]
          (is (= {:status 422,
                  :error  :shacl/violation,
                  :report
                  {:type        :sh/ValidationReport,
                   :sh/conforms false,
                   :sh/result
                   [{:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape :ex/pshape1"}
                    {:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape :ex/pshape2"}
                    {:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape :ex/pshape3"}]}}
                 (ex-data db-greeting-incorrect)))
          (is (= "Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape :ex/pshape1.
Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape :ex/pshape2.
Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape :ex/pshape3."
                 (ex-message db-greeting-incorrect))))))))

(deftest ^:integration shacl-and-tests
  (let [conn    @(fluree/connect-memory)
        ledger  @(fluree/create conn "shacl-and")
        context ["https://ns.flur.ee" test-utils/default-str-context {"ex" "http://example.org/ns/"}]
        db0     (fluree/db ledger)
        db1     @(fluree/stage db0 {"@context" context
                                    "insert"
                                    {"@id" "ex:andShape"
                                     "@type" "sh:NodeShape"
                                     "sh:targetNode" {"@id" "ex:a"}
                                     "sh:and" [{"id" "ex:pshape1"
                                                "sh:path" {"@id" "ex:width"}
                                                "sh:minCount" 1}
                                               {"id" "ex:pshape2"
                                                "sh:path" {"@id" "ex:width"}
                                                "sh:datatype" {"@id" "xsd:integer"}}
                                               {"id" "ex:pshape3"
                                                "sh:path" {"@id" "ex:height"}
                                                "sh:minCount" 1}
                                               {"id" "ex:pshape4"
                                                "sh:path" {"@id" "ex:height"}
                                                "sh:datatype" {"@id" "xsd:integer"}}]}})]
    (testing "conforms to all shapes"
      (let [db2 @(fluree/stage db1 {"@context" context
                                    "insert" {"@id" "ex:a" "ex:height" 3 "ex:width" 4}})]
        (is (nil? (ex-data db2)))))

    (testing "conforms to only two shapes"
      (let [db2 @(fluree/stage db1 {"@context" context
                                    "insert" {"@id" "ex:a" "ex:height" 3}})]
        (is (= {:status 422,
                :error :shacl/violation,
                :report
                {"type" "sh:ValidationReport",
                 "sh:conforms" false,
                 "sh:result"
                 [{"type" "sh:ValidationResult",
                   "sh:resultSeverity" "sh:Violation",
                   "sh:focusNode" "ex:a",
                   "sh:constraintComponent" "sh:and",
                   "sh:sourceShape" "ex:andShape",
                   "sh:value" "ex:a",
                   "sh:resultMessage"
                   "ex:a failed to conform to all sh:and shapes: [\"ex:pshape1\" \"ex:pshape2\" \"ex:pshape3\" \"ex:pshape4\"]"}]}}
               (ex-data db2)))
        (is (= "Subject ex:a violates constraint sh:and of shape ex:andShape - ex:a failed to conform to all sh:and shapes: [\"ex:pshape1\" \"ex:pshape2\" \"ex:pshape3\" \"ex:pshape4\"]."
               (ex-message db2)))))))

(deftest ^:integration shacl-or-tests
  (let [conn    @(fluree/connect-memory)
        ledger  @(fluree/create conn "shacl-or")
        context ["https://ns.flur.ee" test-utils/default-str-context {"ex" "http://example.org/ns/"}]
        db0     (fluree/db ledger)
        db1     @(fluree/stage db0 {"@context" context
                                    "insert"
                                    {"@id" "ex:orShape"
                                     "@type" "sh:NodeShape"
                                     "sh:targetClass" {"@id" "ex:Dimensional"}
                                     "sh:or" [{"id" "ex:pshape1"
                                               "sh:path" {"@id" "ex:height"}
                                               "sh:minCount" 1
                                               "sh:datatype" {"@id" "xsd:integer"}}
                                              {"id" "ex:pshape2"
                                               "sh:path" {"@id" "ex:width"}
                                               "sh:minCount" 1
                                               "sh:datatype" {"@id" "xsd:integer"}}
                                              {"id" "ex:pshape3"
                                               "sh:path" {"@id" "ex:depth"}
                                               "sh:minCount" 1
                                               "sh:datatype" {"@id" "xsd:integer"}}]}})]
    (testing "conforms to one shape"
      (let [db2 @(fluree/stage db1 {"@context" context
                                    "insert" {"@id" "ex:1" "@type" "ex:Dimensional" "ex:height" 8}})]
        (is (nil? (ex-data db2)))))

    (testing "conforms to no shapes"
      (let [db2 @(fluree/stage db1 {"@context" context
                                    "insert" {"@id" "ex:2" "@type" "ex:Dimensional" "ex:bigness" "yup it's big"}})]
        (is (= {:status 422,
                :error :shacl/violation,
                :report
                {"type" "sh:ValidationReport",
                 "sh:conforms" false,
                 "sh:result"
                 [{"type" "sh:ValidationResult",
                   "sh:resultSeverity" "sh:Violation",
                   "sh:focusNode" "ex:2",
                   "sh:constraintComponent" "sh:or",
                   "sh:sourceShape" "ex:orShape",
                   "sh:value" "ex:2",
                   "sh:resultMessage" "ex:2 failed to conform to any of the following shapes: [\"ex:pshape1\" \"ex:pshape2\" \"ex:pshape3\"]"}]}}
               (ex-data db2)))
        (is (= "Subject ex:2 violates constraint sh:or of shape ex:orShape - ex:2 failed to conform to any of the following shapes: [\"ex:pshape1\" \"ex:pshape2\" \"ex:pshape3\"]."
               (ex-message db2)))))))

(deftest ^:integration shacl-xone-tests
  (let [conn    @(fluree/connect-memory)
        ledger  @(fluree/create conn "shacl-or")
        context ["https://ns.flur.ee" test-utils/default-str-context {"ex" "http://example.org/ns/"}]
        db0     (fluree/db ledger)
        db1     @(fluree/stage db0 {"@context" context
                                    "insert"
                                    {"@id"           "ex:orShape"
                                     "@type"         "sh:NodeShape"
                                     "sh:targetNode" {"@id" "ex:Named"}
                                     "sh:xone"       [{"@id" "ex:one-part"
                                                       "sh:property"
                                                       {"sh:path"     {"@id" "ex:fullName"}
                                                        "sh:minCount" 1}}
                                                      {"@id" "ex:two-parts"
                                                       "sh:property"
                                                       [{"sh:path"     {"@id" "ex:firstName"}
                                                         "sh:minCount" 1}
                                                        {"sh:path"     {"@id" "ex:lastName"}
                                                         "sh:minCount" 1}]}]}})]
    (testing "conforms to one shape"
      (let [db2 @(fluree/stage db1 {"@context" context
                                    "insert"   {"@id"         "ex:Named"
                                                "ex:fullName" "George Washington"}})]
        (is (nil? (ex-data db2)))))

    (testing "conforms to no shapes"
      (let [db2 @(fluree/stage db1 {"@context" context
                                    "insert"   {"@id" "ex:Named" "ex:nickname" "Father G"}})]
        (is (= {:status 422,
                :error :shacl/violation,
                :report
                {"type" "sh:ValidationReport",
                 "sh:conforms" false,
                 "sh:result"
                 [{"type" "sh:ValidationResult",
                   "sh:resultSeverity" "sh:Violation",
                   "sh:focusNode" "ex:Named",
                   "sh:constraintComponent" "sh:xone",
                   "sh:sourceShape" "ex:orShape",
                   "sh:value" ["Father G"],
                   "sh:resultMessage" "values conformed to 0 of the following sh:xone shapes: [\"ex:one-part\" \"ex:two-parts\"]; must only conform to one"}]}}
               (ex-data db2)))
        (is (= "Subject ex:Named violates constraint sh:xone of shape ex:orShape - values conformed to 0 of the following sh:xone shapes: [\"ex:one-part\" \"ex:two-parts\"]; must only conform to one."
               (ex-message db2)))))

    (testing "conforms to more than one shapes"
      (let [db2 @(fluree/stage db1 {"@context" context
                                    "insert"   {"@id"          "ex:Named"
                                                "ex:fullName"  "George Washington"
                                                "ex:firstName" "George"
                                                "ex:lastName"  "Washington"}})]
        (is (= {:status 422,
                :error :shacl/violation,
                :report
                {"type" "sh:ValidationReport",
                 "sh:conforms" false,
                 "sh:result"
                 [{"type" "sh:ValidationResult",
                   "sh:resultSeverity" "sh:Violation",
                   "sh:focusNode" "ex:Named",
                   "sh:constraintComponent" "sh:xone",
                   "sh:sourceShape" "ex:orShape",
                   "sh:value" ["George" "George Washington" "Washington"],
                   "sh:resultMessage" "values conformed to 2 of the following sh:xone shapes: [\"ex:one-part\" \"ex:two-parts\"]; must only conform to one"}]}}
               (ex-data db2)))
        (is (= "Subject ex:Named violates constraint sh:xone of shape ex:orShape - values conformed to 2 of the following sh:xone shapes: [\"ex:one-part\" \"ex:two-parts\"]; must only conform to one."
               (ex-message db2)))))))
