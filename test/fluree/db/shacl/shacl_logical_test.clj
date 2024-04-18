(ns fluree.db.shacl.shacl-logical-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.core :as util]))

(use-fixtures :each test-utils/deterministic-blank-node-fixture)

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
                          :sh/not         [{:sh/path     :schema/companyName
                                            :sh/minCount 1}
                                           {:sh/path   :schema/name
                                            :sh/equals :schema/callSign}]
                          :sh/property    [{:sh/path     :schema/callSign
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
          (is (= {:status 400,
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
                     :sh/resultMessage       ":ex/john conforms to shape _:fdb-2"}]}}
                 (ex-data db-company-name)))
          (is (= "Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape _:fdb-2."
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
          (is (= {:status 400,
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
                     :sh/resultMessage       ":ex/john conforms to shape _:fdb-2"}]}}
                 (ex-data db-two-names)))
          (is (= "Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape _:fdb-2."
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
          (is (= {:status 400,
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
                     :sh/resultMessage       ":ex/john conforms to shape _:fdb-3"}]}}
                 (ex-data db-callsign-name)))
          (is (= "Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape _:fdb-3."
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
                          :sh/not         [{:sh/path         :schema/age
                                            :sh/minInclusive 130}
                                           {:sh/path         :schema/favNums
                                            :sh/maxExclusive 9000}]
                          :sh/property    [{:sh/path     :schema/age
                                            :sh/minCount 1
                                            :sh/maxCount 1
                                            :sh/datatype :xsd/long}]}})



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
          (is (= {:status 400,
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
                     :sh/resultMessage       ":ex/john conforms to shape _:fdb-10"}
                    {:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape _:fdb-11"}]}}
                 (ex-data db-too-old)))
          (is (= "Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape _:fdb-10.
Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape _:fdb-11."
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
          (is (= {:status 400,
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
                     :sh/resultMessage       ":ex/john conforms to shape _:fdb-11"}]}}
                 (ex-data db-too-low)))
          (is (= "Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape _:fdb-11."
                 (ex-message db-too-low)))))
      (testing "conforms to min and max exclusive"
        (let []
          (is (= {:status 400,
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
                     :sh/resultMessage       ":ex/john conforms to shape _:fdb-10"}
                    {:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape _:fdb-11"}]}}
                 (ex-data db-two-probs)))
          (is (= "Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape _:fdb-10.
Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape _:fdb-11."
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
                          :sh/not         [{:sh/path      :ex/tag
                                            :sh/minLength 4}
                                           {:sh/path      :schema/name
                                            :sh/maxLength 10}
                                           {:sh/path    :ex/greeting
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
          (is (= {:status 400,
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
                     :sh/resultMessage       ":ex/john conforms to shape _:fdb-18"}
                    {:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape _:fdb-19"}
                    {:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape _:fdb-20"}]}}
                 (ex-data db-name-too-short)))
          (is (= "Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape _:fdb-18.
Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape _:fdb-19.
Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape _:fdb-20."
                 (ex-message db-name-too-short)))))
      (testing "tag conforms"
        (let [db-tag-too-long @(fluree/stage
                                 db
                                 {"@context" ["https://ns.flur.ee" context]
                                  "insert"
                                  {:id     :ex/john,
                                   :type   [:ex/User],
                                   :ex/tag 12345}})]
          (is (= {:status 400,
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
                     :sh/resultMessage       ":ex/john conforms to shape _:fdb-18"}
                    {:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape _:fdb-19"}
                    {:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape _:fdb-20"}]}}
                 (ex-data db-tag-too-long)))
          (is (= "Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape _:fdb-18.
Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape _:fdb-19.
Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape _:fdb-20."
                 (ex-message db-tag-too-long)))))
      (testing "greeting conforms"
        (let [db-greeting-incorrect @(fluree/stage
                                       db
                                       {"@context" ["https://ns.flur.ee" context]
                                        "insert"
                                        {:id          :ex/john,
                                         :type        [:ex/User],
                                         :ex/greeting "hello!"}})]
          (is (= {:status 400,
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
                     :sh/resultMessage       ":ex/john conforms to shape _:fdb-18"}
                    {:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape _:fdb-19"}
                    {:type                   :sh/ValidationResult,
                     :sh/resultSeverity      :sh/Violation,
                     :sh/focusNode           :ex/john,
                     :sh/constraintComponent :sh/not,
                     :sh/sourceShape         :ex/UserShape,
                     :sh/value               :ex/john,
                     :sh/resultMessage       ":ex/john conforms to shape _:fdb-20"}]}}
                 (ex-data db-greeting-incorrect)))
          (is (= "Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape _:fdb-18.
Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape _:fdb-19.
Subject :ex/john violates constraint :sh/not of shape :ex/UserShape - :ex/john conforms to shape _:fdb-20."
                 (ex-message db-greeting-incorrect))))))))
