(ns fluree.db.reasoner.datalog-test
  (:require [clojure.test :refer :all]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(def reasoning-db-data
  {"@context" {"ex" "http://example.org/"}
   "insert"   [{"@id"         "ex:brian"
                "ex:name"     "Brian"
                "ex:uncle"    {"@id" "ex:jim"}
                "ex:sibling"  [{"@id" "ex:laura"} {"@id" "ex:bob"}]
                "ex:children" [{"@id" "ex:alice"}]
                "ex:address"  {"ex:country" {"@id" "ex:Canada"}}
                "ex:age"      42
                "ex:parents"  {"@id"        "ex:carol"
                               "ex:name"    "Carol"
                               "ex:age"     72
                               "ex:address" {"ex:country" {"@id" "ex:Singapore"}}
                               "ex:brother" {"@id" "ex:mike"}}}
               {"@id"     "ex:laura"
                "ex:name" "Laura"}
               {"@id"       "ex:bob"
                "ex:name"   "Bob"
                "ex:gender" {"@id" "ex:Male"}}]})


(deftest ^:integration basic-datalog-rule
  (testing "Some basic datalog rules"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "reasoner/basic-datalog" nil)
          db0    @(fluree/stage @(fluree/db ledger) reasoning-db-data)]



      (testing "A standard relationship"
        (let [grandparent-db  @(fluree/stage
                                 db0
                                 {"@context" {"f"  "https://ns.flur.ee/ledger#"
                                              "ex" "http://example.org/"},
                                  "insert"
                                  {"@id"    "ex:grandParentRule"
                                   "f:rule" {"@type"  "@json"
                                             "@value" {"@context" {"ex" "http://example.org/"}
                                                       "where"    {"ex:children" "?children"
                                                                   "ex:parents"  "?parents"}
                                                       "insert"   {"@id"            "?children",
                                                                   "ex:grandParent" {"@id" "?parents"}}}}}})

              grandparent-db* @(fluree/reason grandparent-db :datalog)


              grandparents-of @(fluree/query grandparent-db*
                                             {:context {"ex" "http://example.org/"}
                                              :select  ["?grandParent" "?person"]
                                              :where   {"@id"            "?person",
                                                        "ex:grandParent" "?grandParent"}})]

          (testing "Reasoner type correctly set"
            (is (= #{:datalog} (-> grandparent-db* :reasoner))))

          (is (= #{["ex:carol" "ex:alice"]}
                 (set grandparents-of)))

          (is (= 1 (fluree/reasoned-count grandparent-db*))
              "Only one reasoned triple should be added")))

      (testing "A filter rule works"
        (let [senior-db  @(fluree/stage
                            db0
                            {"@context" {"f"  "https://ns.flur.ee/ledger#"
                                         "ex" "http://example.org/"},
                             "insert"   {"@id"    "ex:seniorRule"
                                         "f:rule" {"@type"  "@json"
                                                   "@value" {"@context" {"ex" "http://example.org/"}
                                                             "where"    [{"@id"    "?person",
                                                                          "ex:age" "?age"}
                                                                         ["filter" "(>= ?age 62)"]]
                                                             "insert"   {"@id"              "?person",
                                                                         "ex:seniorCitizen" true}}}}})
              senior-db* @(fluree/reason senior-db :datalog)

              seniors    @(fluree/query
                            senior-db* {:context {"ex" "http://example.org/"}
                                        :select  "?s"
                                        :where   {"@id"              "?s",
                                                  "ex:seniorCitizen" true}})]
          (is (= ["ex:carol"]
                 seniors))

          (is (= 1 (fluree/reasoned-count senior-db*))
              "Only one reasoned triple should be added")))


      (testing "Inferring based on a relationship and IRI value"
        (let [brother-db  @(fluree/stage
                             db0
                             {"@context" {"f"  "https://ns.flur.ee/ledger#"
                                          "ex" "http://example.org/"}
                              "insert"   {"@id"    "ex:brotherRule"
                                          "f:rule" {"@type"  "@json"
                                                    "@value" {"@context" {"ex" "http://example.org/"}
                                                              "where"    {"@id"        "?person",
                                                                          "ex:sibling" {"@id"       "?sibling"
                                                                                        "ex:gender" {"@id" "ex:Male"}}}
                                                              "insert"   {"@id"        "?person",
                                                                          "ex:brother" "?sibling"}}}}})
              brother-db* @(fluree/reason brother-db :datalog)]

          (is (= #{["ex:mike" "ex:carol"] ;; <- explicitly set
                   ["ex:bob" "ex:brian"]} ;; <- inferred
                 (-> @(fluree/query
                        brother-db*
                        {:context {"ex" "http://example.org/"}
                         :select  ["?brother" "?s"]
                         :where   {"@id"        "?s",
                                   "ex:brother" "?brother"}})
                     set)))

          (is (= 1 (fluree/reasoned-count brother-db*))
              "Only one reasoned triple should be added"))))))

(deftest ^:integration reason-graph-supplied
  (testing "Datalog rules given as JSON-LD at query-time"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "reasoner/basic-datalog-rules" nil)
          db0    @(fluree/stage @(fluree/db ledger) reasoning-db-data)]

      (testing "A recursive relationship"
        (let [grandparents-db @(fluree/reason db0 :datalog {"@context" {"f"  "https://ns.flur.ee/ledger#"
                                                                        "ex" "http://example.org/"},
                                                            "@id"      "ex:grandParentRule"
                                                            "f:rule"   {"@type"  "@json"
                                                                        "@value" {"@context" {"ex" "http://example.org/"}
                                                                                  "where"    {"ex:children" "?children"
                                                                                              "ex:parents"  "?parents"}
                                                                                  "insert"   {"@id"            "?children",
                                                                                              "ex:grandParent" {"@id" "?parents"}}}}})
              grandparents-of @(fluree/query grandparents-db
                                             {:context {"ex" "http://example.org/"}
                                              :select  ["?grandParent" "?person"]
                                              :where   {"@id"            "?person",
                                                        "ex:grandParent" "?grandParent"}})]

          (is (= #{["ex:carol" "ex:alice"]}
                 (set grandparents-of)))

          (is (= 1 (fluree/reasoned-count grandparents-db))
              "Only one reasoned triple should be added"))))))


(deftest ^:integration recursive-datalog-rule
  (testing "Some basic datalog rules"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "reasoner/recursive-datalog" nil)
          db0    @(fluree/stage
                    @(fluree/db ledger)
                    {"@context" {"ex" "http://example.org/"}
                     "insert"   [{"@id"                    "ex:task1"
                                  "ex:description"         "Task 1 (Top Level)"
                                  "ex:hasImmediateSubTask" [{"@id" "ex:task1-1"}
                                                            {"@id" "ex:task1-2"}]}
                                 {"@id"                    "ex:task1-1"
                                  "ex:description"         "Task 1-1 (Second Level)"
                                  "ex:hasImmediateSubTask" [{"@id" "ex:task1-1-1"}
                                                            {"@id" "ex:task1-1-2"}]}
                                 {"@id"                    "ex:task1-2"
                                  "ex:description"         "Task 1-2 (Second Level)"
                                  "ex:hasImmediateSubTask" [{"@id" "ex:task1-2-1"}
                                                            {"@id" "ex:task1-2-2"}]}
                                 {"@id"                    "ex:task1-1-1"
                                  "ex:description"         "Task 1-1-1 (Third Level)"
                                  "ex:hasImmediateSubTask" [{"@id" "ex:task1-1-1-1"}
                                                            {"@id" "ex:task1-1-1-2"}
                                                            {"@id" "ex:task1-1-1-3"}]}
                                 {"@id"                    "ex:task1-1-2"
                                  "ex:description"         "Task 1-1-2 (Third Level)"
                                  "ex:hasImmediateSubTask" [{"@id" "ex:task1-1-2-1"}
                                                            {"@id" "ex:task1-1-2-2"}]}]})]


      (testing "A recursive relationship"
        (let [db1  @(fluree/stage
                      db0
                      {"@context" {"f"  "https://ns.flur.ee/ledger#"
                                   "ex" "http://example.org/"},
                       "insert"
                       [{"@id"    "ex:hasSubTaskRule"
                         "f:rule" {"@type"  "@json"
                                   "@value" {"@context" {"ex" "http://example.org/"}
                                             "where"    {"@id"                    "?task"
                                                         "ex:hasImmediateSubTask" "?sub-task"}
                                             "insert"   {"@id"           "?task",
                                                         "ex:hasSubTask" {"@id" "?sub-task"}}}}}
                        {"@id"    "ex:hasSubTaskTransitive"
                         "f:rule" {"@type"  "@json"
                                   "@value" {"@context" {"ex" "http://example.org/"}
                                             "where"    {"@id"           "?task"
                                                         "ex:hasSubTask" {"ex:hasSubTask" "?sub-sub-task"}}
                                             "insert"   {"@id"           "?task",
                                                         "ex:hasSubTask" {"@id" "?sub-sub-task"}}}}}]})
              db1* @(fluree/reason db1 :datalog)]



          (is (= ["ex:task1-1"
                  "ex:task1-1-1"
                  "ex:task1-1-1-1"
                  "ex:task1-1-1-2"
                  "ex:task1-1-1-3"
                  "ex:task1-1-2"
                  "ex:task1-1-2-1"
                  "ex:task1-1-2-2"
                  "ex:task1-2"
                  "ex:task1-2-1"
                  "ex:task1-2-2"]
                 (-> @(fluree/query
                        db1* {:context {"ex" "http://example.org/"}
                              :select  "?subtask"
                              :where   {"@id"           "ex:task1",
                                        "ex:hasSubTask" "?subtask"}})
                     sort
                     vec))
              "Subtasks from every level should show at top level"))))))
