(ns fluree.db.reasoner.datalog-test
  (:require [clojure.test :refer :all]
            [fluree.db.api :as fluree]
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
                "ex:parents"   [{"@id"        "ex:carol"
                                 "ex:name"    "Carol"
                                 "ex:age"     72
                                 "ex:address" {"ex:country" {"@id" "ex:Singapore"}}
                                 "ex:brother" {"@id" "ex:mike"}}]}
               {"@id"     "ex:laura"
                "ex:name" "Laura"}
               {"@id"       "ex:bob"
                "ex:name"   "Bob"
                "ex:gender" {"@id" "ex:Male"}}
               {"@id"       "ex:jim"
                "ex:name"   "Jim"
                "ex:spouse" {"@id" "ex:janine"}}
               {"@id"       "ex:janine"
                "ex:name"   "Janine"
                "ex:gender" {"@id" "ex:Female"}}
               {"@id"       "ex:mike"
                "ex:name"   "Mike"
                "ex:spouse" {"@id" "ex:holly"}}
               {"@id"       "ex:holly"
                "ex:name"   "Holly"
                "ex:gender" {"@id" "ex:Female"}}]})

(def uncle-rule
  {"@context" {"f"  "https://ns.flur.ee/ledger#"
               "ex" "http://example.org/"},
   "@id"      "ex:uncleRule"
   "f:rule"   {"@type"  "@json"
               "@value" {"@context" {"ex" "http://example.org/"}
                         "where"    {"@id"       "?person",
                                     "ex:parents" {"ex:brother" {"@id" "?pBrother"}}},
                         "insert"   {"@id"      "?person",
                                     "ex:uncle" "?pBrother"}}}})

(def aunt-rule
  {"@context" {"f"  "https://ns.flur.ee/ledger#"
               "ex" "http://example.org/"},
   "@id"      "ex:auntRule"
   "f:rule"   {"@type"  "@json"
               "@value" {"@context" {"ex" "http://example.org/"}
                         "where"    {"@id"       "?person",
                                     "ex:uncle" {"ex:spouse" {"@id" "?aunt"
                                                              "ex:gender" {"@id" "ex:Female"}}}},
                         "insert"   {"@id"      "?person",
                                     "ex:aunt" "?aunt"}}}})

(def sibling-rule
  {"@context" {"f"  "https://ns.flur.ee/ledger#"
               "ex" "http://example.org/"},
   "@id"      "ex:siblingRule"
   "f:rule"   {"@type"  "@json"
               "@value" {"@context" {"ex" "http://example.org/"}
                         "where"    {"@id"        "?person",
                                     "ex:sibling" "?sibling"
                                     "ex:parents"  "?parent"},
                         "insert"   {"@id"       "?sibling",
                                     "ex:parents" "?parent"}}}})

(def brother-rule
  {"@context" {"f"  "https://ns.flur.ee/ledger#"
               "ex" "http://example.org/"},
   "@id"      "ex:brotherRule"
   "f:rule"   {"@type"  "@json"
               "@value" {"@context" {"ex" "http://example.org/"}
                         "where"    {"@id"        "?person",
                                     "ex:sibling" {"@id"       "?sibling"
                                                   "ex:gender" {"@id" "ex:Male"}}}
                         "insert"   {"@id"        "?person",
                                     "ex:brother" "?sibling"}}}})

(def grandparent-rule
  {"@context" {"f"  "https://ns.flur.ee/ledger#"
               "ex" "http://example.org/"}
   "@id"    "ex:grandParentRule"
   "f:rule" {"@type"  "@json"
             "@value" {"@context" {"ex" "http://example.org/"}
                       "where"    {"ex:children" "?children"
                                   "ex:parents"  "?parents"}
                       "insert"   {"@id"            "?children",
                                   "ex:grandParent" {"@id" "?parents"}}}}})

(def senior-rule
  {"@context" {"f"  "https://ns.flur.ee/ledger#"
               "ex" "http://example.org/"}
   "@id"    "ex:seniorRule"
   "f:rule" {"@type"  "@json"
             "@value" {"@context" {"ex" "http://example.org/"}
                       "where"    [{"@id"    "?person",
                                    "ex:age" "?age"}
                                   ["filter" "(>= ?age 62)"]]
                       "insert"   {"@id"              "?person",
                                   "ex:seniorCitizen" true}}}})

(deftest ^:integration basic-datalog-rule
  (testing "Some basic datalog rules"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "reasoner/basic-datalog" nil)
          db0    @(fluree/stage (fluree/db ledger) reasoning-db-data)]



      (testing "A standard relationship"
        (let [grandparent-db  @(fluree/stage db0 {"insert" [grandparent-rule]})

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
                            {"insert" [senior-rule]})
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
                             {"insert" [brother-rule]})
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
          db0    @(fluree/stage (fluree/db ledger) reasoning-db-data)]

      (testing "A recursive relationship"
        (let [grandparents-db @(fluree/reason db0 :datalog {:rule-graphs [grandparent-rule]})
              grandparents-of @(fluree/query grandparents-db
                                             {:context {"ex" "http://example.org/"}
                                              :select  ["?grandParent" "?person"]
                                              :where   {"@id"            "?person",
                                                        "ex:grandParent" "?grandParent"}})]

          (is (= #{["ex:carol" "ex:alice"]}
                 (set grandparents-of)))

          (is (= 1 (fluree/reasoned-count grandparents-db))
              "Only one reasoned triple should be added"))))))

(deftest ^:integration multiple-sources
  (testing "multiple rule sources"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "reasoner/multiple-rule-dbs")
          db0    @(fluree/stage (fluree/db ledger) reasoning-db-data)

          rule-ledger-1 @(fluree/create conn "reasoner/rule-ledger-1")
          rule-db-1     @(fluree/stage (fluree/db rule-ledger-1) {"insert" [uncle-rule]})

          rule-ledger-2 @(fluree/create conn "reasoner/rule-ledger-2")
          rule-db-2     @(fluree/stage (fluree/db rule-ledger-2) {"insert" [aunt-rule]})]


      (testing "multiple graphs as rule sources"
        (let [reasoned-db @(fluree/reason db0 :datalog {:rule-graphs [uncle-rule aunt-rule]})]

          (is (= [["ex:brian" "ex:holly"] ["ex:brian" "ex:janine"]]
                 @(fluree/query reasoned-db {:context {"ex" "http://example.org/"}
                                             :select  ["?s" "?aunt"]
                                             :where   {"@id"     "?s",
                                                       "ex:aunt" "?aunt"}}))
              "Multiple rule graphs can be used to reason about data.")))

      (testing "multiple dbs as rule sources"
        (let [reasoned-db @(fluree/reason db0 :datalog {:rule-dbs [rule-db-1 rule-db-2]})]

          (is (= [["ex:brian" "ex:holly"] ["ex:brian" "ex:janine"]]
                 @(fluree/query reasoned-db {:context {"ex" "http://example.org/"}
                                             :select  ["?s" "?aunt"]
                                             :where   {"@id"     "?s",
                                                       "ex:aunt" "?aunt"}}))
              "Multiple rule dbs can be used to reason about data.")))

      (testing "a mixture of graphs and dbs as rule sources"
        (let [reasoned-db @(fluree/reason db0 :datalog {:rule-dbs [rule-db-1]
                                                        :rule-graphs [aunt-rule]})]

          (is (= [["ex:brian" "ex:holly"] ["ex:brian" "ex:janine"]]
                 @(fluree/query reasoned-db {:context {"ex" "http://example.org/"}
                                             :select  ["?s" "?aunt"]
                                             :where   {"@id"     "?s",
                                                       "ex:aunt" "?aunt"}}))
              "Multiple rule dbs can be used to reason about data."))))))


(def has-subtask-rule
  {"@context" {"f"  "https://ns.flur.ee/ledger#"
               "ex" "http://example.org/"}
   "@id"    "ex:hasSubTaskRule"
   "f:rule" {"@type"  "@json"
             "@value" {"@context" {"ex" "http://example.org/"}
                       "where"    {"@id"                    "?task"
                                   "ex:hasImmediateSubTask" "?sub-task"}
                       "insert"   {"@id"           "?task",
                                   "ex:hasSubTask" {"@id" "?sub-task"}}}}})


(def has-subtask-transitive
  {"@context" {"f"  "https://ns.flur.ee/ledger#"
               "ex" "http://example.org/"}
   "@id"    "ex:hasSubTaskTransitive"
   "f:rule" {"@type"  "@json"
             "@value" {"@context" {"ex" "http://example.org/"}
                       "where"    {"@id"           "?task"
                                   "ex:hasSubTask" {"ex:hasSubTask" "?sub-sub-task"}}
                       "insert"   {"@id"           "?task",
                                   "ex:hasSubTask" {"@id" "?sub-sub-task"}}}}})

(deftest ^:integration recursive-datalog-rule
  (testing "Some basic datalog rules"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "reasoner/recursive-datalog" nil)
          db0    @(fluree/stage
                    (fluree/db ledger)
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
                      {"insert" [has-subtask-rule has-subtask-transitive]})
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
