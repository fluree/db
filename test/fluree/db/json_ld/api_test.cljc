(ns fluree.db.json-ld.api-test
  (:require #?(:clj  [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer-macros [deftest is testing async]])
            #?@(:cljs [[clojure.core.async :refer [go <!]]
                       [clojure.core.async.interop :refer [<p!]]])
            [fluree.db.dbproto :as dbproto]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            #?(:clj  [test-with-files.tools :refer [with-tmp-dir]
                      :as twf]
               :cljs [test-with-files.tools :as-alias twf])))

(deftest exists?-test
  (testing "returns false before committing data to a ledger"
    #?(:clj
       (let [conn         (test-utils/create-conn)
             ledger-alias "testledger"
             check1       @(fluree/exists? conn ledger-alias)
             ledger       @(fluree/create conn ledger-alias)
             check2       @(fluree/exists? conn ledger-alias)
             _            @(fluree/stage (fluree/db ledger)
                                         [{"id"           "f:me"
                                           "type"         "schema:Person"
                                           "schema:fname" "Me"}])
             check3       @(fluree/exists? conn ledger-alias)]
         (is (every? false? [check1 check2 check3])))))
  (testing "returns true after committing data to a ledger"
    #?(:clj
       (let [conn         (test-utils/create-conn)
             ledger-alias "testledger"
             ledger       @(fluree/create conn ledger-alias)
             db           @(fluree/stage (fluree/db ledger)
                                         [{"id"           "f:me"
                                           "type"         "schema:Person"
                                           "schema:fname" "Me"}])]
         @(fluree/commit! ledger db)
         (is (test-utils/retry-exists? conn ledger-alias 100))
         (is (not @(fluree/exists? conn "notaledger"))))

       :cljs
       (async done
         (go
          (let [conn         (<! (test-utils/create-conn))
                ledger-alias "testledger"
                ledger       (<p! (fluree/create conn ledger-alias))
                db           (<p! (fluree/stage (fluree/db ledger)
                                                [{"id"           "f:me"
                                                  "type"         "schema:Person"
                                                  "schema:fname" "Me"}]))]
            (<p! (fluree/commit! ledger db))
            (is (test-utils/retry-exists? conn ledger-alias 100))
            (is (not (<p! (fluree/exists? conn "notaledger"))))
            (done)))))))

(deftest create-test
  (testing "ledger context gets correctly merged with conn context when requested"
    #?(:clj
       (let [conn           (test-utils/create-conn)
             ledger-alias   "testledger"
             ledger-context {"ex"  "http://example.com/"
                             "foo" "http://foobar.com/"}
             ledger         @(fluree/create conn ledger-alias
                                            {"defaults"
                                             {"@context" ["" ledger-context]}})
             merged-context (merge test-utils/default-context ledger-context)]
         (is (= merged-context (dbproto/-default-context (fluree/db ledger))))))))

#?(:clj
   (deftest load-from-file-test
     (testing "can load a file ledger with single cardinality predicates"
       (with-tmp-dir storage-path
         (let [conn         @(fluree/connect
                              {"method" "file", "storage-path" storage-path
                               "defaults"
                               {"@context" test-utils/default-context}})
               ledger-alias "load-from-file-test-single-card"
               ledger       @(fluree/create conn ledger-alias
                                            {"defaults"
                                             {"@context"
                                              ["" {"ex" "http://example.org/ns/"}]}})
               db           @(fluree/stage
                              (fluree/db ledger)
                              [{"id"           "ex:brian"
                                "type"         "ex:User"
                                "schema:name"  "Brian"
                                "schema:email" "brian@example.org"
                                "schema:age"   50
                                "ex:favNums"   7
                                "ex:height"    6.2}

                               {"id"           "ex:cam"
                                "type"         "ex:User"
                                "schema:name"  "Cam"
                                "schema:email" "cam@example.org"
                                "schema:age"   34
                                "ex:favNums"   5
                                "ex:friend"    "ex:brian"}])
               db           @(fluree/commit! ledger db)
               db           @(fluree/stage
                              db
                              ;; test a retraction
                              {"f:retract" {"id"         "ex:brian"
                                            "ex:favNums" 7}})
               _            @(fluree/commit! ledger db)
               ;; TODO: Replace this w/ :syncTo equivalent once we have it
               loaded       (test-utils/retry-load conn ledger-alias 100)
               loaded-db    (fluree/db loaded)]
           (is (= (:t db) (:t loaded-db)))
           (is (= (:context ledger) (:context loaded))))))

     (testing "can load a file ledger with multi-cardinality predicates"
       (with-tmp-dir storage-path
         (let [conn         @(fluree/connect
                              {"method"       "file"
                               "storage-path" storage-path
                               "defaults"
                               {"@context" test-utils/default-context}})
               ledger-alias "load-from-file-test-multi-card"
               ledger       @(fluree/create conn ledger-alias
                                            {"defaults"
                                             {"@context"
                                              ["" {"ex" "http://example.org/ns/"}]}})
               db           @(fluree/stage
                              (fluree/db ledger)
                              [{"id"           "ex:brian"
                                "type"         "ex:User"
                                "schema:name"  "Brian"
                                "schema:email" "brian@example.org"
                                "schema:age"   50
                                "ex:favNums"   7}

                               {"id"           "ex:alice"
                                "type"         "ex:User"
                                "schema:name"  "Alice"
                                "schema:email" "alice@example.org"
                                "schema:age"   50
                                "ex:favNums"   [42 76 9]}

                               {"id"           "ex:cam"
                                "type"         "ex:User"
                                "schema:name"  "Cam"
                                "schema:email" "cam@example.org"
                                "schema:age"   34
                                "ex:favNums"   [5 10]
                                "ex:friend"    ["ex:brian" "ex:alice"]}])
               db           @(fluree/commit! ledger db)
               db           @(fluree/stage
                              db
                              ;; test a multi-cardinality retraction
                              [{"f:retract" {"id"         "ex:alice"
                                             "ex:favNums" [42 76 9]}}])
               _            @(fluree/commit! ledger db)
               ;; TODO: Replace this w/ :syncTo equivalent once we have it
               loaded       (test-utils/retry-load conn ledger-alias 100)
               loaded-db    (fluree/db loaded)]
           (is (= (:t db) (:t loaded-db)))
           (is (= (dbproto/-default-context db) (dbproto/-default-context loaded-db))))))

     (testing "can load a file ledger with its own context"
       (with-tmp-dir storage-path #_{::twf/delete-dir false}
         #_(println "storage path:" storage-path)
         (let [conn-context   {"id"  "@id", "type" "@type"
                               "xsd" "http://www.w3.org/2001/XMLSchema#"}
               ledger-context {"ex"     "http://example.com/"
                               "schema" "http://schema.org/"}
               conn           @(fluree/connect
                                {"method"       "file"
                                 "storage-path" storage-path
                                 "defaults"     {"@context" conn-context}})
               ledger-alias   "load-from-file-with-context"
               ledger         @(fluree/create conn ledger-alias
                                              {"defaults"
                                               {"@context"
                                                ["" ledger-context]}})
               db             @(fluree/stage
                                (fluree/db ledger)
                                [{"id"             "ex:wes"
                                  "type"           "ex:User"
                                  "schema:name"    "Wes"
                                  "schema:email"   "wes@example.org"
                                  "schema:age"     42
                                  "schema:favNums" [1 2 3]
                                  "ex:friend"      {"id"           "ex:jake"
                                                    "type"         "ex:User"
                                                    "schema:name"  "Jake"
                                                    "schema:email" "jake@example.org"}}])
               db             @(fluree/commit! ledger db)
               loaded         (test-utils/retry-load conn ledger-alias 100)
               loaded-db      (fluree/db loaded)
               merged-ctx     (merge conn-context ledger-context)
               query          {"where"  '[[?p "schema:email" "wes@example.org"]]
                               "select" '{?p ["*"]}}
               results        @(fluree/query loaded-db query)
               full-type-url  "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]
           (is (= (:t db) (:t loaded-db)))
           (is (= merged-ctx (dbproto/-default-context loaded-db)))
           (is (= [{full-type-url    ["ex:User"]
                    "id"             "ex:wes"
                    "schema:age"     42
                    "schema:email"   "wes@example.org"
                    "schema:favNums" [1 2 3]
                    "schema:name"    "Wes"
                    "ex:friend"      {"id" "ex:jake"}}]
                  results)))))

     (testing "query returns the correct results from a loaded ledger"
       (with-tmp-dir storage-path
         (let [conn-context   {"id" "@id", "type" "@type"}
               ledger-context {"ex"     "http://example.com/"
                               "schema" "http://schema.org/"}
               conn           @(fluree/connect
                                {"method"   "file", "storage-path" storage-path
                                 "defaults" {"@context" conn-context}})
               ledger-alias   "load-from-file-query"
               ledger         @(fluree/create conn ledger-alias
                                              {"defaults"
                                               {"@context"
                                                ["" ledger-context]}})
               db             @(fluree/stage
                                (fluree/db ledger)
                                [{"id"          "ex:Andrew"
                                  "type"        "schema:Person"
                                  "schema:name" "Andrew"
                                  "ex:friend"   {"id"          "ex:Jonathan"
                                                 "type"        "schema:Person"
                                                 "schema:name" "Jonathan"}}])
               query          {"select" '{?s ["*"]}
                               "where"  '[[?s "id" "ex:Andrew"]]}
               res1           @(fluree/query db query)
               _              @(fluree/commit! ledger db)
               loaded         (test-utils/retry-load conn ledger-alias 100)
               loaded-db      (fluree/db loaded)
               res2           @(fluree/query loaded-db query)]
           (is (= res1 res2)))))

     (testing "can load a ledger with `list` values"
       (with-tmp-dir storage-path
         (let [conn         @(fluree/connect
                              {"method"       "file"
                               "storage-path" storage-path
                               "defaults"
                               {"@context" (merge test-utils/default-context
                                                  {"ex" "http://example.org/ns/"})}})
               ledger-alias "load-lists-test"
               ledger       @(fluree/create conn ledger-alias)
               db           @(fluree/stage
                              (fluree/db ledger)
                              [{"id"         "ex:alice",
                                "type"       "ex:User",
                                "ex:friends" {"@list" [{"id" "ex:john"}
                                                       {"id" "ex:cam"}]}}
                               {"id"         "ex:cam",
                                "type"       "ex:User"
                                "ex:numList" {"@list" [7 8 9 10]}}
                               {"id"   "ex:john",
                                "type" "ex:User"}])
               db           @(fluree/commit! ledger db)
               loaded       (test-utils/retry-load conn ledger-alias 100)
               loaded-db    (fluree/db loaded)]
           (is (= (:t db) (:t loaded-db)))
           (testing "query returns expected `list` values"
             (is (= #{{"id"         "ex:cam",
                       "rdf:type"   ["ex:User"],
                       "ex:numList" [7 8 9 10]}
                      {"id" "ex:john", "rdf:type" ["ex:User"]}
                      {"id"         "ex:alice",
                       "rdf:type"   ["ex:User"],
                       "ex:friends" [{"id" "ex:john"} {"id" "ex:cam"}]}}
                     (set
                      @(fluree/query loaded-db '{"select" {?s ["*"]}
                                                 "where"  [[?s "rdf:type" "ex:User"]]})))))))

       (testing "can load with policies"
         (with-tmp-dir storage-path
           (let [conn         @(fluree/connect
                                {"method"       "file"
                                 "storage-path" storage-path
                                 "defaults"
                                 {"@context" (merge test-utils/default-context
                                                    {"ex" "http://example.org/ns/"})}})
                 ledger-alias "load-policy-test"
                 ledger       @(fluree/create conn ledger-alias)
                 db           @(fluree/stage
                                (fluree/db ledger)
                                [{"id"          "ex:alice",
                                  "type"        "ex:User",
                                  "schema:name" "Alice"
                                  "schema:ssn"  "111-11-1111"
                                  "ex:friend"   {"id" "ex:john"}}
                                 {"id"          "ex:john",
                                  "schema:name" "John"
                                  "type"        "ex:User",
                                  "schema:ssn"  "888-88-8888"}
                                 {"id"      "did:fluree:123"
                                  "ex:user" {"id" "ex:alice"}
                                  "f:role"  {"id" "ex:userRole"}}])
                 db+policy    @(fluree/stage
                                db
                                [{"id"            "ex:UserPolicy"
                                  "type"          ["f:Policy"]
                                  "f:targetClass" {"id" "ex:User"}
                                  "f:allow"       [{"id"           "ex:globalViewAllow"
                                                    "f:targetRole" {"id" "ex:userRole"}
                                                    "f:action"     [{"id" "f:view"}]}]
                                  "f:property"
                                  [{"f:path" {"id" "schema:ssn"}
                                    "f:allow"
                                    [{"id"           "ex:ssnViewRule"
                                      "f:targetRole" {"id" "ex:userRole"}
                                      "f:action"     [{"id" "f:view"}]
                                      "f:equals"     {"@list" [{"id" "f:$identity"}
                                                               {"id" "ex:user"}]}}]}]}])
                 db+policy    @(fluree/commit! ledger db+policy)
                 loaded       (test-utils/retry-load conn ledger-alias 100)
                 loaded-db    (fluree/db loaded)]
             (is (= (:t db) (:t loaded-db)))
             (testing "query returns expected policy"
               (is (= [{"id"            "ex:UserPolicy",
                        "rdf:type"      ["f:Policy"],
                        "f:allow"
                        {"id"           "ex:globalViewAllow",
                         "f:action"     {"id" "f:view"},
                         "f:targetRole" {"_id" 211106232532995}},
                        "f:property"
                        {"id"     "_:f211106232532999",
                         "f:allow"
                         {"id"           "ex:ssnViewRule",
                          "f:action"     {"id" "f:view"},
                          "f:targetRole" {"_id" 211106232532995},
                          "f:equals"     [{"id" "f:$identity"} {"id" "ex:user"}]},
                         "f:path" {"id" "schema:ssn"}},
                        "f:targetClass" {"id" "ex:User"}}]
                      @(fluree/query
                        loaded-db
                        '{"select" {?s ["*"
                                        {"rdf:type" ["_id"]}
                                        {"f:allow" ["*" {"f:targetRole" ["_id"]}]}
                                        {"f:property" ["*" {"f:allow" ["*" {"f:targetRole" ["_id"]}]}]}]}
                          "where"  [[?s "rdf:type" "f:Policy"]]}))))))))))

#?(:clj
   (deftest load-from-memory-test
     (testing "can load a memory ledger with single cardinality predicates"
       (let [conn         @(fluree/connect
                            {"method" "memory"
                             "defaults"
                             {"@context" test-utils/default-context}})
             ledger-alias "load-from-memory-test-single-card"
             ledger       @(fluree/create conn ledger-alias
                                          {"defaults"
                                           {"@context"
                                            ["" {"ex" "http://example.org/ns/"}]}})
             db           @(fluree/stage
                            (fluree/db ledger)
                            [{"id"           "ex:brian"
                              "type"         "ex:User"
                              "schema:name"  "Brian"
                              "schema:email" "brian@example.org"
                              "schema:age"   50
                              "ex:favNums"   7}

                             {"id"           "ex:cam"
                              "type"         "ex:User"
                              "schema:name"  "Cam"
                              "schema:email" "cam@example.org"
                              "schema:age"   34
                              "ex:favNums"   5
                              "ex:friend"    "ex:brian"}])
             db           @(fluree/commit! ledger db)
             db           @(fluree/stage
                            db
                            {"id"         "ex:brian"
                             "ex:favNums" 7})
             _            @(fluree/commit! ledger db)
             ;; TODO: Replace this w/ :syncTo equivalent once we have it
             loaded       (test-utils/retry-load conn ledger-alias 100)
             loaded-db    (fluree/db loaded)]
         (is (= (:t db) (:t loaded-db)))
         (is (= (:context ledger) (:context loaded)))))

     (testing "can load a memory ledger with multi-cardinality predicates"
       (let [conn         @(fluree/connect
                            {"method" "memory"
                             "defaults"
                             {"@context" test-utils/default-context}})
             ledger-alias "load-from-memory-test-multi-card"
             ledger       @(fluree/create conn ledger-alias
                                          {"defaults"
                                           {"@context"
                                            ["" {"ex" "http://example.org/ns/"}]}})
             db           @(fluree/stage
                            (fluree/db ledger)
                            [{"id"           "ex:brian"
                              "type"         "ex:User"
                              "schema:name"  "Brian"
                              "schema:email" "brian@example.org"
                              "schema:age"   50
                              "ex:favNums"   7}

                             {"id"           "ex:alice"
                              "type"         "ex:User"
                              "schema:name"  "Alice"
                              "schema:email" "alice@example.org"
                              "schema:age"   50
                              "ex:favNums"   [42 76 9]}

                             {"id"           "ex:cam"
                              "type"         "ex:User"
                              "schema:name"  "Cam"
                              "schema:email" "cam@example.org"
                              "schema:age"   34
                              "ex:favNums"   [5 10]
                              "ex:friend"    ["ex:brian" "ex:alice"]}])
             db           @(fluree/commit! ledger db)
             db           @(fluree/stage
                            db
                            ;; test a multi-cardinality retraction
                            [{"id"         "ex:alice"
                              "ex:favNums" [42 76 9]}])
             _            @(fluree/commit! ledger db)
             ;; TODO: Replace this w/ :syncTo equivalent once we have it
             loaded       (test-utils/retry-load conn ledger-alias 100)
             loaded-db    (fluree/db loaded)]
         (is (= (:t db) (:t loaded-db)))
         (is (= (dbproto/-default-context db) (dbproto/-default-context loaded-db)))))

     (testing "can load a memory ledger with its own context"
       (let [conn-context   {"id"  "@id", "type" "@type"
                             "xsd" "http://www.w3.org/2001/XMLSchema#"}
             ledger-context {"ex"     "http://example.com/"
                             "schema" "http://schema.org/"}
             conn           @(fluree/connect
                              {"method"   "memory"
                               "defaults" {"@context" conn-context}})
             ledger-alias   "load-from-memory-with-context"
             ledger         @(fluree/create conn ledger-alias
                                            {"defaults"
                                             {"@context"
                                              ["" ledger-context]}})
             db             @(fluree/stage
                              (fluree/db ledger)
                              [{"id"             "ex:wes"
                                "type"           "ex:User"
                                "schema:name"    "Wes"
                                "schema:email"   "wes@example.org"
                                "schema:age"     42
                                "schema:favNums" [1 2 3]
                                "ex:friend"      {"id"           "ex:jake"
                                                  "type"         "ex:User"
                                                  "schema:name"  "Jake"
                                                  "schema:email" "jake@example.org"}}])
             db             @(fluree/commit! ledger db)
             loaded         (test-utils/retry-load conn ledger-alias 100)
             loaded-db      (fluree/db loaded)
             merged-ctx     (merge conn-context ledger-context)
             query          {"where"  '[[?p "schema:email" "wes@example.org"]]
                             "select" '{?p ["*"]}}
             results        @(fluree/query loaded-db query)
             full-type-url  "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]
         (is (= (:t db) (:t loaded-db)))
         (is (= merged-ctx (dbproto/-default-context loaded-db)))
         (is (= [{full-type-url    ["ex:User"]
                  "id"             "ex:wes"
                  "schema:age"     42
                  "schema:email"   "wes@example.org"
                  "schema:favNums" [1 2 3]
                  "schema:name"    "Wes"
                  "ex:friend"      {"id" "ex:jake"}}]
                results))))

     (testing "query returns the correct results from a loaded ledger"
       (let [conn-context   {"id" "@id", "type" "@type"}
             ledger-context {"ex"     "http://example.com/"
                             "schema" "http://schema.org/"}
             conn           @(fluree/connect
                              {"method"   "memory"
                               "defaults" {"@context" conn-context}})
             ledger-alias   "load-from-memory-query"
             ledger         @(fluree/create conn ledger-alias
                                            {"defaults"
                                             {"@context"
                                              ["" ledger-context]}})
             db             @(fluree/stage
                              (fluree/db ledger)
                              [{"id"          "ex:Andrew"
                                "type"        "schema:Person"
                                "schema:name" "Andrew"
                                "ex:friend"   {"id"          "ex:Jonathan"
                                               "type"        "schema:Person"
                                               "schema:name" "Jonathan"}}])
             query          {"select" '{?s ["*"]}
                             "where"  '[[?s "id" "ex:Andrew"]]}
             res1           @(fluree/query db query)
             _              @(fluree/commit! ledger db)
             loaded         (test-utils/retry-load conn ledger-alias 100)
             loaded-db      (fluree/db loaded)
             res2           @(fluree/query loaded-db query)]
         (is (= res1 res2))))

     (testing "can load a ledger with `list` values"
       (let [conn         @(fluree/connect
                            {"method" "memory"
                             "defaults"
                             {"@context" (merge test-utils/default-context
                                                {"ex" "http://example.org/ns/"})}})
             ledger-alias "load-lists-test"
             ledger       @(fluree/create conn ledger-alias)
             db           @(fluree/stage
                            (fluree/db ledger)
                            [{"id"         "ex:alice",
                              "type"       "ex:User",
                              "ex:friends" {"@list" [{"id" "ex:john"}
                                                     {"id" "ex:cam"}]}}
                             {"id"         "ex:cam",
                              "type"       "ex:User"
                              "ex:numList" {"@list" [7 8 9 10]}}
                             {"id"   "ex:john",
                              "type" "ex:User"}])
             db           @(fluree/commit! ledger db)
             loaded       (test-utils/retry-load conn ledger-alias 100)
             loaded-db    (fluree/db loaded)]
         (is (= (:t db) (:t loaded-db)))
         (testing "query returns expected `list` values"
           (is (= [{"id"         "ex:cam"
                    "rdf:type"   ["ex:User"]
                    "ex:numList" [7 8 9 10]}
                   {"id" "ex:john", "rdf:type" ["ex:User"]}
                   {"id"         "ex:alice"
                    "rdf:type"   ["ex:User"]
                    "ex:friends" [{"id" "ex:john"} {"id" "ex:cam"}]}]
                  @(fluree/query loaded-db '{"select" {?s ["*"]}
                                             "where"  [[?s "rdf:type" "ex:User"]]})))))

       (testing "can load with policies"
         (let [conn         @(fluree/connect
                              {"method" "memory"
                               "defaults"
                               {"@context" (merge test-utils/default-context
                                                  {"ex" "http://example.org/ns/"})}})
               ledger-alias "load-policy-test"
               ledger       @(fluree/create conn ledger-alias)
               db           @(fluree/stage
                              (fluree/db ledger)
                              [{"id"          "ex:alice",
                                "type"        "ex:User",
                                "schema:name" "Alice"
                                "schema:ssn"  "111-11-1111"
                                "ex:friend"   {"id" "ex:john"}}
                               {"id"          "ex:john",
                                "schema:name" "John"
                                "type"        "ex:User",
                                "schema:ssn"  "888-88-8888"}
                               {"id"      "did:fluree:123"
                                "ex:user" {"id" "ex:alice"}
                                "f:role"  {"id" "ex:userRole"}}])
               db+policy    @(fluree/stage
                              db
                              [{"id"            "ex:UserPolicy",
                                "type"          ["f:Policy"],
                                "f:targetClass" {"id" "ex:User"}
                                "f:allow"       [{"id"           "ex:globalViewAllow"
                                                  "f:targetRole" {"id" "ex:userRole"}
                                                  "f:action"     [{"id" "f:view"}]}]
                                "f:property"
                                [{"f:path"  {"id" "schema:ssn"}
                                  "f:allow" [{"id"           "ex:ssnViewRule"
                                              "f:targetRole" {"id" "ex:userRole"}
                                              "f:action"     [{"id" "f:view"}]
                                              "f:equals"     {"@list" [{"id" "f:$identity"}
                                                                       {"id" "ex:user"}]}}]}]}])
               db+policy    @(fluree/commit! ledger db+policy)
               loaded       (test-utils/retry-load conn ledger-alias 100)
               loaded-db    (fluree/db loaded)]
           (is (= (:t db) (:t loaded-db)))
           (testing "query returns expected policy"
             (is (= [{"id"            "ex:UserPolicy",
                      "rdf:type"      ["f:Policy"],
                      "f:allow"
                      {"id"           "ex:globalViewAllow",
                       "f:action"     {"id" "f:view"},
                       "f:targetRole" {"_id" 211106232532995}},
                      "f:property"
                      {"id"     "_:f211106232532999",
                       "f:allow"
                       {"id"           "ex:ssnViewRule",
                        "f:action"     {"id" "f:view"},
                        "f:targetRole" {"_id" 211106232532995},
                        "f:equals"     [{"id" "f:$identity"} {"id" "ex:user"}]},
                       "f:path" {"id" "schema:ssn"}},
                      "f:targetClass" {"id" "ex:User"}}]
                    @(fluree/query loaded-db
                                   '{"select" {?s ["*"
                                                   {"rdf:type" ["_id"]}
                                                   {"f:allow" ["*" {"f:targetRole" ["_id"]}]}
                                                   {"f:property" ["*" {"f:allow" ["*" {"f:targetRole" ["_id"]}]}]}]}
                                     "where"  [[?s "rdf:type" "f:Policy"]]})))))))))
