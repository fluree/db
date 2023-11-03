(ns fluree.db.transact.update-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]))

(deftest ^:integration deleting-data
  (testing "Deletions of entire subjects."
    (let [conn             (test-utils/create-conn)
          ledger           @(fluree/create conn "tx/delete"
                                           {:defaultContext
                                            ["" {:ex "http://example.org/ns/"}]})
          db               @(fluree/stage
                             (fluree/db ledger)
                             {:graph [{:id           :ex/alice
                                       :type         :ex/User
                                       :schema/name  "Alice"
                                       :schema/email "alice@flur.ee"
                                       :schema/age   42}
                                      {:id          :ex/bob
                                       :type        :ex/User
                                       :schema/name "Bob"
                                       :schema/age  22}
                                      {:id           :ex/jane
                                       :type         :ex/User
                                       :schema/name  "Jane"
                                       :schema/email "jane@flur.ee"
                                       :schema/age   30}]})

          ;; delete everything for :ex/alice
          db-subj-delete   @(fluree/stage db
                                          '{:delete {:id :ex/alice, ?p ?o}
                                            :where  {:id :ex/alice, ?p ?o}})

          ;; delete any :schema/age values for :ex/bob
          db-subj-pred-del @(fluree/stage db
                                          '{:delete {:id :ex/bob, :schema/age ?o}
                                            :where  {:id :ex/bob, :schema/age ?o}})

          ;; delete all subjects with a :schema/email predicate
          db-all-preds     @(fluree/stage db
                                          '{:delete {:id ?s, ?p ?o}
                                            :where  {:id           ?s
                                                     :schema/email ?x
                                                     ?p            ?o}})

          ;; delete all subjects where :schema/age = 30
          db-age-delete    @(fluree/stage db
                                          '{:delete {:id ?s, ?p ?o}
                                            :where  {:id         ?s
                                                     :schema/age 30
                                                     ?p          ?o}})

          ;; Change Bob's age - but only if his age is still 22
          db-update-bob    @(fluree/stage db
                                          '{:delete {:id :ex/bob, :schema/age 22}
                                            :insert {:id :ex/bob, :schema/age 23}
                                            :where  {:id :ex/bob, :schema/age 22}})

          ;; Shouldn't change Bob's age as the current age is not a match
          db-update-bob2   @(fluree/stage db
                                          '{:delete {:id :ex/bob, :schema/age 99}
                                            :insert {:id :ex/bob, :schema/age 23}
                                            :where  {:id :ex/bob, :schema/age 99}})

          ;; change Jane's age regardless of its current value
          db-update-jane   @(fluree/stage db
                                          '{:delete {:id :ex/jane, :schema/age ?current-age}
                                            :insert {:id :ex/jane, :schema/age 31}
                                            :where  {:id :ex/jane, :schema/age ?current-age}})]

      (is (= @(fluree/query db-subj-delete
                            '{:select ?name
                              :where  {:schema/name ?name}})
             ["Bob" "Jane"])
          "Only Jane and Bob should be left in the db.")

      (is (= @(fluree/query db-subj-pred-del
                            '{:selectOne {:ex/bob [:*]}})
             {:id          :ex/bob,
              :type        :ex/User,
              :schema/name "Bob"})
          "Bob should no longer have an age property.")

      (is (= @(fluree/query db-all-preds
                            '{:select ?name
                              :where  {:schema/name ?name}})
             ["Bob"])
          "Only Bob should be left, as he is the only one without an email.")

      (is (= @(fluree/query db-age-delete
                            '{:select ?name
                              :where  {:schema/name ?name}})
             ["Alice" "Bob"])
          "Only Bob and Alice should be left in the db.")

      (testing "Updating property value only if its current value is a match."
        (is (= [{:id          :ex/bob,
                 :type        :ex/User,
                 :schema/name "Bob"
                 :schema/age  23}]
               @(fluree/query db-update-bob
                              '{:select {:ex/bob [:*]}}))
            "Bob's age should now be updated to 23 (from 22)."))

      (testing "No update should happen if there is no match."
        (is (= [{:id          :ex/bob,
                 :type        :ex/User,
                 :schema/name "Bob"
                 :schema/age  22}]
               @(fluree/query db-update-bob2
                              '{:select {:ex/bob [:*]}}))
            "Bob's age should have not been changed and still be 22."))

      (testing "Replacing existing property value with new property value."
        (is (= [{:id           :ex/jane,
                 :type         :ex/User,
                 :schema/name  "Jane"
                 :schema/email "jane@flur.ee"
                 :schema/age   31}]
               @(fluree/query db-update-jane
                              '{:select {:ex/jane [:*]}}))
            "Jane's age should now be updated to 31 (from 30).")))))

(deftest transaction-functions
  (let [conn   @(fluree/connect {:method :memory})
        ledger @(fluree/create conn "functions" {:defaultContext [test-utils/default-str-context
                                                                  {"ex" "http://example.com/"}]})
        db1    (fluree/db ledger)]

    (testing "hash functions"
      (with-redefs [fluree.db.query.exec.eval/now (fn [] "2023-06-13T19:53:57.234345Z")]
        (let [updated (-> @(fluree/stage db1 [{"id"     "ex:create-predicates"
                                               "ex:md5" 0 "ex:sha1" 0 "ex:sha256" 0 "ex:sha384" 0 "ex:sha512" 0}
                                              {"id"         "ex:hash-fns"
                                               "ex:message" "abc"}])
                          (fluree/stage {"delete" []
                                         "where"  [{"id"         "ex:hash-fns"
                                                    "ex:message" "?message"}
                                                   ["bind"
                                                    "?sha256" "(sha256 ?message)"
                                                    "?sha512" "(sha512 ?message)"]]
                                         "insert" {"id"        "ex:hash-fns"
                                                   "ex:sha256" "?sha256"
                                                   "ex:sha512" "?sha512"}}))]
          (is (= {"ex:sha512" "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
                  "ex:sha256" "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}
                 @(fluree/query @updated {"selectOne" {"ex:hash-fns" ["ex:sha512" "ex:sha256"]}}))))))
    (testing "datetime functions"
      (with-redefs [fluree.db.query.exec.eval/now (fn [] "2023-06-13T19:53:57.234345Z")]
        (let [updated (-> @(fluree/stage db1 [{"id"         "ex:create-predicates"
                                               "ex:now"     0 "ex:year" 0 "ex:month" 0 "ex:day" 0 "ex:hours" 0
                                               "ex:minutes" 0 "ex:seconds" 0 "ex:timezone" 0 "ex:tz" 0}
                                              {"id"                "ex:datetime-fns"
                                               "ex:localdatetime"  "2023-06-13T14:17:22.435"
                                               "ex:offsetdatetime" "2023-06-13T14:17:22.435-05:00"
                                               "ex:utcdatetime"    "2023-06-13T14:17:22.435Z"}])
                          (fluree/stage {"delete" []
                                         "where"  [{"id"                "?s"
                                                    "ex:localdatetime"  "?localdatetime"
                                                    "ex:offsetdatetime" "?offsetdatetime"
                                                    "ex:utcdatetime"    "?utcdatetime"}
                                                   ["bind"
                                                    "?now" "(now)"
                                                    "?year" "(year ?localdatetime)"
                                                    "?month" "(month ?localdatetime)"
                                                    "?day" "(day ?localdatetime)"
                                                    "?hours" "(hours ?localdatetime)"
                                                    "?minutes" "(minutes ?localdatetime)"
                                                    "?seconds" "(seconds ?localdatetime)"
                                                    "?tz1" "(tz ?utcdatetime)"
                                                    "?tz2" "(tz ?offsetdatetime)"]]
                                         "insert" [{"id"         "?s"
                                                    "ex:now"     "?now"
                                                    "ex:year"    "?year"
                                                    "ex:month"   "?month"
                                                    "ex:day"     "?day"
                                                    "ex:hours"   "?hours"
                                                    "ex:minutes" "?minutes"
                                                    "ex:seconds" "?seconds"
                                                    "ex:tz"      ["?tz1" "?tz2"]}]
                                         "values" ["?s" ["ex:datetime-fns"]]}))]
          (is (= {"ex:now"     "2023-06-13T19:53:57.234345Z"
                  "ex:year"    2023
                  "ex:month"   6
                  "ex:day"     13
                  "ex:hours"   14
                  "ex:minutes" 17
                  "ex:seconds" 22
                  "ex:tz"      ["-05:00" "Z"]}
                 @(fluree/query @updated
                                {"selectOne"
                                 {"ex:datetime-fns" ["ex:now" "ex:year"
                                                     "ex:month" "ex:day"
                                                     "ex:hours" "ex:minutes"
                                                     "ex:seconds" "ex:tz"]}}))))))

    (testing "numeric functions"
      (let [updated (-> @(fluree/stage db1 [{"id"     "ex:create-predicates"
                                             "ex:abs" 0 "ex:round" 0 "ex:ceil" 0 "ex:floor" 0 "ex:rand" 0}
                                            {"id"         "ex:numeric-fns"
                                             "ex:pos-int" 2
                                             "ex:neg-int" -2
                                             "ex:decimal" 1.4}])
                        (fluree/stage {"delete" []
                                       "where"  [{"id"         "?s"
                                                  "ex:pos-int" "?pos-int"
                                                  "ex:neg-int" "?neg-int"
                                                  "ex:decimal" "?decimal"}
                                                 ["bind"
                                                  "?abs" "(abs ?neg-int)"
                                                  "?round" "(round ?decimal)"
                                                  "?ceil" "(ceil ?decimal)"
                                                  "?floor" "(floor ?decimal)"
                                                  "?rand" "(rand)"]]
                                       "insert" {"id"       "?s"
                                                 "ex:abs"   "?abs"
                                                 "ex:round" "?round"
                                                 "ex:ceil"  "?ceil"
                                                 "ex:floor" "?floor"
                                                 "ex:rand"  "?rand"}
                                       "values" ["?s" ["ex:numeric-fns"]]}))]
        (is (= {"ex:abs"   2
                "ex:round" 1
                "ex:ceil"  2
                "ex:floor" 1}
               @(fluree/query @updated
                              {"selectOne"
                               {"ex:numeric-fns" ["ex:abs" "ex:round" "ex:ceil"
                                                  "ex:floor"]}})))
        (is (pos? @(fluree/query @updated {"where"     {"id"      "ex:numeric-fns"
                                                        "ex:rand" "?rand"}
                                           "selectOne" "?rand"})))))
    (testing "string functions"
      (let [updated (-> @(fluree/stage db1 [{"id"             "ex:create-predicates"
                                             "ex:strLen"      0 "ex:subStr" 0 "ex:ucase" 0 "ex:lcase" 0 "ex:strStarts" 0 "ex:strEnds" 0
                                             "ex:contains"    0 "ex:strBefore" 0 "ex:strAfter" 0 "ex:encodeForUri" 0 "ex:concat" 0
                                             "ex:langMatches" 0 "ex:regex" 0 "ex:replace" 0}
                                            {"id"      "ex:string-fns"
                                             "ex:text" "Abcdefg"}])
                        (fluree/stage {"delete" []
                                       "where"  [{"id"      "?s"
                                                  "ex:text" "?text"}
                                                 ["bind"
                                                  "?strlen" "(strLen ?text)"
                                                  "?sub1" "(subStr ?text 5)"
                                                  "?sub2" "(subStr ?text 1 4)"
                                                  "?upcased" "(ucase ?text)"
                                                  "?downcased" "(lcase ?text)"
                                                  "?a-start" "(strStarts ?text \"x\")"
                                                  "?a-end" "(strEnds ?text \"x\")"
                                                  "?contains" "(contains ?text \"x\")"
                                                  "?strBefore" "(strBefore ?text \"bcd\")"
                                                  "?strAfter" "(strAfter ?text \"bcd\")"
                                                  "?concatted" "(concat ?text \" \" \"STR1 \" \"STR2\")"
                                                  "?matched" "(regex ?text \"^Abc\")"]]
                                       "insert" [{"id"           "?s"
                                                  "ex:strStarts" "?a-start"
                                                  "ex:strEnds"   "?a-end"
                                                  "ex:subStr"    ["?sub1" "?sub2"]
                                                  "ex:strLen"    "?strlen"
                                                  "ex:ucase"     "?upcased"
                                                  "ex:lcase"     "?downcased"
                                                  "ex:contains"  "?contains"
                                                  "ex:strBefore" "?strBefore"
                                                  "ex:strAfter"  "?strAfter"
                                                  "ex:concat"    "?concatted"
                                                  "ex:regex"     "?matched"}]
                                       "values" ["?s" ["ex:string-fns"]]}))]
        (is (= {"ex:strEnds"   false
                "ex:strStarts" false
                "ex:contains"  false
                "ex:regex"     true
                "ex:subStr"    ["Abcd" "efg"]
                "ex:strLen"    7
                "ex:ucase"     "ABCDEFG"
                "ex:lcase"     "abcdefg"
                "ex:strBefore" "A"
                "ex:strAfter"  "efg"
                "ex:concat"    "Abcdefg STR1 STR2"}
               @(fluree/query
                 @updated
                 {"selectOne"
                  {"ex:string-fns"
                   ["ex:strLen" "ex:subStr" "ex:ucase" "ex:lcase" "ex:strStarts"
                    "ex:strEnds" "ex:contains" "ex:strBefore" "ex:strAfter"
                    "ex:encodeForUri" "ex:concat" "ex:langMatches" "ex:regex"
                    "ex:replace"]}})))))
    (testing "rdf term functions"
      (with-redefs [fluree.db.query.exec.eval/uuid    (fn [] "urn:uuid:34bdb25f-9fae-419b-9c50-203b5f306e47")
                    fluree.db.query.exec.eval/struuid (fn [] "34bdb25f-9fae-419b-9c50-203b5f306e47")]
        (let [updated (-> @(fluree/stage db1 [{"id"         "ex:create-predicates"
                                               "ex:isBlank" 0 "ex:isNumeric" 0 "ex:str" 0 "ex:uuid" 0
                                               "ex:struuid" 0 "ex:isNotNumeric" 0 "ex:isNotBlank" 0}
                                              ;; "ex:isIRI" 0 "ex:isURI" 0 "ex:isLiteral" 0 "ex:lang" 0 "ex:IRI" 0
                                              ;; "ex:datatype" 0 "ex:bnode" 0 "ex:strdt" 0 "ex:strLang" 0

                                              {"id"        "ex:rdf-term-fns"
                                               "ex:text"   "Abcdefg"
                                               "ex:number" 1
                                               "ex:ref"    {"ex:bool" false}}
                                              {"ex:foo" "bar"}])
                          (fluree/stage {"delete" []
                                         "where"  [{"id"        "?s"
                                                    "ex:text"   "?text"
                                                    "ex:number" "?num"
                                                    "ex:ref"    "?r"}
                                                   ["bind"
                                                    "?str" "(str ?num)"
                                                    "?uuid" "(uuid)"
                                                    "?struuid" "(struuid)"
                                                    "?isBlank" "(isBlank ?s)"
                                                    "?isNotBlank" "(isBlank ?num)"
                                                    "?isnum" "(isNumeric ?num)"
                                                    "?isNotNum" "(isNumeric ?text)"]]
                                         "insert" [{"id"              "?s"
                                                    "ex:uuid"         "?uuid"
                                                    "ex:struuid"      "?struuid"
                                                    "ex:str"          ["?str" "?str2"]
                                                    "ex:isNumeric"    "?isnum"
                                                    "ex:isNotNumeric" "?isNotNum"
                                                    "ex:isBlank"      "?isBlank"
                                                    "ex:isNotBlank"   "?isNotBlank"}]
                                         "values" ["?s" ["ex:rdf-term-fns"]]}))]
          (is (= {"ex:str"          "1"
                  "ex:uuid"         "urn:uuid:34bdb25f-9fae-419b-9c50-203b5f306e47"
                  "ex:struuid"      "34bdb25f-9fae-419b-9c50-203b5f306e47",
                  "ex:isBlank"      false
                  "ex:isNotBlank"   false
                  "ex:isNumeric"    true
                  "ex:isNotNumeric" false}
                 @(fluree/query @updated {"selectOne" {"ex:rdf-term-fns" ["ex:isIRI" "ex:isURI" "ex:isLiteral"
                                                                          "ex:lang" "ex:datatype" "ex:IRI" "ex:bnode" "ex:strdt" "ex:strLang"
                                                                          "ex:isBlank"
                                                                          "ex:isNotBlank"
                                                                          "ex:isNumeric"
                                                                          "ex:isNotNumeric"
                                                                          "ex:str"
                                                                          "ex:uuid"
                                                                          "ex:struuid"]}}))))))

    (testing "functional forms"
      (let [updated (-> @(fluree/stage db1 [{"id"               "ex:create-predicates"
                                             "ex:bound"         0
                                             "ex:if"            0
                                             "ex:coalesce"      0
                                             "ex:not-exists"    0
                                             "ex:exists"        0
                                             "ex:logical-or"    0
                                             "ex:logical-and"   0
                                             "ex:rdfterm-equal" 0
                                             "ex:sameTerm"      0
                                             "ex:in"            0
                                             "ex:not-in"        0}
                                            {"id"      "ex:functional-fns"
                                             "ex:text" "Abcdefg"}])
                        (fluree/stage {"delete" []
                                       "where"  [{"id" "?s", "ex:text" "?text"}
                                                 ["bind" "?bound" "(bound ?text)"]]
                                       "insert" {"id" "?s", "ex:bound" "?bound"}
                                       "values" ["?s" ["ex:functional-fns"]]}))]
        (is (= {"ex:bound" true}
               @(fluree/query @updated {"selectOne" {"ex:functional-fns" ["ex:bound"
                                                                          "ex:if"
                                                                          "ex:coalesce"
                                                                          "ex:not-exists"
                                                                          "ex:exists"
                                                                          "ex:logical-or"
                                                                          "ex:logical-and"
                                                                          "ex:rdfterm-equal"
                                                                          "ex:sameTerm"
                                                                          "ex:in"
                                                                          "ex:not-in"]}})))))
    (testing "error handling"
      (let [db2       @(fluree/stage db1 [{"id"       "ex:create-predicates"
                                           "ex:text"  0
                                           "ex:error" 0}
                                          {"id"      "ex:error"
                                           "ex:text" "Abcdefg"}])
            parse-err @(fluree/stage db2 {"delete" []
                                          "where"  [{"id" "?s", "ex:text" "?text"}
                                                    ["bind" "?err" "(foo ?text)"]]
                                          "insert" {"id" "?s", "ex:text" "?err"}
                                          "values" ["?s" ["ex:error"]]})

            run-err   @(fluree/stage db2 {"delete" []
                                          "where"  [{"id" "?s", "ex:text" "?text"}
                                                    ["bind" "?err" "(abs ?text)"]]
                                          "insert" {"id" "?s", "ex:error" "?err"}
                                          "values" ["?s" ["ex:error"]]})]
        (is (= "Query function references illegal symbol: foo"
               (-> parse-err
                   Throwable->map
                   :cause))
            "mdfn parse error")
        (is (= "Query function references illegal symbol: foo"
               (-> @(fluree/query db2 {"where"  [{"id"      "ex:error"
                                                  "ex:text" "?text"}
                                                 ["bind" "?err" "(foo ?text)"]]
                                       "select" "?err"})
                   Throwable->map
                   :cause))
            "query parse error")))))

(deftest ^:integration subject-object-scan-deletions
  (let [conn      @(fluree/connect {:method   :memory
                                    :defaults {:context-type :string
                                               :context      {"id"     "@id",
                                                              "type"   "@type",
                                                              "ex"     "http://example.org/",
                                                              "f"      "https://ns.flur.ee/ledger#",
                                                              "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                                                              "rdfs"   "http://www.w3.org/2000/01/rdf-schema#",
                                                              "schema" "http://schema.org/",
                                                              "xsd"    "http://www.w3.org/2001/XMLSchema#"}}})
        ledger-id "test/love"
        ledger    @(fluree/create conn ledger-id)
        love      @(fluree/stage (fluree/db ledger)
                                 [{"@id"                "ex:fluree",
                                   "@type"              "schema:Organization",
                                   "schema:description" "We ❤️ Data"}
                                  {"@id"                "ex:w3c",
                                   "@type"              "schema:Organization",
                                   "schema:description" "We ❤️ Internet"}
                                  {"@id"                "ex:mosquitos",
                                   "@type"              "ex:Monster",
                                   "schema:description" "We ❤️ Human Blood"}]
                                 {})
        db1       @(fluree/commit! ledger love)]
    (testing "before deletion"
      (let [q       '{:select [?s ?p ?o]
                      :where  {"@id"                ?s
                               "schema:description" ?o
                               ?p                   ?o}}
            subject @(fluree/query db1 q)]
        (is (= [["ex:fluree" "schema:description" "We ❤️ Data"]
                ["ex:mosquitos" "schema:description" "We ❤️ Human Blood"]
                ["ex:w3c" "schema:description" "We ❤️ Internet"]]
               subject)
            "returns all results")))
    (testing "after deletion"
      @(fluree/transact! conn
                         {:context  {:id "@id", :graph "@graph",
                                     :f  "https://ns.flur.ee/ledger#"}
                          :f/ledger ledger-id
                          :graph    {:delete '{"id" ?s, ?p ?o}
                                     :where  '{"id"                 ?s
                                               "schema:description" ?o
                                               ?p                   ?o}}}
                         nil)
      (let [db2     (fluree/db @(fluree/load conn ledger-id))
            q       '{:select [?s ?p ?o]
                      :where  {"id"                 ?s
                               "schema:description" ?o
                               ?p                   ?o}}
            subject @(fluree/query db2 q)]
        (is (= []
               subject)
            "returns no results")))))

(deftest ^:pending ^:integration random-transaction-test
  (testing "this exists b/c it throws an 'Illegal reference object value' error"
    (let [conn        (test-utils/create-conn
                       {:context      test-utils/default-str-context
                        :context-type :string})
          ledger-name "rando-txn"
          ledger      @(fluree/create conn "rando-txn")
          db0         (fluree/db ledger)
          db1         @(fluree/stage2
                        db0
                        {"@context" "https://ns.flur.ee"
                         "ledger"   ledger-name
                         "insert"
                         [{"@id"                "ex:fluree"
                           "@type"              "schema:Organization"
                           "schema:description" "We ❤️ Data"}
                          {"@id"                "ex:w3c"
                           "@type"              "schema:Organization"
                           "schema:description" "We ❤️ Internet"}
                          {"@id"                "ex:mosquitos"
                           "@type"              "ex:Monster"
                           "schema:description" "We ❤️ Human Blood"}]})
          db2         @(fluree/stage2
                        db1
                        {"@context" "https://ns.flur.ee"
                         "ledger"   ledger-name
                         "where"    {"@id"                "ex:mosquitos"
                                     "schema:description" "?o"}
                         "delete"   {"@id"                "ex:mosquitos"
                                     "schema:description" "?o"}
                         "insert"   {"@id"                "ex:mosquitos"
                                     "schema:description" "We ❤️ All Blood"}})]
      (is (= [{"@id" "ex:mosquitos", "schema:description" "We ❤️ All Blood"}]
             @(fluree/query db2 {:select {"ex:mosquitos" ["*"]}}))))))
