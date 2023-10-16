(ns fluree.db.transact.update-test
  (:require [clojure.test :refer :all]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]))

(deftest ^:integration deleting-data
  (testing "Deletions of entire subjects."
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "tx/delete" {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          db     @(fluree/stage
                    (fluree/db ledger)
                    {:graph [{:id           :ex/alice,
                              :type         :ex/User,
                              :schema/name  "Alice"
                              :schema/email "alice@flur.ee"
                              :schema/age   42}
                             {:id          :ex/bob,
                              :type        :ex/User,
                              :schema/name "Bob"
                              :schema/age  22}
                             {:id           :ex/jane,
                              :type         :ex/User,
                              :schema/name  "Jane"
                              :schema/email "jane@flur.ee"
                              :schema/age   30}]})

          ;; delete everything for :ex/alice
          db-subj-delete @(fluree/stage db
                                        '{:delete [:ex/alice ?p ?o]
                                          :where  [[:ex/alice ?p ?o]]})

          ;; delete any :schema/age values for :ex/bob
          db-subj-pred-del @(fluree/stage db
                                          '{:delete [:ex/bob :schema/age ?o]
                                            :where  [[:ex/bob :schema/age ?o]]})

          ;; delete all subjects with a :schema/email predicate
          db-all-preds @(fluree/stage db
                                      '{:delete [?s ?p ?o]
                                        :where  [[?s :schema/email ?x]
                                                 [?s ?p ?o]]})

          ;; delete all subjects where :schema/age = 30
          db-age-delete @(fluree/stage db
                                       '{:delete [?s ?p ?o]
                                         :where  [[?s :schema/age 30]
                                                  [?s ?p ?o]]})

          ;; Change Bob's age - but only if his age is still 22
          db-update-bob @(fluree/stage db
                                       '{:delete [:ex/bob :schema/age 22]
                                         :insert [:ex/bob :schema/age 23]
                                         :where  [[:ex/bob :schema/age 22]]})

          ;; Shouldn't change Bob's age as the current age is not a match
          db-update-bob2 @(fluree/stage db
                                        '{:delete [:ex/bob :schema/age 99]
                                          :insert [:ex/bob :schema/age 23]
                                          :where  [[:ex/bob :schema/age 99]]})

          ;; change Jane's age regardless of its current value
          db-update-jane @(fluree/stage db
                                        '{:delete [:ex/jane :schema/age ?current-age]
                                          :insert [:ex/jane :schema/age 31]
                                          :where  [[:ex/jane :schema/age ?current-age]]})]

      (is (= @(fluree/query db-subj-delete
                            '{:select ?name
                              :where  [[?s :schema/name ?name]]})
             ["Bob" "Jane"])
          "Only Jane and Bob should be left in the db.")

      (is (= @(fluree/query db-subj-pred-del
                            '{:selectOne {?s [:*]}
                              :where     [[?s :id :ex/bob]]})
             {:id          :ex/bob,
              :type    :ex/User,
              :schema/name "Bob"})
          "Bob should no longer have an age property.")

      (is (= @(fluree/query db-all-preds
                            '{:select ?name
                              :where  [[?s :schema/name ?name]]})
             ["Bob"])
          "Only Bob should be left, as he is the only one without an email.")

      (is (= @(fluree/query db-age-delete
                            '{:select ?name
                              :where  [[?s :schema/name ?name]]})
             ["Alice" "Bob"])
          "Only Bob and Alice should be left in the db.")

      (testing "Updating property value only if its current value is a match."
        (is (= [{:id          :ex/bob,
                 :type    :ex/User,
                 :schema/name "Bob"
                 :schema/age  23}]
               @(fluree/query db-update-bob
                              '{:select {?s [:*]}
                                :where  [[?s :id :ex/bob]]}))
            "Bob's age should now be updated to 23 (from 22)."))

      (testing "No update should happen if there is no match."
        (is (= [{:id          :ex/bob,
                 :type    :ex/User,
                 :schema/name "Bob"
                 :schema/age  22}]
               @(fluree/query db-update-bob2
                              '{:select {?s [:*]}
                                :where  [[?s :id :ex/bob]]}))
            "Bob's age should have not been changed and still be 22."))

      (testing "Replacing existing property value with new property value."
        (is (= [{:id           :ex/jane,
                 :type     :ex/User,
                 :schema/name  "Jane"
                 :schema/email "jane@flur.ee"
                 :schema/age   31}]
               @(fluree/query db-update-jane
                              '{:select {?s [:*]}
                                :where  [[?s :id :ex/jane]]}))
            "Jane's age should now be updated to 31 (from 30).")))))

(deftest transaction-functions
  (let [conn @(fluree/connect {:method :memory})
        ledger @(fluree/create conn "functions" {:defaultContext [test-utils/default-str-context
                                                                  {"ex" "http://example.com/"}]})
        db1 (fluree/db ledger)]

    (testing "hash functions"
      (with-redefs [fluree.db.query.exec.eval/now (fn [] "2023-06-13T19:53:57.234345Z")]
        (let [updated (-> @(fluree/stage db1 [{"id" "ex:create-predicates"
                                               "ex:md5" 0 "ex:sha1" 0 "ex:sha256" 0 "ex:sha384" 0 "ex:sha512" 0}
                                              {"id" "ex:hash-fns"
                                               "ex:message" "abc"}])
                          (fluree/stage {"delete" []
                                         "where" [["?s" "id" "ex:hash-fns"]
                                                  ["?s" "ex:message" "?message"]
                                                  {"bind" {"?sha256" "(sha256 ?message)"
                                                           "?sha512" "(sha512 ?message)"}}]
                                         "insert" [["?s" "ex:sha256" "?sha256"]
                                                   ["?s" "ex:sha512" "?sha512"]]}))]
          (is (= {"ex:sha512" "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
                  "ex:sha256" "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}
                 @(fluree/query @updated {"where" [["?s" "id" "ex:hash-fns"]]
                                          "selectOne" {"?s" ["ex:sha512"
                                                             "ex:sha256"]}}))))))
    (testing "datetime functions"
      (with-redefs [fluree.db.query.exec.eval/now (fn [] "2023-06-13T19:53:57.234345Z")]
        (let [updated (-> @(fluree/stage db1 [{"id" "ex:create-predicates"
                                               "ex:now" 0 "ex:year" 0 "ex:month" 0 "ex:day" 0 "ex:hours" 0
                                               "ex:minutes" 0 "ex:seconds" 0 "ex:timezone" 0 "ex:tz" 0}
                                              {"id" "ex:datetime-fns"
                                               "ex:localdatetime" "2023-06-13T14:17:22.435"
                                               "ex:offsetdatetime" "2023-06-13T14:17:22.435-05:00"
                                               "ex:utcdatetime" "2023-06-13T14:17:22.435Z"}])
                          (fluree/stage {"delete" []
                                         "where" [["?s" "id" "ex:datetime-fns"]
                                                  ["?s" "ex:localdatetime" "?localdatetime"]
                                                  ["?s" "ex:offsetdatetime" "?offsetdatetime"]
                                                  ["?s" "ex:utcdatetime" "?utcdatetime"]
                                                  {"bind" {"?now" "(now)"
                                                           "?year" "(year ?localdatetime)"
                                                           "?month" "(month ?localdatetime)"
                                                           "?day" "(day ?localdatetime)"
                                                           "?hours" "(hours ?localdatetime)"
                                                           "?minutes" "(minutes ?localdatetime)"
                                                           "?seconds" "(seconds ?localdatetime)"
                                                           "?tz1" "(tz ?utcdatetime)"
                                                           "?tz2" "(tz ?offsetdatetime)"}}]
                                         "insert" [["?s" "ex:now" "?now"]
                                                   ["?s" "ex:year" "?year"]
                                                   ["?s" "ex:month" "?month"]
                                                   ["?s" "ex:day" "?day"]
                                                   ["?s" "ex:hours" "?hours"]
                                                   ["?s" "ex:minutes" "?minutes"]
                                                   ["?s" "ex:seconds" "?seconds"]
                                                   ["?s" "ex:tz" "?tz1"]
                                                   ["?s" "ex:tz" "?tz2"]]}))]
          (is (= {"ex:now" "2023-06-13T19:53:57.234345Z"
                  "ex:year" 2023
                  "ex:month" 6
                  "ex:day" 13
                  "ex:hours" 14
                  "ex:minutes" 17
                  "ex:seconds" 22
                  "ex:tz" ["-05:00" "Z"]}
                 @(fluree/query @updated {"where" [["?s" "id" "ex:datetime-fns"]]
                                          "selectOne" {"?s" ["ex:now" "ex:year" "ex:month" "ex:day" "ex:hours" "ex:minutes" "ex:seconds"
                                                             "ex:tz"]}}))))))

    (testing "numeric functions"
      (let [updated (-> @(fluree/stage db1 [{"id" "ex:create-predicates"
                                             "ex:abs" 0 "ex:round" 0 "ex:ceil" 0 "ex:floor" 0 "ex:rand" 0}
                                            {"id" "ex:numeric-fns"
                                             "ex:pos-int" 2
                                             "ex:neg-int" -2
                                             "ex:decimal" 1.4}])
                        (fluree/stage {"delete" []
                                       "where" [["?s" "id" "ex:numeric-fns"]
                                                ["?s" "ex:pos-int" "?pos-int"]
                                                ["?s" "ex:neg-int" "?neg-int"]
                                                ["?s" "ex:decimal" "?decimal"]
                                                {"bind" {"?abs" "(abs ?neg-int)"
                                                         "?round" "(round ?decimal)"
                                                         "?ceil" "(ceil ?decimal)"
                                                         "?floor" "(floor ?decimal)"
                                                         "?rand" "(rand)"}}]
                                       "insert" [["?s" "ex:abs" "?abs"]
                                                 ["?s" "ex:round" "?round"]
                                                 ["?s" "ex:ceil" "?ceil"]
                                                 ["?s" "ex:floor" "?floor"]
                                                 ["?s" "ex:rand" "?rand"]]}))]
        (is (= {"ex:abs" 2
                "ex:round" 1
                "ex:ceil" 2
                "ex:floor" 1}
               @(fluree/query @updated {"where" [["?s" "id" "ex:numeric-fns"]]
                                        "selectOne" {"?s" ["ex:abs"
                                                           "ex:round"
                                                           "ex:ceil"
                                                           "ex:floor"]}})))
        (is (pos? @(fluree/query @updated {"where" [["?s" "id" "ex:numeric-fns"]
                                                    ["?s" "ex:rand" "?rand"]]
                                           "selectOne" "?rand"})))))

    (testing "string functions"
      (let [updated  (-> @(fluree/stage db1 [{"id" "ex:create-predicates"
                                              "ex:strLen" 0 "ex:subStr" 0 "ex:ucase" 0 "ex:lcase" 0 "ex:strStarts" 0 "ex:strEnds" 0
                                              "ex:contains" 0 "ex:strBefore" 0 "ex:strAfter" 0 "ex:encodeForUri" 0 "ex:concat" 0
                                              "ex:langMatches" 0 "ex:regex" 0 "ex:replace" 0}
                                             {"id" "ex:string-fns"
                                              "ex:text" "Abcdefg"}])
                         (fluree/stage {"delete" []
                                        "where" [["?s" "id" "ex:string-fns"]
                                                 ["?s" "ex:text" "?text"]
                                                 {"bind" {"?strlen" "(strLen ?text)"
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
                                                          "?matched" "(regex ?text \"^Abc\")"}}]
                                        "insert" [["?s" "ex:strStarts" "?a-start"]
                                                  ["?s" "ex:strEnds" "?a-end"]
                                                  ["?s" "ex:subStr" "?sub1"]
                                                  ["?s" "ex:subStr" "?sub2"]
                                                  ["?s" "ex:strLen" "?strlen"]
                                                  ["?s" "ex:ucase" "?upcased"]
                                                  ["?s" "ex:lcase" "?downcased"]
                                                  ["?s" "ex:contains" "?contains"]
                                                  ["?s" "ex:strBefore" "?strBefore"]
                                                  ["?s" "ex:strAfter" "?strAfter"]
                                                  ["?s" "ex:concat" "?concatted"]
                                                  ["?s" "ex:regex" "?matched"]]}))]
        (is (= {"ex:strEnds" false
                "ex:strStarts" false
                "ex:contains" false
                "ex:regex" true
                "ex:subStr" ["Abcd" "efg"]
                "ex:strLen" 7
                "ex:ucase" "ABCDEFG"
                "ex:lcase" "abcdefg"
                "ex:strBefore" "A"
                "ex:strAfter" "efg"
                "ex:concat" "Abcdefg STR1 STR2"}
               @(fluree/query @updated {"where" [["?s" "id" "ex:string-fns"]]
                                        "selectOne" {"?s" ["ex:strLen" "ex:subStr" "ex:ucase" "ex:lcase" "ex:strStarts" "ex:strEnds"
                                                           "ex:contains" "ex:strBefore" "ex:strAfter" "ex:encodeForUri" "ex:concat"
                                                           "ex:langMatches" "ex:regex" "ex:replace"]}})))))
    (testing "rdf term functions"
      (with-redefs [fluree.db.query.exec.eval/uuid (fn [] "urn:uuid:34bdb25f-9fae-419b-9c50-203b5f306e47")
                    fluree.db.query.exec.eval/struuid (fn [] "34bdb25f-9fae-419b-9c50-203b5f306e47")]
        (let [updated  (-> @(fluree/stage db1 [{"id" "ex:create-predicates"
                                                "ex:isBlank" 0 "ex:isNumeric" 0 "ex:str" 0 "ex:uuid" 0
                                                "ex:struuid" 0 "ex:isNotNumeric" 0 "ex:isNotBlank" 0}
                                                ;; "ex:isIRI" 0 "ex:isURI" 0 "ex:isLiteral" 0 "ex:lang" 0 "ex:IRI" 0
                                                ;; "ex:datatype" 0 "ex:bnode" 0 "ex:strdt" 0 "ex:strLang" 0

                                               {"id" "ex:rdf-term-fns"
                                                "ex:text" "Abcdefg"
                                                "ex:number" 1
                                                "ex:ref" {"ex:bool" false}}
                                               {"ex:foo" "bar"}])
                           (fluree/stage {"delete" []
                                          "where" [["?s" "id" "ex:rdf-term-fns"]
                                                   ["?s" "ex:text" "?text"]
                                                   ["?s" "ex:number" "?num"]
                                                   ["?s" "ex:ref" "?r"]
                                                   {"bind" {"?str" "(str ?num)"
                                                            "?uuid" "(uuid)"
                                                            "?struuid" "(struuid)"
                                                            "?isBlank" "(isBlank ?s)"
                                                            "?isNotBlank" "(isBlank ?num)"
                                                            "?isnum" "(isNumeric ?num)"
                                                            "?isNotNum" "(isNumeric ?text)"}}]
                                          "insert" [["?s" "ex:uuid" "?uuid"]
                                                    ["?s" "ex:struuid" "?struuid"]
                                                    ["?s" "ex:str" "?str"]
                                                    ["?s" "ex:str" "?str2"]
                                                    ["?s" "ex:isNumeric" "?isnum"]
                                                    ["?s" "ex:isNotNumeric" "?isNotNum"]
                                                    ["?s" "ex:isBlank" "?isBlank"]
                                                    ["?s" "ex:isNotBlank" "?isNotBlank"]]}))]
          (is (= {"ex:str" "1"
                  "ex:uuid" "urn:uuid:34bdb25f-9fae-419b-9c50-203b5f306e47"
                  "ex:struuid" "34bdb25f-9fae-419b-9c50-203b5f306e47",
                  "ex:isBlank" false
                  "ex:isNotBlank" false
                  "ex:isNumeric" true
                  "ex:isNotNumeric" false}
                 @(fluree/query @updated {"where" [["?s" "id" "ex:rdf-term-fns"]]
                                          "selectOne" {"?s" ["ex:isIRI" "ex:isURI" "ex:isLiteral"
                                                             "ex:lang" "ex:datatype" "ex:IRI" "ex:bnode" "ex:strdt" "ex:strLang"
                                                             "ex:isBlank"
                                                             "ex:isNotBlank"
                                                             "ex:isNumeric"
                                                             "ex:isNotNumeric"
                                                             "ex:str"
                                                             "ex:uuid"
                                                             "ex:struuid"]}}))))))

    (testing "functional forms"
      (let [updated (-> @(fluree/stage db1 [{"id" "ex:create-predicates"
                                             "ex:bound" 0
                                             "ex:if" 0
                                             "ex:coalesce" 0
                                             "ex:not-exists" 0
                                             "ex:exists" 0
                                             "ex:logical-or" 0
                                             "ex:logical-and" 0
                                             "ex:rdfterm-equal" 0
                                             "ex:sameTerm" 0
                                             "ex:in" 0
                                             "ex:not-in" 0}
                                            {"id" "ex:functional-fns"
                                             "ex:text" "Abcdefg"}])
                        (fluree/stage {"delete" []
                                       "where" [["?s" "id" "ex:functional-fns"]
                                                ["?s" "ex:text" "?text"]
                                                {"bind" {"?bound" "(bound ?text)"}}]
                                       "insert" [["?s" "ex:bound" "?bound"]]}))]
        (is (= {"ex:bound" true}
               @(fluree/query @updated {"where" [["?s" "id" "ex:functional-fns"]]
                                        "selectOne" {"?s" ["ex:bound"
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
      (let [db2 @(fluree/stage db1 [{"id" "ex:create-predicates"
                                     "ex:text" 0
                                     "ex:error" 0}
                                    {"id" "ex:error"
                                     "ex:text" "Abcdefg"}])
            parse-err @(fluree/stage db2 {"delete" []
                                          "where" [["?s" "id" "ex:error"]
                                                   ["?s" "ex:text" "?text"]
                                                   {"bind" {"?err" "(foo ?text)"}}]
                                          "insert" [["?s" "ex:text" "?err"]]})

            run-err   @(fluree/stage db2 {"delete" []
                                          "where" [["?s" "id" "ex:error"]
                                                   ["?s" "ex:text" "?text"]
                                                   {"bind" {"?err" "(abs ?text)"}}]
                                          "insert" [["?s" "ex:error" "?err"]]})]
        (is (= "Query function references illegal symbol: foo"
               (-> parse-err
                   Throwable->map
                   :cause))
            "mdfn parse error")
        (is (= "Query function references illegal symbol: foo"
               (-> @(fluree/query db2 {"where" [["?s" "id" "ex:error"]
                                                ["?s" "ex:text" "?text"]
                                                {"bind" {"?err" "(foo ?text)"}}]
                                       "select" "?err"})
                   Throwable->map
                   :cause))
            "query parse error")))))

(deftest ^:integration subject-object-scan-deletions
  (let [conn @(fluree/connect {:method :memory
                               :defaults {:context-type :string
                                          :context      {"id"     "@id",
                                                         "type"   "@type",
                                                         "ex"     "http://example.org/",
                                                         "f"      "https://ns.flur.ee/ledger#",
                                                         "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                                                         "rdfs"   "http://www.w3.org/2000/01/rdf-schema#",
                                                         "schema" "http://schema.org/",
                                                         "xsd"    "http://www.w3.org/2001/XMLSchema#"}}})
        ledger-id  "test/love"
        ledger @(fluree/create conn ledger-id)
        love @(fluree/stage (fluree/db ledger)
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
        db1 @(fluree/commit! ledger love)]
    (testing "before deletion"
      (let [q       '{:select [?s ?p ?o]
                      :where  [[?s "schema:description" ?o]
                               [?s ?p ?o]]}
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
                          :graph    {:delete '[?s ?p ?o]
                                     :where  '[[?s "schema:description" ?o]
                                               [?s ?p ?o]]}} nil)
      (let [db2   (fluree/db @(fluree/load conn ledger-id))
            q       '{:select [?s ?p ?o]
                      :where  [[?s "schema:description" ?o]
                               [?s ?p ?o]]}
            subject @(fluree/query db2 q)]
        (is (= []
               subject)
            "returns no results")))))

(comment

  (def conn @(fluree/connect {:method :memory}))

  (def ledger @(fluree/create conn "update" {:defaultContext [test-utils/default-str-context {"ex" "ns:ex/"}]}))

  (def db0 (fluree/db ledger))
  (-> db0 :novelty :spot)
  #{#Flake [203 0 "http://www.w3.org/2000/01/rdf-schema#Class" 1 -1 true nil]
    #Flake [200 0 "@type" 1 -1 true nil]
    #Flake [0 0 "@id" 1 -1 true nil]}

  (def db1 @(fluree/stage2 db0 {"@context" "https://flur.ee"
                                "insert" [{"@id" "ex:dp"
                                           "ex:name" "Dan"
                                           "ex:child" [{"@id" "ex:ap" "ex:name" "AP"}
                                                       {"@id" "ex:np" "ex:name" "NP"}]
                                           "ex:spouse" [{"@id" "ex:kp" "ex:name" "KP"
                                                         "ex:spouse" {"@id" "ex:dp"}}]}]}))

  db1
  #{#Flake [211106232532994 0 "ns:ex/kp" 1 -1 true nil]
    #Flake [211106232532994 1000 "KP" 1 -1 true nil]
    #Flake [211106232532994 1002 211106232532991 1 -1 true nil]
    #Flake [211106232532993 0 "ns:ex/np" 1 -1 true nil]
    #Flake [211106232532993 1000 "NP" 1 -1 true nil]
    #Flake [211106232532992 0 "ns:ex/ap" 1 -1 true nil]
    #Flake [211106232532992 1000 "AP" 1 -1 true nil]
    #Flake [211106232532991 0 "ns:ex/dp" 1 -1 true nil]
    #Flake [211106232532991 1000 "Dan" 1 -1 true nil]
    #Flake [211106232532991 1001 211106232532992 1 -1 true nil]
    #Flake [211106232532991 1001 211106232532993 1 -1 true nil]
    #Flake [211106232532991 1002 211106232532994 1 -1 true nil]
    #Flake [1002 0 "ns:ex/spouse" 1 -1 true nil]
    #Flake [1001 0 "ns:ex/child" 1 -1 true nil]
    #Flake [1000 0 "ns:ex/name" 1 -1 true nil]}



  (def db1* @(fluree/stage db0 [{"@id" "ex:dp"
                                 "ex:name" "Dan"
                                 "ex:child" [{"@id" "ex:ap" "ex:name" "AP"}
                                             {"@id" "ex:np" "ex:name" "NP"}]
                                 "ex:spouse" [{"@id" "ex:kp" "ex:name" "KP"
                                               "ex:spouse" {"@id" "ex:dp"}}]}]))

  (-> db1* :novelty :spot)
  #{#Flake [211106232532995 0 "ns:ex/kp" 1 -1 true nil]
    #Flake [211106232532995 1001 "KP" 1 -1 true nil]
    #Flake [211106232532995 1003 211106232532992 0 -1 true nil]
    #Flake [211106232532994 0 "ns:ex/np" 1 -1 true nil]
    #Flake [211106232532994 1001 "NP" 1 -1 true nil]
    #Flake [211106232532993 0 "ns:ex/ap" 1 -1 true nil]
    #Flake [211106232532993 1001 "AP" 1 -1 true nil]
    #Flake [211106232532992 0 "ns:ex/dp" 1 -1 true nil]
    #Flake [211106232532992 1001 "Dan" 1 -1 true nil]
    #Flake [211106232532992 1002 211106232532993 0 -1 true nil]
    #Flake [211106232532992 1002 211106232532994 0 -1 true nil]
    #Flake [211106232532992 1003 211106232532995 0 -1 true nil]
    #Flake [1003 0 "ns:ex/spouse" 1 -1 true nil]
    #Flake [1002 0 "ns:ex/child" 1 -1 true nil]
    #Flake [1001 0 "ns:ex/name" 1 -1 true nil]
    #Flake [203 0 "http://www.w3.org/2000/01/rdf-schema#Class" 1 0 true nil]
    #Flake [203 0 "http://www.w3.org/2000/01/rdf-schema#Class" 1 -1 true nil]
    #Flake [200 0 "@type" 1 0 true nil]
    #Flake [200 0 "@type" 1 -1 true nil]
    #Flake [0 0 "@id" 1 0 true nil]
    #Flake [0 0 "@id" 1 -1 true nil]}


  (def db2 @(fluree/stage2 db1* {"@context" "https://flur.ee"
                                 "where" [["?s" "ex:name" "?name"]]
                                 "delete" {"@graph"
                                           [{"@id" "?s" "ex:name" "?name"}]}
                                 "insert" {"@context" {"ex:zip" {"@type" "ex:PostalCode"}}
                                           "@graph"
                                           [{"@id" "?s", "ex:name" "WAT"}
                                            {"@id" "ex:mp",
                                             "@type" "ex:Cat"
                                             "ex:isPerson" false
                                             "ex:isOrange" true
                                             "ex:nickname" {"@language" "en" "@value" "The Wretch"}
                                             "ex:name" "Murray",
                                             "ex:address"
                                             {"ex:street" "55 Bashford", "ex:city" "St. Paul", "ex:zip" 55105, "ex:state" "MN"},
                                             "ex:favs" {"@list" ["Persey" {"@id" "ex:dp"}]}}]}}))

  db2
  (count #{#Flake [211106232532997 0 "ns:ex/Cat" 1 -1 true nil]
           #Flake [211106232532996 0 "ns:ex/mp" 1 -1 true nil]
           #Flake [211106232532996 200 211106232532997 0 -1 true nil]
           #Flake [211106232532996 1001 "Murray" 1 -1 true nil]
           #Flake [211106232532996 1008 211106232532995 0 -1 true nil]
           #Flake [211106232532996 1009 211106232532992 0 -1 true {:i 1}]
           #Flake [211106232532996 1009 "Persey" 1 -1 true {:i 0}]
           #Flake [211106232532996 1010 true 2 -1 true nil]
           #Flake [211106232532996 1011 nil nil -1 true nil]
           #Flake [211106232532996 1012 "The Wretch" 205 -1 true {:lang "en"}]
           #Flake [211106232532995 0 "_:211106232532995" 1 -1 true nil]
           #Flake [211106232532995 1001 "KP" 1 -1 false nil]
           #Flake [211106232532995 1001 "WAT" 1 -1 true nil]
           #Flake [211106232532995 1003 "St. Paul" 1 -1 true nil]
           #Flake [211106232532995 1004 "55 Bashford" 1 -1 true nil]
           #Flake [211106232532995 1005 55105 1006 -1 true nil]
           #Flake [211106232532995 1007 "MN" 1 -1 true nil]
           #Flake [211106232532994 1001 "NP" 1 -1 false nil]
           #Flake [211106232532994 1001 "WAT" 1 -1 true nil]
           #Flake [211106232532993 1001 "AP" 1 -1 false nil]
           #Flake [211106232532993 1001 "WAT" 1 -1 true nil]
           #Flake [211106232532992 1001 "Dan" 1 -1 false nil]
           #Flake [211106232532992 1001 "WAT" 1 -1 true nil]
           #Flake [1012 0 "ns:ex/nickname" 1 -1 true nil]
           #Flake [1011 0 "ns:ex/isPerson" 1 -1 true nil]
           #Flake [1010 0 "ns:ex/isOrange" 1 -1 true nil]
           #Flake [1009 0 "ns:ex/favs" 1 -1 true nil]
           #Flake [1008 0 "ns:ex/address" 1 -1 true nil]
           #Flake [1007 0 "ns:ex/state" 1 -1 true nil]
           #Flake [1006 0 "ns:ex/PostalCode" 1 -1 true nil]
           #Flake [1005 0 "ns:ex/zip" 1 -1 true nil]
           #Flake [1004 0 "ns:ex/street" 1 -1 true nil]
           #Flake [1003 0 "ns:ex/city" 1 -1 true nil]})

  #{#Flake [211106232532995 1001 "KP" 1 -1 false nil]
    #Flake [211106232532995 1001 "WAT" 1 -1 true nil]
    #Flake [211106232532994 1001 "NP" 1 -1 false nil]
    #Flake [211106232532994 1001 "WAT" 1 -1 true nil]
    #Flake [211106232532993 1001 "AP" 1 -1 false nil]
    #Flake [211106232532993 1001 "WAT" 1 -1 true nil]
    #Flake [211106232532992 1001 "Dan" 1 -1 false nil]
    #Flake [211106232532992 1001 "WAT" 1 -1 true nil]}


  #{#Flake [211106232532995 1001 "KP" 1 -1 false nil]
    #Flake [211106232532994 1001 "NP" 1 -1 false nil]
    #Flake [211106232532993 1001 "AP" 1 -1 false nil]
    #Flake [211106232532992 1001 "Dan" 1 -1 false nil]}
  (count #{#Flake [211106232532997 0 "ns:ex/Cat" 1 -1 true nil]
           #Flake [211106232532996 0 "ns:ex/mp" 1 -1 true nil]
           #Flake [211106232532996 200 211106232532997 0 -1 true nil]
           #Flake [211106232532996 1004 211106232532995 0 -1 true nil]
           #Flake [211106232532996 1006 211106232532992 0 -1 true {:i 1}]
           #Flake [211106232532995 0 "_:211106232532995" 1 -1 true nil]
           #Flake [211106232532995 1001 "KP" 1 -1 false nil]
           #Flake [211106232532995 1003 55105 1005 -1 true nil]
           #Flake [211106232532994 1001 "NP" 1 -1 false nil]
           #Flake [211106232532993 1001 "AP" 1 -1 false nil]
           #Flake [211106232532992 1001 "Dan" 1 -1 false nil]
           #Flake [1006 0 "ns:ex/favs" 1 -1 true nil]
           #Flake [1005 0 "ns:ex/PostalCode" 1 -1 true nil]
           #Flake [1004 0 "ns:ex/address" 1 -1 true nil]
           #Flake [1003 0 "ns:ex/zip" 1 -1 true nil]})


  ;; I expect these flakes
  (count [["ex:dp" "name" "WAT"]
          ["ex:kp" "name" "WAT"]
          ["ex:ap" "name" "WAT"]
          ["ex:np" "name" "WAT"]
          ["ex:dp" "name" "Dan"]
          ["ex:kp" "name" "kp"]
          ["ex:ap" "name" "ap"]
          ["ex:np" "name" "np"]
          ["ex:mp" 200 "ex:Cat"]
          ["ex:mp" "isPerson" false]
          ["ex:mp" "isOrange" true]
          ["ex:mp" "nickname" "The Wretch"]
          ["ex:mp" "name" "Murray"]
          ["ex:mp" "address" "address-sid"]
          ["address-sid" "street" "55 B"]
          ["address-sid" "city" "St. P"]
          ["address-sid" "state" "MN"]
          ["address-sid" "zip" "55105"]
          ["ex:mp" "favs" "Persey"]
          ["ex:mp" "favs" "ex:dan"]
          ["ex:mp" 0 "ex:mp"]
          ["ex:Cat" 0 "ex:Cat"]
          ["isPerson" 0 "isPerson"]
          ["isOrange" 0 "isOrange"]
          ["nickname" 0 "nickname"]
          ["favs" 0 "favs"]
          ["address" 0 "address"]
          ["street" 0 "street"]
          ["city" 0 "city"]
          ["state" 0 "state"]
          ["zip" 0 "zip"]
          ["PostalCode" 0 "PostalCode"]
          ["address-sid" 0 "address-bnode"]])
  33
  (+
    ;; new id flakes
    13
    ;; var solutions
    8
    ;; plain data
    12)
  33





  db2

  #{#Flake [211106232532997 0 "_:211106232532997" 1 -1 true nil]
    #Flake [211106232532997 1004 55105 1005 -1 true nil]
    #Flake [211106232532996 0 "ns:ex/Cat" 1 -1 true nil]
    #Flake [211106232532995 0 "ns:ex/mp" 1 -1 true nil]
    #Flake [211106232532995 200 211106232532996 0 -1 true nil]
    #Flake [211106232532995 1001 "KP" 1 -1 false nil]
    #Flake [211106232532995 1003 211106232532997 0 -1 true nil]
    #Flake [211106232532995 1006 211106232532992 0 -1 true {:i 1}]
    #Flake [211106232532994 1001 "NP" 1 -1 false nil]
    #Flake [211106232532993 1001 "AP" 1 -1 false nil]
    #Flake [211106232532992 1001 "Dan" 1 -1 false nil]
    #Flake [1006 0 "ns:ex/favs" 1 -1 true nil]
    #Flake [1005 0 "ns:ex/PostalCode" 1 -1 true nil]
    #Flake [1004 0 "ns:ex/zip" 1 -1 true nil]
    #Flake [1003 0 "ns:ex/address" 1 -1 true nil]}


  #{#Flake [211106232532997 0 "_:211106232532997" 1 -1 true nil]
    #Flake [211106232532997 0 "_:fdb1" 1 -1 true nil]
    #Flake [211106232532997 1003 55105 1005 -1 true nil]
    #Flake [211106232532996 0 "ns:ex/Cat" 1 -1 true nil]
    #Flake [211106232532995 0 "ns:ex/mp" 1 -1 true nil]
    #Flake [211106232532995 200 211106232532996 0 -1 true nil]
    #Flake [211106232532995 1001 "KP" 1 -1 false nil]
    #Flake [211106232532995 1004 211106232532997 0 -1 true nil]
    #Flake [211106232532995 1006 211106232532992 0 -1 true {:i 1}]
    #Flake [211106232532994 1001 "NP" 1 -1 false nil]
    #Flake [211106232532993 1001 "AP" 1 -1 false nil]
    #Flake [211106232532992 1001 "Dan" 1 -1 false nil]
    #Flake [1006 0 "ns:ex/favs" 1 -1 true nil]
    #Flake [1005 0 "ns:ex/PostalCode" 1 -1 true nil]
    #Flake [1004 0 "ns:ex/address" 1 -1 true nil]
    #Flake [1003 0 "ns:ex/zip" 1 -1 true nil]}

  #{#Flake [211106232532998 0 "_:fdb1" 1 -1 true nil]
    #Flake [211106232532997 0 "_:211106232532997" 1 -1 true nil]
    #Flake [211106232532997 1003 55105 1005 -1 true nil]
    #Flake [211106232532996 0 "ns:ex/Cat" 1 -1 true nil]
    #Flake [211106232532995 0 "ns:ex/mp" 1 -1 true nil]
    #Flake [211106232532995 200 211106232532996 0 -1 true nil]
    #Flake [211106232532995 1001 "KP" 1 -1 false nil]
    #Flake [211106232532995 1004 211106232532998 0 -1 true nil]
    #Flake [211106232532995 1006 211106232532992 0 -1 true {:i 1}]
    #Flake [211106232532994 1001 "NP" 1 -1 false nil]
    #Flake [211106232532993 1001 "AP" 1 -1 false nil]
    #Flake [211106232532992 1001 "Dan" 1 -1 false nil]
    #Flake [1006 0 "ns:ex/favs" 1 -1 true nil]
    #Flake [1005 0 "ns:ex/PostalCode" 1 -1 true nil]
    #Flake [1004 0 "ns:ex/address" 1 -1 true nil]
    #Flake [1003 0 "ns:ex/zip" 1 -1 true nil]}

  #{#Flake [211106232532998 0 "ns:ex/Cat" 1 -1 true nil]
    #Flake [211106232532997 0 "_:fdb1" 1 -1 true nil]
    #Flake [211106232532996 0 "ns:ex/mp" 1 -1 true nil]
    #Flake [211106232532996 200 211106232532998 0 -1 true nil]
    #Flake [211106232532996 1004 211106232532997 0 -1 true nil]
    #Flake [211106232532996 1006 nil 0 -1 true {:i 1}]
    #Flake [211106232532995 0 "_:211106232532995" 1 -1 true nil]
    #Flake [211106232532995 1001 "KP" 1 -1 false nil]
    #Flake [211106232532995 1003 55105 1005 -1 true nil]
    #Flake [211106232532994 1001 "NP" 1 -1 false nil]
    #Flake [211106232532993 1001 "AP" 1 -1 false nil]
    #Flake [211106232532992 1001 "Dan" 1 -1 false nil]
    #Flake [1006 0 "ns:ex/favs" 1 -1 true nil]
    #Flake [1005 0 "ns:ex/PostalCode" 1 -1 true nil]
    #Flake [1004 0 "ns:ex/address" 1 -1 true nil]
    #Flake [1003 0 "ns:ex/zip" 1 -1 true nil]
    }
  #{#Flake [211106232532999 0 "ns:ex/Cat" 1 -1 true nil]
    #Flake [211106232532998 0 "_:fdb1" 1 -1 true nil]
    #Flake [211106232532997 0 "ns:ex/PostalCode" 1 -1 true nil]
    #Flake [211106232532996 0 "ns:ex/mp" 1 -1 true nil]
    #Flake [211106232532996 200 211106232532999 0 -1 true nil]
    #Flake [211106232532996 1004 211106232532998 0 -1 true nil]
    #Flake [211106232532996 1005 nil 0 -1 true {:i 1}]
    #Flake [211106232532995 0 "_:211106232532995" 1 -1 true nil]
    #Flake [211106232532995 1001 "KP" 1 -1 false nil]
    #Flake [211106232532995 1003 55105 211106232532997 -1 true nil]
    #Flake [211106232532994 1001 "NP" 1 -1 false nil]
    #Flake [211106232532993 1001 "AP" 1 -1 false nil]
    #Flake [211106232532992 1001 "Dan" 1 -1 false nil]
    #Flake [1005 0 "ns:ex/favs" 1 -1 true nil]
    #Flake [1004 0 "ns:ex/address" 1 -1 true nil]
    #Flake [1003 0 "ns:ex/zip" 1 -1 true nil]}

  #{#Flake [211106232532999 0 "_:fdb1" 1 -1 true nil]
    #Flake [211106232532998 0 "ns:ex/PostalCode" 1 -1 true nil]
    #Flake [211106232532997 0 "_:211106232532997" 1 -1 true nil]
    #Flake [211106232532997 1004 "55 Bashford" 1 -1 true nil]
    #Flake [211106232532997 1005 "St. Paul" 1 -1 true nil]
    #Flake [211106232532997 1006 55105 211106232532998 -1 true nil]
    #Flake [211106232532997 1007 "MN" 1 -1 true nil]
    #Flake [211106232532996 0 "ns:ex/Cat" 1 -1 true nil]
    #Flake [211106232532995 0 "ns:ex/mp" 1 -1 true nil]
    #Flake [211106232532995 200 211106232532996 0 -1 true nil]
    #Flake [211106232532995 1001 "KP" 1 -1 false nil]
    #Flake [211106232532995 1001 "Murray" 1 -1 true nil]
    #Flake [211106232532995 1001 "WAT" 1 -1 true nil]
    #Flake [211106232532995 1003 "The Wretch" 205 -1 true {:lang "en"}]
    #Flake [211106232532995 1008 211106232532999 0 -1 true nil]
    #Flake [211106232532995 1009 nil 0 -1 true {:i 1}]
    #Flake [211106232532995 1009 "Persey" 1 -1 true {:i 0}]
    #Flake [211106232532994 1001 "NP" 1 -1 false nil]
    #Flake [211106232532994 1001 "WAT" 1 -1 true nil]
    #Flake [211106232532993 1001 "AP" 1 -1 false nil]
    #Flake [211106232532993 1001 "WAT" 1 -1 true nil]
    #Flake [211106232532992 1001 "Dan" 1 -1 false nil]
    #Flake [211106232532992 1001 "WAT" 1 -1 true nil]
    #Flake [1009 0 "ns:ex/favs" 1 -1 true nil]
    #Flake [1008 0 "ns:ex/address" 1 -1 true nil]
    #Flake [1007 0 "ns:ex/state" 1 -1 true nil]
    #Flake [1006 0 "ns:ex/zip" 1 -1 true nil]
    #Flake [1005 0 "ns:ex/city" 1 -1 true nil]
    #Flake [1004 0 "ns:ex/street" 1 -1 true nil]
    #Flake [1003 0 "ns:ex/nickname" 1 -1 true nil]}







  (require '[fluree.json-ld :as json-ld])
  (json-ld/expand {"@context" {"ex:zip" {"@type" "ex:PostalCode"}}
                   "@graph"
                   [{"@id" "?s", "ex:name" "WAT"}
                    {"@id" "ex:mp",
                     "@type" "ex:Cat"
                     "ex:nickname" {"@language" "en" "@value" "The Wretch"}
                     "ex:name" "Murray",
                     "ex:address"
                     {"ex:street" "55 Bashford", "ex:city" "St. Paul", "ex:zip" 55105, "ex:state" "MN"},
                     "ex:favs" {"@list" ["Persey" {"@id" "ex:dp"}]}}]}
                  (fluree.db.dbproto/-context db1))
  [{:idx ["@graph" 0],
    :id "?s",
    "ns:ex/name" [{:value "WAT", :type nil, :idx ["@graph" 0 "ex:name"]}]}
   {:idx ["@graph" 1],
    :type ["ns:ex/Cat"],
    :id "ns:ex/mp",
    "ns:ex/nickname" [{:value "The Wretch", :language "en", :idx ["@graph" 1 "ex:nickname"]}],
    "ns:ex/name" [{:value "Murray", :type nil, :idx ["@graph" 1 "ex:name"]}],
    "ns:ex/address" [{:idx ["@graph" 1 "ex:address"],
                      "ns:ex/street" [{:value "55 Bashford", :type nil, :idx ["@graph" 1 "ex:address" "ex:street"]}],
                      "ns:ex/city" [{:value "St. Paul", :type nil, :idx ["@graph" 1 "ex:address" "ex:city"]}],
                      "ns:ex/zip" [{:value 55105, :type "ns:ex/PostalCode", :idx ["@graph" 1 "ex:address" "ex:zip"]}],
                      "ns:ex/state" [{:value "MN", :type nil, :idx ["@graph" 1 "ex:address" "ex:state"]}]}],
    "ns:ex/favs"
    [{:list
      [{:value "Persey", :type nil, :idx ["@graph" 1 "ex:favs" "@list" 0]}
       {:idx ["@graph" 1 "ex:favs" "@list" 1], :id "ns:ex/dp"}]}]}]

  (fluree.db.query.fql.parse/parse-triples
    [{:idx ["@graph" 0],
      :id "?s",
      "ns:ex/name" [{:value "WAT", :type nil, :idx ["@graph" 0 "ex:name"]}]}
     {:idx ["@graph" 1],
      :type ["ns:ex/Cat"],
      :id "ns:ex/mp",
      "ns:ex/nickname" [{:value "The Wretch", :language "en", :idx ["@graph" 1 "ex:nickname"]}],
      "ns:ex/name" [{:value "Murray", :type nil, :idx ["@graph" 1 "ex:name"]}],
      "ns:ex/address" [{:idx ["@graph" 1 "ex:address"],
                        "ns:ex/street" [{:value "55 Bashford", :type nil, :idx ["@graph" 1 "ex:address" "ex:street"]}],
                        "ns:ex/city" [{:value "St. Paul", :type nil, :idx ["@graph" 1 "ex:address" "ex:city"]}],
                        "ns:ex/zip" [{:value 55105, :type "ns:ex/PostalCode", :idx ["@graph" 1 "ex:address" "ex:zip"]}],
                        "ns:ex/state" [{:value "MN", :type nil, :idx ["@graph" 1 "ex:address" "ex:state"]}]}],
      "ns:ex/favs"
      [{:list
        [{:value "Persey", :type nil, :idx ["@graph" 1 "ex:favs" "@list" 0]}
         {:idx ["@graph" 1 "ex:favs" "@list" 1], :id "ns:ex/dp"}]}]}])
  [[{:var ?s} {:val "ns:ex/name"} {:val "WAT", :datatype 1, :m nil}]
   [{:val "ns:ex/mp"} {:val "@type"} {:val "ns:ex/Cat", :datatype 0}]
   [{:val "ns:ex/mp"} {:val "ns:ex/nickname"} {:val "The Wretch", :datatype 205, :m {:lang "en"}}]
   [{:val "ns:ex/mp"} {:val "ns:ex/name"} {:val "Murray", :datatype 1, :m nil}]
   [{:val "_:fdb2"} {:val "ns:ex/street"} {:val "55 Bashford", :datatype 1, :m nil}]
   [{:val "_:fdb2"} {:val "ns:ex/city"} {:val "St. Paul", :datatype 1, :m nil}]
   [{:val "_:fdb2"} {:val "ns:ex/zip"} {:val 55105, :datatype "ns:ex/PostalCode", :m nil}]
   [{:val "_:fdb2"} {:val "ns:ex/state"} {:val "MN", :datatype 1, :m nil}]
   [{:val "ns:ex/mp"} {:val "ns:ex/address"} {:val "_:fdb1", :datatype 0}]
   [{:val "ns:ex/mp"} {:val "ns:ex/favs"} {:val "Persey", :datatype 1, :m {:i 0}}]
   [{:val "ns:ex/mp"} {:val "ns:ex/favs"} {:val "ns:ex/dp", :datatype 0, :m {:i 1}}]]

  ,)
