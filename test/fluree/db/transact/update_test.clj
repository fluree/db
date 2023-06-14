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
             ["Jane" "Bob"])
          "Only Jane and Bob should be left in the db.")

      (is (= @(fluree/query db-subj-pred-del
                            '{:selectOne {?s [:*]}
                              :where     [[?s :id :ex/bob]]})
             {:id          :ex/bob,
              :rdf/type    [:ex/User],
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
             ["Bob" "Alice"])
          "Only Bob and Alice should be left in the db.")

      (testing "Updating property value only if it's current value is a match."
        (is (= [{:id          :ex/bob,
                 :rdf/type    [:ex/User],
                 :schema/name "Bob"
                 :schema/age  23}]
               @(fluree/query db-update-bob
                              '{:select {?s [:*]}
                                :where  [[?s :id :ex/bob]]}))
            "Bob's age should now be updated to 23 (from 22)."))

      (testing "No update should happen if there is no match."
        (is (= [{:id          :ex/bob,
                 :rdf/type    [:ex/User],
                 :schema/name "Bob"
                 :schema/age  22}]
               @(fluree/query db-update-bob2
                              '{:select {?s [:*]}
                                :where  [[?s :id :ex/bob]]}))
            "Bob's age should have not been changed and still be 22."))

      (testing "Replacing existing property value with new property value."
        (is (= [{:id           :ex/jane,
                 :rdf/type     [:ex/User],
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
                                                  {"bind" {"?sha256" "(sha256 ?message)"}}
                                                  {"bind" {"?sha512" "(sha512 ?message)"}}]
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
                                                  {"bind" {"?now" "(now)"}}
                                                  {"bind" {"?year" "(year ?localdatetime)"}}
                                                  {"bind" {"?month" "(month ?localdatetime)"}}
                                                  {"bind" {"?day" "(day ?localdatetime)"}}
                                                  {"bind" {"?hours" "(hours ?localdatetime)"}}
                                                  {"bind" {"?minutes" "(minutes ?localdatetime)"}}
                                                  {"bind" {"?seconds" "(seconds ?localdatetime)"}}
                                                  {"bind" {"?tz1" "(tz ?utcdatetime)"}}
                                                  {"bind" {"?tz2" "(tz ?offsetdatetime)"}}]
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
                                                {"bind" {"?abs" "(abs ?neg-int)"}}
                                                {"bind" {"?round" "(round ?decimal)"}}
                                                {"bind" {"?ceil" "(ceil ?decimal)"}}
                                                {"bind" {"?floor" "(floor ?decimal)"}}
                                                {"bind" {"?rand" "(rand)"}}]
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
                                                 {"bind" {"?strlen" "(strLen ?text)"}}
                                                 {"bind" {"?sub1" "(subStr ?text 5)"}}
                                                 {"bind" {"?sub2" "(subStr ?text 1 4)"}}
                                                 {"bind" {"?upcased" "(ucase ?text)"}}
                                                 {"bind" {"?downcased" "(lcase ?text)"}}
                                                 {"bind" {"?a-start" "(strStarts ?text \"x\")"}}
                                                 {"bind" {"?a-end" "(strEnds ?text \"x\")"}}
                                                 {"bind" {"?contains" "(contains ?text \"x\")"}}
                                                 {"bind" {"?strBefore" "(strBefore ?text \"bcd\")"}}
                                                 {"bind" {"?strAfter" "(strAfter ?text \"bcd\")"}}
                                                 {"bind" {"?concatted" "(concat ?text \" \" \"STR1 \" \"STR2\")"}}
                                                 {"bind" {"?matched" "(regex ?text \"^Abc\")"}}]
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

    #_(testing "functional forms")
    #_(testing "scalar functions")))
