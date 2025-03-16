(ns fluree.db.transact.update-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.constants :as const]
            [fluree.db.query.exec.eval :as eval]
            [fluree.db.test-utils :as test-utils])
  (:import [java.time OffsetDateTime]))

(defn const-now
  []
  {:value (OffsetDateTime/parse "2024-06-13T19:53:57.000Z")
   :datatype-iri const/iri-xsd-dateTime})

(deftest ^:integration deleting-data
  (testing "Deletions of entire subjects."
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "tx/delete")
          db     @(fluree/stage (fluree/db ledger)
                                {"@context" [test-utils/default-context
                                             {:ex "http://example.org/ns/"}]
                                 "insert"
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
                                           :schema/age   30}]}})

          ;; delete everything for :ex/alice
          db-subj-delete @(fluree/stage db
                                        {"@context" [test-utils/default-context
                                                     {:ex "http://example.org/ns/"}]
                                         "where"    '{:id :ex/alice, "?p" "?o"}
                                         "delete"   '{:id :ex/alice, "?p" "?o"}})

          ;; delete any :schema/age values for :ex/bob
          db-subj-pred-del @(fluree/stage db
                                          {"@context" [test-utils/default-context
                                                       {:ex "http://example.org/ns/"}]
                                           "delete"   {:id :ex/bob, :schema/age "?o"}
                                           "where"    {:id :ex/bob, :schema/age "?o"}})

          ;; delete all subjects with a :schema/email predicate
          db-all-preds @(fluree/stage db
                                      {"@context" [test-utils/default-context
                                                   {:ex "http://example.org/ns/"}]
                                       "delete"   {:id "?s", "?p" "?o"}
                                       "where"    {:id           "?s"
                                                   :schema/email "?x"
                                                   "?p"          "?o"}})

          ;; delete all subjects where :schema/age = 30
          db-age-delete @(fluree/stage db
                                       {"@context" [test-utils/default-context
                                                    {:ex "http://example.org/ns/"}]
                                        "delete"   {:id "?s", "?p" "?o"}
                                        "where"    {:id         "?s"
                                                    :schema/age 30
                                                    "?p"        "?o"}})

          ;; Change Bob's age - but only if his age is still 22
          db-update-bob @(fluree/stage db
                                       {"@context" [test-utils/default-context
                                                    {:ex "http://example.org/ns/"}]
                                        "delete"   {:id :ex/bob, :schema/age 22}
                                        "insert"   {:id :ex/bob, :schema/age 23}
                                        "where"    {:id :ex/bob, :schema/age 22}})

          ;; Shouldn't change Bob's age as the current age is not a match
          db-update-bob2 @(fluree/stage db
                                        {"@context" [test-utils/default-context
                                                     {:ex "http://example.org/ns/"}]
                                         "delete"   {:id "?s" :schema/age 99}
                                         "insert"   {:id "?s" :schema/age 23}
                                         "where"    {:id "?s" :schema/age 99}})

          ;; change Jane's age regardless of its current value
          db-update-jane @(fluree/stage db
                                        {"@context" [test-utils/default-context
                                                     {:ex "http://example.org/ns/"}]
                                         "delete"   {:id :ex/jane, :schema/age "?current-age"}
                                         "insert"   {:id :ex/jane, :schema/age 31}
                                         "where"    {:id :ex/jane, :schema/age "?current-age"}})]

      (is (= @(fluree/query db-subj-delete
                            {:context [test-utils/default-context
                                       {:ex "http://example.org/ns/"}]
                             :select '?name
                             :where  {:schema/name '?name}})
             ["Bob" "Jane"])
          "Only Jane and Bob should be left in the db.")

      (is (= @(fluree/query db-subj-pred-del
                            {:context [test-utils/default-context
                                       {:ex "http://example.org/ns/"}]
                             :selectOne {:ex/bob [:*]}})
             {:id          :ex/bob,
              :type        :ex/User,
              :schema/name "Bob"})
          "Bob should no longer have an age property.")

      (is (= @(fluree/query db-all-preds
                            {:context [test-utils/default-context
                                       {:ex "http://example.org/ns/"}]
                             :select '?name
                             :where  {:schema/name '?name}})
             ["Bob"])
          "Only Bob should be left, as he is the only one without an email.")

      (is (= @(fluree/query db-age-delete
                            {:context [test-utils/default-context
                                       {:ex "http://example.org/ns/"}]
                             :select '?name
                             :where  {:schema/name '?name}})
             ["Alice" "Bob"])
          "Only Bob and Alice should be left in the db.")

      (testing "Updating property value only if its current value is a match."
        (is (= [{:id          :ex/bob,
                 :type        :ex/User,
                 :schema/name "Bob"
                 :schema/age  23}]
               @(fluree/query db-update-bob
                              {:context [test-utils/default-context
                                         {:ex "http://example.org/ns/"}]
                               :select {:ex/bob [:*]}}))
            "Bob's age should now be updated to 23 (from 22)."))

      (testing "No update should happen if there is no match."
        (is (= [{:id          :ex/bob,
                 :type        :ex/User,
                 :schema/name "Bob"
                 :schema/age  22}]
               @(fluree/query db-update-bob2
                              {:context [test-utils/default-context
                                         {:ex "http://example.org/ns/"}]
                               :select {:ex/bob [:*]}}))
            "Bob's age should have not been changed and still be 22."))

      (testing "Replacing existing property value with new property value."
        (is (= [{:id           :ex/jane,
                 :type         :ex/User,
                 :schema/name  "Jane"
                 :schema/email "jane@flur.ee"
                 :schema/age   31}]
               @(fluree/query db-update-jane
                              {:context [test-utils/default-context
                                         {:ex "http://example.org/ns/"}]
                               :select {:ex/jane [:*]}}))
            "Jane's age should now be updated to 31 (from 30).")))))

(deftest transaction-functions
  (let [conn   @(fluree/connect-memory)
        ledger @(fluree/create conn "functions")
        db1    (fluree/db ledger)]

    (testing "hash functions"
      (with-redefs [eval/now const-now]
        (let [updated (-> @(fluree/stage db1 {"@context" [test-utils/default-str-context
                                                          {"ex" "http://example.com/"}]
                                              "insert"   [{"id"     "ex:create-predicates"
                                                           "ex:md5" 0 "ex:sha1" 0 "ex:sha256" 0 "ex:sha384" 0 "ex:sha512" 0}
                                                          {"id"         "ex:hash-fns"
                                                           "ex:message" "abc"}]})
                          (fluree/stage {"@context" [test-utils/default-str-context
                                                     {"ex" "http://example.com/"}]
                                         "delete"   []
                                         "where"    [{"id"         "ex:hash-fns"
                                                      "ex:message" "?message"}
                                                     ["bind"
                                                      "?sha256" "(sha256 ?message)"
                                                      "?sha512" "(sha512 ?message)"]]
                                         "insert"   {"id"        "ex:hash-fns"
                                                     "ex:sha256" "?sha256"
                                                     "ex:sha512" "?sha512"}}))]
          (is (= {"ex:sha512" "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
                  "ex:sha256" "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}
                 @(fluree/query @updated {"@context"  [test-utils/default-str-context
                                                       {"ex" "http://example.com/"}]
                                          "selectOne" {"ex:hash-fns" ["ex:sha512" "ex:sha256"]}}))))))
    (testing "datetime functions"
      (with-redefs [fluree.db.query.exec.eval/now const-now]
        (let [db2 @(fluree/stage db1 {"@context" [test-utils/default-str-context
                                                  {"ex" "http://example.com/"}]
                                      "insert"
                                      [{"id"         "ex:create-predicates"
                                        "ex:now"     0 "ex:year"    0 "ex:month"    0 "ex:day" 0 "ex:hours" 0
                                        "ex:minutes" 0 "ex:seconds" 0 "ex:timezone" 0 "ex:tz"  0}
                                       {"id"                "ex:datetime-fns"
                                        "ex:localdatetime"  {"@value" "2023-06-13T14:17:22.435"
                                                             "@type"  const/iri-xsd-dateTime}
                                        "ex:offsetdatetime" {"@value" "2023-06-13T14:17:22.435-05:00"
                                                             "@type"  const/iri-xsd-dateTime}
                                        "ex:utcdatetime"    {"@value" "2023-06-13T14:17:22.435Z"
                                                             "@type"  const/iri-xsd-dateTime}}]})
              db3 @(fluree/stage db2 {"@context" [test-utils/default-str-context
                                                  {"ex" "http://example.com/"}]
                                      "values"   ["?s" [{"@value" "ex:datetime-fns" "@type" "@id"}]]
                                      "where"    [{"id"                "?s"
                                                   "ex:localdatetime"  "?localdatetime"
                                                   "ex:offsetdatetime" "?offsetdatetime"
                                                   "ex:utcdatetime"    "?utcdatetime"}
                                               ["bind"
                                                "?now" "(str (now))"
                                                "?year" "(year ?localdatetime)"
                                                "?month" "(month ?localdatetime)"
                                                "?day" "(day ?localdatetime)"
                                                "?hours" "(hours ?localdatetime)"
                                                "?minutes" "(minutes ?localdatetime)"
                                                "?seconds" "(seconds ?localdatetime)"
                                                "?tz1" "(tz ?utcdatetime)"
                                                "?tz2" "(tz ?offsetdatetime)"
                                                "?comp=" "(= ?localdatetime (now))"
                                                "?comp<" "(< ?localdatetime (now))"
                                                "?comp<=" "(<= ?localdatetime (now))"
                                                "?comp>" "(> ?localdatetime (now))"
                                                "?comp>=" "(>= ?localdatetime (now))"]]
                                      "insert"   [{"id"         "?s"
                                                   "ex:now"     "?now"
                                                   "ex:year"    "?year"
                                                   "ex:month"   "?month"
                                                   "ex:day"     "?day"
                                                   "ex:hours"   "?hours"
                                                   "ex:minutes" "?minutes"
                                                   "ex:seconds" "?seconds"
                                                   "ex:tz"      ["?tz1" "?tz2"]
                                                   "ex:comp="   "?comp="
                                                   "ex:comp<"   "?comp<"
                                                   "ex:comp<="  "?comp<="
                                                   "ex:comp>"   "?comp>"
                                                   "ex:comp>="  "?comp>="}]})]
          (is (= {"ex:now"     "2024-06-13T19:53:57Z"
                  "ex:year"    2023
                  "ex:month"   6
                  "ex:day"     13
                  "ex:hours"   14
                  "ex:minutes" 17
                  "ex:seconds" 22
                  "ex:tz"      ["-05:00" "Z"]
                  "ex:comp="   false
                  "ex:comp<"   true
                  "ex:comp<="  true
                  "ex:comp>"   false
                  "ex:comp>="  false}
                 @(fluree/query db3
                                {"@context" [test-utils/default-str-context
                                             {"ex" "http://example.com/"}]
                                 "selectOne"
                                 {"ex:datetime-fns" ["ex:now" "ex:year"
                                                     "ex:month" "ex:day"
                                                     "ex:hours" "ex:minutes"
                                                     "ex:seconds" "ex:tz"
                                                     "ex:comp="
                                                     "ex:comp<" "ex:comp<="
                                                     "ex:comp>" "ex:comp>="]}}))))))

    (testing "numeric functions"
      (let [updated (-> @(fluree/stage db1 {"@context" [test-utils/default-str-context
                                                        {"ex" "http://example.com/"}]
                                            "insert"   [{"id"     "ex:create-predicates"
                                                         "ex:abs" 0 "ex:round" 0 "ex:ceil" 0 "ex:floor" 0 "ex:rand" 0}
                                                        {"id"         "ex:numeric-fns"
                                                         "ex:pos-int" 2
                                                         "ex:neg-int" -2
                                                         "ex:decimal" 1.4}]})
                        (fluree/stage {"@context" [test-utils/default-str-context
                                                   {"ex" "http://example.com/"}]
                                       "where"    [{"id"         "?s"
                                                    "ex:pos-int" "?pos-int"
                                                    "ex:neg-int" "?neg-int"
                                                    "ex:decimal" "?decimal"}
                                                   ["bind"
                                                    "?abs" "(abs ?neg-int)"
                                                    "?round" "(round ?decimal)"
                                                    "?ceil" "(ceil ?decimal)"
                                                    "?floor" "(floor ?decimal)"
                                                    "?rand" "(rand)"]]
                                       "insert"   {"id"       "?s"
                                                   "ex:abs"   "?abs"
                                                   "ex:round" "?round"
                                                   "ex:ceil"  "?ceil"
                                                   "ex:floor" "?floor"
                                                   "ex:rand"  "?rand"}
                                       "values"   ["?s" [{"@value" "ex:numeric-fns" "@type" "@id"}]]}))]
        (is (= {"ex:abs"   2
                "ex:round" 1
                "ex:ceil"  2
                "ex:floor" 1}
               @(fluree/query @updated
                              {"@context" [test-utils/default-str-context
                                           {"ex" "http://example.com/"}]
                               "selectOne"
                               {"ex:numeric-fns" ["ex:abs" "ex:round" "ex:ceil"
                                                  "ex:floor"]}})))
        (is (pos? @(fluree/query @updated {"@context"  [test-utils/default-str-context
                                                        {"ex" "http://example.com/"}]
                                           "where"     {"id"      "ex:numeric-fns"
                                                        "ex:rand" "?rand"}
                                           "selectOne" "?rand"})))))

    (testing "string functions"
      (let [updated (-> @(fluree/stage db1 {"@context" [test-utils/default-str-context
                                                        {"ex" "http://example.com/"}]
                                            "insert"   [{"id"              "ex:create-predicates"
                                                         "ex:strLen"       0 "ex:subStr"    0 "ex:ucase"    0
                                                         "ex:lcase"        0 "ex:strStarts" 0 "ex:strEnds"  0
                                                         "ex:contains"     0 "ex:strBefore" 0 "ex:strAfter" 0
                                                         "ex:encodeForUri" 0 "ex:concat"    0
                                                         "ex:langMatches"  0 "ex:regex"     0 "ex:replace"  0}
                                                        {"id"      "ex:string-fns"
                                                         "ex:text" "Abcdefg"}]})
                        (fluree/stage {"@context" [test-utils/default-str-context
                                                   {"ex" "http://example.com/"}]
                                       "where"    [{"id"      "?s"
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
                                       "insert"   [{"id"           "?s"
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
                                       "values"   ["?s" [{"@value" "ex:string-fns" "@type" "@id"}]]}))]
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
                 {"@context" [test-utils/default-str-context
                              {"ex" "http://example.com/"}]
                  "selectOne"
                  {"ex:string-fns"
                   ["ex:strLen" "ex:subStr" "ex:ucase" "ex:lcase" "ex:strStarts"
                    "ex:strEnds" "ex:contains" "ex:strBefore" "ex:strAfter"
                    "ex:encodeForUri" "ex:concat" "ex:langMatches" "ex:regex"
                    "ex:replace"]}})))))

    (testing "rdf term functions"
      (with-redefs [fluree.db.query.exec.eval/uuid    (fn [] {:value "urn:uuid:34bdb25f-9fae-419b-9c50-203b5f306e47" :datatype-iri "@id"})
                    fluree.db.query.exec.eval/struuid (fn [] {:value "34bdb25f-9fae-419b-9c50-203b5f306e47" :datatype-iri const/iri-string})]
        (let [updated (-> @(fluree/stage db1 {"@context" [test-utils/default-str-context
                                                          {"ex" "http://example.com/"}]
                                              "insert"   [{"id"           "ex:create-predicates"
                                                           "ex:isBlank"   0 "ex:isNumeric"    0 "ex:str"        0 "ex:uuid"  0
                                                           "ex:struuid"   0 "ex:isNotNumeric" 0 "ex:isNotBlank" 0
                                                           "ex:lang"      0 "ex:datatype"     0 "ex:IRI"        0 "ex:isIRI" 0
                                                           "ex:isLiteral" 0 "ex:strdt"        0 "ex:strLang"    0
                                                           "ex:bnode"     0}
                                                          {"id"          "ex:rdf-term-fns"
                                                           "ex:text"     "Abcdefg"
                                                           "ex:langText" {"@value"    "hola"
                                                                          "@language" "es"}
                                                           "ex:number"   1
                                                           "ex:ref"      {"ex:bool" false}}
                                                          {"ex:foo" "bar"}]})
                          (fluree/stage {"@context" [test-utils/default-str-context
                                                     {"ex" "http://example.com/"}]
                                         "where"    [{"id"          "?s"
                                                      "ex:text"     "?text"
                                                      "ex:langText" "?langtext"
                                                      "ex:number"   "?num"
                                                      "ex:ref"      "?r"}
                                                     ["bind"
                                                      "?str" "(str ?num)"
                                                      "?str2" "(str ?text)"
                                                      "?uuid" "(uuid)"
                                                      "?struuid" "(struuid)"
                                                      "?isBlank" "(isBlank ?s)"
                                                      "?isNotBlank" "(isBlank ?num)"
                                                      "?isnum" "(isNumeric ?num)"
                                                      "?isNotNum" "(isNumeric ?text)"
                                                      "?lang" "(lang ?langtext)"
                                                      "?datatype" "(datatype ?langtext)"
                                                      "?IRI" "(iri (concat \"ex:\" ?text))"
                                                      "?isIRI" "(is-iri ?IRI)"
                                                      "?isLiteral" "(is-literal ?num)"
                                                      "?strdt" "(str-dt ?text \"ex:mystring\")"
                                                      "?strLang" "(str-lang ?text \"foo\")"
                                                      "?bnode" "(bnode)"]]
                                         "insert"   [{"id"              "?s"
                                                      "ex:uuid"         "?uuid"
                                                      "ex:struuid"      "?struuid"
                                                      "ex:str"          ["?str" "?str2"]
                                                      "ex:isNumeric"    "?isnum"
                                                      "ex:isNotNumeric" "?isNotNum"
                                                      "ex:isBlank"      "?isBlank"
                                                      "ex:isNotBlank"   "?isNotBlank"
                                                      "ex:lang"         "?lang"
                                                      "ex:datatype"     "?datatype"
                                                      "ex:IRI"          "?IRI"
                                                      "ex:isIRI"        "?isIRI"
                                                      "ex:isLiteral"    "?isLiteral"
                                                      "ex:strdt"        "?strdt"
                                                      "ex:strLang"      "?strLang"
                                                      "ex:bnode"        "?bnode"}]
                                         "values"   ["?s" [{"@value" "ex:rdf-term-fns" "@type" "@id"}]]}))]
          (is (test-utils/pred-match? {"ex:str"          ["1" "Abcdefg"]
                                       "ex:uuid"         {"id" "urn:uuid:34bdb25f-9fae-419b-9c50-203b5f306e47"}
                                       "ex:struuid"      "34bdb25f-9fae-419b-9c50-203b5f306e47",
                                       "ex:isBlank"      false
                                       "ex:isNotBlank"   false
                                       "ex:isNumeric"    true
                                       "ex:isNotNumeric" false
                                       "ex:lang"         "es"
                                       "ex:datatype"     {"id" "rdf:langString"}
                                       "ex:IRI"          {"id" "ex:Abcdefg"}
                                       "ex:isIRI"        true
                                       "ex:isLiteral"    true
                                       "ex:strLang"      "Abcdefg"
                                       "ex:strdt"        "Abcdefg"
                                       "ex:bnode"        {"id" test-utils/blank-node-id?}}
                                      @(fluree/query @updated {"@context"  [test-utils/default-str-context
                                                                            {"ex" "http://example.com/"}]
                                                               "selectOne" {"ex:rdf-term-fns" ["ex:isIRI" "ex:isURI" "ex:isLiteral"
                                                                                               "ex:lang" "ex:datatype" "ex:IRI"
                                                                                               "ex:bnode" "ex:strdt" "ex:strLang"
                                                                                               "ex:isBlank"
                                                                                               "ex:isNotBlank"
                                                                                               "ex:isNumeric"
                                                                                               "ex:isNotNumeric"
                                                                                               "ex:str"
                                                                                               "ex:uuid"
                                                                                               "ex:struuid"]}}))))))

    (testing "functional forms"
      (let [updated (-> @(fluree/stage db1 {"@context" [test-utils/default-str-context
                                                        {"ex" "http://example.com/"}]
                                            "insert"   [{"id"               "ex:create-predicates"
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
                                                         "ex:text" "Abcdefg"}]})
                        (fluree/stage {"@context" [test-utils/default-str-context
                                                   {"ex" "http://example.com/"}]
                                       "where"    [{"id" "?s", "ex:text" "?text"}
                                                   ["bind"
                                                    "?bound" "(bound ?text)"
                                                    "?in" "(in (strLen ?text) [(+ 6 1) 8 9])"
                                                    "?not-in" "(not (in (strLen ?text) [(+ 6 1) 8 9]))"]]
                                       "insert"   {"id"        "?s",
                                                   "ex:bound"  "?bound"
                                                   "ex:in"     "?in"
                                                   "ex:not-in" "?not-in"}
                                       "values"   ["?s" [{"@value" "ex:functional-fns" "@type" "@id"}]]}))]
        (is (= {"ex:bound"  true
                "ex:in"     true
                "ex:not-in" false}
               @(fluree/query @updated {"@context"  [test-utils/default-str-context
                                                     {"ex" "http://example.com/"}]
                                        "selectOne" {"ex:functional-fns" ["ex:bound"
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
      (let [db2       @(fluree/stage db1 {"@context" [test-utils/default-str-context
                                                      {"ex" "http://example.com/"}]
                                          "insert"   [{"id"       "ex:create-predicates"
                                                       "ex:text"  0
                                                       "ex:error" 0}
                                                      {"id"      "ex:error"
                                                       "ex:text" "Abcdefg"}]})
            parse-err @(fluree/stage db2 {"@context" [test-utils/default-str-context
                                                      {"ex" "http://example.com/"}]
                                          "where"    [{"id" "?s", "ex:text" "?text"}
                                                      ["bind" "?err" "(foo ?text)"]]
                                          "insert"   {"id" "?s", "ex:text" "?err"}
                                          "values"   ["?s" [{"@value" "ex:error" "@type" "@id"}]]})

            _run-err @(fluree/stage db2 {"@context" [test-utils/default-str-context
                                                     {"ex" "http://example.com/"}]
                                         "where"    [{"id" "?s", "ex:text" "?text"}
                                                     ["bind" "?err" "(abs ?text)"]]
                                         "insert"   {"id" "?s", "ex:error" "?err"}
                                         "values"   ["?s" [{"@value" "ex:error" "@type" "@id"}]]})]
        (is (= "Query function references illegal symbol: foo"
               (-> parse-err
                   Throwable->map
                   :cause))
            "mdfn parse error")
        (is (= "Query function references illegal symbol: foo"
               (-> @(fluree/query db2 {"@context" [test-utils/default-str-context
                                                   {"ex" "http://example.com/"}]
                                       "where"    [{"id"      "ex:error"
                                                    "ex:text" "?text"}
                                                   ["bind" "?err" "(foo ?text)"]]
                                       "select"   "?err"})
                   Throwable->map
                   :cause))
            "query parse error")))))

(deftest ^:integration subject-object-scan-deletions
  (let [conn      @(fluree/connect-memory)
        ledger-id "test/love"
        ledger    @(fluree/create conn ledger-id)
        context   {"id"     "@id",
                   "type"   "@type",
                   "ex"     "http://example.org/",
                   "f"      "https://ns.flur.ee/ledger#",
                   "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                   "rdfs"   "http://www.w3.org/2000/01/rdf-schema#",
                   "schema" "http://schema.org/",
                   "xsd"    "http://www.w3.org/2001/XMLSchema#"}
        love      @(fluree/stage (fluree/db ledger)
                                 {"@context" context
                                  "insert"
                                  [{"@id"                "ex:fluree",
                                    "@type"              "schema:Organization",
                                    "schema:description" "We ❤️ Data"}
                                   {"@id"                "ex:w3c",
                                    "@type"              "schema:Organization",
                                    "schema:description" "We ❤️ Internet"}
                                   {"@id"                "ex:mosquitos",
                                    "@type"              "ex:Monster",
                                    "schema:description" "We ❤️ Human Blood"}]}
                                 {})
        db1       @(fluree/commit! ledger love)]
    (testing "before deletion"
      (let [q       {:context context
                     :select  '[?s ?p ?o]
                     :where   '{"@id"                ?s
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
                         {"@context" [context
                                      {:id "@id", :graph "@graph",
                                       :f  "https://ns.flur.ee/ledger#"}]
                          "ledger"   ledger-id
                          "where"    '{"id"                 ?s
                                       "schema:description" ?o
                                       ?p                   ?o}
                          "delete"   '{"id" "?s", "?p" "?o"}})
      (let [db2     (fluree/db @(fluree/load conn ledger-id))
            q       {:context context
                     :select '[?s ?p ?o]
                     :where  '{"id"                 ?s
                               "schema:description" ?o
                               ?p                   ?o}}
            subject @(fluree/query db2 q)]
        (is (= []
               subject)
            "returns no results")))))

(deftest ^:integration issue-core-49-transaction-test
  (testing "issue https://github.com/fluree/core/issues/49 stays fixed"
    (let [conn        (test-utils/create-conn)
          ledger-name "rando-txn"
          ledger      @(fluree/create conn "rando-txn")
          db0         (fluree/db ledger)
          db1         @(fluree/stage
                        db0
                        {"@context" test-utils/default-str-context
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
          db2         @(fluree/stage
                        db1
                        {"@context" [test-utils/default-str-context]
                         "ledger"   ledger-name
                         "where"    {"@id"                "ex:mosquitos"
                                     "schema:description" "?o"}
                         "delete"   {"@id"                "ex:mosquitos"
                                     "schema:description" "?o"}
                         "insert"   {"@id"                "ex:mosquitos"
                                     "schema:description" "We ❤️ All Blood"}})]
      (is (= [{"id"                 "ex:mosquitos"
               "type"               "ex:Monster"
               "schema:description" "We ❤️ All Blood"}]
             @(fluree/query db2 {"@context" test-utils/default-str-context
                                 :select    {"ex:mosquitos" ["*"]}}))))))

(deftest ^:integration updates-only-on-existence
  (testing "Updating data with iri values bindings"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "update-without-insert")
          db0    (fluree/db ledger)
          db1    @(fluree/stage db0
                                {"@context" [test-utils/default-context
                                             {:ex "http://example.org/ns/"}]
                                 "insert"
                                 {:graph [{:id           :ex/rutherford
                                           :type         :ex/User
                                           :schema/name  "Rutherford"
                                           :schema/email "rbhayes@usa.gov"
                                           :schema/age   55}
                                          {:id          :ex/millard
                                           :type        :ex/User
                                           :schema/name "Millard Fillmore"
                                           :schema/age  62}]}})]
      (testing "on existing subjects"
        (let [db2 @(fluree/stage
                    db1
                    {"@context" [{:ex "http://example.org/ns/", :id "@id", :value "@value"}
                                 test-utils/default-context]
                     "where"    {:id "?s", :schema/name "?o"}
                     "delete"   {:id "?s", :schema/name "?o"}
                     "insert"   {:id "?s", :schema/name "Rutherford B. Hayes"}
                     "values"   ["?s" [{:value :ex/rutherford, :type :id}]]})]
          (is (= [{:type :ex/User,
                   :schema/age 55,
                   :schema/email "rbhayes@usa.gov",
                   :schema/name "Rutherford B. Hayes",
                   :id :ex/rutherford}]
                 @(fluree/query db2 {"@context" [test-utils/default-context
                                                 {:ex "http://example.org/ns/"}]
                                     :select    {:ex/rutherford [:*]}}))
              "updates the specified properties on the specified subjects")
          (is (= [{:id          :ex/millard
                   :type        :ex/User
                   :schema/name "Millard Fillmore"
                   :schema/age  62}]
                 @(fluree/query db2 {"@context" [test-utils/default-context
                                                 {:ex "http://example.org/ns/"}]
                                     :select    {:ex/millard [:*]}}))
              "does not update different subjects")))
      (testing "on nonexistent subjects"
        (let [db2 @(fluree/stage
                    db1
                    {"@context" [{:ex "http://example.org/ns/", :id "@id", :value "@value"}
                                 test-utils/default-context]
                     "where"    {:id "?s", :schema/name "?o"}
                     "delete"   {:id "?s", :schema/name "?o"}
                     "insert"   {:id "?s", :schema/name "Chester A. Arthur"}
                     "values"   ["?s" [{:value :ex/chester, :type :id}]]})]
          (is (= [{:type :ex/User,
                   :schema/age 55,
                   :schema/email "rbhayes@usa.gov",
                   :schema/name "Rutherford",
                   :id :ex/rutherford}]
                 @(fluree/query db2 {"@context" [test-utils/default-context
                                                 {:ex "http://example.org/ns/"}]
                                     :select    {:ex/rutherford [:*]}}))
              "does not update existing subjects")
          (is (= [{:id :ex/chester}]
                 @(fluree/query db2 {"@context" [test-utils/default-context
                                                 {:ex "http://example.org/ns/"}]
                                     :select    {:ex/chester [:*]}}))
              "does not add any facts for non-existing subjects"))))))
