(ns fluree.db.query.construct-test
  (:require [clojure.test :as test :refer [deftest testing is]]
            [fluree.db.api :as fluree]))

(def people-data
  [{"@id"             "ex:jdoe"
    "@type"           "ex:Person"
    "person:handle"   "jdoe"
    "person:fullName" "Jane Doe"
    "person:favNums"  [3 7 42 99]}
   {"@id"             "ex:bbob"
    "@type"           "ex:Person"
    "person:handle"   "bbob"
    "person:fullName" "Billy Bob"
    "person:friend"   {"@id" "ex:jbob"}
    "person:favNums"  [23]}
   {"@id"             "ex:jbob"
    "@type"           "ex:Person"
    "person:handle"   "jbob"
    "person:friend"   {"@id" "ex:fbueller"}
    "person:fullName" "Jenny Bob"
    "person:favNums"  [8 6 7 5 3 0 9]}
   {"@id"             "ex:fbueller"
    "@type"           "ex:Person"
    "person:handle"   "dankeshön"
    "person:fullName" "Ferris Bueller"}
   {"@id"              "ex:alice"
    "foaf:givenname"   "Alice"
    "foaf:family_name" "Hacker"}
   {"@id"            "ex:bob"
    "foaf:firstname" "Bob"
    "foaf:surname"   "Hacker"}
   {"@id"    "ex:fran"
    "name"   {"@value" "Francois" "@language" "fr"}
    "config" {"@type" "@json" "@value" {"paths" ["dev" "src"]}}
    "date"   {"@value" "2020-10-20" "@type" "http://www.w3.org/2001/XMLSchema#date"}}])

(deftest construct-test
  (let [conn    @(fluree/connect-memory)
        ledger  @(fluree/create conn "people")
        db0     (fluree/db ledger)
        context {"person" "http://example.org/Person#"
                 "ex" "http://example.org/"}
        db1     @(fluree/stage db0 {"@context" context "insert" people-data})]
    (testing "basic"
      (is (= {"@context" {"person" "http://example.org/Person#", "ex" "http://example.org/"}
              "@graph"
              [{"@id" "ex:bbob", "label" ["Billy Bob"]}
               {"@id" "ex:fbueller", "label" ["Ferris Bueller"]}
               {"@id" "ex:jbob", "label" ["Jenny Bob"]}
               {"@id" "ex:jdoe", "label" ["Jane Doe"]}]}
             @(fluree/query db1 {"@context" context
                                 "where" [{"@id" "?s" "person:fullName" "?fullName"}]
                                 "construct" [{"@id" "?s" "label" "?fullName"}]}))))
    (testing "nil context"
      (is (= {"@graph"
              [{"@id" "http://example.org/bbob", "ex:label" ["Billy Bob"]}
               {"@id" "http://example.org/fbueller", "ex:label" ["Ferris Bueller"]}
               {"@id" "http://example.org/jbob", "ex:label" ["Jenny Bob"]}
               {"@id" "http://example.org/jdoe", "ex:label" ["Jane Doe"]}]}
             @(fluree/query db1 {"@context" nil
                                 "where" [{"@id" "?s" "http://example.org/Person#fullName" "?fullName"}]
                                 "construct" [{"@id" "?s" "ex:label" "?fullName"}]}))))
    (testing "multiple clauses"
      (is (= {"@context" {"person" "http://example.org/Person#", "ex" "http://example.org/" "id" "@id"}
              "@graph"
              [{"id" "ex:bbob", "name" ["Billy Bob"], "num" [23]}
               {"id" "ex:jbob", "name" ["Jenny Bob"], "num" [0 3 5 6 7 8 9]}
               {"id" "ex:jdoe", "name" ["Jane Doe"], "num" [3 7 42 99]}]}
             @(fluree/query db1 {"@context" (assoc context "id" "@id")
                                 "where" [{"@id" "?s" "person:fullName" "?fullName"}
                                          {"@id" "?s" "person:favNums" "?num"}]
                                 "construct" [{"@id" "?s" "name" "?fullName"}
                                              {"@id" "?s" "num" "?num"}]}))))
    (testing "multiple clauses, different subjects"
      (is (= {"@context" {"person" "http://example.org/Person#", "ex" "http://example.org/"}
              "@graph"
              [{"@id" "ex:bbob", "myname" ["Billy Bob"], "friendname" ["Jenny Bob"]}
               {"@id" "ex:jbob", "name" ["Jenny Bob"], "num" [0 3 5 6 7 8 9]}]}
             @(fluree/query db1 {"@context" context
                                 "where" [{"@id" "?s" "person:fullName" "?fullName"}
                                          {"@id" "?s" "person:friend" "?friend"}
                                          {"@id" "?friend" "person:fullName" "?friendName"}
                                          {"@id" "?friend" "person:favNums" "?friendNum"}]
                                 "construct" [{"@id" "?s" "myname" "?fullName"}
                                              {"@id" "?s" "friendname" "?friendName"}
                                              {"@id" "?friend" "name" "?friendName"}
                                              {"@id" "?friend" "num" "?friendNum"}]}))))
    (testing "value metadata displays"
      (is (= {"@graph"
              [{"@id" "ex:fran",
                "json" [{"@value" "{\"paths\":[\"dev\",\"src\"]}",
                         "@type" "@json"}],
                "name" [{"@value" "Francois", "@language" "fr"}],
                "date" [{"@value" #time/date "2020-10-20",
                         "@type" "http://www.w3.org/2001/XMLSchema#date"}]}],
              "@context"
              {"person" "http://example.org/Person#", "ex" "http://example.org/"}}
             @(fluree/query db1 {"@context" context
                                 "where" [{"@id" "?s" "config" "?config"}
                                          {"@id" "?s" "name" "?name"}
                                          {"@id" "?s" "date" "?date"}]
                                 "construct" [{"@id" "?s" "json" "?config"}
                                              {"@id" "?s" "name" "?name"}
                                              {"@id" "?s" "date" "?date"}]}))))
    (testing "@type values are unwrapped"
      (is (= {"@context" {"person" "http://example.org/Person#", "ex" "http://example.org/"}
              "@graph" [{"@id" "ex:bbob", "@type" ["ex:Person"]}
                        {"@id" "ex:fbueller", "@type" ["ex:Person"]}
                        {"@id" "ex:jbob", "@type" ["ex:Person"]}
                        {"@id" "ex:jdoe", "@type" ["ex:Person"]}]}
             @(fluree/query db1 {"@context" context
                                 "where" [{"@id" "?s" "@type" "?o"}]
                                 "construct" [{"@id" "?s" "@type" "?o"}]}))))
    (testing ":class patterns are constructed correctly"
      (is (= {"@context" {"person" "http://example.org/Person#", "ex" "http://example.org/"}
              "@graph" [{"@id" "ex:bbob", "@type" ["ex:Human"]}
                        {"@id" "ex:fbueller", "@type" ["ex:Human"]}
                        {"@id" "ex:jbob", "@type" ["ex:Human"]}
                        {"@id" "ex:jdoe", "@type" ["ex:Human"]}]}
             @(fluree/query db1 {"@context" context
                                 "where" [{"@id" "?s" "@type" "ex:Person"}]
                                 ;; :class pattern in construct clause
                                 "construct" [{"@id" "?s" "@type" "ex:Human"}]}))))
    (testing ":id patterns cannot produce valid triples"
      (is (= {"@context" {"person" "http://example.org/Person#", "ex" "http://example.org/"}
              "@graph" []}
             @(fluree/query db1 {"@context" context
                                 "where" [{"@id" "?s" "@type" "ex:Person"}]
                                 ;; :id pattern in construct clause
                                 "construct" [{"@id" "?s"}]}))))
    (testing "unbound vars are not included"
      (is (= {"@context" {"person" "http://example.org/Person#", "ex" "http://example.org/"}
              "@graph" [{"@id" "ex:alice", "ex:name" ["Alice"]}
                        {"@id" "ex:bbob", "@type" ["ex:Person"]}
                        {"@id" "ex:fbueller" "@type" ["ex:Person"]}
                        {"@id" "ex:jbob" "@type" ["ex:Person"]}
                        {"@id" "ex:jdoe" "@type" ["ex:Person"]}]}
             @(fluree/query db1 {"@context" context
                                 "where" [{"@id" "?s" "?p" "?o"}
                                          ["optional" {"@id" "?s" "@type" "?type"}]
                                          ["optional" {"@id" "?s" "foaf:givenname" "?name"}]]
                                 "construct" [{"@id" "?s"  "ex:name" "?name" "@type" "?type"}]}))))

    #_(testing "bnode template"
        (is (= {"@context" {"person" "http://example.org/Person#", "ex" "http://example.org/"}
                "@graph"
                [{"@id" "_:v1",
                  "vcard:givenName" ["Bob"],
                  "vcard:familyName" ["Hacker"]}
                 {"@id" "_:v2",
                  "vcard:givenName" ["Alice"],
                  "vcard:familyName" ["Hacker"]}
                 {"@id" "ex:alice", "vcard:N" [{"@id" "_:v2"}]}
                 {"@id" "ex:bob", "vcard:N" [{"@id" "_:v1"}]}]}
               @(fluree/query db1
                              {:context context
                               :where [[:union
                                        [{"@id" "?x", "foaf:firstname" "?gname"}]
                                        [{"@id" "?x", "foaf:givenname" "?gname"}]]
                                       [:union
                                        [{"@id" "?x", "foaf:surname" "?fname"}]
                                        [{"@id" "?x", "foaf:family_name" "?fname"}]]]
                               :construct [{"@id" "?x", "vcard:N" "_:v"}
                                           {"@id" "_:v", "vcard:givenName" "?gname"}
                                           {"@id" "_:v", "vcard:familyName" "?fname"}]}))))))
