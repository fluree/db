(ns fluree.db.query.construct-test
  (:require [clojure.test :as test :refer [deftest testing is]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

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
    "person:fullName" "Ferris Bueller"}])

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
                                              {"@id" "?friend" "num" "?friendNum"}]}))))))
