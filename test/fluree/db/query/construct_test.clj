(ns fluree.db.query.construct-test
  (:require [clojure.test :as test :refer [deftest testing is]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(def people-data
  [{"id"              "ex:jdoe"
    "type"            "ex:Person"
    "person:handle"   "jdoe"
    "person:fullName" "Jane Doe"
    "person:favNums"  [3 7 42 99]}
   {"id"              "ex:bbob"
    "type"            "ex:Person"
    "person:handle"   "bbob"
    "person:fullName" "Billy Bob"
    "person:favNums"  [23]}
   {"id"              "ex:jbob"
    "type"            "ex:Person"
    "person:handle"   "jbob"
    "person:fullName" "Jenny Bob"
    "person:favNums"  [8 6 7 5 3 0 9]}
   {"id"              "ex:fbueller"
    "type"            "ex:Person"
    "person:handle"   "dankeshön"
    "person:fullName" "Ferris Bueller"}])

(deftest construct-test
  (let [conn    @(fluree/connect-memory)
        ledger  @(fluree/create conn "people")
        db0     (fluree/db ledger)
        context ["https://ns.flur.ee" test-utils/default-str-context {"person" "http://example.org/Person#"}]
        db1     @(fluree/stage db0 {"@context" context "insert" people-data})]
    (testing "basic"
      (is (= [{"@id" "ex:bbob", "label" "Billy Bob"}
              {"@id" "ex:fbueller", "label" "Ferris Bueller"}
              {"@id" "ex:jdoe", "label" "Jane Doe"}
              {"@id" "ex:jbob", "label" "Jenny Bob"}]
             @(fluree/query db1 {"@context" context
                                 "where" [{"@id" "?s" "person:fullName" "?fullName"}]
                                 "construct" [{"@id" "?s" "label" "?fullName"}]}))))))
