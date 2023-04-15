(ns fluree.db.query.property-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]))

(deftest ^:integration equivalent-properties-test
  (testing "Equivalent properties"
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "query/equivalent-properties")
          context {"vocab1" "http://vocab1.example.org/"
                   "vocab2" "http://vocab1.example.org/"
                   "ex"     "http://example.org/ns/"
                   "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                   "owl"    "http://www.w3.org/2002/07/owl#"}
          db      (-> ledger
                      fluree/db
                      (fluree/stage {"@context" context
                                     "@graph"   [{"@id"   "vocab1:givenName"
                                                  "@type" "rdf:Property"}
                                                 {"@id"                    "vocab2:firstName"
                                                  "@type"                  "rdf:Property"
                                                  "owl:equivalentProperty" {"@id" "vocab1:givenName"}}]})
                      deref
                      (fluree/stage {"@context" context
                                     "@graph"   [{"@id"              "ex:brian"
                                                  "vocab1:givenName" "Brian"}
                                                 {"@id"              "ex:ben"
                                                  "vocab2:firstName" "Ben"}]})
                      deref)]
      (testing "querying for the property defined to be equivalent"
        (is (= [["Brian"] ["Ben"]]
               @(fluree/query db '{"@context" {"vocab1" "http://vocab1.example.org/"
                                               "vocab2" "http://vocab1.example.org/"}
                                   :select    [?name]
                                   :where     [[?s "vocab2:firstName" ?name]]}))
            "returns all values")))))
