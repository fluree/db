(ns fluree.db.query.property-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]))

(deftest ^:integration equivalent-properties-test
  (testing "Equivalent properties"
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "query/equivalent-properties")
          context {"vocab1" "http://vocab1.example.org/"
                   "vocab2" "http://vocab2.example.org/"
                   "vocab3" "http://vocab3.example.fr/"
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
                                                  "owl:equivalentProperty" {"@id" "vocab1:givenName"}}
                                                 {"@id"                    "vocab3:prenom"
                                                  "@type"                  "rdf:Property"
                                                  "owl:equivalentProperty" {"@id" "vocab2:firstName"}}]})
                      deref
                      (fluree/stage {"@context" context
                                     "@graph"   [{"@id"              "ex:brian"
                                                  "ex:age"           50
                                                  "vocab1:givenName" "Brian"}
                                                 {"@id"              "ex:ben"
                                                  "vocab2:firstName" "Ben"}
                                                 {"@id"              "ex:francois"
                                                  "vocab3:prenom" "Francois"}]})
                      deref)]
      (testing "querying for the property defined to be equivalent"
        (is (= [["Brian"] ["Ben"] ["Francois"]]
               @(fluree/query db '{"@context" {"vocab1" "http://vocab1.example.org/"
                                               "vocab2" "http://vocab2.example.org/"}
                                   :select    [?name]
                                   :where     [[?s "vocab2:firstName" ?name]]}))
            "returns all values"))
      (testing "querying for the symmetric property"
        (is (= [["Brian"] ["Ben"] ["Francois"]]
               @(fluree/query db '{"@context" {"vocab1" "http://vocab1.example.org/"
                                               "vocab2" "http://vocab2.example.org/"}
                                   :select    [?name]
                                   :where     [[?s "vocab1:givenName" ?name]]}))
            "returns all values"))
      (testing "querying for the transitive properties"
        (is (= [["Brian"] ["Ben"] ["Francois"]]
               @(fluree/query db '{"@context" {"vocab1" "http://vocab1.example.org/"
                                               "vocab3" "http://vocab3.example.fr/"}
                                   :select    [?name]
                                   :where     [[?s "vocab3:prenom" ?name]]}))
            "returns all values"))
      (testing "querying with graph crawl"
        (is (= [{"@id" "ex:brian"
                 "vocab1:givenName" "Brian"
                 "ex:age" 50}
                {"@id" "ex:ben"
                 "vocab2:firstName" "Ben"}
                {"@id" "ex:francois"
                 "vocab3:prenom" "Francois"}]
               @(fluree/query db '{"@context"  {"ex" "http://example.org/ns/"
                                                "vocab1" "http://vocab1.example.org/"
                                                "vocab2" "http://vocab2.example.org/"
                                                "vocab3" "http://vocab3.example.fr/"}
                                   :select    {?s [:*]}
                                   :where     [[?s "vocab2:firstName" ?name]]}))
            "returns all values")))))

(deftest ^:integration subjects-as-predicates
  (testing "predicate iri-cache loookups"
    (let [conn   @(fluree/connect {:method :memory})
          ledger @(fluree/create conn "propertypathstest" {:defaultContext [test-utils/default-str-context {"ex" "http://example.com/"}]})
          db0    (fluree/db ledger)
          db1    @(fluree/stage db0 [{"@id"            "ex:unlabeled-pred"
                                      "ex:description" "created as a subject first"}
                                     {"@id"            "ex:labeled-pred"
                                      "@type"          "rdf:Property"
                                      "ex:description" "created as a subject first, labelled as Property"}])
          db2    @(fluree/stage db1 [{"@id"               "ex:subject-as-predicate"
                                      "ex:labeled-pred"   "labeled"
                                      "ex:unlabeled-pred" "unlabeled"
                                      "ex:new-pred"       {"@id"               "ex:nested"
                                                           "ex:unlabeled-pred" "unlabeled-nested"}}])
          db3   @(fluree/stage db1 [{"@id" "ex:subject-as-predicate"
                                     "ex:labeled-pred" "labeled"
                                     "ex:unlabeled-pred" {"@id" "ex:nested"
                                                          "ex:unlabeled-pred" "unlabeled-nested"}}])]
      (is (= [{"id"                "ex:subject-as-predicate"
               "ex:new-pred"       {"id" "ex:nested"}
               "ex:labeled-pred"   "labeled"
               "ex:unlabeled-pred" "unlabeled"}]
             @(fluree/query db2 {"select" {"?s" ["*"]}
                                 "where"  [["?s" "@id" "ex:subject-as-predicate"]]}))
          "via subgraph selector")

      (is (= [["id"] ["ex:labeled-pred"] ["ex:new-pred"] ["ex:unlabeled-pred"]]
             @(fluree/query db2 {"select" ["?p"]
                                 "where"  [["?s" "@id" "ex:subject-as-predicate"]
                                           ["?s" "?p" "?o"]]}))
          "via variable selector")
      (is (= [["id" {"id"                "ex:subject-as-predicate",
                     "ex:labeled-pred"   "labeled",
                     "ex:new-pred"       {"id" "ex:nested"}
                     "ex:unlabeled-pred" "unlabeled"}]
              ["ex:labeled-pred" {"id"                "ex:subject-as-predicate",
                                  "ex:labeled-pred"   "labeled",
                                  "ex:new-pred"       {"id" "ex:nested"},
                                  "ex:unlabeled-pred" "unlabeled"}]
              ["ex:new-pred" {"id"                "ex:subject-as-predicate",
                              "ex:labeled-pred"   "labeled",
                              "ex:new-pred"       {"id" "ex:nested"},
                              "ex:unlabeled-pred" "unlabeled"}]
              ["ex:unlabeled-pred" {"id"                "ex:subject-as-predicate",
                                    "ex:labeled-pred"   "labeled",
                                    "ex:new-pred"       {"id" "ex:nested"},
                                    "ex:unlabeled-pred" "unlabeled"}]]
             @(fluree/query db2 {"select" ["?p" {"?s" ["*"]}]
                                 "where"  [["?s" "@id" "ex:subject-as-predicate"]
                                           ["?s" "?p" "?o"]]}))
          "via variable+subgraph selector")

      (is (= [{"id" "ex:nested"
               "ex:reversed-pred"
               {"id"                "ex:subject-as-predicate"
                "ex:labeled-pred"   "labeled"
                "ex:new-pred"       {"id" "ex:nested"}
                "ex:unlabeled-pred" "unlabeled"}}]
             @(fluree/query db2 {"@context" ["" {"ex:reversed-pred" {"@reverse" "ex:new-pred"}}]
                                 "select"   {"?s" ["id" {"ex:reversed-pred" ["*"]} ]}
                                 "where"    [["?s" "@id" "ex:nested"]]}))
          "via reverse crawl")
      (is (= [{"id" "ex:nested", "ex:reversed-pred" "ex:subject-as-predicate"}]
             @(fluree/query db2 {"@context" ["" {"ex:reversed-pred" {"@reverse" "ex:unlabeled-pred"}}]
                                 "select"   {"?s" ["id" "ex:reversed-pred"]}
                                 "where"    [["?s" "@id" "ex:nested"]]}))
          "via reverse no subgraph"))))
