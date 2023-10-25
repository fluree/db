(ns fluree.db.query.property-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]
            [test-with-files.tools :refer [with-tmp-dir] :as twf]))

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
                                 "select"   {"?s" ["id" {"ex:reversed-pred" ["*"]}]}
                                 "where"    [["?s" "@id" "ex:nested"]]}))
          "via reverse crawl")
      (is (= [{"id" "ex:nested", "ex:reversed-pred" "ex:subject-as-predicate"}]
             @(fluree/query db2 {"@context" ["" {"ex:reversed-pred" {"@reverse" "ex:unlabeled-pred"}}]
                                 "select"   {"?s" ["id" "ex:reversed-pred"]}
                                 "where"    [["?s" "@id" "ex:nested"]]}))
          "via reverse no subgraph"))))

(deftest nested-properties
  (with-tmp-dir storage-path
    (let [conn   @(fluree/connect {:method :file, :storage-path storage-path
                                   :defaults {:context test-utils/default-str-context}})
          ledger-id "bugproperty-iri"
          ledger @(fluree/create conn ledger-id
                                 {:defaultContext
                                  ["" {"ex" "http://example.com/"
                                       "owl" "http://www.w3.org/2002/07/owl#"}]})
          db0      (->> @(fluree/stage (fluree/db ledger) {"ex:new" true})
                        (fluree/commit! ledger)
                        (deref))


          db1    @(fluree/transact!
                   conn {"f:ledger" ledger-id
                         "@graph"
                         [{"@id" "ex:givenName"
                           "@type" "rdf:Property"
                           "owl:equivalentProperty" {"@id" "ex:firstName"
                                                     "@type" "rdf:Property"}
                           "ex:preds" {"@list" [{"@id" "ex:cool"
                                                 "@type" "rdf:Property"}
                                                {"@id" "ex:fool"
                                                 "@type" "rdf:Property"}]}}]}
                   nil)

          db2    @(fluree/transact!
                   conn {"f:ledger" ledger-id
                         "@graph"   [{"@id" "ex:andrew"
                                      "ex:firstName" "Andrew"
                                      "ex:age" 35}
                                     {"@id" "ex:dan"
                                      "ex:givenName" "Dan"}
                                     {"@id" "ex:other"
                                      "ex:fool" false
                                      "ex:cool" true}]}
                   nil)
          loaded @(fluree/load conn ledger-id)
          dbl    (fluree/db loaded)]
      (testing "before load"
        (is (= [{"id" "ex:dan", "ex:givenName" "Dan"}
                {"id" "ex:andrew", "ex:firstName" "Andrew", "ex:age" 35}]
               @(fluree/query db2 {"select" {"?s" ["*"]}
                                   "where" [["?s" "ex:givenName" "?o"]]})))
        (is (= [{"id" "ex:dan", "ex:givenName" "Dan"}
                {"id" "ex:andrew", "ex:firstName" "Andrew", "ex:age" 35}]
               @(fluree/query db2 {"select" {"?s" ["*"]}
                                   "where" [["?s" "ex:firstName" "?o"]]})))

        (is (= [["ex:other" true false]]
               @(fluree/query db2 {"select" ["?s" "?cool" "?fool"]
                                   "where" [["?s" "ex:cool" "?cool"]
                                            ["?s" "ex:fool" "?fool"]]}))
            "handle list values"))
      (testing "after load"
        (is (= [{"id" "ex:dan", "ex:givenName" "Dan"}
                {"id" "ex:andrew", "ex:firstName" "Andrew", "ex:age" 35}]
               @(fluree/query dbl {"select" {"?s" ["*"]}
                                   "where"  [["?s" "ex:givenName" "?o"]]})))
        (is (= [{"id" "ex:dan", "ex:givenName" "Dan"}
                {"id" "ex:andrew", "ex:firstName" "Andrew", "ex:age" 35}]
               @(fluree/query dbl {"select" {"?s" ["*"]}
                                   "where"  [["?s" "ex:firstName" "?o"]]})))

        (is (= [["ex:other" true false]]
               @(fluree/query dbl {"select" ["?s" "?cool" "?fool"]
                                   "where"  [["?s" "ex:cool" "?cool"]
                                             ["?s" "ex:fool" "?fool"]]}))
            "handle list values")))))
