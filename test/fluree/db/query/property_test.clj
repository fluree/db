(ns fluree.db.query.property-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.api :as fluree]
            [test-with-files.tools :refer [with-tmp-dir] :as twf]))

(deftest ^:integration equivalent-properties-test
  (testing "Equivalent properties"
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "query/equivalent-properties")
          db      (-> ledger
                      (fluree/db)
                      (fluree/stage {"@context" {"vocab1" "http://vocab1.example.org/"
                                                 "vocab2" "http://vocab2.example.org/"
                                                 "vocab3" "http://vocab3.example.fr/"
                                                 "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                                                 "owl"    "http://www.w3.org/2002/07/owl#"}
                                     "insert"   [{"@id"   "vocab1:givenName"
                                                  "@type" "rdf:Property"}
                                                 {"@id"                    "vocab2:firstName"
                                                  "@type"                  "rdf:Property"
                                                  "owl:equivalentProperty" {"@id" "vocab1:givenName"}}
                                                 {"@id"                    "vocab3:prenom"
                                                  "@type"                  "rdf:Property"
                                                  "owl:equivalentProperty" {"@id" "vocab2:firstName"}}]})
                      deref
                      (fluree/stage {"@context" {"vocab1" "http://vocab1.example.org/"
                                                 "vocab2" "http://vocab2.example.org/"
                                                 "vocab3" "http://vocab3.example.fr/"
                                                 "ex"     "http://example.org/ns/"}
                                     "insert"   [{"@id"              "ex:brian"
                                                  "ex:age"           50
                                                  "vocab1:givenName" "Brian"}
                                                 {"@id"              "ex:ben"
                                                  "vocab2:firstName" "Ben"}
                                                 {"@id"           "ex:francois"
                                                  "vocab3:prenom" "Francois"}]})
                      deref)]
      (testing "querying for the property defined to be equivalent"
        (is (= [["Ben"] ["Brian"] ["Francois"]]
               @(fluree/query db '{"@context" {"vocab2" "http://vocab2.example.org/"}
                                   :select    [?name]
                                   :where     {"vocab2:firstName" ?name}}))
            "returns all values"))
      (testing "querying for the symmetric property"
        (is (= [["Ben"] ["Brian"] ["Francois"]]
               @(fluree/query db '{"@context" {"vocab1" "http://vocab1.example.org/"}
                                   :select    [?name]
                                   :where     {"vocab1:givenName" ?name}}))
            "returns all values"))
      (testing "querying for the transitive properties"
        (is (= [["Ben"] ["Brian"] ["Francois"]]
               @(fluree/query db '{"@context" {"vocab3" "http://vocab3.example.fr/"}
                                   :select    [?name]
                                   :where     {"vocab3:prenom" ?name}}))
            "returns all values"))
      (testing "querying with graph crawl"
        (is (= [{"@id"              "ex:ben"
                 "vocab2:firstName" "Ben"}
                {"@id"              "ex:brian"
                 "vocab1:givenName" "Brian"
                 "ex:age"           50}
                {"@id"           "ex:francois"
                 "vocab3:prenom" "Francois"}]
               @(fluree/query db '{"@context" {"ex"     "http://example.org/ns/"
                                               "vocab1" "http://vocab1.example.org/"
                                               "vocab2" "http://vocab2.example.org/"
                                               "vocab3" "http://vocab3.example.fr/"}
                                   :select    {?s [:*]}
                                   :where     {"@id" ?s, "vocab2:firstName" ?name}}))
            "returns all values")))))

(deftest ^:integration rdfs-subpropertyof-test
  (testing "Sub-properties - rdfs:subPropertyOf"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "query/rdfs-subpropertyof")
          db     (-> ledger
                     (fluree/db)
                     (fluree/stage {"@context" {"ex"   "http://example.org/ns/"
                                                "rdf"  "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                                                "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                                    "insert"   [{"@id"                "ex:biologicalMother"
                                                 "@type"              "rdf:Property"
                                                 "rdfs:subPropertyOf" [{"@id" "ex:mother"} {"@id" "ex:biologicalParent"}]}
                                                {"@id"                "ex:biologicalFather"
                                                 "@type"              "rdf:Property"
                                                 "rdfs:subPropertyOf" [{"@id" "ex:father"} {"@id" "ex:biologicalParent"}]}]})
                     deref
                     (fluree/stage {"@context" {"ex"   "http://example.org/ns/"
                                                "rdf"  "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                                                "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                                    "insert"   [{"@id"                "ex:biologicalParent"
                                                 "@type"              "rdf:Property"
                                                 "rdfs:subPropertyOf" {"@id" "ex:parent"}}
                                                {"@id"                "ex:stepParent"
                                                   "@type"              "rdf:Property"
                                                   "rdfs:subPropertyOf" {"@id" "ex:parent"}}
                                                {"@id"                "ex:father"
                                                 "@type"              "rdf:Property"
                                                 "rdfs:subPropertyOf" {"@id" "ex:parent"}}
                                                {"@id"                "ex:stepFather"
                                                   "@type"              "rdf:Property"
                                                   "rdfs:subPropertyOf" {"@id" "ex:stepParent"}}
                                                {"@id"                "ex:stepMother"
                                                   "@type"              "rdf:Property"
                                                   "rdfs:subPropertyOf" {"@id" "ex:stepParent"}}]})
                     deref
                     (fluree/stage {"@context" {"ex"   "http://example.org/ns/"
                                                "rdf"  "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                                                "rdfs" "http://www.w3.org/2000/01/rdf-schema#"
                                                "owl"  "http://www.w3.org/2002/07/owl#"}
                                    "insert"   [{"@id"                    "ex:stepDad"
                                                 "@type"                  "rdf:Property"
                                                 "owl:equivalentProperty" {"@id" "ex:stepFather"}}]})
                     deref
                     (fluree/stage {"@context" {"ex" "http://example.org/ns/"}
                                    "insert"   [{"@id"                 "ex:bob"
                                                 "ex:biologicalMother" {"@id" "ex:alice"}
                                                 "ex:biologicalFather" {"@id" "ex:george"}
                                                 "ex:stepFather"       {"@id" "ex:john"}
                                                 "ex:stepDad"          {"@id" "ex:jerry"}
                                                 "ex:stepMother"       {"@id" "ex:mary"}}]})
                     deref)]

      (testing "querying one-level up in subproperty hierarchy"
        (is (= ["ex:alice" "ex:george"]
               (vec
                 (sort
                   @(fluree/query db '{"@context" {"ex" "http://example.org/ns/"}
                                       :select    ?parent
                                       :where     {"@id"                 "ex:bob"
                                                   "ex:biologicalParent" ?parent}}))))
            "returns all sub properties of ex:parent property"))

      (testing "querying the top level property which includes equivalent property"
        (is (= ["ex:alice" "ex:george" "ex:jerry" "ex:john" "ex:mary"]
               (vec
                 (sort
                   @(fluree/query db '{"@context" {"ex" "http://example.org/ns/"}
                                       :select    ?parent
                                       :where     {"@id"       "ex:bob"
                                                   "ex:parent" ?parent}}))))
            "returns all sub properties of ex:parent property")))))

(deftest ^:integration subjects-as-predicates
  (testing "predicate iri-cache loookups"
    (let [conn    @(fluree/connect-memory)
          ledger  @(fluree/create conn "propertypathstest")
          db0     (fluree/db ledger)
          context [test-utils/default-str-context {"ex" "http://example.com/"}]
          db1     @(fluree/stage db0 {"@context" ["https://ns.flur.ee" context]
                                      "insert"   [{"@id"            "ex:unlabeled-pred"
                                                   "ex:description" "created as a subject first"}
                                                  {"@id"            "ex:labeled-pred"
                                                   "@type"          "rdf:Property"
                                                   "ex:description" "created as a subject first, labelled as Property"}]})
          db2     @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                      "insert"   [{"@id"               "ex:subject-as-predicate"
                                                   "ex:labeled-pred"   "labeled"
                                                   "ex:unlabeled-pred" "unlabeled"
                                                   "ex:new-pred"       {"@id"               "ex:nested"
                                                                        "ex:unlabeled-pred" "unlabeled-nested"}}]})
          db3     @(fluree/stage db1 {"@context" ["https://ns.flur.ee" context]
                                      "insert"   [{"@id"               "ex:subject-as-predicate"
                                                   "ex:labeled-pred"   "labeled"
                                                   "ex:unlabeled-pred" {"@id"               "ex:nested"
                                                                        "ex:unlabeled-pred" "unlabeled-nested"}}]})]
      (is (= [{"id"                "ex:subject-as-predicate"
               "ex:new-pred"       {"id" "ex:nested"}
               "ex:labeled-pred"   "labeled"
               "ex:unlabeled-pred" "unlabeled"}]
             @(fluree/query db2 {"@context" context
                                 "select"   {"ex:subject-as-predicate" ["*"]}}))
          "via subgraph selector")

      (is (= [["ex:labeled-pred"] ["ex:new-pred"] ["ex:unlabeled-pred"]]
             @(fluree/query db2 {"@context" context
                                 "select"   ["?p"]
                                 "where"    {"@id" "ex:subject-as-predicate"
                                             "?p"  "?o"}}))
          "via variable selector")
      (is (= [["ex:labeled-pred" {"id"                "ex:subject-as-predicate",
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
             @(fluree/query db2 {"@context" context
                                 "select"   ["?p" {"ex:subject-as-predicate" ["*"]}]
                                 "where"    {"@id" "ex:subject-as-predicate"
                                             "?p"  "?o"}}))
          "via variable+subgraph selector")

      (is (= [{"id" "ex:nested"
               "ex:reversed-pred"
               {"id"                "ex:subject-as-predicate"
                "ex:labeled-pred"   "labeled"
                "ex:new-pred"       {"id" "ex:nested"}
                "ex:unlabeled-pred" "unlabeled"}}]
             @(fluree/query db2 {"@context" [context {"ex:reversed-pred" {"@reverse" "ex:new-pred"}}]
                                 "select"   {"ex:nested" ["id" {"ex:reversed-pred" ["*"]}]}}))
          "via reverse crawl")
      (is (= [{"id" "ex:nested", "ex:reversed-pred" "ex:subject-as-predicate"}]
             @(fluree/query db3 {"@context" [context {"ex:reversed-pred" {"@reverse" "ex:unlabeled-pred"}}]
                                 "select"   {"ex:nested" ["id" "ex:reversed-pred"]}}))
          "via reverse no subgraph"))))

(deftest ^:integration nested-properties
  (with-tmp-dir storage-path
    (let [conn      @(fluree/connect-file {:storage-path storage-path})
          ledger-id "bugproperty-iri"
          context   [test-utils/default-str-context
                     {"ex"  "http://example.com/"
                      "owl" "http://www.w3.org/2002/07/owl#"}]
          ledger    @(fluree/create conn ledger-id)
          db0       (->> @(fluree/stage (fluree/db ledger) {"@context" ["https://ns.flur.ee" context]
                                                            "insert"   {"ex:new" true}})
                         (fluree/commit! ledger)
                         (deref))


          db1 @(fluree/transact!
                 conn {"ledger"   ledger-id
                       "@context" ["https://ns.flur.ee" context]
                       "insert"
                       [{"@id"                    "ex:givenName"
                         "@type"                  "rdf:Property"
                         "owl:equivalentProperty" {"@id"   "ex:firstName"
                                                   "@type" "rdf:Property"}
                         "ex:preds"               {"@list" [{"@id"   "ex:cool"
                                                             "@type" "rdf:Property"}
                                                            {"@id"   "ex:fool"
                                                             "@type" "rdf:Property"}]}}]})

          db2    @(fluree/transact!
                    conn {"ledger"   ledger-id
                          "@context" ["https://ns.flur.ee" context]
                          "insert"   [{"@id"          "ex:andrew"
                                       "ex:firstName" "Andrew"
                                       "ex:age"       35}
                                      {"@id"          "ex:dan"
                                       "ex:givenName" "Dan"}
                                      {"@id"     "ex:other"
                                       "ex:fool" false
                                       "ex:cool" true}]})
          loaded @(fluree/load conn ledger-id)
          dbl    (fluree/db loaded)]
      (testing "before load"
        (is (= [{"id" "ex:dan", "ex:givenName" "Dan"}
                {"id" "ex:andrew", "ex:firstName" "Andrew", "ex:age" 35}]
               @(fluree/query db2 {"@context" context
                                   "select"   {"?s" ["*"]}
                                   "where"    {"@id" "?s", "ex:givenName" "?o"}})))
        (is (= [{"id" "ex:dan", "ex:givenName" "Dan"}
                {"id" "ex:andrew", "ex:firstName" "Andrew", "ex:age" 35}]
               @(fluree/query db2 {"@context" context
                                   "select"   {"?s" ["*"]}
                                   "where"    {"@id" "?s", "ex:firstName" "?o"}})))
        (is (= [["ex:other" true false]]
               @(fluree/query db2 {"@context" context
                                   "select"   ["?s" "?cool" "?fool"]
                                   "where"    {"@id"     "?s",
                                               "ex:cool" "?cool"
                                               "ex:fool" "?fool"}}))
            "handle list values"))
      (testing "after load"
        (is (= [{"id" "ex:dan", "ex:givenName" "Dan"}
                {"id" "ex:andrew", "ex:firstName" "Andrew", "ex:age" 35}]
               @(fluree/query dbl {"@context" context
                                   "select"   {"?s" ["*"]}
                                   "where"    {"@id" "?s", "ex:givenName" "?o"}})))
        (is (= [{"id" "ex:dan", "ex:givenName" "Dan"}
                {"id" "ex:andrew", "ex:firstName" "Andrew", "ex:age" 35}]
               @(fluree/query dbl {"@context" context
                                   "select"   {"?s" ["*"]}
                                   "where"    {"@id" "?s", "ex:firstName" "?o"}})))

        (is (= [["ex:other" true false]]
               @(fluree/query dbl {"@context" context
                                   "select"   ["?s" "?cool" "?fool"]
                                   "where"    {"@id"     "?s"
                                               "ex:cool" "?cool"
                                               "ex:fool" "?fool"}}))
            "handle list values")))))
