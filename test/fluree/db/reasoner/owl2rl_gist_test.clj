(ns fluree.db.reasoner.owl2rl-gist-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all]
            [fluree.db :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.core :as util]
            [fluree.db.util.json :as json]))

;; tests for the owl2rl reasoning using gist ontology

(deftest ^:integration owl-gist-core-can-reason
  (testing "Working with entire ontology, ensure it can be passed to reasoner without exceptions"
    (let [gist-ontology (json/parse (slurp (io/resource "gistCore12.1.0.jsonld")) false)
          conn          (test-utils/create-conn)
          ledger        @(fluree/create conn "reasoner/owl-gist-core-can-reason" nil)
          db-base       @(fluree/stage (fluree/db ledger)
                                       {"@context" {"ex"   "http://example.org/"
                                                    "gist" "https://ontologies.semanticarts.com/gist/"}
                                        "insert"   [{"@id"               "ex:is-account"
                                                     "@type"             "gist:Agreement"
                                                     "gist:hasMagnitude" []}]})]

      (testing "Pass ontology directly to reasoner (not inside db)"
        (let [db-reason @(fluree/reason db-base :owl2rl [gist-ontology])]

          (is (not (util/exception? db-reason))
              "No exceptions should be thrown when reasoning with the entire ontology")))

      (testing "Transact ontology into db then reason"
        (let [db+ontology @(fluree/stage db-base {"insert" gist-ontology})
              db-reason   @(fluree/reason db+ontology :owl2rl)]

          (is (not (util/exception? db-reason))
              "No exceptions should be thrown when reasoning with the entire ontology")))

      (testing "Transact ontology into a different db, then reason"
        (let [ontology-ledger @(fluree/create conn "reasoner/owl-gist-ontology" nil)
              ontology-db     @(fluree/stage (fluree/db ontology-ledger) {"insert" gist-ontology})
              db-reason       @(fluree/reason db-base :owl2rl [ontology-db])]

          (is (not (util/exception? db-reason))
              "No exceptions should be thrown when reasoning with the entire ontology"))))))

#_(deftest ^:integration owl-gist-account
  (testing "gist:Account description described in owl with owl2rl reasoning"
    (let [conn      (test-utils/create-conn)
          ledger    @(fluree/create conn "reasoner/owl-equiv" nil)
          db-base   @(fluree/stage (fluree/db ledger)
                                   {"@context" {"ex"   "http://example.org/"
                                                "gist" "https://ontologies.semanticarts.com/gist/"}
                                    "insert"   [{"@id"               "ex:is-account"
                                                 "@type"             "gist:Agreement"
                                                 "gist:hasMagnitude" []}]})

          db-reason @(fluree/reason db-base :owl2rl
                                    [{"@context"            {"gist" "https://w3id.org/semanticarts/ns/ontology/gist/",
                                                             "rdf"  "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                                                             "owl"  "http://www.w3.org/2002/07/owl#",
                                                             "skos" "http://www.w3.org/2004/02/skos/core#",
                                                             "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                                      "@id"                 "gist:GeoSegment",
                                      "@type"               "owl:Class",
                                      "rdfs:subClassOf"     {"@id" "gist:Place"},
                                      "rdfs:isDefinedBy"    {"@id" "https://w3id.org/semanticarts/ontology/gistCore"},
                                      "owl:equivalentClass" {"@type"              "owl:Class",
                                                             "owl:intersectionOf" {"@list" [{"@type"                    "owl:Restriction",
                                                                                             "owl:onProperty"           {"@id" "gist:comesFromPlace"},
                                                                                             "owl:onClass"              {"@id" "gist:GeoPoint"},
                                                                                             "owl:qualifiedCardinality" {"@type"  "xsd:nonNegativeInteger",
                                                                                                                         "@value" "1"}}
                                                                                            {"@type"                    "owl:Restriction",
                                                                                             "owl:onProperty"           {"@id" "gist:goesToPlace"},
                                                                                             "owl:onClass"              {"@id" "gist:GeoPoint"},
                                                                                             "owl:qualifiedCardinality" {"@type"  "xsd:nonNegativeInteger",
                                                                                                                         "@value" "1"}}]}},
                                      "skos:definition"     {"@type"  "xsd:string",
                                                             "@value" "A single portion of a GeoRegion which has been divided (i.e., segmented)."},
                                      "skos:prefLabel"      {"@type" "xsd:string", "@value" "Geo Segment"}}])]

      (is (= ["ex:is-account"]
             @(fluree/q db-reason {:context {"gist" "https://ontologies.semanticarts.com/gist/"
                                                 "ex"   "http://example.org/"}
                                       :select  "?id"
                                       :where   {"@id"   "?id"
                                                 "@type" "gist:Account"}}))
          "ex:doc-commitment is both @type = gist:Commitment with a gist:hasParty value of @type = gist:Person"))))

(deftest ^:integration owl-gist-agreement
  (testing "gist:Agreement description described in owl with owl2rl reasoning"
    (let [conn      (test-utils/create-conn)
          ledger    @(fluree/create conn "reasoner/owl-equiv" nil)
          db-base   @(fluree/stage (fluree/db ledger)
                                   {"@context" {"ex"   "http://example.org/"
                                                "gist" "https://ontologies.semanticarts.com/gist/"}
                                    "insert"   [{"@id"                "ex:doc-commitment"
                                                 "@type"              "gist:Commitment"
                                                 "gist:hasParty"      {"@id"   "ex:brian"
                                                                       "@type" "gist:Person"}
                                                 "gist:hasDirectPart" [{"@id"   "ex:doc-obligation-1"
                                                                        "@type" "gist:Obligation"}
                                                                       {"@id"   "ex:doc-obligation-2"
                                                                        "@type" "gist:Obligation"}]}]})

          db-reason @(fluree/reason db-base :owl2rl
                                    [{"@context"            {"gist" "https://ontologies.semanticarts.com/gist/"
                                                             "rdf"  "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                                                             "owl"  "http://www.w3.org/2002/07/owl#"
                                                             "skos" "http://www.w3.org/2004/02/skos/core#"
                                                             "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                                      "@id"                 "gist:Agreement"
                                      "@type"               "owl:Class"
                                      "rdfs:isDefinedBy"    {"@id" "https://ontologies.semanticarts.com/o/gistCore"}
                                      "owl:equivalentClass" {"@type"              "owl:Class"
                                                             "owl:intersectionOf" {"@list" [{"@id" "gist:Commitment"}
                                                                                            {"@type"              "owl:Restriction"
                                                                                             "owl:onProperty"     {"@id" "gist:hasParty"}
                                                                                             "owl:someValuesFrom" {"@type"       "owl:Class"
                                                                                                                   "owl:unionOf" {"@list" [{"@id" "gist:Organization"}
                                                                                                                                           {"@id" "gist:Person"}]}}}
                                                                                            ;; TODO owl:minQualifiedCardinality is not supported in owl2rl, but may support it in extended profile in future
                                                                                            #_{"@type"                       "owl:Restriction"
                                                                                               "owl:onProperty"              {"@id" "gist:hasDirectPart"}
                                                                                               "owl:onClass"                 {"@id" "gist:Obligation"}
                                                                                               "owl:minQualifiedCardinality" {"@type"  "xsd:nonNegativeInteger"
                                                                                                                              "@value" "2"}}]}}
                                      "skos:definition"     {"@type"  "xsd:string"
                                                             "@value" "Something which two or more People or Organizations mutually commit to do."}
                                      "skos:prefLabel"      {"@type"  "xsd:string"
                                                             "@value" "Agreement"}}])]
      
      (is (= ["ex:doc-commitment"]
             @(fluree/q db-reason {:context {"gist" "https://ontologies.semanticarts.com/gist/"
                                                 "ex"   "http://example.org/"}
                                       :select  "?id"
                                       :where   {"@id"   "?id"
                                                 "@type" "gist:Agreement"}}))
          "ex:doc-commitment is both @type = gist:Commitment with a gist:hasParty value of @type = gist:Person"))))


(deftest ^:integration owl-gist-baseunit
  (testing "gist:BaseUnit description described in owl with owl2rl reasoning"
    (let [conn      (test-utils/create-conn)
          ledger    @(fluree/create conn "reasoner/owl-equiv" nil)
          db-base   @(fluree/stage (fluree/db ledger)
                                   {"@context" {"ex"   "http://example.org/"
                                                "gist" "https://ontologies.semanticarts.com/gist/"}
                                    "insert"   [{"@id"   "ex:some-data-to-create-db"
                                                 "@type" "ex:IgnoreThis"}]})

          db-reason @(fluree/reason db-base :owl2rl
                                    [{"@context"            {"gist" "https://ontologies.semanticarts.com/gist/"
                                                             "rdf"  "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                                                             "owl"  "http://www.w3.org/2002/07/owl#"
                                                             "skos" "http://www.w3.org/2004/02/skos/core#"
                                                             "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                                      "@id"                 "gist:BaseUnit"
                                      "@type"               "owl:Class"
                                      "rdfs:subClassOf"     {"@id" "gist:SimpleUnitOfMeasure"}
                                      "rdfs:isDefinedBy"    {"@id" "https://ontologies.semanticarts.com/o/gistCore"}
                                      "owl:equivalentClass" {"@type"     "owl:Class"
                                                             "owl:oneOf" {"@list" [{"@id" "gist:_USDollar"}
                                                                                   {"@id" "gist:_ampere"}
                                                                                   {"@id" "gist:_bit"}
                                                                                   {"@id" "gist:_candela"}
                                                                                   {"@id" "gist:_each"}
                                                                                   {"@id" "gist:_kelvin"}
                                                                                   {"@id" "gist:_kilogram"}
                                                                                   {"@id" "gist:_meter"}
                                                                                   {"@id" "gist:_mole"}
                                                                                   {"@id" "gist:_second"}]}}
                                      "skos:definition"     {"@type"  "xsd:string"
                                                             "@value" "A primitive unit that cannot be decomposed into other units. It can be converted from one measurement system to another.  The base units in gist are the seven primitive units from the System Internationale (SI): (meter, second, kilogram, ampere, kelvin, mole, candela), plus three convenience ones: each. bit and usDollar."}
                                      "skos:prefLabel"      {"@type"  "xsd:string"
                                                             "@value" "Base Unit"}}])]
      
      (is (= #{"gist:_USDollar", "gist:_ampere", "gist:_bit", "gist:_candela",
               "gist:_each", "gist:_kelvin", "gist:_kilogram", "gist:_meter",
               "gist:_mole", "gist:_second"}
             (set @(fluree/q db-reason {:context {"gist" "https://ontologies.semanticarts.com/gist/"}
                                            :select  "?id"
                                            :where   {"@id"   "?id"
                                                      "@type" "gist:BaseUnit"}})))
          "all items in the owl:oneOf list should now be of @type gist:BaseUnit"))))



