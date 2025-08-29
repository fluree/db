(ns fluree.db.reasoner.owl-datalog-edge-cases-test
  "Comprehensive edge case tests for owl-datalog reasoner features
   including unionOf and inverse roles with complex patterns."
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(deftest ^:integration unionOf-edge-cases-test
  (testing "UnionOf edge cases - 3+ branches, nested unions, union with intersection"
    (let [conn (test-utils/create-conn)
          db @(fluree/create conn "reasoner/unionof-edge-cases" nil)

          ;; Test 3+ branches and nested unions
          ontology {"@context" {"ex"   "http://example.org/"
                                "owl"  "http://www.w3.org/2002/07/owl#"
                                "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                    "insert"   [;; 3+ branch union
                                {"@id"                  "ex:MultiTarget"
                                 "@type"                "owl:Class"
                                 "owl:equivalentClass"  {"@type"       "owl:Class"
                                                         "owl:unionOf" {"@list" [{"@id" "ex:Protein"}
                                                                                 {"@id" "ex:Receptor"}
                                                                                 {"@id" "ex:Enzyme"}
                                                                                 {"@id" "ex:Antibody"}]}}}

                                ;; Nested union
                                {"@id"                  "ex:NestedTarget"
                                 "@type"                "owl:Class"
                                 "owl:equivalentClass"  {"@type"       "owl:Class"
                                                         "owl:unionOf" {"@list" [{"@id" "ex:SimpleTarget"}
                                                                                 {"@type"       "owl:Class"
                                                                                  "owl:unionOf" {"@list" [{"@id" "ex:ComplexA"}
                                                                                                          {"@id" "ex:ComplexB"}]}}]}}}

                                ;; Union combined with intersection: (A ∪ B) ∩ ∃R.C
                                {"@id"                 "ex:UnionIntersection"
                                 "@type"               "owl:Class"
                                 "owl:equivalentClass" {"@type"              "owl:Class"
                                                        "owl:intersectionOf" {"@list" [{"@type"       "owl:Class"
                                                                                        "owl:unionOf" {"@list" [{"@id" "ex:DrugTarget"}
                                                                                                                {"@id" "ex:Biomarker"}]}}
                                                                                       {"@type"              "owl:Restriction"
                                                                                        "owl:onProperty"     {"@id" "ex:hasFunction"}
                                                                                        "owl:someValuesFrom" {"@id" "ex:TherapeuticFunction"}}]}}}

                                ;; Union via subclassing
                                {"@id"                  "ex:SubclassUnion"
                                 "@type"                "owl:Class"
                                 "owl:equivalentClass"  {"@type"       "owl:Class"
                                                         "owl:unionOf" {"@list" [{"@id" "ex:BaseClass"}
                                                                                 {"@id" "ex:DerivedClass"}]}}}

                                {"@id"              "ex:DerivedClass"
                                 "@type"            "owl:Class"
                                 "rdfs:subClassOf"  {"@id" "ex:BaseClass"}}

                                ;; Union of restrictions
                                {"@id"                  "ex:UnionOfRestrictions"
                                 "@type"                "owl:Class"
                                 "owl:equivalentClass"  {"@type"              "owl:Restriction"
                                                         "owl:onProperty"     {"@id" "ex:refersTo"}
                                                         "owl:someValuesFrom" {"@type"       "owl:Class"
                                                                               "owl:unionOf" {"@list" [{"@id" "ex:Protein"}
                                                                                                       {"@type"              "owl:Restriction"
                                                                                                        "owl:onProperty"     {"@id" "ex:hasType"}
                                                                                                        "owl:someValuesFrom" {"@id" "ex:ReceptorType"}}]}}}}

                                ;; Define all referenced classes
                                {"@id" "ex:Protein" "@type" "owl:Class"}
                                {"@id" "ex:Receptor" "@type" "owl:Class"}
                                {"@id" "ex:Enzyme" "@type" "owl:Class"}
                                {"@id" "ex:Antibody" "@type" "owl:Class"}
                                {"@id" "ex:SimpleTarget" "@type" "owl:Class"}
                                {"@id" "ex:ComplexA" "@type" "owl:Class"}
                                {"@id" "ex:ComplexB" "@type" "owl:Class"}
                                {"@id" "ex:DrugTarget" "@type" "owl:Class"}
                                {"@id" "ex:Biomarker" "@type" "owl:Class"}
                                {"@id" "ex:TherapeuticFunction" "@type" "owl:Class"}
                                {"@id" "ex:BaseClass" "@type" "owl:Class"}
                                {"@id" "ex:ReceptorType" "@type" "owl:Class"}

                                ;; Properties
                                {"@id" "ex:hasFunction" "@type" "owl:ObjectProperty"}
                                {"@id" "ex:refersTo" "@type" "owl:ObjectProperty"}
                                {"@id" "ex:hasType" "@type" "owl:ObjectProperty"}]}

          db-with-ontology @(fluree/update db ontology)

          ;; Test data
          instance-data {"@context" {"ex" "http://example.org/"}
                         "insert"   [;; Test data for 3+ branch union
                                     {"@id" "ex:prot1" "@type" "ex:Protein"}
                                     {"@id" "ex:rec1" "@type" "ex:Receptor"}
                                     {"@id" "ex:enz1" "@type" "ex:Enzyme"}
                                     {"@id" "ex:ab1" "@type" "ex:Antibody"}

                                   ;; Test data for nested union
                                     {"@id" "ex:simple1" "@type" "ex:SimpleTarget"}
                                     {"@id" "ex:complexA1" "@type" "ex:ComplexA"}
                                     {"@id" "ex:complexB1" "@type" "ex:ComplexB"}

                                   ;; Test data for union with intersection
                                     {"@id"           "ex:drug1"
                                      "@type"         "ex:DrugTarget"
                                      "ex:hasFunction" {"@id" "ex:func1" "@type" "ex:TherapeuticFunction"}}

                                   ;; Test data for subclass union
                                     {"@id" "ex:derived1" "@type" "ex:DerivedClass"}

                                   ;; Test data for union of restrictions
                                     {"@id"        "ex:ref1"
                                      "ex:refersTo" {"@id" "ex:prot2" "@type" "ex:Protein"}}
                                     {"@id"        "ex:ref2"
                                      "ex:refersTo" {"@id"       "ex:thing1"
                                                     "ex:hasType" {"@id"   "ex:recType1"
                                                                   "@type" "ex:ReceptorType"}}}]}

          db-with-data @(fluree/update db-with-ontology instance-data)
          db-reasoned @(fluree/reason db-with-data :owl-datalog)]

      (testing "3+ branch union"
        (doseq [id ["ex:prot1" "ex:rec1" "ex:enz1" "ex:ab1"]]
          (is (contains? (set @(fluree/query db-reasoned
                                             {:context {"ex" "http://example.org/"}
                                              :select  "?type"
                                              :where   {"@id"   id
                                                        "@type" "?type"}}))
                         "ex:MultiTarget")
              (str id " should be inferred as MultiTarget"))))

      (testing "Nested union"
        (doseq [id ["ex:simple1" "ex:complexA1" "ex:complexB1"]]
          (is (contains? (set @(fluree/query db-reasoned
                                             {:context {"ex" "http://example.org/"}
                                              :select  "?type"
                                              :where   {"@id"   id
                                                        "@type" "?type"}}))
                         "ex:NestedTarget")
              (str id " should be inferred as NestedTarget"))))

      (testing "Union with intersection"
        (is (contains? (set @(fluree/query db-reasoned
                                           {:context {"ex" "http://example.org/"}
                                            :select  "?type"
                                            :where   {"@id"   "ex:drug1"
                                                      "@type" "?type"}}))
                       "ex:UnionIntersection")
            "drug1 should be inferred as UnionIntersection"))

      (testing "Subclass union"
        (is (contains? (set @(fluree/query db-reasoned
                                           {:context {"ex" "http://example.org/"}
                                            :select  "?type"
                                            :where   {"@id"   "ex:derived1"
                                                      "@type" "?type"}}))
                       "ex:SubclassUnion")
            "derived1 should be inferred as SubclassUnion"))

      (testing "Union of restrictions"
        (doseq [id ["ex:ref1" "ex:ref2"]]
          (is (contains? (set @(fluree/query db-reasoned
                                             {:context {"ex" "http://example.org/"}
                                              :select  "?type"
                                              :where   {"@id"   id
                                                        "@type" "?type"}}))
                         "ex:UnionOfRestrictions")
              (str id " should be inferred as UnionOfRestrictions"))))

      (testing "Negative test: neither branch gets union class"
        (let [neither-data {"@context" {"ex" "http://example.org/"}
                            "insert"   {"@id" "ex:neither" "@type" "ex:UnrelatedClass"}}
              db2 @(fluree/update db-with-data neither-data)
              db2-reasoned @(fluree/reason db2 :owl-datalog)]
          (is (not (contains? (set @(fluree/query db2-reasoned
                                                  {:context {"ex" "http://example.org/"}
                                                   :select  "?type"
                                                   :where   {"@id"   "ex:neither"
                                                             "@type" "?type"}}))
                              "ex:MultiTarget"))
              "neither should not be inferred as MultiTarget"))))))

(deftest ^:integration inverse-roles-edge-cases-test
  (testing "Inverse role edge cases - deeper chains, combined with intersections, double inverse"
    (let [conn (test-utils/create-conn)
          db @(fluree/create conn "reasoner/inverse-edge-cases" nil)

          ontology {"@context" {"ex"   "http://example.org/"
                                "owl"  "http://www.w3.org/2002/07/owl#"
                                "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                    "insert"   [;; Inverse in deeper chain (length >= 3)
                                {"@id"                  "ex:ChainedClass"
                                 "@type"                "owl:Class"
                                 "owl:equivalentClass"  {"@type"              "owl:Restriction"
                                                         "owl:onProperty"     {"@type"                   "owl:ObjectProperty"
                                                                               "owl:propertyChainAxiom" {"@list" [{"@id" "ex:hasParent"}
                                                                                                                  {"@type"         "owl:ObjectProperty"
                                                                                                                   "owl:inverseOf" {"@id" "ex:hasChild"}}
                                                                                                                  {"@id" "ex:hasSibling"}]}}
                                                         "owl:someValuesFrom" {"@id" "ex:Person"}}}

                                ;; Inverse inside intersection-based equivalentClass
                                {"@id"                 "ex:IntersectionWithInverse"
                                 "@type"               "owl:Class"
                                 "owl:equivalentClass" {"@type"              "owl:Class"
                                                        "owl:intersectionOf" {"@list" [{"@type"              "owl:Restriction"
                                                                                        "owl:onProperty"     {"@type"         "owl:ObjectProperty"
                                                                                                              "owl:inverseOf" {"@id" "ex:manages"}}
                                                                                        "owl:someValuesFrom" {"@id" "ex:Manager"}}
                                                                                       {"@type"              "owl:Restriction"
                                                                                        "owl:onProperty"     {"@id" "ex:worksIn"}
                                                                                        "owl:someValuesFrom" {"@id" "ex:Department"}}]}}}

                                ;; Double inverse normalization test
                                {"@id"                  "ex:DoubleInverseClass"
                                 "@type"                "owl:Class"
                                 "owl:equivalentClass"  {"@type"              "owl:Restriction"
                                                         "owl:onProperty"     {"@type"         "owl:ObjectProperty"
                                                                               "owl:inverseOf" {"@type"         "owl:ObjectProperty"
                                                                                                "owl:inverseOf" {"@id" "ex:originalProp"}}}
                                                         "owl:someValuesFrom" {"@id" "ex:Target"}}}

                                ;; Identity-like chain (R ∘ R⁻) safety check
                                {"@id"                  "ex:IdentityChain"
                                 "@type"                "owl:Class"
                                 "owl:equivalentClass"  {"@type"              "owl:Restriction"
                                                         "owl:onProperty"     {"@type"                   "owl:ObjectProperty"
                                                                               "owl:propertyChainAxiom" {"@list" [{"@id" "ex:relatesTo"}
                                                                                                                  {"@type"         "owl:ObjectProperty"
                                                                                                                   "owl:inverseOf" {"@id" "ex:relatesTo"}}]}}
                                                         "owl:someValuesFrom" {"@id" "ex:Thing"}}}

                                ;; Define properties
                                {"@id" "ex:hasParent" "@type" "owl:ObjectProperty"}
                                {"@id" "ex:hasChild" "@type" "owl:ObjectProperty"
                                 "owl:inverseOf" {"@id" "ex:hasParent"}}
                                {"@id" "ex:hasSibling" "@type" "owl:ObjectProperty"}
                                {"@id" "ex:manages" "@type" "owl:ObjectProperty"}
                                {"@id" "ex:worksIn" "@type" "owl:ObjectProperty"}
                                {"@id" "ex:originalProp" "@type" "owl:ObjectProperty"}
                                {"@id" "ex:relatesTo" "@type" "owl:ObjectProperty"}

                                ;; Define classes
                                {"@id" "ex:Person" "@type" "owl:Class"}
                                {"@id" "ex:Manager" "@type" "owl:Class"}
                                {"@id" "ex:Department" "@type" "owl:Class"}
                                {"@id" "ex:Target" "@type" "owl:Class"}
                                {"@id" "ex:Thing" "@type" "owl:Class"}]}

          db-with-ontology @(fluree/update db ontology)

          instance-data {"@context" {"ex" "http://example.org/"}
                         "insert"   [;; Test data for inverse in chain
                                     ;; The chain: hasParent ∘ hasChild⁻ ∘ hasSibling
                                     ;; Pattern: X hasParent Y, Z hasChild Y, Z hasSibling W (W is Person)
                                     {"@id" "ex:alice" "@type" "ex:Person"
                                      "ex:hasParent" {"@id" "ex:bob" "@type" "ex:Person"}}
                                     ;; carol has bob as child (not bob has carol as child)
                                     {"@id" "ex:carol" "@type" "ex:Person"
                                      "ex:hasChild" {"@id" "ex:bob"}}
                                     {"@id" "ex:carol"
                                      "ex:hasSibling" {"@id" "ex:dave" "@type" "ex:Person"}}

                                   ;; Test data for intersection with inverse
                                     {"@id" "ex:emp1" "@type" "ex:Person"
                                      "ex:worksIn" {"@id" "ex:dept1" "@type" "ex:Department"}}
                                     {"@id" "ex:mgr1" "@type" "ex:Manager"
                                      "ex:manages" {"@id" "ex:emp1"}}

                                   ;; Test data for double inverse (should normalize to original)
                                     {"@id" "ex:source1"
                                      "ex:originalProp" {"@id" "ex:target1" "@type" "ex:Target"}}

                                   ;; Test data for identity chain
                                     {"@id" "ex:thing1" "@type" "ex:Thing"
                                      "ex:relatesTo" {"@id" "ex:thing2" "@type" "ex:Thing"}}

                                   ;; Negative test case - broken chain
                                     {"@id" "ex:broken1" "@type" "ex:Person"
                                      "ex:hasParent" {"@id" "ex:broken2" "@type" "ex:Person"}}
                                   ;; Missing middle link - broken2 has no hasChild to broken3
                                     {"@id" "ex:broken3" "@type" "ex:Person"
                                      "ex:hasSibling" {"@id" "ex:broken4" "@type" "ex:Person"}}]}

          db-with-data @(fluree/update db-with-ontology instance-data)
          db-reasoned @(fluree/reason db-with-data :owl-datalog)]

      (testing "Inverse in deeper chain"
        (is (contains? (set @(fluree/query db-reasoned
                                           {:context {"ex" "http://example.org/"}
                                            :select  "?type"
                                            :where   {"@id"   "ex:alice"
                                                      "@type" "?type"}}))
                       "ex:ChainedClass")
            "alice should be inferred as ChainedClass through inverse in chain"))

      (testing "Intersection with inverse"
        (is (contains? (set @(fluree/query db-reasoned
                                           {:context {"ex" "http://example.org/"}
                                            :select  "?type"
                                            :where   {"@id"   "ex:emp1"
                                                      "@type" "?type"}}))
                       "ex:IntersectionWithInverse")
            "emp1 should be inferred as IntersectionWithInverse"))

      (testing "Double inverse normalization"
        (is (contains? (set @(fluree/query db-reasoned
                                           {:context {"ex" "http://example.org/"}
                                            :select  "?type"
                                            :where   {"@id"   "ex:source1"
                                                      "@type" "?type"}}))
                       "ex:DoubleInverseClass")
            "source1 should be inferred as DoubleInverseClass (double inverse normalizes)"))

      (testing "Identity-like chain doesn't explode"
        (is (contains? (set @(fluree/query db-reasoned
                                           {:context {"ex" "http://example.org/"}
                                            :select  "?type"
                                            :where   {"@id"   "ex:thing1"
                                                      "@type" "?type"}}))
                       "ex:IdentityChain")
            "thing1 should be inferred as IdentityChain without infinite loop"))

      (testing "Broken chain negative test"
        (is (not (contains? (set @(fluree/query db-reasoned
                                                {:context {"ex" "http://example.org/"}
                                                 :select  "?type"
                                                 :where   {"@id"   "ex:broken1"
                                                           "@type" "?type"}}))
                            "ex:ChainedClass"))
            "broken1 should NOT be inferred as ChainedClass due to broken chain"))

      (testing "Forward and inverse facts lead to same inference"
        (let [test-data {"@context" {"ex" "http://example.org/"}
                         "insert"   [{"@id" "ex:test1" "@type" "ex:Person"
                                      "ex:hasParent" {"@id" "ex:test2"}}
                                     {"@id" "ex:test3" "@type" "ex:Person"}
                                     {"@id" "ex:test2"
                                      "ex:hasChild" {"@id" "ex:test3"}}]}
              db2 @(fluree/update db-with-data test-data)
              db2-reasoned @(fluree/reason db2 :owl-datalog)]
          (is (= ["ex:test2"]
                 @(fluree/query db2-reasoned
                                {:context {"ex" "http://example.org/"}
                                 :select  "?parent"
                                 :where   {"@id"         "ex:test1"
                                           "ex:hasParent" "?parent"}}))
              "Forward relation should be preserved")
          (is (or (= "ex:test1"
                     @(fluree/query db2-reasoned
                                    {:context {"ex" "http://example.org/"}
                                     :select  "?child"
                                     :where   {"@id"        "ex:test2"
                                               "ex:hasChild" "?child"}}))
                  (some #(= % "ex:test1")
                        @(fluree/query db2-reasoned
                                       {:context {"ex" "http://example.org/"}
                                        :select  "?child"
                                        :where   {"@id"        "ex:test2"
                                                  "ex:hasChild" "?child"}})))
              "Inverse relation should be inferred"))))))

(deftest ^:integration negative-pathology-checks-test
  (testing "Negative tests - ensure no inference when conditions aren't fully met"
    (let [conn (test-utils/create-conn)
          db @(fluree/create conn "reasoner/negative-checks" nil)

          ontology {"@context" {"ex"   "http://example.org/"
                                "owl"  "http://www.w3.org/2002/07/owl#"
                                "rdfs" "http://www.w3.org/2000/01/rdf-schema#"}
                    "insert"   [;; Conjunction where only part holds
                                {"@id"                 "ex:ConjunctiveClass"
                                 "@type"               "owl:Class"
                                 "owl:equivalentClass" {"@type"              "owl:Class"
                                                        "owl:intersectionOf" {"@list" [{"@type"              "owl:Restriction"
                                                                                        "owl:onProperty"     {"@id" "ex:hasA"}
                                                                                        "owl:someValuesFrom" {"@id" "ex:ClassA"}}
                                                                                       {"@type"              "owl:Restriction"
                                                                                        "owl:onProperty"     {"@id" "ex:hasB"}
                                                                                        "owl:someValuesFrom" {"@id" "ex:ClassB"}}
                                                                                       {"@type"              "owl:Restriction"
                                                                                        "owl:onProperty"     {"@id" "ex:hasC"}
                                                                                        "owl:someValuesFrom" {"@id" "ex:ClassC"}}]}}}

                                ;; Properties
                                {"@id" "ex:hasA" "@type" "owl:ObjectProperty"}
                                {"@id" "ex:hasB" "@type" "owl:ObjectProperty"}
                                {"@id" "ex:hasC" "@type" "owl:ObjectProperty"}

                                ;; Classes
                                {"@id" "ex:ClassA" "@type" "owl:Class"}
                                {"@id" "ex:ClassB" "@type" "owl:Class"}
                                {"@id" "ex:ClassC" "@type" "owl:Class"}]}

          db-with-ontology @(fluree/update db ontology)

          instance-data {"@context" {"ex" "http://example.org/"}
                         "insert"   [;; Test data - only 2 of 3 conditions met
                                     {"@id" "ex:partial1"
                                      "ex:hasA" {"@id" "ex:a1" "@type" "ex:ClassA"}
                                      "ex:hasB" {"@id" "ex:b1" "@type" "ex:ClassB"}}
                                   ;; Missing hasC

                                   ;; Test data - all 3 conditions met
                                     {"@id" "ex:complete1"
                                      "ex:hasA" {"@id" "ex:a2" "@type" "ex:ClassA"}
                                      "ex:hasB" {"@id" "ex:b2" "@type" "ex:ClassB"}
                                      "ex:hasC" {"@id" "ex:c2" "@type" "ex:ClassC"}}]}

          db-with-data @(fluree/update db-with-ontology instance-data)
          db-reasoned @(fluree/reason db-with-data :owl-datalog)]

      (testing "Partial conditions shouldn't trigger inference"
        (is (not (contains? (set @(fluree/query db-reasoned
                                                {:context {"ex" "http://example.org/"}
                                                 :select  "?type"
                                                 :where   {"@id"   "ex:partial1"
                                                           "@type" "?type"}}))
                            "ex:ConjunctiveClass"))
            "partial1 should NOT be inferred as ConjunctiveClass (missing condition)"))

      (testing "All conditions should trigger inference"
        (is (contains? (set @(fluree/query db-reasoned
                                           {:context {"ex" "http://example.org/"}
                                            :select  "?type"
                                            :where   {"@id"   "ex:complete1"
                                                      "@type" "?type"}}))
                       "ex:ConjunctiveClass")
            "complete1 should be inferred as ConjunctiveClass (all conditions met)")))))