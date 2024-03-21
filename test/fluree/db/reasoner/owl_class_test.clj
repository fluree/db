(ns fluree.db.reasoner.owl-class-test
  (:require [clojure.test :refer :all]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

;; tests for OWL class reasoning rules

(deftest ^:integration owl-equivalent-class
  (testing "owl:equivalentClass test"
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "reasoner/owl-equiv" nil)
          db-base @(fluree/stage (fluree/db ledger)
                                 {"@context" {"ex" "http://example.org/"}
                                  "insert"   [{"@id"     "ex:brian"
                                               "@type"   "ex:Person"
                                               "ex:name" "Brian"}
                                              {"@id"     "ex:laura"
                                               "@type"   "ex:Human"
                                               "ex:name" "Laura"}
                                              {"@id"     "ex:alice"
                                               "@type"   "ex:HumanBeing"
                                               "ex:name" "Alice"}]})]

      (testing "Testing single owl:equivalentClass declaration"
        (let [db-equiv   @(fluree/reason db-base :owl2rl
                                         [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                  "ex"  "http://example.org/"}
                                           "@id"                 "ex:Human",
                                           "@type"               ["owl:Class"],
                                           "owl:equivalentClass" {"@id" "ex:Person"}}])
              qry-person @(fluree/query db-equiv
                                        {:context {"ex"  "http://example.org/"
                                                   "owl" "http://www.w3.org/2002/07/owl#"}
                                         :select  "?s"
                                         :where   {"@id"   "?s",
                                                   "@type" "ex:Person"}})
              qry-human  @(fluree/query db-equiv
                                        {:context {"ex"  "http://example.org/"
                                                   "owl" "http://www.w3.org/2002/07/owl#"}
                                         :select  "?s"
                                         :where   {"@id"   "?s",
                                                   "@type" "ex:Human"}})]
          (is (= #{"ex:brian" "ex:laura"}
                 (set qry-person))
              "both ex:brian and ex:laura should be of type ex:Person")

          (is (= #{"ex:brian" "ex:laura"}
                 (set qry-human))
              "both ex:brian and ex:laura should be of type ex:Human")))

      (testing "Testing multiple owl:equivalentClass declaration"
        (let [db-equiv   @(fluree/reason db-base :owl2rl
                                         [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                  "ex"  "http://example.org/"}
                                           "@id"                 "ex:Person",
                                           "@type"               ["owl:Class"],
                                           "owl:equivalentClass" [{"@id" "ex:Human"} {"@id" "ex:HumanBeing"}]}])
              qry-person @(fluree/query db-equiv
                                        {:context {"ex" "http://example.org/"}
                                         :select  "?s"
                                         :where   {"@id"   "?s",
                                                   "@type" "ex:Person"}})
              qry-human  @(fluree/query db-equiv
                                        {:context {"ex" "http://example.org/"}
                                         :select  "?s"
                                         :where   {"@id"   "?s",
                                                   "@type" "ex:Human"}})
              qry-humanb @(fluree/query db-equiv
                                        {:context {"ex" "http://example.org/"}
                                         :select  "?s"
                                         :where   {"@id"   "?s",
                                                   "@type" "ex:HumanBeing"}})]
          (is (= #{"ex:brian" "ex:laura" "ex:alice"}
                 (set qry-person))
              "ex:brian, ex:laura and ex:alice should be of type ex:Person")

          (is (= #{"ex:brian" "ex:laura" "ex:alice"}
                 (set qry-human))
              "ex:brian, ex:laura and ex:alice should be of type ex:Human")

          (is (= #{"ex:brian" "ex:laura" "ex:alice"}
                 (set qry-humanb))
              "ex:brian, ex:laura and ex:alice should be of type ex:HumanBeing"))))))

(deftest ^:integration owl-restriction-has-value
  (testing "owl:Restriction hasValue test - cls-hv1, cls-hv2"
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "reasoner/owl-cls-hv" nil)
          db-base @(fluree/stage (fluree/db ledger)
                                 {"@context" {"ex" "http://example.org/"}
                                  "insert"   [{"@id"           "ex:alice"
                                               "@type"         "ex:Person"
                                               "ex:hasAccount" true
                                               "ex:age"        21}
                                              {"@id"           "ex:bob"
                                               "@type"         "ex:Person"
                                               "ex:hasAccount" false
                                               "ex:age"        12}
                                              {"@id"   "ex:susan"
                                               "@type" ["ex:Person" "ex:Customer"]}]})]

      (testing "Testing single owl:Restriction for a property value"
        (let [db-equiv     @(fluree/reason db-base :owl2rl
                                           [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                    "ex"  "http://example.org/"}
                                             "@id"                 "ex:Customer",
                                             "@type"               ["owl:Class"],
                                             "owl:equivalentClass" [{"@type"          "owl:Restriction"
                                                                     "owl:onProperty" {"@id" "ex:hasAccount"}
                                                                     "owl:hasValue"   true}]}])
              qry-customer @(fluree/query db-equiv
                                          {:context {"ex" "http://example.org/"}
                                           :select  "?s"
                                           :where   {"@id"   "?s",
                                                     "@type" "ex:Customer"}})
              qry-has-acct @(fluree/query db-equiv
                                          {:context {"ex" "http://example.org/"}
                                           :select  "?s"
                                           :where   {"@id"           "?s",
                                                     "ex:hasAccount" true}})]
          (is (= #{"ex:alice" "ex:susan"}
                 (set qry-customer))
              "ex:alice has property ex:hasAccount with value true, ex:susan was explicitly declared as ex:Customer")

          (is (= #{"ex:alice" "ex:susan"}
                 (set qry-has-acct))
              "ex:susan should have ex:hasAccount: true inferred based on declared class.")))

      (testing "Testing single owl:Restriction where property value is not an IRI"
        (let [db-equiv     @(fluree/reason db-base :owl2rl
                                           [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                    "ex"  "http://example.org/"}
                                             "@id"                 "ex:Customer",
                                             "@type"               ["owl:Class"],
                                             "owl:equivalentClass" [{"@type"          "owl:Restriction"
                                                                     "owl:onProperty" "ex:hasAccount" ;; OOPS! should be an IRI
                                                                     "owl:hasValue"   true}]}])
              qry-customer @(fluree/query db-equiv
                                          {:context {"ex" "http://example.org/"}
                                           :select  "?s"
                                           :where   {"@id"   "?s",
                                                     "@type" "ex:Customer"}})]
          (is (= ["ex:susan"]
                 qry-customer)
              "Rule warning should be logged and no inference should be made."))))))


(deftest ^:integration owl-restriction-some-values-from,
  (testing "owl:Restriction owl:someValuesFrom test - cls-svf1, cls-svf2"
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "reasoner/owl-cls-svf" nil)
          db-base @(fluree/stage (fluree/db ledger)
                                 {"@context" {"ex" "http://example.org/"}
                                  "insert"   [{"@id"   "ex:winery1"
                                               "@type" "ex:Winery"}
                                              {"@id"   "ex:textile-company"
                                               "@type" "ex:TextileFactory"}
                                              {"@id"   "ex:winery2"
                                               "@type" "ex:Winery"}
                                              {"@id"         "ex:maybe-a-wine"
                                               "@type"       "ex:Product"
                                               "ex:hasMaker" [{"@id" "ex:winery1"}
                                                              {"@id" "ex:textile-company"}]}
                                              {"@id"         "ex:a-wine-1"
                                               "@type"       "ex:Product"
                                               "ex:hasMaker" [{"@id" "ex:winery1"}
                                                              {"@id" "ex:winery2"}]}
                                              {"@id"         "ex:a-wine-2"
                                               "@type"       "ex:Product"
                                               "ex:hasMaker" {"@id" "ex:winery2"}}
                                              {"@id"         "ex:not-a-wine-1"
                                               "@type"       "ex:Product"
                                               "ex:hasMaker" {"@id" "ex:textile-company"}}]})]
      (testing "Testing single owl:Restriction someValuesFrom for a property value"
        (let [db-some-val @(fluree/reason db-base :owl2rl
                                          [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                   "ex"  "http://example.org/"}
                                            "@id"                 "ex:Wine",
                                            "@type"               ["owl:Class"],
                                            "owl:equivalentClass" [{"@type"              "owl:Restriction"
                                                                    "owl:onProperty"     {"@id" "ex:hasMaker"}
                                                                    "owl:someValuesFrom" {"@id" "ex:Winery"}}]}])
              qry-wines   @(fluree/query db-some-val
                                         {:context {"ex" "http://example.org/"}
                                          :select  "?s"
                                          :where   {"@id"   "?s",
                                                    "@type" "ex:Wine"}})]
          (is (= #{"ex:maybe-a-wine" "ex:a-wine-1" "ex:a-wine-2"}
                 (set qry-wines))
              "only one hasMaker must be a winery to qualify as an ex:Wine")))

      (testing "Testing single owl:Restriction allValuesFrom for a property value"
        (let [db-all-val @(fluree/reason db-base :owl2rl
                                         [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                  "ex"  "http://example.org/"}
                                           "@id"                 "ex:Wine",
                                           "@type"               ["owl:Class"],
                                           "owl:equivalentClass" [{"@type"             "owl:Restriction"
                                                                   "owl:onProperty"    {"@id" "ex:hasMaker"}
                                                                   "owl:allValuesFrom" {"@id" "ex:Winery"}}]}])
              qry-winery @(fluree/query db-all-val
                                        {:context {"ex" "http://example.org/"}
                                         :select  "?s"
                                         :where   {"@id"   "?s",
                                                   "@type" "ex:Winery"}})]
          (is (= #{"ex:winery1" "ex:textile-company" "ex:winery2"}
                 (set qry-winery))
              "because every hasMaker must be a winery, they are all wineries"))))))

;; TODO - re-enable once filter function bug is fixed
#_(deftest ^:integration owl-max-cardinality
    (testing "owl:maxCardinality test - rule cls-maxc2 (cls-maxc1 is 'false' and ignored)"
      (let [conn    (test-utils/create-conn)
            ledger  @(fluree/create conn "reasoner/owl-max-card" nil)
            db-base @(fluree/stage (fluree/db ledger)
                                   {"@context" {"ex" "http://example.org/"}
                                    "insert"   [{"@id"       "ex:brian"
                                                 "@type"     "ex:Person"
                                                 "ex:mother" [{"@id" "ex:carol"}
                                                              {"@id" "ex:carol2"}
                                                              {"@id" "ex:carol3"}]}]})]

        (testing "Testing owl:maxCardinality=1 declaration (rule cls-maxc2)"
          (let [db-equiv   @(fluree/reason db-base :owl2rl
                                           [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                    "ex"  "http://example.org/"}
                                             "@id"                 "ex:Person"
                                             "owl:equivalentClass" [{"@type"              ["owl:Restriction"]
                                                                     "owl:onProperty"     {"@id" "ex:mother"}
                                                                     "owl:maxCardinality" 1}]}])
                qry-carol  @(fluree/query db-equiv
                                          {:context {"ex"  "http://example.org/"
                                                     "owl" "http://www.w3.org/2002/07/owl#"}
                                           :select  "?s"
                                           :where   {"@id"        "ex:carol",
                                                     "owl:sameAs" "?same"}})
                qry-carol2 @(fluree/query db-equiv
                                          {:context {"ex"  "http://example.org/"
                                                     "owl" "http://www.w3.org/2002/07/owl#"}
                                           :select  "?s"
                                           :where   {"@id"        "ex:carol2",
                                                     "owl:sameAs" "?same"}})
                qry-carol3 @(fluree/query db-equiv
                                          {:context {"ex"  "http://example.org/"
                                                     "owl" "http://www.w3.org/2002/07/owl#"}
                                           :select  "?s"
                                           :where   {"@id"        "ex:carol3",
                                                     "owl:sameAs" "?same"}})]
            (is (= #{"ex:carol2" "ex:carol3"}
                   (set qry-carol))
                "ex:carol2 and ex:carol3 should be deemed the same as ex:carol")

            (is (= #{"ex:carol" "ex:carol3"}
                   (set qry-carol2))
                "ex:carol and ex:carol3 should be deemed the same as ex:carol2")

            (is (= #{"ex:carol" "ex:carol2"}
                   (set qry-carol3))
                "ex:carol and ex:carol2 should be deemed the same as ex:carol3")))

        (testing "Testing owl:maxCardinality > 1"
          (let [db-42     @(fluree/reason db-base :owl2rl
                                          [{"@context"           {"owl" "http://www.w3.org/2002/07/owl#"
                                                                  "ex"  "http://example.org/"}
                                            "@id"                "ex:Human"
                                            "@type"              ["owl:Class"]
                                            "owl:onProperty"     {"@id" "ex:mother"}
                                            "owl:maxCardinality" 42}])
                qry-carol @(fluree/query db-42
                                         {:context {"ex"  "http://example.org/"
                                                    "owl" "http://www.w3.org/2002/07/owl#"}
                                          :select  "?s"
                                          :where   {"@id"        "ex:carol",
                                                    "owl:sameAs" "?same"}})]
            (is (= []
                   qry-carol)
                "with maxCardinality > 1, no inferences can be made"))))))

(deftest ^:integration owl-max-qual-cardinality
  (testing "owl:maxQualifiedCardinality test - rules cls-maxqc3, cls-maxqc4"
    (let [conn    (test-utils/create-conn)
          ledger  @(fluree/create conn "reasoner/owl-max-card" nil)
          db-base @(fluree/stage (fluree/db ledger)
                                 {"@context" {"ex" "http://example.org/"}
                                  "insert"   [{"@id"       "ex:brian"
                                               "@type"     "ex:Person"
                                               "ex:mother" [{"@id"   "ex:carol"
                                                             "@type" "ex:Parent"}
                                                            {"@id"   "ex:carol2"
                                                             "@type" "ex:Parent"}
                                                            {"@id"   "ex:carol-not"
                                                             "@type" "ex:NotParent"}]}]})]

      (testing "Testing owl:maxQualifiedCardinality=1 declaration (rule cls-maxqc3)"
        (let [db-equiv      @(fluree/reason db-base :owl2rl
                                            [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                     "ex"  "http://example.org/"}
                                              "@id"                 "ex:Parent"
                                              "@type"               ["owl:Class"]
                                              "owl:equivalentClass" [{"owl:onProperty"              {"@id" "ex:mother"}
                                                                      "owl:onClass"                 {"@id" "ex:Parent"}
                                                                      "owl:maxQualifiedCardinality" 1}]}])
              qry-carol     @(fluree/query db-equiv
                                           {:context {"ex"  "http://example.org/"
                                                      "owl" "http://www.w3.org/2002/07/owl#"}
                                            :select  "?s"
                                            :where   {"@id"        "ex:carol",
                                                      "owl:sameAs" "?same"}})
              qry-carol2    @(fluree/query db-equiv
                                           {:context {"ex"  "http://example.org/"
                                                      "owl" "http://www.w3.org/2002/07/owl#"}
                                            :select  "?s"
                                            :where   {"@id"        "ex:carol2",
                                                      "owl:sameAs" "?same"}})
              qry-carol-not @(fluree/query db-equiv
                                           {:context {"ex"  "http://example.org/"
                                                      "owl" "http://www.w3.org/2002/07/owl#"}
                                            :select  "?s"
                                            :where   {"@id"        "ex:carol-not",
                                                      "owl:sameAs" "?same"}})]
          (is (= ["ex:carol2"]
                 qry-carol)
              "ex:carol and ex:carol2 should be sameAs because their classes are same as owl:onClass restriction")

          (is (= ["ex:carol"]
                 qry-carol2)
              "ex:carol and ex:carol2 should be sameAs because their classes are same as owl:onClass restriction")

          (is (= []
                 qry-carol-not)
              "ex:carol-not is a different class and therefore not equivalent to anyone else")))

      (testing "Testing owl:maxQualifiedCardinality=1 declaration where owl:onClass = owl:Thing (rule cls-maxqc4)"
        (let [db-equiv      @(fluree/reason db-base :owl2rl
                                            [{"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                     "ex"  "http://example.org/"}
                                              "@id"                 "ex:Parent"
                                              "@type"               ["owl:Class"]
                                              "owl:equivalentClass" [{"owl:onProperty"              {"@id" "ex:mother"}
                                                                      "owl:onClass"                 {"@id" "owl:Thing"}
                                                                      "owl:maxQualifiedCardinality" 1}]}])
              qry-carol     @(fluree/query db-equiv
                                           {:context {"ex"  "http://example.org/"
                                                      "owl" "http://www.w3.org/2002/07/owl#"}
                                            :select  "?s"
                                            :where   {"@id"        "ex:carol",
                                                      "owl:sameAs" "?same"}})
              qry-carol2    @(fluree/query db-equiv
                                           {:context {"ex"  "http://example.org/"
                                                      "owl" "http://www.w3.org/2002/07/owl#"}
                                            :select  "?s"
                                            :where   {"@id"        "ex:carol2",
                                                      "owl:sameAs" "?same"}})
              qry-carol-not @(fluree/query db-equiv
                                           {:context {"ex"  "http://example.org/"
                                                      "owl" "http://www.w3.org/2002/07/owl#"}
                                            :select  "?s"
                                            :where   {"@id"        "ex:carol-not",
                                                      "owl:sameAs" "?same"}})]
          (is (= #{"ex:carol" "ex:carol2" "ex:carol-not"}
                 (set qry-carol))
              "with owl:onClass=owl:Thing, class doesn't matter so all should be sameAs")

          (is (= #{"ex:carol" "ex:carol2" "ex:carol-not"}
                 (set qry-carol2))
              "with owl:onClass=owl:Thing, class doesn't matter so all should be sameAs")

          (is (= #{"ex:carol" "ex:carol2" "ex:carol-not"}
                 (set qry-carol-not))
              "with owl:onClass=owl:Thing, class doesn't matter so all should be sameAs")))

      (testing "Testing owl:maxQualifiedCardinality > 1"
        (let [db-42     @(fluree/reason db-base :owl2rl
                                        [{"@context"                    {"owl" "http://www.w3.org/2002/07/owl#"
                                                                         "ex"  "http://example.org/"}
                                          "@id"                         "ex:Human"
                                          "@type"                       ["owl:Class"]
                                          "owl:onProperty"              {"@id" "ex:mother"}
                                          "owl:onClass"                 {"@id" "owl:Thing"}
                                          "owl:maxQualifiedCardinality" 42}])
              qry-carol @(fluree/query db-42
                                       {:context {"ex"  "http://example.org/"
                                                  "owl" "http://www.w3.org/2002/07/owl#"}
                                        :select  "?s"
                                        :where   {"@id"        "ex:carol",
                                                  "owl:sameAs" "?same"}})]
          (is (= []
                 qry-carol)
              "with maxQualifiedCardinality > 1, no inferences can be made"))))))




