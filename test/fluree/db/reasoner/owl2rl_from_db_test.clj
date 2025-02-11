(ns fluree.db.reasoner.owl2rl-from-db-test
  (:require [clojure.test :refer :all]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]))
(deftest ^:integration owl-stored-in-host-db
  (testing "Testing nested owl restrictions coming from same db as data"
    (let [conn          (test-utils/create-conn)
          ledger        @(fluree/create conn "reasoner/owl-in-db" nil)
          db-base       @(fluree/stage (fluree/db ledger)
                                       {"@context" {"ex" "http://example.org/"}
                                        "insert"   [{"@id"   "ex:winery1"
                                                     "@type" "ex:Winery"}
                                                    {"@id"   "ex:winery2"
                                                     "@type" "ex:Winery"}
                                                    {"@id"   "ex:textile-factory"
                                                     "@type" "ex:TextileFactory"}
                                                    {"@id"         "ex:a-wine-1"
                                                     "@type"       "ex:Wine"
                                                     "ex:hasMaker" "ex:winery1"}
                                                    {"@id"         "ex:a-wine-2"
                                                     "@type"       "ex:Wine"
                                                     "ex:hasMaker" "ex:winery2"}
                                                    {"@id"         "ex:maybe-a-wine"
                                                     "@type"       "ex:Wine"
                                                     "ex:hasMaker" "ex:textile-factory"}]})
          ;; store OWL rules in same db as data
          db-with-rules @(fluree/stage db-base
                                       {"insert" {"@context"            {"owl" "http://www.w3.org/2002/07/owl#"
                                                                         "ex"  "http://example.org/"}
                                                  "@id"                 "ex:Wine"
                                                  "@type"               ["owl:Class"]
                                                  "owl:equivalentClass" [{"@type"              "owl:Restriction"
                                                                          "owl:onProperty"     {"@id" "ex:hasMaker"}
                                                                          "owl:someValuesFrom" {"@type"     "owl:Class"
                                                                                                "owl:oneOf" {"@list" [{"@id" "ex:winery2"}
                                                                                                                      {"@id" "ex:winery1"}]}}}]}})
          ;; reasoner not supplied with rules, will look in internal db for rules (inserted above)
          db-some-val   @(fluree/reason db-with-rules :owl2rl)]
      (is (= (list "ex:a-wine-1" "ex:a-wine-2" "ex:maybe-a-wine")
             (sort
              @(fluree/query db-some-val
                             {:context {"ex" "http://example.org/"}
                              :select  "?s"
                              :where   {"@id"   "?s"
                                        "@type" "ex:Wine"}})))
          "hasMaker ref can no be of either ex:Winery or ex:TextileFactory to qualify as an ex:Wine"))))
