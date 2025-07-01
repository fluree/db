(ns fluree.db.query.datatype-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.core :refer [exception?]]))

(def default-context
  {:id     "@id"
   :type   "@type"
   :schema "http://schema.org/"
   :ex     "http://example.org/ns/"
   :rdfs   "http://www.w3.org/2000/01/rdf-schema#"
   :rdf    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"})

(deftest ^:integration mixed-datatypes-test
  (let [conn   (test-utils/create-conn)
        ledger @(fluree/create conn "ledger/datatype")]
    (testing "Querying predicates with mixed datatypes"
      (let [mixed-db @(fluree/stage (fluree/db ledger)
                                    {"insert"
                                     [{:context     default-context
                                       :id          :ex/coco
                                       :type        :schema/Person
                                       :schema/name "Coco"}
                                      {:context     default-context
                                       :id          :ex/halie
                                       :type        :schema/Person
                                       :schema/name "Halie"}
                                      {:context     default-context
                                       :id          :ex/john
                                       :type        :schema/Person
                                       :schema/name 3}]})]
        (is (= [{:id          :ex/halie
                 :type        :schema/Person
                 :schema/name "Halie"}]
               @(fluree/query mixed-db
                              {:context default-context
                               :select  {'?u [:*]}
                               :where   {:id '?u, :schema/name "Halie"}}))
            "only returns the data type queried")
        (is (= []
               @(fluree/query mixed-db
                              {:context default-context
                               :select  {'?u [:*]}
                               :where   {:id '?u, :schema/name "a"}}))
            "does not return results without matching subjects")
        (is (= [{:id          :ex/john
                 :type        :schema/Person
                 :schema/name 3}]
               @(fluree/query mixed-db
                              {:context default-context
                               :select  {'?u [:*]}
                               :where   {:id '?u, :schema/name 3}}))
            "only returns the data type queried")))))

(deftest ^:integration datatype-test
  (testing "querying with datatypes"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "people")
          db     @(fluree/stage
                   (fluree/db ledger)
                   {"@context" [test-utils/default-context
                                {:ex    "http://example.org/ns/"
                                 :value "@value"
                                 :type  "@type"}]
                    "insert"
                    [{:id      :ex/homer
                      :ex/name "Homer"
                      :ex/age  36}
                     {:id      :ex/marge
                      :ex/name "Marge"
                      :ex/age  {:value 36
                                :type  :xsd/int}}
                     {:id      :ex/bart
                      :ex/name "Bart"
                      :ex/age  "forever 10"}]})]
      (testing "with literal values"
        (testing "specifying an explicit data type"
          (testing "compatible with the value"
            (let [query   {:context [test-utils/default-context
                                     {:ex    "http://example.org/ns/"
                                      :value "@value"
                                      :type  "@type"}]
                           :select  '[?name]
                           :where   '{:ex/name ?name
                                      :ex/age  {:value 36
                                                :type  :xsd/int}}}
                  results @(fluree/query db query)]
              (is (= [["Marge"]] results)
                  "should only return the matching items with the specified type")))
          (testing "not compatible with the value"
            (let [query   {:context [test-utils/default-context
                                     {:ex    "http://example.org/ns/"
                                      :value "@value"
                                      :type  "@type"}]
                           :select  '[?name]
                           :where   '{:ex/name ?name
                                      :ex/age  {:value 36
                                                :type  :xsd/string}}}
                  results @(fluree/query db query)]
              (is (exception? results)
                  "should return an error")))))
      (testing "bound to variables in 'bind' patterns"
        (testing "included datatype in query results"
          (let [query   {:context [test-utils/default-context
                                   {:ex "http://example.org/ns/"}]
                         :select  '[?name ?age ?dt]
                         :where   '[{:ex/name ?name
                                     :ex/age  ?age}
                                    [:bind ?dt (datatype ?age)]]}
                results @(fluree/query db query)]
            (is (= [["Bart" "forever 10" :xsd/string]
                    ["Homer" 36 :xsd/integer]
                    ["Marge" 36 :xsd/int]]
                   results))))
        (testing "filtered with the datatype function"
          (let [query   {:context [test-utils/default-context
                                   {:ex "http://example.org/ns/"}]
                         :select  '[?name ?age ?dt]
                         :where   '[{:ex/name ?name
                                     :ex/age  ?age}
                                    [:bind ?dt (datatype ?age)]
                                    [:filter (= (iri :xsd/integer) ?dt)]]}
                results @(fluree/query db query)]
            (is (= [["Homer" 36 :xsd/integer]]
                   results)))))
      (testing "filtered in value maps"
        (testing "with explicit type IRIs"
          (let [query   {:context [test-utils/default-context
                                   {:ex    "http://example.org/ns/"
                                    :value "@value"
                                    :type  "@type"}]
                         :select  '[?name ?age]
                         :where   '[{:ex/name ?name
                                     :ex/age  {:value ?age
                                               :type  :xsd/string}}]}
                results @(fluree/query db query)]
            (is (= [["Bart" "forever 10"]]
                   results))))
        (testing "with variable types"
          (let [query   {:context [test-utils/default-context
                                   {:ex    "http://example.org/ns/"
                                    :value "@value"
                                    :type  "@type"}]
                         :select  '[?name ?age ?ageType]
                         :where   '[{:ex/name ?name
                                     :ex/age  {:value ?age
                                               :type  ?ageType}}
                                    [:bind ?ageType (iri :xsd/int)]]}
                results @(fluree/query db query)]
            (is (= [["Marge" 36 :xsd/int]]
                   results))))))))
