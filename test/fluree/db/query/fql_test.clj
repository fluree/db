(ns fluree.db.query.fql-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils :refer [pred-match?]]))

(deftest ^:integration grouping-test
  (testing "grouped queries"
    (let [conn   (test-utils/create-conn)
          people (test-utils/load-people conn)
          db     (fluree/db people)]
      (testing "with a single grouped-by field"
        (let [qry     {:context  [test-utils/default-context
                                  {:ex "http://example.org/ns/"}]
                       :select   '[?name ?email ?age ?favNums]
                       :where    '{:schema/name  ?name
                                   :schema/email ?email
                                   :schema/age   ?age
                                   :ex/favNums   ?favNums}
                       :group-by '?name
                       :order-by '?name}
              subject @(fluree/query db qry)]
          (is (= [["Alice"
                   ["alice@example.org" "alice@example.org" "alice@example.org"]
                   [50 50 50]
                   [9 42 76]]
                  ["Brian" ["brian@example.org"] [50] [7]]
                  ["Cam" ["cam@example.org" "cam@example.org"] [34 34] [5 10]]
                  ["Liam" ["liam@example.org" "liam@example.org"] [13 13] [11 42]]]
                 subject)
              "returns grouped results")))

      (testing "with multiple grouped-by fields"
        (let [qry     {:context  [test-utils/default-context
                                  {:ex "http://example.org/ns/"}]
                       :select   '[?name ?email ?age ?favNums]
                       :where    '{:schema/name  ?name
                                   :schema/email ?email
                                   :schema/age   ?age
                                   :ex/favNums   ?favNums}
                       :group-by '[?name ?email ?age]
                       :order-by '?name}
              subject @(fluree/query db qry)]
          (is (= [["Alice" "alice@example.org" 50 [9 42 76]]
                  ["Brian" "brian@example.org" 50 [7]]
                  ["Cam" "cam@example.org" 34 [5 10]]
                  ["Liam" "liam@example.org" 13 [11 42]]]
                 subject)
              "returns grouped results"))

        (testing "with having clauses"
          (is (= [["Alice" [9 42 76]] ["Cam" [5 10]] ["Liam" [11 42]]]
                 @(fluree/query db {:context  [test-utils/default-context
                                               {:ex "http://example.org/ns/"}]
                                    :select   '[?name ?favNums]
                                    :where    '{:schema/name ?name
                                                :ex/favNums  ?favNums}
                                    :group-by '?name
                                    :having   '(>= (count ?favNums) 2)}))
              "filters results according to the supplied having function code")

          (is (= [["Alice" [9 42 76]] ["Liam" [11 42]]]
                 @(fluree/query db {:context  [test-utils/default-context
                                               {:ex "http://example.org/ns/"}]
                                    :select   '[?name ?favNums]
                                    :where    '{:schema/name ?name
                                                :ex/favNums  ?favNums}
                                    :group-by '?name
                                    :having   '(>= (avg ?favNums) 10)}))
              "filters results according to the supplied having function code"))))))

(deftest ^:integration ordering-test
  (testing "Queries with order"
    (let [conn   (test-utils/create-conn)
          people (test-utils/load-people conn)
          db     (fluree/db people)]
      (testing "with a single ordered field"
        (let [qry     {:context  [test-utils/default-context
                                  {:ex "http://example.org/ns/"}]
                       :select   '[?name ?email ?age]
                       :where    '{:schema/name  ?name
                                   :schema/email ?email
                                   :schema/age   ?age}
                       :order-by '?name}
              subject @(fluree/query db qry)]
          (is (= [["Alice" "alice@example.org" 50]
                  ["Brian" "brian@example.org" 50]
                  ["Cam" "cam@example.org" 34]
                  ["Liam" "liam@example.org" 13]]
                 subject)
              "returns ordered results"))
        (testing "with a specified direction"
          (let [qry     {:context  [test-utils/default-context
                                    {:ex "http://example.org/ns/"}]
                         :select   '[?name ?email ?age]
                         :where    '{:schema/name  ?name
                                     :schema/email ?email
                                     :schema/age   ?age}
                         :order-by '(desc ?name)}
                subject @(fluree/query db qry)]
            (is (= [["Liam" "liam@example.org" 13]
                    ["Cam" "cam@example.org" 34]
                    ["Brian" "brian@example.org" 50]
                    ["Alice" "alice@example.org" 50]]
                   subject)
                "returns ordered results"))))

      (testing "with multiple ordered fields"
        (let [qry     {:context  [test-utils/default-context
                                  {:ex "http://example.org/ns/"}]
                       :select   '[?name ?email ?age]
                       :where    '{:schema/name  ?name
                                   :schema/email ?email
                                   :schema/age   ?age}
                       :order-by '[?age ?name]}
              subject @(fluree/query db qry)]
          (is (= [["Liam" "liam@example.org" 13]
                  ["Cam" "cam@example.org" 34]
                  ["Alice" "alice@example.org" 50]
                  ["Brian" "brian@example.org" 50]]
                 subject)
              "returns ordered results"))
        (testing "with a specified direction"
          (let [qry     {"@context" [test-utils/default-str-context
                                     {"ex" "http://example.org/ns/"}]
                         "select"   ["?name" "?email" "?age"]
                         "where"    {"schema:name"  "?name"
                                     "schema:email" "?email"
                                     "schema:age"   "?age"}
                         "orderBy"  ["(desc ?age)" "?name"]}
                subject @(fluree/query db qry)]
            (is (= [["Alice" "alice@example.org" 50]
                    ["Brian" "brian@example.org" 50]
                    ["Cam" "cam@example.org" 34]
                    ["Liam" "liam@example.org" 13]]
                   subject)
                "returns ordered results")))))))

(deftest ^:integration select-distinct-test
  (let [conn   (test-utils/create-conn)
        people (test-utils/load-people conn)
        db     (fluree/db people)]
    (testing "distinct results"
      (is (= [["Alice" "alice@example.org"]
              ["Brian" "brian@example.org"]
              ["Cam" "cam@example.org"]
              ["Liam" "liam@example.org"]]
             @(fluree/query db {:context [test-utils/default-context
                                          {:ex "http://example.org/ns/"}]
                                :select-distinct '[?name ?email]
                                :where '{:schema/name ?name
                                         :schema/email ?email
                                         :ex/favNums ?favNum}}))
          "return results without repeated entries"))
    (testing "distinct results with limit and offset"
      (is (= [["Brian" "brian@example.org"]
              ["Cam" "cam@example.org"]]
             @(fluree/query db {:context [test-utils/default-context
                                          {:ex "http://example.org/ns/"}]
                                :select-distinct '[?name ?email]
                                :where '{:schema/name ?name
                                         :schema/email ?email
                                         :ex/favNums ?favNum}
                                :limit 2 :offset 1}))
          "return results without repeated entries"))))

(deftest ^:integration select-one-test
  (let [conn   (test-utils/create-conn)
        people (test-utils/load-people conn)
        db     (fluree/db people)]
    (testing "select-one"
      (testing "with result"
        (testing "with sequential select"
          (is (= [9]
                 @(fluree/query db {:context [test-utils/default-context {:ex "http://example.org/ns/"}]
                                    :selectOne '[?favNum]
                                    :where '[{:id ?s :schema/name "Alice"}
                                             {:id ?s :ex/favNums ?favNum}]
                                    :order-by '?favNum}))))
        (testing "with single select"
          (is (= 9
                 @(fluree/query db {:context [test-utils/default-context {:ex "http://example.org/ns/"}]
                                    :selectOne '?favNum
                                    :where '[{:id ?s :schema/name "Alice"}
                                             {:id ?s :ex/favNums ?favNum}]
                                    :order-by '?favNum})))))
      (testing "with no result"
        (testing "with sequential select"
          (is (= nil
                 @(fluree/query db {:context [test-utils/default-context {:ex "http://example.org/ns/"}]
                                    :selectOne '[?s]
                                    :where '{:id ?s :schema/name "Bob"}}))))
        (testing "with single select"
          (is (= nil
                 @(fluree/query db {:context [test-utils/default-context {:ex "http://example.org/ns/"}]
                                    :selectOne '?s
                                    :where '{:id ?s :schema/name "Bob"}}))))))))

(deftest ^:integration values-test
  (testing "Queries with pre-specified values"
    (let [conn   (test-utils/create-conn)
          people (test-utils/load-people conn)
          db     (fluree/db people)]
      (testing "binding a single variable"
        (testing "with a single value"
          (let [q {:context [test-utils/default-context
                             {:ex "http://example.org/ns/"}]
                   :select  '[?name ?age]
                   :where   '{:schema/email ?email
                              :schema/name  ?name
                              :schema/age   ?age}
                   :values  '[?email ["alice@example.org"]]}]
            (is (= [["Alice" 50]]
                   @(fluree/query db q))
                "returns only the results related to the bound value")))
        (testing "with an iri"
          (let [q {:context [test-utils/default-context
                             {:ex    "http://example.org/ns/"
                              :value "@value"
                              :id "@id"}]
                   :select  '[?name ?age]
                   :where   '{:id          ?s
                              :schema/name ?name
                              :schema/age  ?age}
                   :values  '[?s [{:value :ex/alice, :type :id}]]}]
            (is (= [["Alice" 50]]
                   @(fluree/query db q))
                "returns only the results related to the bound value")))
        (testing "with multiple values"
          (let [q {:context [test-utils/default-context
                             {:ex "http://example.org/ns/"}]
                   :select  '[?name ?age]
                   :where   '{:schema/email ?email
                              :schema/name  ?name
                              :schema/age   ?age}
                   :values  '[?email ["alice@example.org" "cam@example.org"]]}]
            (is (= [["Alice" 50] ["Cam" 34]]
                   @(fluree/query db q))
                "returns only the results related to the bound values"))))
      (testing "binding multiple variables"
        (testing "with multiple values"
          (let [q {:context [test-utils/default-context
                             {:ex "http://example.org/ns/"}]
                   :select  '[?name ?age]
                   :where   '{:schema/email ?email
                              :ex/favNums   ?favNum
                              :schema/name  ?name
                              :schema/age   ?age}
                   :values  '[[?email ?favNum] [["alice@example.org" 42]
                                                ["cam@example.org" 10]]]}]
            (is (= [["Alice" 50] ["Cam" 34]]
                   @(fluree/query db q))
                "returns only the results related to the bound values")))
        (testing "with some values not present"
          (let [q {:context [test-utils/default-context
                             {:ex "http://example.org/ns/"}]
                   :select  '[?name ?age]
                   :where   '{:schema/email ?email
                              :ex/favNums   ?favNum
                              :schema/name  ?name
                              :schema/age   ?age}
                   :values  '[[?email ?favNum] [["alice@example.org" 42]
                                                ["cam@example.org" 37]]]}]
            (is (= [["Alice" 50]]
                   @(fluree/query db q))
                "returns only the results related to the existing bound values"))))
      (testing "with string vars"
        (let [q {:context [test-utils/default-context
                           {:ex "http://example.org/ns/"}]
                 :select  ["?name" "?age"]
                 :where   {:schema/email "?email"
                           :schema/name  "?name"
                           :schema/age   "?age"}
                 :values  ["?age" [13]]}]
          (is (= [["Liam" 13]]
                 @(fluree/query db q))
              "returns only the results related to the bound values"))))))

(deftest ^:integration bind-query-test
  (let [conn   (test-utils/create-conn)
        people (test-utils/load-people conn)
        db     (fluree/db people)]
    (testing "with 2 separate fn binds"
      (let [q   {:context  [test-utils/default-context
                            {:ex "http://example.org/ns/"}]
                 :select   '[?firstLetterOfName ?name ?decadesOld]
                 :where    '[{:schema/age  ?age
                              :schema/name ?name}
                             [:bind ?decadesOld (quot ?age 10)]
                             [:bind ?firstLetterOfName (subStr ?name 1 1)]]
                 :order-by '?firstLetterOfName}
            res @(fluree/query db q)]
        (is (= [["A" "Alice" 5]
                ["B" "Brian" 5]
                ["C" "Cam" 3]
                ["L" "Liam" 1]]
               res))))

    (testing "with 2 fn binds in one bind pattern"
      (let [q   {:context  [test-utils/default-context
                            {:ex "http://example.org/ns/"}]
                 :select   '[?firstLetterOfName ?name ?canVote]
                 :where    '[{:schema/age  ?age
                              :schema/name ?name}
                             [:bind
                              ?firstLetterOfName (subStr ?name 1 1)
                              ?canVote           (>= ?age 18)]]
                 :order-by '?name}
            res @(fluree/query db q)]
        (is (= [["A" "Alice" true]
                ["B" "Brian" true]
                ["C" "Cam" true]
                ["L" "Liam" false]]
               res))))

    (testing "with static binds"
      (let [q   {:context  [test-utils/default-context
                            {:ex "http://example.org/ns/"}]
                 :construct '[{:id ?s
                               :ex/firstLetter ?firstLetterOfName
                               :ex/const ?const
                               :ex/greeting ?langstring
                               :ex/date     ?date}]
                 :where    '[{:id ?s
                              :schema/age  ?age
                              :schema/name ?name}
                             [:bind
                              ?firstLetterOfName (subStr ?name 1 1)
                              ?const             "const"
                              ?langstring        {"@value" "hola" "@language" "es"}
                              ?bool              false
                              ?date              {"@value" "2020-01-10" "@type" "ex:mydate"}]]
                 :order-by '?name}
            res (-> @(fluree/query db q) (get "@graph"))]
        (is (= [{:id :ex/alice,
                 :ex/firstLetter ["A"],
                 :ex/const ["const"],
                 :ex/greeting [{:value "hola", "@language" "es"}],
                 :ex/date [{:value "2020-01-10", :type "ex:mydate"}]}
                {:id :ex/brian,
                 :ex/firstLetter ["B"],
                 :ex/const ["const"],
                 :ex/greeting [{:value "hola", "@language" "es"}],
                 :ex/date [{:value "2020-01-10", :type "ex:mydate"}]}
                {:id :ex/cam,
                 :ex/firstLetter ["C"],
                 :ex/const ["const"],
                 :ex/greeting [{:value "hola", "@language" "es"}],
                 :ex/date [{:value "2020-01-10", :type "ex:mydate"}]}
                {:id :ex/liam,
                 :ex/firstLetter ["L"],
                 :ex/const ["const"],
                 :ex/greeting [{:value "hola", "@language" "es"}],
                 :ex/date [{:value "2020-01-10", :type "ex:mydate"}]}]
               res))))

    (testing "with invalid aggregate fn"
      (let [q {:context  [test-utils/default-context
                          {:ex "http://example.org/ns/"}]
               :select   '[?sumFavNums ?name ?canVote]
               :where    '[{:schema/age  ?age
                            :ex/favNums  ?favNums
                            :schema/name ?name}
                           [:bind
                            ?sumFavNums (sum ?favNums)
                            ?canVote    (>= ?age 18)]]
               :order-by '?name}]
        (is (re-matches
             #"Aggregate function sum is only valid for grouped values"
             (ex-message @(fluree/query db q))))))))

(deftest ^:integration iri-test
  (let [conn   (test-utils/create-conn)
        movies (test-utils/load-movies conn)
        db     (fluree/db movies)]
    (testing "iri references"
      (let [test-subject @(fluree/query db {:context test-utils/default-context
                                            :select  '[?name]
                                            :where   '{:schema/name      ?name
                                                       :schema/isBasedOn {:id :wiki/Q3107329}}})]
        (is (= [["The Hitchhiker's Guide to the Galaxy"]]
               test-subject))))))

(deftest ^:integration id-test
  (let [conn   (test-utils/create-conn)
        movies (test-utils/load-movies conn)
        db     (fluree/db movies)]
    (testing "searching for bare id maps"
      (let [test-subject @(fluree/query db {:context test-utils/default-context
                                            :select  '[?id]
                                            :where   '{:id ?id}})]
        (is (pred-match? [[test-utils/db-id?]
                          [test-utils/db-id?]
                          [test-utils/db-id?]
                          [test-utils/db-id?]
                          [test-utils/commit-id?]
                          [test-utils/commit-id?]
                          [test-utils/commit-id?]
                          [test-utils/commit-id?]
                          [:wiki/Q109331]
                          [:wiki/Q173540]
                          [:wiki/Q230552]
                          [:wiki/Q2870]
                          [:wiki/Q2875]
                          [:wiki/Q3107329]
                          [:wiki/Q42]
                          [:wiki/Q836821]
                          [:wiki/Q91540]]
                         test-subject)
            "returns all the subject ids in the ledger")))))

(deftest language-test
  (testing "Querying ledgers loaded with language-tagged strings"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "jobs")
          db     @(fluree/update
                   (fluree/db ledger)
                   {"@context" {"ex"         "http://example.com/vocab/"
                                "occupation" {"@id"        "ex:occupation"
                                              "@container" "@language"}}
                    "insert"   [{"@id"        "ex:frank"
                                 "occupation" {"en" {"@value" "Ninja"}
                                               "ja" "忍者"}}
                                {"@id"             "ex:bob"
                                 "ex:nativeTongue" "fr"
                                 "occupation"      {"en" "Boss"
                                                    "fr" "Chef"
                                                    "de" "Bossin"}}
                                {"@id"             "ex:jack"
                                 "ex:nativeTongue" "de"
                                 "occupation"      {"en" {"@value" "Chef"}
                                                    "fr" {"@value" "Cuisinier"}
                                                    "de" {"@value" "Köchin"}}}]})]
      (testing "with bound language tags"
        (let [sut @(fluree/query db '{"@context" {"ex" "http://example.com/vocab/"}
                                      :select    [?job ?lang]
                                      :where     [{"@id"           "ex:frank"
                                                   "ex:occupation" ?job}
                                                  [:bind ?lang "(lang ?job)"]]})]
          (is (= #{["Ninja" "en"] ["忍者" "ja"]} (set sut))
              "return the correct language tags.")))

      (testing "filtering by language tags"
        (let [sut @(fluree/query db '{"@context" {"ex" "http://example.com/vocab/"}
                                      :select    [?s ?job]
                                      :where     [{"@id"           ?s
                                                   "ex:occupation" ?job}
                                                  [:filter "(= \"en\" (lang ?job))"]]})]
          (is (= #{["ex:bob" "Boss"] ["ex:jack" "Chef"] ["ex:frank" "Ninja"]} (set sut))
              "returns correctly filtered results")))

      (testing "filtering with value maps"
        (testing "with scalar language tag"
          (let [sut @(fluree/query db '{"@context" {"ex" "http://example.com/vocab/"}
                                        :select    [?s]
                                        :where     {"@id"           ?s
                                                    "ex:occupation" {"@value"    "Chef"
                                                                     "@language" "fr"}}})]
            (is (= [["ex:bob"]] sut)
                "returns correctly filtered results")))

        (testing "with variable language tag binding"
          (let [sut @(fluree/query db '{"@context" {"ex" "http://example.com/vocab/"}
                                        :select    [?s]
                                        :where     {"@id"             ?s
                                                    "ex:nativeTongue" ?lang
                                                    "ex:occupation"   {"@value"    "Chef"
                                                                       "@language" ?lang}}})]
            (is (= [["ex:bob"]] sut)
                "returns correctly filtered results")))))))

(deftest ^:integration t-test
  (testing "querying with t values"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "people")
          db1    @(fluree/update (fluree/db ledger)
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
                                    :ex/age  "forever 10"}]})
          db1*   @(fluree/commit! ledger db1)
          db2    @(fluree/update db1* {"@context" [test-utils/default-context
                                                   {:ex    "http://example.org/ns/"
                                                    :value "@value"
                                                    :type  "@type"}]
                                       "insert"
                                       [{:id     :ex/homer
                                         :ex/son {:id :ex/bart}}
                                        {:id            :ex/bart
                                         :ex/dad        {:id :ex/homer}
                                         :ex/occupation "Getting into mischief"}]})
          db2*   @(fluree/commit! ledger db2)
          db3    @(fluree/update db2* {"@context" [test-utils/default-context
                                                   {:ex    "http://example.org/ns/"
                                                    :value "@value"
                                                    :type  "@type"}]
                                       "insert"
                                       [{:id     :ex/marge
                                         :ex/son {:id :ex/bart}}
                                        {:id     :ex/bart
                                         :ex/mom {:id :ex/marge}}]})
          db3*   @(fluree/commit! ledger db3)]
      (testing "using a specific t"
        (let [query   {:context [test-utils/default-context
                                 {:ex    "http://example.org/ns/"
                                  :value "@value"
                                  :type  "@type"
                                  :t     "@t"}]
                       :select  '[?p ?o]
                       :where   '[{:id :ex/bart
                                   ?p  {:value ?o
                                        :t     2}}]}
              results @(fluree/query db3* query)]
          (is (= [[:ex/dad :ex/homer]
                  [:ex/occupation "Getting into mischief"]]
                 results)
              "returns only data set in that transaction")))
      (testing "using a variable t"
        (let [query   {:context [test-utils/default-context
                                 {:ex    "http://example.org/ns/"
                                  :value "@value"
                                  :type  "@type"
                                  :t     "@t"}]
                       :select  '[?p ?o ?t]
                       :where   '[{:id :ex/bart
                                   ?p  {:value ?o
                                        :t     ?t}}]}
              results @(fluree/query db3* query)]
          (is (= [[:ex/age "forever 10" 1]
                  [:ex/dad :ex/homer 2]
                  [:ex/mom :ex/marge 3]
                  [:ex/name "Bart" 1]
                  [:ex/occupation "Getting into mischief" 2]]
                 results)
              "returns the correct transaction number for each result"))))))

(deftest ^:integration subject-object-test
  (let [conn   (test-utils/create-conn)
        ledger @(fluree/create conn "test/love")
        db     @(fluree/update (fluree/db ledger)
                               {"@context" {"id"     "@id",
                                            "type"   "@type",
                                            "ex"     "http://example.org/",
                                            "f"      "https://ns.flur.ee/ledger#",
                                            "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                                            "rdfs"   "http://www.w3.org/2000/01/rdf-schema#",
                                            "schema" "http://schema.org/",
                                            "xsd"    "http://www.w3.org/2001/XMLSchema#"}
                                "insert"
                                [{"@id"                "ex:fluree",
                                  "@type"              "schema:Organization",
                                  "schema:description" "We ❤️ Data"}
                                 {"@id"                "ex:w3c",
                                  "@type"              "schema:Organization",
                                  "schema:description" "We ❤️ Internet"}
                                 {"@id"                "ex:mosquitos",
                                  "@type"              "ex:Monster",
                                  "schema:description" "We ❤️ Human Blood"}]}
                               {})]
    (testing "subject-object scans"
      (let [q       {:context {"id"     "@id",
                               "type"   "@type",
                               "ex"     "http://example.org/",
                               "f"      "https://ns.flur.ee/ledger#",
                               "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                               "rdfs"   "http://www.w3.org/2000/01/rdf-schema#",
                               "schema" "http://schema.org/",
                               "xsd"    "http://www.w3.org/2001/XMLSchema#"}
                     :select  '[?s ?p ?o]
                     :where   '[{"@id"                ?s
                                 "schema:description" ?o}
                                {"@id" ?s
                                 ?p    ?o}]}
            subject @(fluree/query db q)]
        (is (= [["ex:fluree" "schema:description" "We ❤️ Data"]
                ["ex:mosquitos" "schema:description" "We ❤️ Human Blood"]
                ["ex:w3c" "schema:description" "We ❤️ Internet"]]
               subject)
            "returns all results")))))

(deftest bnode-variables-test
  (let [conn   (test-utils/create-conn)
        ledger @(fluree/create conn "test/bnodes")
        db     @(fluree/stage (fluree/db ledger)
                              {"@context" {"ex" "http://example.org/"}
                               "insert"
                               [{"@id"    "ex:a",
                                 "@type"  "ex:Thing",
                                 "ex:foo" "_bar"}
                                {"@id"    "ex:b",
                                 "@type"  "ex:Thing",
                                 "ex:foo" "_foo"}]})]
    (testing "_ prefix is a literal value"
      (is (= ["ex:b"]
             @(fluree/query db {"@context" {"ex" "http://example.org/"}
                                "where" [{"@id" "?s" "ex:foo" "_foo"}]
                                "select" "?s"}))))
    (testing "_: prefix matches everything"
      (is (= ["ex:a" "ex:b"]
             @(fluree/query db {"@context" {"ex" "http://example.org/"}
                                "where" [{"@id" "?s" "ex:foo" "_:foo"}]
                                "select" "?s"}))))))

(deftest ^:integration select-star-no-graph-crawl-test
  (let [conn   (test-utils/create-conn)
        ledger (test-utils/load-people conn)
        db     (fluree/db ledger)]
    (testing "select * w/o graph crawl returns all vars bound in where clause"
      (let [query   {:context [test-utils/default-context
                               {:ex "http://example.org/ns/"}]
                     :select  :*
                     :where   '{:id          ?s
                                :schema/name ?name
                                :ex/favNums  ?favNums}}
            results @(fluree/query db query)]
        (is (= '[{?favNums 9, ?name "Alice", ?s :ex/alice}
                 {?favNums 42, ?name "Alice", ?s :ex/alice}
                 {?favNums 76, ?name "Alice", ?s :ex/alice}
                 {?favNums 7, ?name "Brian", ?s :ex/brian}
                 {?favNums 5, ?name "Cam", ?s :ex/cam}
                 {?favNums 10, ?name "Cam", ?s :ex/cam}
                 {?favNums 11, ?name "Liam", ?s :ex/liam}
                 {?favNums 42, ?name "Liam", ?s :ex/liam}]
               results))))
    (testing "select * w/o graph crawl returns all vars bound in where clause w/ grouping"
      (let [query   {:context  [test-utils/default-context
                                {:ex "http://example.org/ns/"}]
                     :select   :*
                     :where    '{:id          ?s
                                 :schema/name ?name
                                 :ex/favNums  ?favNums}
                     :group-by '[?s ?name]}
            results @(fluree/query db query)]
        (is (= '[{?favNums [9 42 76], ?name "Alice", ?s :ex/alice}
                 {?favNums [7], ?name "Brian", ?s :ex/brian}
                 {?favNums [5 10], ?name "Cam", ?s :ex/cam}
                 {?favNums [11 42], ?name "Liam", ?s :ex/liam}]
               results))))
    (testing "select * does not compose with other selectors"
      (let [query {:context {:ex "http://example.com/ns/"}
                   :select [:* '?foo]
                   :where [{:id '?s :ex/foo '?foo}]}]
        (is (= "Error in value for \"select\"; Select must be a valid selector, a wildcard symbol (`*`), or a vector of selectors; Provided: [:* ?foo];  See documentation for details: https://next.developers.flur.ee/docs/reference/errorcodes#query-invalid-select"
               (ex-message @(fluree/query db query))))))))
