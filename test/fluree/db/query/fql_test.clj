(ns fluree.db.query.fql-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]))

(deftest ^:integration grouping-test
  (testing "grouped queries"
    (let [conn   (test-utils/create-conn)
          people (test-utils/load-people conn)
          db     (fluree/db people)]
      (testing "with a single grouped-by field"
        (let [qry     '{:select   [?name ?email ?age ?favNums]
                        :where    {:schema/name  ?name
                                   :schema/email ?email
                                   :schema/age   ?age
                                   :ex/favNums   ?favNums}
                        :group-by ?name
                        :order-by ?name}
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
        (let [qry     '{:select   [?name ?email ?age ?favNums]
                        :where    {:schema/name  ?name
                                   :schema/email ?email
                                   :schema/age   ?age
                                   :ex/favNums   ?favNums}
                        :group-by [?name ?email ?age]
                        :order-by ?name}
              subject @(fluree/query db qry)]
          (is (= [["Alice" "alice@example.org" 50 [9 42 76]]
                  ["Brian" "brian@example.org" 50 [7]]
                  ["Cam" "cam@example.org" 34 [5 10]]
                  ["Liam" "liam@example.org" 13 [11 42]]]
                 subject)
              "returns grouped results"))

        (testing "with having clauses"
          (is (= [["Alice" [9 42 76]] ["Cam" [5 10]] ["Liam" [11 42]]]
                 @(fluree/query db '{:select   [?name ?favNums]
                                     :where    {:schema/name ?name
                                                :ex/favNums  ?favNums}
                                     :group-by ?name
                                     :having   (>= (count ?favNums) 2)}))
              "filters results according to the supplied having function code")

          (is (= [["Alice" [9 42 76]] ["Liam" [11 42]]]
                 @(fluree/query db '{:select   [?name ?favNums]
                                     :where    {:schema/name ?name
                                                :ex/favNums  ?favNums}
                                     :group-by ?name
                                     :having   (>= (avg ?favNums) 10)}))
              "filters results according to the supplied having function code"))))))

(deftest ^:integration select-distinct-test
  (testing "Distinct queries"
    (let [conn   (test-utils/create-conn)
          people (test-utils/load-people conn)
          db     (fluree/db people)
          q      '{:select-distinct [?name ?email]
                   :where           {:schema/name  ?name
                                     :schema/email ?email
                                     :ex/favNums   ?favNum}
                   :order-by        ?favNum}]
      (is (= [["Cam" "cam@example.org"]
              ["Brian" "brian@example.org"]
              ["Alice" "alice@example.org"]
              ["Liam" "liam@example.org"]]
             @(fluree/query db q))
          "return results without repeated entries"))))

(deftest ^:integration values-test
  (testing "Queries with pre-specified values"
    (let [conn   (test-utils/create-conn)
          people (test-utils/load-people conn)
          db     (fluree/db people)]
      (testing "binding a single variable"
        (testing "with a single value"
          (let [q '{:select [?name ?age]
                    :where  {:schema/email ?email
                             :schema/name  ?name
                             :schema/age   ?age}
                    :values [?email ["alice@example.org"]]}]
            (is (= [["Alice" 50]]
                   @(fluree/query db q))
                "returns only the results related to the bound value")))
        (testing "with multiple values"
          (let [q '{:select [?name ?age]
                    :where  {:schema/email ?email
                             :schema/name  ?name
                             :schema/age   ?age}
                    :values [?email ["alice@example.org" "cam@example.org"]]}]
            (is (= [["Alice" 50] ["Cam" 34]]
                   @(fluree/query db q))
                "returns only the results related to the bound values"))))
      (testing "binding multiple variables"
        (testing "with multiple values"
          (let [q '{:select [?name ?age]
                    :where  {:schema/email ?email
                             :ex/favNums   ?favNum
                             :schema/name  ?name
                             :schema/age   ?age}
                    :values [[?email ?favNum] [["alice@example.org" 42]
                                               ["cam@example.org" 10]]]}]
            (is (= [["Alice" 50] ["Cam" 34]]
                   @(fluree/query db q))
                "returns only the results related to the bound values")))
        (testing "with some values not present"
          (let [q '{:select [?name ?age]
                    :where  {:schema/email ?email
                             :ex/favNums   ?favNum
                             :schema/name  ?name
                             :schema/age   ?age}
                    :values [[?email ?favNum] [["alice@example.org" 42]
                                               ["cam@example.org" 37]]]}]
            (is (= [["Alice" 50]]
                   @(fluree/query db q))
                "returns only the results related to the existing bound values"))))
      (testing "with string vars"
        (let [q {:select ["?name" "?age"]
                 :where  {:schema/email "?email"
                          :schema/name  "?name"
                          :schema/age   "?age"}
                 :values ["?age" [13]]}]
          (is (= [["Liam" 13]]
                 @(fluree/query db q))
              "returns only the results related to the bound values"))))))

(deftest ^:integration bind-query-test
  (let [conn   (test-utils/create-conn)
        people (test-utils/load-people conn)
        db     (fluree/db people)]
    (testing "with 2 separate fn binds"
      (let [q   '{:select   [?firstLetterOfName ?name ?decadesOld]
                  :where    [{:schema/age  ?age
                              :schema/name ?name}
                             [:bind ?decadesOld (quot ?age 10)]
                             [:bind ?firstLetterOfName (subStr ?name 1 1)]]
                  :order-by ?firstLetterOfName}
            res @(fluree/query db q)]
        (is (= [["A" "Alice" 5]
                ["B" "Brian" 5]
                ["C" "Cam" 3]
                ["L" "Liam" 1]]
               res))))

    (testing "with 2 fn binds in one bind pattern"
      (let [q   '{:select   [?firstLetterOfName ?name ?canVote]
                  :where    [{:schema/age  ?age
                              :schema/name ?name}
                             [:bind
                              ?firstLetterOfName (subStr ?name 1 1)
                              ?canVote           (>= ?age 18)]]
                  :order-by ?name}
            res @(fluree/query db q)]
        (is (= [["A" "Alice" true]
                ["B" "Brian" true]
                ["C" "Cam" true]
                ["L" "Liam" false]]
               res))))

    (testing "with invalid aggregate fn"
      (let [q '{:select   [?sumFavNums ?name ?canVote]
                :where    [{:schema/age  ?age
                            :ex/favNums  ?favNums
                            :schema/name ?name}
                           [:bind
                            ?sumFavNums (sum ?favNums)
                            ?canVote    (>= ?age 18)]]
                :order-by ?name}]
        (is (re-matches
             #"Aggregate function sum is only valid for grouped values"
             (ex-message @(fluree/query db q))))))))

(deftest ^:integration iri-test
  (let [conn   (test-utils/create-conn)
        movies (test-utils/load-movies conn)
        db     (fluree/db movies)]
    (testing "iri references"
      (let [test-subject @(fluree/query db '{:select [?name]
                                             :where  {:schema/name      ?name
                                                      :schema/isBasedOn {:id :wiki/Q3107329}}})]
        (is (= [["The Hitchhiker's Guide to the Galaxy"]]
               test-subject))))))

;; TODO: for some reason the last test in this suite isn't passing
(deftest ^:pending language-test
  (testing "Querying ledgers loaded with language-tagged strings"
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "jobs")
          db     @(fluree/stage
                    (fluree/db ledger)
                    {"@context" ["https://ns.flur.ee"
                                 {"ex"         "http://example.com/vocab/"
                                  "occupation" {"@id"        "ex:occupation"
                                                "@container" "@language"}}]
                     "insert"   [{"@id"        "ex:frank"
                                  "occupation" {"en" {"@value" "Ninja"}
                                                "ja" "忍者"}}
                                 {"@id"        "ex:bob"
                                  "occupation" {"en" "Boss"
                                                "fr" "Chef"}}
                                 {"@id"        "ex:jack"
                                  "occupation" {"en" {"@value" "Chef"}
                                                "fr" {"@value" "Cuisinier"}}}]})]
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
        (let [sut @(fluree/query db '{"@context" {"ex" "http://example.com/vocab/"}
                                      :select    [?s]
                                      :where     {"@id"           ?s
                                                  "ex:occupation" {"@value"    "Chef"
                                                                   "@language" "fr"}}})]
          (is (= [["ex:bob"]] sut)
              "returns correctly filtered results"))))))

(deftest ^:integration ^:pending datatype-test
  (let [conn   (test-utils/create-conn)
        ledger @(fluree/create conn "people"
                               {:defaultContext
                                ["" {:ex "http://example.org/ns/"}]})
        db     @(fluree/stage
                  (fluree/db ledger)
                  {"@context" "https://ns.flur.ee"
                   "insert"
                   [{:id      :ex/homer
                     :ex/name "Homer"
                     :ex/age  36}
                    {:id      :ex/bart
                     :ex/name "Bart"
                     :ex/age  "forever 10"}]})]
    (testing "including datatype in query results"
      (let [query   '{:select [?age ?dt]
                      :where  [{:ex/age ?age}
                               [:bind ?dt (datatype ?age)]]}
            results @(fluree/query db query)]
        (is (= [["forever 10" "xsd:string"] [36 "xsd:long"]]
               results))))
    (testing "filtering query results with datatype fn")
    (testing "filtering query results with @type value map")))

(deftest ^:integration subject-object-test
  (let [conn   (test-utils/create-conn
                {:defaults
                 {:context-type :string
                  :context      {"id"     "@id",
                                 "type"   "@type",
                                 "ex"     "http://example.org/",
                                 "f"      "https://ns.flur.ee/ledger#",
                                 "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                                 "rdfs"   "http://www.w3.org/2000/01/rdf-schema#",
                                 "schema" "http://schema.org/",
                                 "xsd"    "http://www.w3.org/2001/XMLSchema#"}}})
        ledger @(fluree/create conn "test/love")
        db     @(fluree/stage (fluree/db ledger)
                               {"@context" "https://ns.flur.ee"
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
      (let [q '{:select [?s ?p ?o]
                :where  [{"@id"                ?s
                          "schema:description" ?o}
                         {"@id" ?s
                          ?p    ?o}]}
            subject @(fluree/query db q)]
        (is (= [["ex:fluree" "schema:description" "We ❤️ Data"]
                ["ex:mosquitos" "schema:description" "We ❤️ Human Blood"]
                ["ex:w3c" "schema:description" "We ❤️ Internet"]]
               subject)
            "returns all results")))))

(deftest ^:integration select-star-no-graph-crawl-test
  (let [conn   (test-utils/create-conn)
        ledger (test-utils/load-people conn)
        db     (fluree/db ledger)]
    (testing "select * w/o graph crawl returns all vars bound in where clause"
      (let [query   '{:select :*
                      :where  {:id          ?s
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
      (let [query   '{:select :*
                      :where  {:id          ?s
                               :schema/name ?name
                               :ex/favNums  ?favNums}
                      :group-by [?s ?name]}
            results @(fluree/query db query)]
        (is (= '[{?favNums [9 42 76], ?name "Alice", ?s :ex/alice}
                 {?favNums [7], ?name "Brian", ?s :ex/brian}
                 {?favNums [5 10], ?name "Cam", ?s :ex/cam}
                 {?favNums [11 42], ?name "Liam", ?s :ex/liam}]
               results))))))

(deftest ^:integration federated-test
  (testing "Federated queries"
    (let [conn (test-utils/create-conn {:defaults {:context-type :string
                                                   :context      {"id"     "@id",
                                                                  "type"   "@type",
                                                                  "ex"     "http://example.org/",
                                                                  "f"      "https://ns.flur.ee/ledger#",
                                                                  "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                                                                  "rdfs"   "http://www.w3.org/2000/01/rdf-schema#",
                                                                  "schema" "http://schema.org/",
                                                                  "xsd"    "http://www.w3.org/2001/XMLSchema#"}}})

          authors @(fluree/create-with-txn conn
                                            {"@context" ["https://ns.flur.ee" "" "https://schema.org"]
                                             "ledger"   "test/authors"
                                             "insert"   [{"@id"   "https://www.wikidata.org/wiki/Q42"
                                                          "@type" "Person"
                                                          "name"  "Douglas Adams"}
                                                         {"@id"   "https://www.wikidata.org/wiki/Q173540"
                                                          "@type" "Person"
                                                          "name"  "Margaret Mitchell"}]})
          books   @(fluree/create-with-txn conn
                                            {"@context" ["https://ns.flur.ee" "" "https://schema.org"]
                                             "ledger"   "test/books"
                                             "insert"   [{"id"     "https://www.wikidata.org/wiki/Q3107329",
                                                          "type"   ["Book"],
                                                          "name"   "The Hitchhiker's Guide to the Galaxy",
                                                          "isbn"   "0-330-25864-8",
                                                          "author" {"@id" "https://www.wikidata.org/wiki/Q42"}}
                                                         {"id"     "https://www.wikidata.org/wiki/Q2870",
                                                          "type"   ["Book"],
                                                          "name"   "Gone with the Wind",
                                                          "isbn"   "0-582-41805-4",
                                                          "author" {"@id" "https://www.wikidata.org/wiki/Q173540"}}]})
          movies  @(fluree/create-with-txn conn
                                            {"@context" ["https://ns.flur.ee" "" "https://schema.org"]
                                             "ledger"   "test/movies"
                                             "insert"   [{"id"                        "https://www.wikidata.org/wiki/Q836821",
                                                          "type"                      ["Movie"],
                                                          "name"                      "The Hitchhiker's Guide to the Galaxy",
                                                          "disambiguatingDescription" "2005 British-American comic science fiction film directed by Garth Jennings",
                                                          "titleEIDR"                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                                                          "isBasedOn"                 {"id" "https://www.wikidata.org/wiki/Q3107329"}}
                                                         {"id"                        "https://www.wikidata.org/wiki/Q91540",
                                                          "type"                      ["Movie"],
                                                          "name"                      "Back to the Future",
                                                          "disambiguatingDescription" "1985 film by Robert Zemeckis",
                                                          "titleEIDR"                 "10.5240/09A3-1F6E-3538-DF46-5C6F-I",
                                                          "followedBy"                {"id"         "https://www.wikidata.org/wiki/Q109331"
                                                                                       "type"       "Movie"
                                                                                       "name"       "Back to the Future Part II"
                                                                                       "titleEIDR"  "10.5240/5DA5-C386-2911-7E2B-1782-L"
                                                                                       "followedBy" {"id" "https://www.wikidata.org/wiki/Q230552"}}}
                                                         {"id"                        "https://www.wikidata.org/wiki/Q230552"
                                                          "type"                      ["Movie"]
                                                          "name"                      "Back to the Future Part III"
                                                          "disambiguatingDescription" "1990 film by Robert Zemeckis"
                                                          "titleEIDR"                 "10.5240/15F9-F913-FF25-8041-E798-O"}
                                                         {"id"                        "https://www.wikidata.org/wiki/Q2875",
                                                          "type"                      ["Movie"],
                                                          "name"                      "Gone with the Wind",
                                                          "disambiguatingDescription" "1939 film by Victor Fleming",
                                                          "titleEIDR"                 "10.5240/FB0D-0A93-CAD6-8E8D-80C2-4",
                                                          "isBasedOn"                 {"id" "https://www.wikidata.org/wiki/Q2870"}}]})]
      (testing "with combined data sets"
        (let [q '{"@context" "https://schema.org"
                  :from      ["test/authors" "test/books" "test/movies"]
                  :select    [?movieName ?bookIsbn ?authorName]
                  :where     {"type"      "Movie"
                              "name"      ?movieName
                              "isBasedOn" {"isbn"   ?bookIsbn
                                           "author" {"name" ?authorName}}}}]

          (is (= [["Gone with the Wind" "0-582-41805-4" "Margaret Mitchell"]
                  ["The Hitchhiker's Guide to the Galaxy" "0-330-25864-8" "Douglas Adams"]]
                 @(fluree/query-connection conn q))
              "returns unified results from each component ledger")))
      (testing "with separate data sets"
        (let [q '{"@context"  "https://schema.org"
                  :from-named ["test/authors" "test/books" "test/movies"]
                  :select     [?movieName ?bookIsbn ?authorName]
                  :where      [[:graph "test/movies" {"id"        ?movie
                                                      "type"      "Movie"
                                                      "name"      ?movieName
                                                      "isBasedOn" ?book}]
                               [:graph "test/books" {"id"     ?book
                                                     "isbn"   ?bookIsbn
                                                     "author" ?author}]
                               [:graph "test/authors" {"id"   ?author
                                                       "name" ?authorName}]]}]
          (is (= [["Gone with the Wind" "0-582-41805-4" "Margaret Mitchell"]
                  ["The Hitchhiker's Guide to the Galaxy" "0-330-25864-8" "Douglas Adams"]]
                 @(fluree/query-connection conn q))
              "returns unified results from each component ledger"))))))
