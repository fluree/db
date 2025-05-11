(ns fluree.db.query.federated-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(deftest ^:integration federated-query-connection-test
  (testing "Federated queries using query-connection"
    (let [conn    (test-utils/create-conn)
          context {"id"     "@id",
                   "type"   "@type",
                   "ex"     "http://example.org/",
                   "f"      "https://ns.flur.ee/ledger#",
                   "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                   "rdfs"   "http://www.w3.org/2000/01/rdf-schema#",
                   "schema" "http://schema.org/",
                   "xsd"    "http://www.w3.org/2001/XMLSchema#"}

          _authors @(fluree/create-with-txn conn
                                            {"@context" [context "https://schema.org"]
                                             "ledger"   "test/authors"
                                             "insert"   [{"@id"   "https://www.wikidata.org/wiki/Q42"
                                                          "@type" "Person"
                                                          "name"  "Douglas Adams"}
                                                         {"@id"   "https://www.wikidata.org/wiki/Q173540"
                                                          "@type" "Person"
                                                          "name"  "Margaret Mitchell"}]})
          _books   @(fluree/create-with-txn conn
                                            {"@context" [context "https://schema.org"]
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
          _movies  @(fluree/create-with-txn conn
                                            {"@context" [context "https://schema.org"]
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
        (testing "directly selecting variables"
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
        (testing "selecting subgraphs"
          (let [q '{"context" ["https://schema.org", {"value" "@value"}]
                    :from     ["test/authors" "test/books" "test/movies"]
                    :select   {?goneWithTheWind [:*]}
                    :depth    3
                    :where    {"@id"  ?goneWithTheWind
                               "type" "Movie"
                               "name" "Gone with the Wind"}}]
            (is (= [{"type"                      "Movie",
                     "disambiguatingDescription" {"value" "1939 film by Victor Fleming",
                                                  "type"  "xsd:string"},
                     "isBasedOn"                 {"type"   "Book",
                                                  "author" {"type" "Person",
                                                            "name" {"value" "Margaret Mitchell"
                                                                    "type"  "xsd:string"},
                                                            "id"   "https://www.wikidata.org/wiki/Q173540"},
                                                  "isbn"   {"value" "0-582-41805-4",
                                                            "type"  "xsd:string"},
                                                  "name"   {"value" "Gone with the Wind",
                                                            "type"  "xsd:string"},
                                                  "id"     "https://www.wikidata.org/wiki/Q2870"},
                     "name"                      {"value" "Gone with the Wind",
                                                  "type"  "xsd:string"},
                     "titleEIDR"                 {"value" "10.5240/FB0D-0A93-CAD6-8E8D-80C2-4",
                                                  "type"  "xsd:string"},
                     "id"                        "https://www.wikidata.org/wiki/Q2875"}]
                   @(fluree/query-connection conn q))
                "returns unified results for the requested subject"))))
      (testing "with separate data sets"
        (testing "directly selecting variables"
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
                "returns unified results from each component ledger")))
        (testing "selecting subgraphs"
          (let [q '{"context"   ["https://schema.org", {"value" "@value"}]
                    :from-named ["test/authors" "test/books" "test/movies"]
                    :select     {?goneWithTheWind [:*]}
                    :depth      3
                    :where      [[:graph "test/movies" {"@id"  ?goneWithTheWind
                                                        "name" "Gone with the Wind"}]]}]
            (is (= [{"type"                      "Movie",
                     "disambiguatingDescription" {"value" "1939 film by Victor Fleming",
                                                  "type"  "xsd:string"},
                     "isBasedOn"                 {"type"   "Book",
                                                  "author" {"type" "Person",
                                                            "name" {"value" "Margaret Mitchell"
                                                                    "type"  "xsd:string"},
                                                            "id"   "https://www.wikidata.org/wiki/Q173540"},
                                                  "isbn"   {"value" "0-582-41805-4",
                                                            "type"  "xsd:string"},
                                                  "name"   {"value" "Gone with the Wind"
                                                            "type"  "xsd:string"},
                                                  "id"     "https://www.wikidata.org/wiki/Q2870"},
                     "name"                      {"value" "Gone with the Wind",
                                                  "type"  "xsd:string"},
                     "titleEIDR"                 {"value" "10.5240/FB0D-0A93-CAD6-8E8D-80C2-4",
                                                  "type"  "xsd:string"},
                     "id"                        "https://www.wikidata.org/wiki/Q2875"}]
                   @(fluree/query-connection conn q))
                "returns unified results for the requested subject")))))))

(deftest ^:integration federated-query-composed-datasets
  (testing "Federated queries using query with composed datasets across connections"
    (let [conn-authors (test-utils/create-conn)
          conn-books   (test-utils/create-conn)
          conn-movies  (test-utils/create-conn)
          context      {"id"     "@id",
                        "type"   "@type",
                        "ex"     "http://example.org/",
                        "f"      "https://ns.flur.ee/ledger#",
                        "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                        "rdfs"   "http://www.w3.org/2000/01/rdf-schema#",
                        "schema" "http://schema.org/",
                        "xsd"    "http://www.w3.org/2001/XMLSchema#"}

          authors @(fluree/create-with-txn conn-authors
                                           {"@context" [context "https://schema.org"]
                                            "ledger"   "test/authors"
                                            "insert"   [{"@id"   "https://www.wikidata.org/wiki/Q42"
                                                         "@type" "Person"
                                                         "name"  "Douglas Adams"}
                                                        {"@id"   "https://www.wikidata.org/wiki/Q173540"
                                                         "@type" "Person"
                                                         "name"  "Margaret Mitchell"}]})
          books   @(fluree/create-with-txn conn-books
                                           {"@context" [context "https://schema.org"]
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
          movies  @(fluree/create-with-txn conn-movies
                                           {"@context" [context "https://schema.org"]
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
                                                         "isBasedOn"                 {"id" "https://www.wikidata.org/wiki/Q2870"}}]})
          dataset (fluree/dataset {"test/authors" authors
                                   "test/books"   books
                                   "test/movies"  movies})]
      (testing "with combined data sets"
        (testing "directly selecting variables"
          (let [q '{"@context" "https://schema.org"
                    :select    [?movieName ?bookIsbn ?authorName]
                    :where     {"type"      "Movie"
                                "name"      ?movieName
                                "isBasedOn" {"isbn"   ?bookIsbn
                                             "author" {"name" ?authorName}}}}]

            (is (= [["Gone with the Wind" "0-582-41805-4" "Margaret Mitchell"]
                    ["The Hitchhiker's Guide to the Galaxy" "0-330-25864-8" "Douglas Adams"]]
                   @(fluree/query dataset q))
                "returns unified results from each component ledger")))
        (testing "selecting subgraphs"
          (let [q '{"context" ["https://schema.org" {"value" "@value"}]
                    :select   {?goneWithTheWind [:*]}
                    :depth    3
                    :where    {"@id"  ?goneWithTheWind
                               "type" "Movie"
                               "name" "Gone with the Wind"}}]
            (is (= [{"type"                      "Movie",
                     "disambiguatingDescription" {"value" "1939 film by Victor Fleming",
                                                  "type"  "xsd:string"},
                     "isBasedOn"                 {"type"   "Book",
                                                  "author" {"type" "Person",
                                                            "name" {"value" "Margaret Mitchell",
                                                                    "type"  "xsd:string"},
                                                            "id"   "https://www.wikidata.org/wiki/Q173540"},
                                                  "isbn"   {"value" "0-582-41805-4",
                                                            "type"  "xsd:string"},
                                                  "name"   {"value" "Gone with the Wind",
                                                            "type"  "xsd:string"},
                                                  "id"     "https://www.wikidata.org/wiki/Q2870"},
                     "name"                      {"value" "Gone with the Wind",
                                                  "type"  "xsd:string"},
                     "titleEIDR"                 {"value" "10.5240/FB0D-0A93-CAD6-8E8D-80C2-4",
                                                  "type"  "xsd:string"},
                     "id"                        "https://www.wikidata.org/wiki/Q2875"}]
                   @(fluree/query dataset q))
                "returns unified results for the requested subject"))))
      (testing "with separate data sets"
        (testing "directly selecting variables"
          (let [q '{"@context" "https://schema.org"
                    :select    [?movieName ?bookIsbn ?authorName]
                    :where     [[:graph "test/movies" {"id"        ?movie
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
                   @(fluree/query dataset q))
                "returns unified results from each component ledger")))
        (testing "selecting subgraphs"
          (let [q '{"context" ["https://schema.org", {"value" "@value"}]
                    :select   {?goneWithTheWind [:*]}
                    :depth    3
                    :where    [[:graph "test/movies" {"@id"  ?goneWithTheWind
                                                      "name" "Gone with the Wind"}]]}]
            (is (= [{"type"                      "Movie",
                     "disambiguatingDescription" {"value" "1939 film by Victor Fleming",
                                                  "type"  "xsd:string"},
                     "isBasedOn"                 {"type"   "Book",
                                                  "author" {"type" "Person",
                                                            "name" {"value" "Margaret Mitchell",
                                                                    "type"  "xsd:string"},
                                                            "id"   "https://www.wikidata.org/wiki/Q173540"},
                                                  "isbn"   {"value" "0-582-41805-4",
                                                            "type"  "xsd:string"},
                                                  "name"   {"value" "Gone with the Wind",
                                                            "type"  "xsd:string"},
                                                  "id"     "https://www.wikidata.org/wiki/Q2870"},
                     "name"                      {"value" "Gone with the Wind",
                                                  "type"  "xsd:string"},
                     "titleEIDR"                 {"value" "10.5240/FB0D-0A93-CAD6-8E8D-80C2-4",
                                                  "type"  "xsd:string"},
                     "id"                        "https://www.wikidata.org/wiki/Q2875"}]
                   @(fluree/query dataset q))
                "returns unified results for the requested subject")))))))
