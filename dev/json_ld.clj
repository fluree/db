(ns json-ld
  (:require [fluree.db.method.ipfs.core :as ipfs]
            [fluree.db.api :as fdb]
            [fluree.db.json-ld-db :as jld-db]
            [fluree.db.json-ld.flakes :as jld-flakes]
            [fluree.db.json-ld.transact :as jld-tx]
            [clojure.core.async :as async]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.async :refer [<?? go-try channel?]]
            [fluree.db.query.range :as query-range]
            [fluree.db.constants :as const]))

(comment

  (def ipfs-conn (fluree/connect-ipfs
                   {:server  nil                            ;; use default
                    :context {"schema" "http://schema.org/"
                              "wiki"   "https://www.wikidata.org/wiki/"}
                    :did     {:id      "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"
                              :private "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"
                              :public  "030be728546a7fe37bb527749e19515bd178ba8a5485ebd1c37cdf093cf2c247ca"}}))

  ipfs-conn

  (def ledger (fluree/create ipfs-conn "test/db1"))


  ledger


  ;; db will contain changes immutably, but link to the main ledger which won't
  ;; be updated until there is a commit
  (def db (fluree/stage
            ledger
            {"@context"                  "https://schema.org",
             "@id"                       "https://www.wikidata.org/wiki/Q836821",
             "@type"                     ["Movie"],
             "name"                      "The Hitchhiker's Guide to the Galaxy",
             "disambiguatingDescription" "2005 British-American comic science fiction film directed by Garth Jennings",
             "titleEIDR"                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
             "isBasedOn"                 {"@id"    "https://www.wikidata.org/wiki/Q3107329",
                                          "@type"  "Book",
                                          "name"   "The Hitchhiker's Guide to the Galaxy",
                                          "isbn"   "0-330-25864-8",
                                          "author" {"@id"   "https://www.wikidata.org/wiki/Q42"
                                                    "@type" "Person"
                                                    "name"  "Douglas Adams"}}}))

  db

  (def db2 (fluree/stage
             db
             {"@context" "https://schema.org",
              "@graph"   [{"@id"          "https://www.wikidata.org/wiki/Q836821"
                           "name"         "NEW TITLE: The Hitchhiker's Guide to the Galaxy",
                           "commentCount" 42}]}))

  db2

  ;; query for Movie and crawl to book
  @(fluree/query db2 {:context {:id     "@id"
                                :type   "@type"
                                :schema "http://schema.org/"
                                :wiki   "https://www.wikidata.org/wiki/"}
                      :select  [:* {:schema/isBasedOn [:*]}]
                      :from    :wiki/Q836821})


  ;; query for Book with reverse reference
  @(fluree/query db2 {:context {:id          "@id"
                                :type        "@type"
                                :schema      "http://schema.org/"
                                :wiki        "https://www.wikidata.org/wiki/"
                                :derivedFrom {"@reverse" "http://schema.org/isBasedOn"}}
                      :select  [:* {:derivedFrom [:*]}]
                      :from    :wiki/Q3107329})



  ;; this will update 'ledger'
  (fluree/commit db2 {:message "First commit contains two transactions!"
                      :push?   false})



  (def db3 (fluree/stage db2
                         {"@context" "https://schema.org",
                          "@graph"   [{"@id"          "https://www.wikidata.org/wiki/Q836821"
                                       "commentCount" 52}]}))

  (def db4 (fluree/stage db3
                         {"@context" "https://schema.org",
                          "@graph"   [{"@id"          "https://www.wikidata.org/wiki/Q836821"
                                       "commentCount" 62}]}))

  (-> db4 :novelty :tspo)

  ;; squash last two commits into a single commit
  (def db4* (fluree/squash db4))


  ;; this will update 'ledger'
  (fluree/commit db4* {:message "Second commit, should only have one transaction (squashed)"})

  ;; get latest db, should be = to db4*
  (def latest-db (fluree/db ledger))

  )
