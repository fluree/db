(ns json-ld
  (:require [fluree.db.method.ipfs.core :as ipfs]
            [fluree.db.api :as fdb]
            [fluree.db.json-ld-db :as jld-db]
            [fluree.db.json-ld.transact :as jld-tx]
            [clojure.core.async :as async]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.async :refer [<?? go-try channel?]]
            [fluree.db.query.range :as query-range]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]))

(comment

  (def ipfs-conn (fluree/connect-ipfs
                   {:server  nil                            ;; use default
                    :context {:id     "@id"
                              :type   "@type"
                              :schema "http://schema.org/"
                              :rdf    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                              :wiki   "https://www.wikidata.org/wiki/"
                              :skos   "http://www.w3.org/2008/05/skos#"
                              :fluree "https://ns.flur.ee/ledger#"}
                    :did     {:id      "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"
                              :private "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"
                              :public  "030be728546a7fe37bb527749e19515bd178ba8a5485ebd1c37cdf093cf2c247ca"}}))

  (def file-conn (fluree/connect
                   {:method       :file
                    :storage-path "data/storage"
                    :publish-path "data/publish"
                    :did          {:id      "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6"
                                   :private "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"
                                   :public  "030be728546a7fe37bb527749e19515bd178ba8a5485ebd1c37cdf093cf2c247ca"}}))


  ipfs-conn

  (def ledger @(fluree/create ipfs-conn "test/db1"))
  (def latest-db (fluree/db ledger))

  (-> latest-db
      :schema)

  (-> ledger)

  @(fluree/query latest-db {:select [:* {:fluree/function [:*]}]
                            :from   "fluree-root-rule"})

  ;(def l1 (fluree/create file-conn "test/db1"))


  ledger
  ;(def x0 (fluree/stage l1
  ;                      {"@context" {:id "@id"}
  ;                       :id (str (java.util.UUID/randomUUID))
  ;                       "book/title" "Anathem"
  ;                       "book/author" "Neal Stephenson"}))
  ;(def x1 (fluree/stage x0 {"@context" {:id "@id"}
  ;                          :id (str (java.util.UUID/randomUUID))
  ;                          "book/title" "Cryptonomicon"
  ;                          "book/author" "Neal Stephenson"}))
  ;(def x2 (fluree/stage x1
  ;                      {"@context" {:id "@id"}
  ;                       :id (str (java.util.UUID/randomUUID))
  ;                       "book/title" "mistborn"
  ;                       "book/author" "brandon sanderson"}))
  ;(def x3 (fluree/commit x2 "Persist to disk!"))
  ;x3
  ;@(:publish x3)
  ;"fluree:file:/home/dan/projects/db/data/publish/HEAD"
  ;
  ;(def y0 (fluree/stage ledger
  ;                      {"@context" {:id "@id"}
  ;                       :id (str (java.util.UUID/randomUUID))
  ;                       "book/title" "Anathem"
  ;                       "book/author" "Neal Stephenson"}))
  ;(def y1 (fluree/stage y0 {"@context" {:id "@id"}
  ;                          :id (str (java.util.UUID/randomUUID))
  ;                          "book/title" "Cryptonomicon"
  ;                          "book/author" "Neal Stephenson"}))
  ;(def y2 (fluree/stage y1
  ;                      {"@context" {:id "@id"}
  ;                       :id (str (java.util.UUID/randomUUID))
  ;                       "book/title" "mistborn"
  ;                       "book/author" "brandon sanderson"}))
  ;(def y3 (fluree/commit y2 "Persist to disk!"))

  ;; db will contain changes immutably, but link to the main ledger which won't
  ;; be updated until there is a commit
  (def db @(fluree/stage
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

  (def db2 @(fluree/stage
              db
              {"@context" "https://schema.org",
               "@graph"   [{"@id"          "https://www.wikidata.org/wiki/Q836821"
                            "name"         "NEW TITLE: The Hitchhiker's Guide to the Galaxy",
                            "commentCount" 42}]}))

  (-> db2 :context)

  ;; query for Movie and crawl to book
  @(fluree/query db2 {:select [:* {:schema/isBasedOn [:*]}]
                      :from   [:wiki/Q836821 :wiki/Q3107329]})

  @(fluree/query db2 {:select {'?s [:* {:schema/isBasedOn [:*]}]}
                      :where  [['?s :id :wiki/Q836821]]})

  (fluree.db.query.analytical-parse/parse
    db2 {:select {'?s [:* {:schema/isBasedOn [:*]}]}
         :where  [['?s :id :wiki/Q836821]]})

  (-> db :novelty :spot)
  (async/<!! (query-range/index-range db2 :spot = [211106232532992 1002]))
  (async/<!! (dbproto/-subid db "https://www.wikidata.org/wiki/Q836821"))
  (async/<!! (query-range/index-range db :post = [0]))



  ;; query for Book with reverse reference
  @(fluree/query db2 {:context {:derivedFrom {"@reverse" "http://schema.org/isBasedOn"}}
                      :select  [:* {:derivedFrom [:*]}]
                      :from    :wiki/Q3107329})



  ;; this will update 'ledger'
  (fluree/commit db2 {:message "First commit contains two transactions!"
                      :push?   false})



  (def db3 @(fluree/stage db2
                          {"@context" "https://schema.org",
                           "@graph"   [{"@id"          "https://www.wikidata.org/wiki/Q836821"
                                        "commentCount" 52}]}))


  (def db4 @(fluree/stage db3
                          {"@context" "https://schema.org",
                           "@graph"   [{"@id"          "https://www.wikidata.org/wiki/Q836821"
                                        "commentCount" 62}]}))


  @(fluree/query db4 {:select [:*]
                      :from   :wiki/Q836821})


  ;; squash last two commits into a single commit
  (def db4* (fluree/squash db4))


  ;; this will update 'ledger'
  (fluree/commit db4* {:message "Second commit, should only have one transaction (squashed)"})

  ;; get latest db, should be = to db4*
  (def latest-db (fluree/db ledger))

  )
