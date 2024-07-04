(ns json-ld
  (:require [fluree.db.method.ipfs.core :as ipfs]
            [fluree.db.db.json-ld :as jld-db]
            [fluree.db.transact :as jld-tx]
            [clojure.core.async :as async]
            [fluree.db.flake :as flake]
            [fluree.db.api :as fluree]
            [fluree.db.util.async :refer [<?? go-try channel?]]
            [fluree.db.query.range :as query-range]
            [fluree.db.constants :as const]
            [fluree.db.did :as did]
            [fluree.db.connection :as connection]
            [fluree.db.util.json :as json]
            [fluree.json-ld :as json-ld]
            [fluree.db.util.log :as log]))


(comment

  (-> (fluree/connect-ipfs
        {:server nil         ;; use default
         :ipns {:key "self"} ;; publish to ipns by default using the provided key/profile
         :context {"id" "@id"
                   "type" "@type"
                   "schema" "http://schema.org/"
                   "rdf" "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                   "rdfs" "http://www.w3.org/2000/01/rdf-schema#"
                   "wiki" "https://www.wikidata.org/wiki/"
                   "skos" "http://www.w3.org/2008/05/skos#"
                   "f" "https://ns.flur.ee/ledger#"}
         :did (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c")})
      (.then (fn [conn]
               (println "conn" (pr-str conn))
               (-> (fluree/create conn "test/dan1")
                   (.then (fn [ledger]
                            (println "ledger" (pr-str ledger))
                            (-> (fluree/query (fluree/db ledger)
                                              {:select {'?s [:* {:f/role [:*]}]}
                                               :where [['?s :type :f/DID]]})
                                (.then (fn [q0] (println "q0" q0))))



                            (-> (fluree/stage
                                  ledger
                                  {"@context" "https://schema.org",
                                   "@id" "https://www.wikidata.org/wiki/Q836821",
                                   "@type" ["Movie"],
                                   "name" "The Hitchhiker's Guide to the Galaxy",
                                   "disambiguatingDescription" "2005 British-American comic science fiction film directed by Garth Jennings",
                                   "titleEIDR" "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                                   "isBasedOn" {"@id" "https://www.wikidata.org/wiki/Q3107329",
                                                "@type" "Book",
                                                "name" "The Hitchhiker's Guide to the Galaxy",
                                                "isbn" "0-330-25864-8",
                                                "author" {"@id" "https://www.wikidata.org/wiki/Q42"
                                                          "@type" "Person"
                                                          "name" "Douglas Adams"}}})
                                (.then (fn [ledger]
                                         (fluree/query ledger
                                                       {:select [:* {:schema/isBasedOn [:*]}]
                                                        :from :wiki/Q836821})))
                                (.then (fn [q1] (println "q1" (pr-str q1)))))

                            (-> (fluree/stage
                                  ledger
                                  {"@context" "https://schema.org",
                                   "@graph" [{"@id" "https://www.wikidata.org/wiki/Q836821"
                                              "name" "NEW TITLE: The Hitchhiker's Guide to the Galaxy",
                                              "commentCount" 42}]})
                                (.then (fn [ledger]
                                         (fluree/query ledger
                                                       {:select [:* {:schema/isBasedOn [:*]}]
                                                        :from :wiki/Q836821})))
                                (.then (fn [q2] (println "q2" q2))))

                            (fluree/commit! ledger {:message "First commit: 2 transactions!" :push? true})

                            #_ (fluree/stage ledger {"@context" "https://schema.org",
                                                     "@graph" [{"@id" "https://www.wikidata.org/wiki/Q836821"
                                                                "commentCount" 52}]})

                            #_ (fluree/commit! ledger {:message "Another commit!!"
                                                       :push? false})))
                   #_(.catch (fn [err] (println "ERR" (pr-str err))))))))









  @(fluree/query (fluree/db ledger)
                 {:select {'?s [:* {:f/role [:*]}]}
                  :where  [['?s :type :f/DID]]})

  (def newdb
    @(fluree/stage
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


  @(fluree/query newdb
                 {:select [:* {:schema/isBasedOn [:*]}]
                  :from   :wiki/Q836821})


  (def db2 @(fluree/stage
              newdb
              {"@context" "https://schema.org",
               "@graph"   [{"@id"          "https://www.wikidata.org/wiki/Q836821"
                            "name"         "NEW TITLE: The Hitchhiker's Guide to the Galaxy",
                            "commentCount" 42}]}))

  @(fluree/query db2
                 {:select [:* {:schema/isBasedOn [:*]}]
                  :from   :wiki/Q836821})

  ;; commit metadata will show IPFS address
  (-> @(fluree/commit! db2 {:message "First commit contains two transactions!"
                            :push?   true})
      :commit)

  (def db3 @(fluree/stage
              ledger
              {"@context" "https://schema.org",
               "@graph"   [{"@id"          "https://www.wikidata.org/wiki/Q836821"
                            "commentCount" 52}]}))

  @(fluree/commit! db3 {:message "Another commit!!"
                        :push?   false})

  (-> ledger
      fluree/db
      :commit)

  ;; load ledger from disk
  (def loaded-ledger @(fluree/load ipfs-conn "fluree:ipfs://QmPTXAvmWrmcbqAPxY82N6nRcLUyEWP51UZVtq15CDMVYs"))

  @(fluree/query (fluree/db loaded-ledger)
                 {:select [:* {:schema/isBasedOn [:*]}]
                  :from   :wiki/Q836821})


  (def file-conn (fluree/connect
                   {:method       :file
                    :storage-path "data/storage"
                    :publish-path "data/publish"
                    :did          (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c")}))


  (def ledger @(fluree/create ipfs-conn "test/db1"
                              {:id "fluree:ipns:k51qzi5uqu5dljuijgifuqz9lt1r45lmlnvmu3xzjew9v8oafoqb122jov0mr2"}))
  (def latest-db (fluree/db ledger))


  (fluree/status ledger)


  @(fluree/query latest-db {:select [:* {:f/function [:*]}]
                            :from   :f/Rule})

  @(fluree/query latest-db {:select {'?s [:* {:f/role [:*]}]}
                            :where  [['?s :type :f/DID]]})



                                        ;(def l1 (fluree/create file-conn "test/db1"))

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


  ;; query for Book with reverse reference
  @(fluree/query db2 {:context {:derivedFrom {"@reverse" "http://schema.org/isBasedOn"}}
                      :select  [:* {:derivedFrom [:*]}]
                      :from    :wiki/Q3107329})



  ;; this will update 'ledger'
  (fluree/commit! db2 {:message "First commit contains two transactions!"
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


  ;; this will update 'ledger'
  (fluree/commit! db4* {:message "Second commit, should only have one transaction (squashed)"})

  ;; get latest db, should be = to db4*
  (def latest-db (fluree/db ledger)))
