(ns indexing-test
  (:require [fluree.db.method.ipfs.core :as ipfs]
            [fluree.db.flake.flake-db :as flake-db]
            [fluree.db.json-ld.transact :as jld-tx]
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
            [fluree.db.indexer.default :as indexer]
            [fluree.db.indexer :as indexer]
            [fluree.db.util.log :as log]
            [fluree.db.flake.index :as index]))

(comment

  (def ipfs-conn @(fluree/connect-ipfs
                    {:server   nil                          ;; use default
                     ;; ledger defaults used for newly created ledgers
                     :defaults {:ipns    {:key "self"}      ;; publish to ipns by default using the provided key/profile
                                :indexer {:reindex-min-bytes 9000
                                          :reindex-max-bytes 10000000}
                                :context {:id     "@id"
                                          :type   "@type"
                                          :schema "http://schema.org/"
                                          :rdf    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                                          :rdfs   "http://www.w3.org/2000/01/rdf-schema#"
                                          :wiki   "https://www.wikidata.org/wiki/"
                                          :skos   "http://www.w3.org/2008/05/skos#"
                                          :f      "https://ns.flur.ee/ledger#"}
                                :did     (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c")}}))


  (def ledger @(fluree/create ipfs-conn "test/db1" {}))

  (def newdb
    @(fluree/stage
       ledger
       {"@context"                  "https://schema.org",
        "id"                        "https://www.wikidata.org/wiki/Q836821",
        "type"                      ["Movie"],
        "name"                      "The Hitchhiker's Guide to the Galaxy",
        "disambiguatingDescription" "2005 British-American comic science fiction film directed by Garth Jennings",
        "titleEIDR"                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
        "isBasedOn"                 {"id"     "https://www.wikidata.org/wiki/Q3107329",
                                     "type"   "Book",
                                     "name"   "The Hitchhiker's Guide to the Galaxy",
                                     "isbn"   "0-330-25864-8",
                                     "author" {"@id"   "https://www.wikidata.org/wiki/Q42"
                                               "@type" "Person"
                                               "name"  "Douglas Adams"}}}))


  (-> @(fluree/commit! newdb {:message "First commit!"
                              :push?   true})
      :commit)


  @(fluree/query (fluree/db ledger)
                 {:select [:* {:schema/isBasedOn [:*]}]
                  :from   :wiki/Q836821})

  @(fluree/query (fluree/db ledger)
                 {:select {'?s [:* {:schema/isBasedOn [:*]}]}
                  :where  [['?s :type :schema/Movie]]})

  (def db2 @(fluree/stage
              ledger
              {"@context" "https://schema.org",
               "@graph"   [{"id"           "https://www.wikidata.org/wiki/Q836821"
                            "commentCount" 52}]}))


  @(fluree/query db2
                 {:select [:* {:schema/isBasedOn [:*]}]
                  :from   :wiki/Q836821})



  (-> @(fluree/commit! db2 {:message "Second commit contains an update"
                              :push?   true})
      :commit)


  (def loaded-ledger @(fluree/load ipfs-conn "fluree:ipns://k51qzi5uqu5dljuijgifuqz9lt1r45lmlnvmu3xzjew9v8oafoqb122jov0mr2/test/db1"))

  @(fluree/query (fluree/db loaded-ledger)
                 {:select {'?s [:* {:schema/isBasedOn [:*]}]}
                  :where  [['?s :type :schema/Movie]]})

  (-> (fluree/db loaded-ledger)
      :commit)

  (def db3 @(fluree/stage
              loaded-ledger
              {"@context" "https://schema.org",
               "@graph"   [{"id"           "https://www.wikidata.org/wiki/Q836821"
                            "commentCount" 62}]}))

  @(fluree/query db3
                 {:select {'?s [:* {:schema/isBasedOn [:*]}]}
                  :where  [['?s :type :schema/Movie]]})

  (-> @(fluree/commit! db3 {:message "Third commit, from loaded ledger"
                            :push?   true})
      :commit)



  )
