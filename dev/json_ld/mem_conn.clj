(ns json-ld.mem-conn
  (:require [fluree.db :as fluree]
            [fluree.db.util.async :refer [<?? go-try channel?]]
            [fluree.db.did :as did]
            [fluree.db.util.log :as log]))



(comment

  (def mem-conn @(fluree/connect-memory
                   {:defaults {:context {:id     "@id"
                                         :type   "@type"
                                         :schema "http://schema.org/"
                                         :rdf    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                                         :rdfs   "http://www.w3.org/2000/01/rdf-schema#"
                                         :wiki   "https://www.wikidata.org/wiki/"
                                         :skos   "http://www.w3.org/2008/05/skos#"
                                         :f      "https://ns.flur.ee/ledger#"}
                               :did     (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c")}}))

  (def ledger @(fluree/create mem-conn "test/db1" {}))

  @(fluree/query (fluree/db ledger)
                 {:select {'?s [:* {:f/role [:*]}]}
                  :where  [['?s :type :f/DID]]})

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


  @(fluree/query newdb
                 {:select [:* {:schema/isBasedOn [:*]}]
                  :from   :wiki/Q836821})


  (def db2 @(fluree/stage
              newdb
              {"@context" "https://schema.org",
               "@graph"   [{"id"           "https://www.wikidata.org/wiki/Q836821"
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
               "@graph"   [{"id"           "https://www.wikidata.org/wiki/Q836821"
                            "commentCount" 52}]}))

  @(fluree/query db3
                 {:select [:* {:schema/isBasedOn [:*]}]
                  :from   :wiki/Q836821})

  @(fluree/commit! db3 {:message "Another commit!!"})


  ;; load ledger from address
  (def loaded-ledger @(fluree/load mem-conn "fluree:memory://test/db1"))

  @(fluree/query (fluree/db loaded-ledger)
                 {:select [:* {:schema/isBasedOn [:*]}]
                  :from   :wiki/Q836821})

  )