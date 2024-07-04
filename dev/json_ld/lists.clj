(ns json-ld.lists
  (:require [fluree.db.method.ipfs.core :as ipfs]
            [fluree.db.db.json-ld :as jld-db]
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
            [fluree.db.index :as index]))

(comment

  (def ipfs-conn @(fluree/connect-ipfs
                    {:server   nil                          ;; use default
                     ;; ledger defaults used for newly created ledgers
                     :defaults {:ipns    {:key "self"}      ;; publish to ipns by default using the provided key/profile
                                :indexer {:reindex-min-bytes 5000
                                          :reindex-max-bytes 10000000}
                                :context {:id     "@id"
                                          :type   "@type"
                                          :schema "http://schema.org/"
                                          :sh     "http://www.w3.org/ns/shacl#"
                                          :rdf    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                                          :rdfs   "http://www.w3.org/2000/01/rdf-schema#"
                                          :wiki   "https://www.wikidata.org/wiki/"
                                          :skos   "http://www.w3.org/2008/05/skos#"
                                          :f      "https://ns.flur.ee/ledger#"}
                                :did     (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c")}}))


  (def ledger @(fluree/create ipfs-conn "lists/a" {}))

  (-> @(fluree/stage
         ledger
         {:context   {:id "@id"
                      :ex "http://example.org/ns#"}
          :id        :ex/myRecord
          :ex/note   "Note query reorders list."
          :ex/myList [42 2 88 1]})
      (fluree/query {:context {:ex "http://example.org/ns#"}
                     :select  [:*]
                     :from    :ex/myRecord})
      deref)

  (-> @(fluree/stage
         ledger
         {:context   {:id        "@id"
                      :ex        "http://example.org/ns#"
                      :ex/myList {"@container" "@list"}}
          :id        :ex/myRecord
          :ex/note   "Don't change my list! (using context)"
          :ex/myList [42 2 88 1]})
      (fluree/query {:context {:ex "http://example.org/ns#"}
                     :select  [:*]
                     :from    :ex/myRecord})
      deref)

  (-> @(fluree/stage
         ledger
         {:context   {:id "@id"
                      :ex "http://example.org/ns#"}
          :id        :ex/myRecord
          :ex/note   "Don't change my list! (embedding directly)"
          :ex/myList {"@list" [42 2 88 1]}})
      (fluree/query {:context {:ex "http://example.org/ns#"}
                     :select  [:*]
                     :from    :ex/myRecord})
      deref)







  (def newdb
    @(fluree/stage
       ledger
       {:context   {:id        "@id"
                    :ex        "http://example.org/ns#"
                    :ex/myList {"@container" "@list"}}
        :id        :ex/myRecord
        :ex/note   "This subject uses an @list to guarantee vector result ordering"
        :ex/myList [1 9 4 8]}))

  @(fluree/query newdb
                 {:context {:ex        "http://example.org/ns#"
                            :ex/mylist {"@container" "@list"}}
                  :select  [:*]
                  :from    :ex/myRecord})


  (-> newdb
      :novelty
      :spot)

  (-> @(fluree/commit! newdb {:message "First commit!"
                              :push?   true})
      :commit)

  (def loaded-ledger @(fluree/load ipfs-conn "fluree:ipns://k51qzi5uqu5dljuijgifuqz9lt1r45lmlnvmu3xzjew9v8oafoqb122jov0mr2/lists/a"))

  @(fluree/query (fluree/db loaded-ledger)
                 {:context {:ex        "http://example.org/ns#"
                            :ex/mylist {"@container" "@list"}}
                  :select  [:*]
                  :from    :ex/myRecord})
  )
