(ns fluree.db.nameservice.stored
  (:require [fluree.db.storage :as storage]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.util.bytes :as bytes]))

(defn full-address
  [prefix ledger-alias]
  (str prefix ledger-alias))

(defn ns-record
  "Generates nameservice metadata map for JSON storage. For now, since we only
  have a single branch possible, always sets default-branch. Eventually will
  need to merge changes from different branches into existing metadata map"
  [ns-address {address "address", alias "alias", branch "branch", :as commit-jsonld}]
  (let [branch-iri (str ns-address "(" branch ")")]
    {"@context"      "https://ns.flur.ee/ledger/v1"
     "@id"           ns-address
     "defaultBranch" branch-iri
     "ledgerAlias"   alias
     "branches"      [{"@id"     branch-iri
                       "address" address
                       "commit"  commit-jsonld}]}))

(defrecord StorageBackedNameService [store address-prefix]
  nameservice/Publisher
  (-push [_ commit-jsonld]
    (let [ledger-alias (get commit-jsonld "alias")
          ns-address   (full-address address-prefix ledger-alias)
          record       (ns-record ns-address commit-jsonld)
          record-bytes (bytes/string->UTF8 record)]
      (storage/write-bytes store ns-address record-bytes)))

  nameservice/iNameService
  (-lookup [_ ledger-alias]
    (let [ns-address (full-address address-prefix ledger-alias)]
      (storage/read store ns-address)))
  )
