(ns fluree.db.nameservice.storage-backed
  (:require [clojure.core.async :refer [go]]
            [fluree.db.storage :as storage]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.util.bytes :as bytes]))

(defrecord StorageBackedNameService [store address-prefix]
  nameservice/Publisher
  (-push [_ commit-jsonld]
    (let [ledger-alias (get commit-jsonld "alias")
          ns-address   (nameservice/full-address address-prefix ledger-alias)
          record       (nameservice/ns-record ns-address commit-jsonld)
          record-bytes (bytes/string->UTF8 record)]
      (storage/write-bytes store ns-address record-bytes)))

  nameservice/iNameService
  (-lookup [_ ledger-alias]
    (let [ns-address (nameservice/full-address address-prefix ledger-alias)]
      (storage/read-bytes store ns-address)))

  (-address [_ ledger-alias branch]
    (go
      (let [branch (if branch (name branch) "main")]
        (str address-prefix ledger-alias "/" branch))))

  (-close [_]
    true))
