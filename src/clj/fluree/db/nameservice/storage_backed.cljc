(ns fluree.db.nameservice.storage-backed
  (:require [clojure.core.async :refer [go]]
            [fluree.db.storage :as storage]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]))

(defn local-filename
  [ledger-alias]
  (str ledger-alias ".json"))

(defrecord StorageBackedNameService [address-prefix store sync?]
  nameservice/Publisher
  (publish [_ commit-jsonld]
    (go-try
      (let [ledger-alias (get commit-jsonld "alias")
            ns-address   (nameservice/full-address address-prefix ledger-alias)
            record       (nameservice/ns-record ns-address commit-jsonld)
            record-bytes (json/stringify-UTF8 record)
            filename     (local-filename ledger-alias)]
        (<? (storage/write-bytes store filename record-bytes)))))

  nameservice/iNameService
  (-lookup [_ ledger-address]
    (go-try
      (let [{:keys [alias _branch]} (nameservice/resolve-address address-prefix ledger-address nil)
            filename                (local-filename alias)]
        (when-let [record-bytes (<? (storage/read-bytes store filename))]
          (let [ns-record (json/parse record-bytes false)]
            (nameservice/commit-address-from-record ns-record nil))))))

  (-address [_ ledger-alias _branch]
    (go
      (str address-prefix ledger-alias)))

  (-sync? [_]
    sync?)

  (-close [_]
    true))

(defn start
  [address-prefix store sync?]
  (->StorageBackedNameService address-prefix store sync?))
