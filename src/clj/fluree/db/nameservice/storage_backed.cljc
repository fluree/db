(ns fluree.db.nameservice.storage-backed
  (:require [clojure.core.async :refer [go]]
            [clojure.string :as str]
            [fluree.db.storage :as storage]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]))

(defn local-filename
  [ledger-alias]
  (str ledger-alias ".json"))

(defrecord StorageBackedNameService [store]
  nameservice/Publisher
  (publish [_ commit-jsonld]
    (go-try
      (let [ledger-alias (get commit-jsonld "alias")
            ns-address   (nameservice/full-address (storage/location store) ledger-alias)
            record       (nameservice/ns-record ns-address commit-jsonld)
            record-bytes (json/stringify-UTF8 record)
            filename     (local-filename ledger-alias)]
        (<? (storage/write-bytes store filename record-bytes)))))

  nameservice/iNameService
  (lookup [_ ledger-address]
    (go-try
      (let [{:keys [alias _branch]} (nameservice/resolve-address (storage/location store) ledger-address nil)
            filename                (local-filename alias)]
        (when-let [record-bytes (<? (storage/read-bytes store filename))]
          (let [ns-record (json/parse record-bytes false)]
            (nameservice/commit-address-from-record ns-record nil))))))

  (address [_ ledger-alias]
    (go
      (storage/build-address (storage/location store) ledger-alias)))

  (alias [_ ledger-address]
    ;; TODO: need to validate that the branch doesn't have a slash?
    (-> (nameservice/address-path ledger-address)
        (str/split #"/")
        (->> (drop-last 2) ; branch-name, head
             (str/join #"/"))))

  (-close [_]
    true))

(defn start
  [store]
  (->StorageBackedNameService store))
