(ns fluree.db.nameservice.storage
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

(defn publishing-address
  [store ledger-alias]
  (-> store
      storage/location
      (storage/build-address ledger-alias)))

(defrecord StorageNameService [store]
  nameservice/Publisher
  (publish [_ commit-jsonld]
    (go-try
      (let [ledger-alias (get commit-jsonld "alias")
            ns-address   (publishing-address store ledger-alias)
            record       (nameservice/ns-record ns-address commit-jsonld)
            record-bytes (json/stringify-UTF8 record)
            filename     (local-filename ledger-alias)]
        (<? (storage/write-bytes store filename record-bytes)))))

  (publishing-address [_ ledger-alias]
    (go (publishing-address store ledger-alias)))

  nameservice/iNameService
  (lookup [_ ledger-address]
    (go-try
      (let [{:keys [alias _branch]} (nameservice/resolve-address (storage/location store) ledger-address nil)
            filename                (local-filename alias)]
        (when-let [record-bytes (<? (storage/read-bytes store filename))]
          (let [ns-record (json/parse record-bytes false)]
            (nameservice/commit-address-from-record ns-record nil))))))

  (alias [_ ledger-address]
    ;; TODO: need to validate that the branch doesn't have a slash?
    (-> (storage/get-local-path ledger-address)
        (str/split #"/")
        (->> (drop-last 2) ; branch-name, head
             (str/join #"/"))))

  (-close [_]
    true))

(defn start
  [store]
  (->StorageNameService store))
