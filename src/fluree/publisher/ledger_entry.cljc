(ns fluree.publisher.ledger-entry
  (:require [clojure.string :as str]
            [fluree.json-ld :as json-ld]
            [fluree.common.identity :as ident]
            [fluree.common.util :as util]
            [fluree.store.api :as store]))


(defn create-entry-id
  "Identify an entry with the sha256 hash of the entry minus the id and the address."
  [entry]
  (ident/create-id "ledger-entry" (json-ld/normalize-data entry)))

(defn create-entry-address
  [store ledger-name entry-id]
  (store/address store :ledger-entry (str ledger-name "/entries/" entry-id)))

(defn create
  [store {:keys [ledger/head ledger/name]} commit-info index-info]
  (let [prev-commit              (:entry/commit head)
        prev-index               (:entry/index head)
        entry                    (cond-> {:type :ledger-entry
                                          :entry/time (util/current-time-iso)}
                                   head        (assoc :entry/previous (:entry/address head))
                                   commit-info (assoc :entry/commit (or commit-info prev-commit))
                                   index-info  (assoc :entry/index (or index-info prev-index)))
        entry-id              (create-entry-id entry)
        {entry-hash :id/hash} (ident/id-parts entry-id)
        entry-address         (create-entry-address store name entry-hash)]
    (assoc entry :id entry-id :entry/address entry-address)))
