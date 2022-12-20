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
  (store/address store :ledger-entry (str ledger-name "/entry/" entry-id)))

(defn create
  [store {:keys [ledger/head ledger/name]} commit-summary db-summary]
  (let [prev-commit           (:entry/commit head)
        prev-index            (:entry/db head)
        entry                 (cond-> {:type :ledger-entry
                                       :entry/time (util/current-time-iso)}
                                head           (assoc :entry/previous (:entry/address head))
                                commit-summary (assoc :entry/commit-summary (or commit-summary prev-commit))
                                db-summary     (assoc :entry/db-summary (or db-summary prev-index)))
        entry-id              (create-entry-id entry)
        {entry-hash :id/hash} (ident/id-parts entry-id)
        entry-address         (create-entry-address store name entry-hash)]
    (assoc entry :id entry-id :entry/address entry-address)))
