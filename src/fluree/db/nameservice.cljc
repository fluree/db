(ns fluree.db.nameservice
  (:refer-clojure :exclude [alias])
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.db.util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol iNameService
  (lookup [nameservice ledger-address]
    "Performs lookup operation on ledger alias and returns map of latest commit
    and other metadata")
  (alias [nameservice ledger-address]
    "Given a ledger address, returns ledger's default alias name else nil, if
    not avail")
  (all-records [nameservice]
    "Returns a channel containing all nameservice records for building in-memory query ledger"))

(defprotocol Publisher
  (publish [publisher commit-jsonld]
    "Publishes new commit data. This is the legacy method that writes both commit
    and index data to a single record. For independent transactor/indexer operations,
    prefer publish-commit and publish-index.")
  (publish-commit [publisher ledger-alias commit-address commit-t]
    "Publishes only commit data (address and t). This allows transactors to update
    commit information without contending with indexers. Only updates if commit-t
    is greater than the existing value.")
  (publish-index [publisher ledger-alias index-address index-t]
    "Publishes only index data (address and t). This allows indexers to update
    index information without contending with transactors. Only updates if index-t
    is greater than the existing value. Writes to a separate file/record to avoid
    contention with commit updates.")
  (retract [publisher ledger-alias]
    "Remove the nameservice record for the ledger.")
  (publishing-address [publisher ledger-alias]
    "Returns the value to write into the commit's ns field for this nameservice.
    This may be a full address/IRI (e.g., fluree:ipns://...) or a resolvable
    identifier such as a ledger alias (e.g., ledger:branch), depending on the
    nameservice implementation. The returned value will be used with this same
    nameservice's lookup function. If publishing should be private, return nil."))

(defprotocol Publication
  (subscribe [publication ledger-alias]
    "Creates a subscription to publication for ledger events. Will call
    callback with event data as received.")
  (unsubscribe [publication ledger-alias]
    "Unsubscribes to publication for ledger events")
  (known-addresses [publication ledger-alias]))

(defn publish-to-all
  [commit-jsonld publishers]
  (->> publishers
       (keep identity)
       (map (fn [ns]
              (go
                (try*
                  (<? (publish ns commit-jsonld))
                  (catch* e
                    (log/warn e "Publisher failed to publish commit" {:alias (or (get commit-jsonld "alias") (get commit-jsonld :alias))})
                    ::publishing-error)))))
       async/merge))

(defn published-ledger?
  [nsv ledger-alias]
  (go-try
    (let [addr (<? (publishing-address nsv ledger-alias))]
      (boolean (<? (lookup nsv addr))))))
