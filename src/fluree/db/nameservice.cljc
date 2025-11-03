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
    "Publishes new commit.")
  (retract [publisher ledger-alias]
    "Remove the nameservice record for the ledger.")
  (publishing-address [publisher ledger-alias]
    "Returns the value to write into the commit's ns field for this nameservice.
    This may be a full address/IRI (e.g., fluree:ipns://...) or a resolvable
    identifier such as a ledger alias (e.g., ledger:branch), depending on the
    nameservice implementation. The returned value will be used with this same
    nameservice's lookup function. If publishing should be private, return nil.")
  (index-start [publisher ledger-alias target-t machine-id]
    "Marks the start of an indexing process for the specified target-t.

    Parameters:
      publisher - The nameservice publisher
      ledger-alias - The ledger being indexed
      target-t - The 't' value being indexed
      machine-id - Machine identifier (hostname:pid) performing the indexing

    Returns a channel that will contain:
    - {:status :started} on success
    - {:status :already-indexing, :started <iso>, :machine-id <id>, :last-heartbeat <iso>}
      if indexing already in progress and not stale (heartbeat within 5 min)")
  (index-heartbeat [publisher ledger-alias]
    "Updates the last-heartbeat timestamp for an in-progress indexing operation.

    Should be called periodically (every ~60 seconds) during indexing to indicate
    the process is still active. If heartbeat stops for > 5 minutes, the indexing
    is considered stale and another process can take over.

    Returns a channel that will contain:
    - {:status :updated} on success
    - {:status :not-indexing} if no indexing in progress")
  (index-finish [publisher ledger-alias]
    "Marks the completion of an indexing process.

    Clears the indexing metadata from the nameservice record.

    Returns a channel that will contain:
    - {:status :completed}"))

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
       (map (fn [ns]
              (go
                (try*
                  (<? (publish ns commit-jsonld))
                  (catch* e
                    (log/warn e "Publisher failed to publish commit")
                    ::publishing-error)))))
       async/merge))

(defn published-ledger?
  [nsv ledger-alias]
  (go-try
    (let [addr (<? (publishing-address nsv ledger-alias))]
      (boolean (<? (lookup nsv addr))))))
