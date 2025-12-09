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
  (publish-commit [publisher ledger-alias commit-address commit-t]
    "Publishes only commit data (address and t). This allows transactors to update
    commit information without contending with indexers. Only updates if commit-t
    is greater than the existing value.")
  (publish-index [publisher ledger-alias index-address index-t]
    "Publishes only index data (address and t). This allows indexers to update
    index information without contending with transactors. Only updates if index-t
    is greater than the existing value. Writes to a separate file/record to avoid
    contention with commit updates.")
  (publish-vg [publisher vg-config]
    "Publishes a virtual graph configuration. The vg-config map should contain
    :vg-name, :vg-type, :config, and optionally :dependencies.")
  (retract [publisher ledger-alias]
    "Remove the nameservice record for the ledger or virtual graph.")
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

(defn primary-publisher
  "Returns the primary publisher from a publishers list."
  [publishers]
  (first publishers))

(defn secondary-publishers
  "Returns the secondary publishers from a publishers list."
  [publishers]
  (rest publishers))

(defn publish-commit-to-all
  "Publishes commit data to all publishers using atomic conditional updates.
   Each result is either the publish-commit response or ::publishing-error on failure.
   This is the safe way to publish commit updates without overwriting index data."
  [ledger-alias commit-address commit-t publishers]
  (let [pub-chs (->> publishers
                     (keep identity)
                     (mapv (fn [ns]
                             (go
                               (try*
                                 (<? (publish-commit ns ledger-alias commit-address commit-t))
                                 (catch* e
                                   (log/warn e "Publisher failed to publish commit"
                                             {:alias ledger-alias :commit-t commit-t})
                                   ::publishing-error))))))]
    (if (seq pub-chs)
      (async/into [] (async/merge pub-chs))
      (go []))))

(defn publish-index-to-all
  "Publishes index data to all publishers using atomic conditional updates.
   Each result is either the publish-index response or ::publishing-error on failure.
   This is the safe way to publish index updates without overwriting commit data."
  [ledger-alias index-address index-t publishers]
  (let [pub-chs (->> publishers
                     (keep identity)
                     (mapv (fn [ns]
                             (go
                               (try*
                                 (<? (publish-index ns ledger-alias index-address index-t))
                                 (catch* e
                                   (log/warn e "Publisher failed to publish index"
                                             {:alias ledger-alias :index-t index-t})
                                   ::publishing-error))))))]
    (if (seq pub-chs)
      (async/into [] (async/merge pub-chs))
      (go []))))

(defn published-ledger?
  [nsv ledger-alias]
  (go-try
    (let [addr (<? (publishing-address nsv ledger-alias))]
      (boolean (<? (lookup nsv addr))))))
