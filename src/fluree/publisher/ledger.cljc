(ns fluree.publisher.ledger
  (:require
   [fluree.common.identity :as ident]
   [fluree.common.iri :as iri]
   [fluree.common.util :as util]
   [fluree.store.api :as store]))

(defn ledger-path
  [ledger-name]
  (str "ledger/" ledger-name))

(defn create-ledger-address
  [store ledger-name]
  (store/address store "ledger" (ledger-path ledger-name)))

(defn create-ledger-entry
  [prev-ledger commit-summary db-summary]
  (let [{prev-head iri/LedgerHead} prev-ledger

        {prev-commit iri/LedgerEntryCommit
         prev-db     iri/LedgerEntryDb} prev-head]
    {iri/type               iri/LedgerEntry
     iri/LedgerEntryCreated (util/current-time-iso)
     iri/LedgerEntryCommit  (or commit-summary prev-commit)
     iri/LedgerEntryDb      (or db-summary prev-db)}))

(defn create-ledger
  [store ledger-name {:keys [context commit-address db-address] :as opts}]
  (let [address (create-ledger-address store ledger-name)]
    {iri/type iri/Ledger
     iri/id address
     iri/LedgerName ledger-name
     iri/LedgerAddress address
     iri/LedgerV 0
     iri/LedgerContext context
     iri/LedgerHead (create-ledger-entry nil
                                         {iri/CommitAddress commit-address}
                                         {iri/DbBlockAddress db-address})}))

(defn create-ledger-id
  "Create a stable ledger-id"
  [ledger-name]
  (ident/create-id "ledger" ledger-name))
