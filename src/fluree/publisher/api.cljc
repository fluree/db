(ns fluree.publisher.api
  "The Publisher keeps track of the current state of the ledger. It consists of a record
  that holds the id and address for the most recent commit and the most recent index for
  a given ledger. It also has a Store where it stores the LedgerCreds, verifiable
  credentials wrapping the Ledger, a durable, verifiable record of the commit and index
  updates."
  (:require [fluree.publisher.core :as pub-impl]
            [fluree.publisher.model :as pub-model]))

(defn start
  "Takes a configuration and returns a publisher."
  [config]
  (pub-impl/start config))

(defn stop
  "Gracefully shuts down a publisher."
  [publisher]
  (pub-impl/stop publisher))

(defn init
  "Initialize a ledger with the given name, returns the ledger address."
  #_:clj-kondo/ignore
  [publisher ledger-name {:keys [context head-address db-address] :as opts}]
  (pub-impl/init publisher ledger-name opts))

(defn push
  "Creates a ledger entry and stores it in store under its id. Returns a ledger document
  with links to the head db and the head commit."
  [publisher ledger-address info]
  (pub-impl/push publisher ledger-address info))

(defn pull
  "If address is an entry-address, returns the corresponding entry. If address is a
  ledger-address, fetches the latest entry ledger document."
  [publisher address]
  (pub-impl/pull publisher address))

;; models

(def EntryInfo pub-model/EntryInfo)
(def LedgerEntry pub-model/LedgerEntry)
(def Ledger pub-model/Ledger)
(def LedgerCred pub-model/LedgerCred)

(def PublisherConfig pub-model/PublisherConfig)

(def Publisher pub-model/Publisher)
