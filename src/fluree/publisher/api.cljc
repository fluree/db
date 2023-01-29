(ns fluree.publisher.api
  "The Publisher keeps track of the current state of the ledger. It consists of a record
  that holds the id and address for the most recent commit and the most recent index for
  a given ledger. It also has a Store where it stores the LedgerCreds, verifiable
  credentials wrapping the Ledger, a durable, verifiable record of the commit and index
  updates."
  (:refer-clojure :exclude [list resolve])
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
  [publisher ledger-name {:keys [context tx-address db-address] :as opts}]
  (pub-impl/init publisher ledger-name opts))

(defn list
  [publisher]
  (pub-impl/list publisher))

(defn publish
  "Creates a ledger entry and stores it in store under its id. Returns a ledger document
  with links to the head db and the head commit."
  [publisher ledger-path {:keys [tx-summary db-summary] :as summary}]
  (pub-impl/publish publisher ledger-path summary))

(defn resolve
  "Fetch the latest"
  [publisher address]
  (pub-impl/resolve publisher address))

;; models

(def LedgerEntry pub-model/LedgerEntry)
(def Ledger pub-model/Ledger)
(def LedgerCred pub-model/LedgerCred)

(def PublisherConfig pub-model/PublisherConfig)

(def Publisher pub-model/Publisher)
