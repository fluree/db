(ns fluree.db.connection
  (:require [clojure.core.async :as async]
            [clojure.pprint :as pprint]
            [clojure.string :as str]
            [fluree.db.constants :as const]
            [fluree.db.storage :as storage]
            [fluree.db.util.core :as util :refer [get-first-value]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.json-ld :as json-ld]
            [fluree.db.ledger :as ledger])
  #?(:clj (:import (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol iConnection
  (-did [conn] "Returns optional default did map if set at connection level")
  (-nameservices [conn] "Returns a sequence of all nameservices configured for the connection."))

(defprotocol iStorage
  (-c-read [conn commit-key] "Reads a commit from storage")
  (-c-write [conn ledger-alias commit-data] "Writes a commit to storage")
  (-txn-write [conn ledger-alias txn-data] "Writes a transaction to storage and returns the key. Expects string keys.")
  (-txn-read [conn txn-key] "Reads a transaction from storage"))

(comment
 ;; state machine looks like this:
 {:ledger {"ledger-a" {:event-fn :main-system-event-fn ;; returns async-chan response once complete
                       :subs     {:sub-id :sub-fn} ;; active subscriptions
                       ;; map of branches, along with current/default branch
                       :branches {}
                       :branch   {}}}


  :await  {:msg-id :async-res-ch} ;; map of msg-ids to response chans for messages awaiting responses
  :stats  {}}) ;; any stats about the connection itself


(defn blank-state
  "Returns top-level state for connection"
  []
  (atom
   {:ledger {}
    :await  {}
    :stats  {}}))

(defn register-ledger
  "Creates a promise-chan and saves it in a cache of ledgers being held
  in-memory on the conn.

  Returns a two-tuple of
  [not-cached? promise-chan]

  where not-cached? is true if a new promise-chan was created, false if an
  existing promise-chan was found.

  promise-chan is the new promise channel that must have the final ledger `put!` into it
  assuming success? is true, otherwise it will return the existing found promise-chan when
  success? is false"
  [{:keys [state] :as _conn} ledger-alias]
  (let [new-p-chan (async/promise-chan)
        new-state  (swap! state update-in [:ledger ledger-alias]
                           (fn [existing]
                             (or existing new-p-chan)))
        p-chan     (get-in new-state [:ledger ledger-alias])
        cached?    (not= p-chan new-p-chan)]
    (log/debug "Registering ledger: " ledger-alias " cached? " cached?)
    [cached? p-chan]))

(defn release-ledger
  "Opposite of register-ledger. Removes reference to a ledger from conn"
  [{:keys [state] :as _conn} ledger-alias]
  (swap! state update :ledger dissoc ledger-alias))

(defn cached-ledger
  "Returns a cached ledger from the connection if it is cached, else nil"
  [{:keys [state] :as _conn} ledger-alias]
  (get-in @state [:ledger ledger-alias]))

(defn notify-ledger
  [conn commit-map]
  (go-try
    (let [expanded-commit (json-ld/expand commit-map)
          ledger-alias    (get-first-value expanded-commit const/iri-alias)]
      (if ledger-alias
        (if-let [ledger-c (cached-ledger conn ledger-alias)]
          (<? (ledger/-notify (<? ledger-c) expanded-commit))
          (log/debug "No cached ledger found for commit: " commit-map))
        (log/warn "Notify called with a data that does not have a ledger alias."
                  "Are you sure it is a commit?: " commit-map)))))

(defn printer-map
  "Returns map of important data for print writer"
  [conn]
  {:id    (:id conn)
   :stats (get @(:state conn) :stats)})

(defrecord Connection [id state parallelism store index-store primary-ns
                       aux-nses serializer cache defaults]
  iStorage
  (-c-read [_ commit-address]
    (storage/read-json store commit-address))
  (-c-write [_ ledger-alias commit-data]
    (let [path (str/join "/" [ledger-alias "commit"])]
      (storage/content-write-json store path commit-data)))
  (-txn-read [_ txn-address]
    (storage/read-json store txn-address))
  (-txn-write [_ ledger-alias txn-data]
    (let [path (str/join "/" [ledger-alias "txn"])]
      (storage/content-write-json store path txn-data)))

  iConnection
  (-did [_] (:did defaults))
  (-nameservices [_]
    (into [primary-ns] aux-nses)))

#?(:clj
   (defmethod print-method Connection [^Connection conn, ^Writer w]
     (.write w (str "#fluree/Connection "))
     (binding [*out* w]
       (pr (printer-map conn))))
   :cljs
     (extend-type Connection
       IPrintWithWriter
       (-pr-writer [conn w _opts]
         (-write w "#fluree/Connection ")
         (-write w (pr (printer-map conn))))))

(defmethod pprint/simple-dispatch Connection [^Connection conn]
  (pr conn))

(defn connect
  [{:keys [parallelism store index-store cache serializer primary-ns
           aux-nses defaults]
    :or   {serializer (json-serde)} :as _opts}]
  (let [id    (random-uuid)
        state (blank-state)]
    (->Connection id state parallelism store index-store primary-ns aux-nses
                  serializer cache defaults)))
