(ns fluree.db.conn.core
  (:require [clojure.core.async :as async]
            [fluree.db.constants :as const]
            [fluree.db.util.core :as util :refer [try* catch* get-first-value]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.json-ld :as json-ld]
            [fluree.db.ledger.proto :as ledger-proto]))

;; state machine for connections

#?(:clj (set! *warn-on-reflection* true))


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
    {:ledger  {}
     :await   {}
     :stats   {}
     :closed? false}))

(defn mark-closed
  [{:keys [state] :as conn}]
  (swap! state assoc :closed? true))


(defn await-response
  "Returns a promise-channel that will contain eventual response for an outgoing message."
  [{:keys [state] :as conn} message]
  (let [ledger (:ledger message)
        id     (random-uuid)
        res-ch (async/promise-chan)]
    (swap! state assoc-in [:ledger ledger :await id] res-ch)
    res-ch))

(defn subscribe
  "Creates a new subscription on given ledger where 'callback' function
  will get executed with every new message.

  Subscription id (sub-id) is opaque, and used to cancel subscription."
  [{:keys [state] :as conn} ledger callback sub-id]
  (let [id (or sub-id (random-uuid))]
    (swap! state assoc-in [:ledger ledger :subs id] callback)))

(defn- message-response
  "Checks for any pending callback functions for incoming messages.
  Calls them and clears them from state machine."
  [{:keys [state] :as conn} {:keys [id] :as msg}]
  (when-let [callback (get-in @state [:await id])]
    (swap! state update :await dissoc id)
    (try* (callback msg)
          (catch* e (log/error e "Callback function error for message: " msg))))
  true)

(defn- conn-event
  "Handles generic connection-related event coming over channel.
  First calls, and waits for response from, the main ledger callback
  function if the respective ledger is active, then calls all registered
  user/api callback functions without waiting for any responses."
  [{:keys [state] :as conn} {:keys [ledger] :as msg}]
  (async/go
    (let [{:keys [event-fn subs]} (get @state ledger)]
      (when event-fn
        (event-fn msg))
      (doseq [[id callback] subs]
        (try* (callback msg)
              (catch* e (log/error e (str "Callback function error for ledger subscription: "
                                          ledger " " id ". Message: " msg))))))))

(defn msg-from-network
  "Records an incoming message from the network.

  Fires off any 'await' calls for message, or triggers subscriptions for
  generic events."
  [conn {:keys [id] :as msg}]
  (let [request-resp? (boolean id)]
    (if request-resp?
      (message-response conn msg)
      (conn-event conn msg))))

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
  (let [new-p-chan  (async/promise-chan)
        new-state   (swap! state update-in [:ledger ledger-alias]
                           (fn [existing]
                             (or existing new-p-chan)))
        p-chan      (get-in new-state [:ledger ledger-alias])
        not-cached? (= p-chan new-p-chan)]
    (log/debug "Registering ledger: " ledger-alias " not-cached? " not-cached?)
    [not-cached? p-chan]))

(defn release-ledger
  "Opposite of register-ledger. Removes reference to a ledger from conn"
  [{:keys [state] :as _conn} ledger-alias]
  (swap! state update :ledger dissoc ledger-alias))

(defn release-all-ledgers
  "Releases all ledgers from conn.
  Typically used when closing a connection to release resources."
  [{:keys [state] :as _conn}]
  (swap! state assoc :ledger {}))

(defn cached-ledger
  "Returns a cached ledger from the connection if it is cached, else nil"
  [{:keys [state] :as _conn} ledger-alias]
  (get-in @state [:ledger ledger-alias]))

(defn notify-ledger
  [conn commit-map]
  (go-try
    (let [expanded-commit (json-ld/expand commit-map)
          ledger-alias    (get-first-value expanded-commit const/iri-alias)
          ledger          (cached-ledger conn ledger-alias)]
      (if ledger
        (<? (ledger-proto/-notify ledger expanded-commit))
        (log/debug "No cached ledger found for commit: " commit-map)))))

(defn printer-map
  "Returns map of important data for print writer"
  [conn]
  {:id              (:id conn)
   :stats           (get @(:state conn) :stats)
   :cached-ledgers  (keys (get @(:state conn) :ledgers))
   :nameservices    (mapv type (:nameservices conn))
   :ledger-defaults (:ledger-defaults conn)
   :parallelism     (:parallelism conn)})
