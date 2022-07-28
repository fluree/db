(ns fluree.db.conn.state-machine
  (:require [fluree.db.util.core :as util]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.log :as log]
            [clojure.core.async :as async]))

;; state machine for connections

#?(:clj (set! *warn-on-reflection* true))


(comment
  ;; state machine looks like this:
  {:ledger {"ledger-a" {:event-fn :main-system-event-fn     ;; returns async-chan response once complete
                        :subs     {:sub-id :sub-fn}         ;; active subscriptions
                        ;; map of branches, along with current/default branch
                        :branches {}
                        :branch   {}

                        }}
   :await  {:msg-id :async-res-ch}                          ;; map of msg-ids to response chans for messages awaiting responses
   :stats  {}                                               ;; any stats about the connection itself
   }

  )


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

(defn clear-state
  []

  )


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


