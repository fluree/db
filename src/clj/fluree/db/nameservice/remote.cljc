(ns fluree.db.nameservice.remote
  (:require [fluree.db.nameservice :as nameservice]
            [fluree.db.remote-system :as remote]
            [clojure.core.async :as async :refer [<! go go-loop]]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn remote-lookup
  [sys ledger-address]
  (go-try
    (let [head-commit  (<? (remote/remote-read sys ledger-address false))
          head-address (get head-commit "address")]
      head-address)))

(defn monitor-socket-messages
  [{:keys [state msg-in] :as _remote-ns} websocket]
  (go-loop []
    (let [next-msg (<! msg-in)]
      (if next-msg
        (let [[_ message] next-msg
              parsed-msg (json/parse message false)
              ledger     (get parsed-msg "ledger")
              callback   (get-in @state [:subscription ledger])]
          (if callback
            (try*
              (callback parsed-msg)
              (catch* e
                      (log/error "Subscription callback for ledger: " ledger " failed with error: " e)))
            (log/warn "No callback registered for ledger: " ledger))
          (recur))
        (do
          (log/info "Websocket messaging connection closed, closing websocket.")
          (remote/close-websocket websocket))))))

(defn launch-subscription-socket
  "Returns chan with websocket after successful connection, or exception. "
  [{:keys [remote-system msg-in msg-out] :as remote-ns}]
  (go
    (let [ws (<! (remote/ws-connect remote-system msg-in msg-out))]
      (if (util/exception? ws)
        (do
          (log/error "Error establishing websocket connection: " (ex-message ws))
          (ex-info (str "Error establishing websocket connection: " (ex-message ws))
                   {:status 400
                    :error  :db/websocket-error}))
        (do
          (log/info "Websocket connection established.")
          (monitor-socket-messages remote-ns ws)
          ws)))))

(defn subscribe
  [ns-state ledger-alias callback]
  (if (fn? callback)
    (swap! ns-state assoc-in [:subscription ledger-alias] callback)
    (throw (ex-info (str "Subscription request for " ledger-alias
                         " failed. Callback must be a function, provided: " (pr-str callback))
                    {:status 400
                     :error  :db/invalid-fn}))))

(defn unsubscribe
  [ns-state ledger-alias]
  (swap! ns-state update :subscription dissoc ledger-alias))

(defrecord RemoteNameService [state remote-system msg-in msg-out]
  nameservice/iNameService
  (lookup [_ ledger-address]
    (remote-lookup remote-system ledger-address))
  (address [_ ledger-alias]
    (go ledger-alias))
  (alias [_ ledger-address]
    ledger-address)
  (-close [_]
    (async/close! msg-in)
    (async/close! msg-out))

  nameservice/Publication
  (-subscribe [_ ledger-alias callback]
    (subscribe state ledger-alias callback))
  (-unsubscribe [_ ledger-alias]
    (unsubscribe state ledger-alias)))

(defn initialize
  [remote-system]
  (go-try
    (let [msg-in    (async/chan)
          msg-out   (async/chan)
          remote-ns (map->RemoteNameService {:remote-system remote-system
                                             :msg-in        msg-in
                                             :msg-out       msg-out
                                             :state         (atom nil)})
          websocket (<! (launch-subscription-socket remote-ns))]
      (if (util/exception? websocket)
        (nameservice/-close remote-ns)
        remote-ns))))
