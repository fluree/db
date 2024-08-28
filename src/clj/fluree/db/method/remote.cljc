(ns fluree.db.method.remote
  (:require [fluree.db.util.xhttp :as xhttp]
            [clojure.string :as str]
            [clojure.core.async :as async]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]))

(def message-codes {:subscribe-ledger   1
                    :unsubscribe-ledger 2})

(defn pick-server
  "Currently does just a round-robin selection if multiple servers are given.
  TODO - add re-tries with a different server if fails to connect. Consider keeping stats to select optimal server."
  [server-state]
  (or (:connected-to @server-state)
      (-> (swap! server-state (fn [{:keys [connected-to servers] :as ss}]
                                (if connected-to
                                  connected-to
                                  (let [chosen-server (rand-nth servers)]
                                    (assoc ss :connected-to chosen-server
                                              :secure? (str/starts-with? chosen-server "https")
                                              :connected-at (util/current-time-millis))))))
          :connected-to)))

(defn remote-read
  "Returns a core async channel with value of remote resource."
  [state server-state commit-key keywordize-keys?]
  (log/debug "[remote conn] remote read initiated for: " commit-key)
  (let [server-host (pick-server server-state)]
    (xhttp/post-json (str server-host "/fluree/remoteResource")
                     {:resource commit-key}
                     {:keywordize-keys keywordize-keys?})))

(defn monitor-messages
  [conn msg-in msg-out]
  (async/go-loop [next-msg (async/<! msg-in)]
    (if next-msg
      (let []
        (log/warn "MESSAGE RECEIVED!!: " next-msg)
        (recur (async/<! msg-in)))
      (log/info "Websocket messaging connection closed."))))

(defn close-websocket
  [websocket]
  (xhttp/close-websocket websocket))

(defn ws-connect
  "Returns channel with websocket or exception."
  [server-state msg-in msg-out]
  (let [current-server (pick-server server-state)
        url            (-> current-server
                           (str/replace-first "http" "ws")
                           (str "/fluree/subscribe"))
        timeout        10000
        close-fn       (fn []
                         (log/warn "Websocket connection closed!"))]
    (try*
      ;; will return chan with socket object or exception
      (xhttp/try-socket url msg-in msg-out timeout close-fn)
      (catch* e
              (log/warn "Exception establishing web socket: " (ex-message e))
              (async/go e)))))


(defn subscribed-ledger?
  [{:keys [server-state] :as _conn} ledger-id]
  (boolean
    (get-in @server-state [:subscriptions ledger-id])))

(defn record-ledger-subscription
  [{:keys [server-state] :as _conn} ledger-id]
  (swap! server-state assoc-in [:subscriptions ledger-id] {:subscribed-at (util/current-time-millis)}))

(defn remove-ledger-subscription
  [{:keys [server-state] :as _conn} ledger-id]
  (swap! server-state update :subscriptions dissoc ledger-id))

(defn subscribe-ledger-msg
  [ledger-id]
  (json/stringify
    [(:subscribe-ledger message-codes) ledger-id]))

;; TODO - remote subscriptions only partially implemented, for now
;; TODO - remote server will send all commits for all ledgers, but
;; TODO - locally, we'll only pay attention to those commits for ledgers
(defn request-ledger-subscribe
  [conn ledger-id]
  #_(connection/-msg-out conn {:action :subscribe
                               :ledger ledger-id}))

(defn request-ledger-unsubscribe
  [conn ledger-id]
  #_(connection/-msg-out conn {:action :unsubscribe
                               :ledger ledger-id}))

(defn unsubscribe-ledger
  [conn ledger-id]
  (log/debug "Subscriptions request for ledger: " ledger-id)
  (if (subscribed-ledger? conn ledger-id)
    (log/info "Subscription requested for ledger already exists: " ledger-id)
    (do
      (remove-ledger-subscription conn ledger-id)
      (request-ledger-unsubscribe conn ledger-id))))

(defn subscribe-ledger
  [conn ledger-id]
  (log/debug "Subscriptions request for ledger: " ledger-id)
  (if (subscribed-ledger? conn ledger-id)
    (log/info "Subscription requested for ledger already exists: " ledger-id)
    (do
      (record-ledger-subscription conn ledger-id)
      (request-ledger-subscribe conn ledger-id))))
