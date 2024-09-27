(ns fluree.db.remote-system
  (:require [clojure.string :as str]
            [clojure.core.async :as async :refer [<! go]]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.storage :as storage]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [fluree.db.util.xhttp :as xhttp]))

(defn pick-server
  "Currently does just a round-robin selection if multiple servers are given.
  TODO - add re-tries with a different server if fails to connect. Consider keeping stats to select optimal server."
  [system-state]
  (or (:connected-to @system-state)
      (-> (swap! system-state (fn [{:keys [connected-to servers] :as ss}]
                                (if connected-to
                                  connected-to
                                  (let [chosen-server (rand-nth servers)]
                                    (assoc ss :connected-to chosen-server
                                              :secure? (str/starts-with? chosen-server "https")
                                              :connected-at (util/current-time-millis))))))
          :connected-to)))

(defn remote-read
  "Returns a core async channel with value of remote resource."
  [system-state commit-key keywordize-keys?]
  (log/debug "Remote read initiated for: " commit-key)
  (let [server-host (pick-server system-state)]
    (xhttp/post-json (str server-host "/fluree/remoteResource")
                     {:resource commit-key}
                     {:keywordize-keys keywordize-keys?})))

(defn remote-lookup
  [system-state ledger-address]
  (go-try
    (let [head-commit  (<? (remote-read system-state ledger-address false))
          head-address (get head-commit "address")]
      head-address)))

(defn close-websocket
  [websocket]
  (xhttp/close-websocket websocket))

(defn ws-connect
  "Returns channel with websocket or exception."
  [system-state msg-in msg-out]
  (let [current-server (pick-server system-state)
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

(defn subscribe
  [current-state ledger-alias callback]
  (if-not (contains? (:subscription current-state) ledger-alias)
    (-> current-state
        (assoc-in [:subscription ledger-alias] callback)
        (update-in [:stats :subscriptions] inc))
    (do (log/info "Subscription requested for ledger" ledger-alias "already exists")
        current-state)))

(defn unsubscribe
  [current-state ledger-alias]
  (if (contains? (:subscription current-state) ledger-alias)
    (-> current-state
        (update :subscription dissoc ledger-alias)
        (update-in [:stats :subscription] dec))
    current-state))

(defrecord RemoteSystem [system-state address-identifiers]
  nameservice/iNameService
  (lookup [_ ledger-address]
    (remote-lookup system-state ledger-address))
  (alias [_ ledger-address]
    ledger-address)
  (address [_ ledger-alias]
    (go ledger-alias))

  nameservice/Publication
  (subscribe [_ ledger-alias callback]
    (if (fn? callback)
      (swap! system-state subscribe ledger-alias callback)
      (throw (ex-info (str "Subscription request for " ledger-alias
                           " failed. Callback must be a function, provided: " (pr-str callback))
                      {:status 400
                       :error  :db/invalid-fn}))))
  (unsubscribe [_ ledger-alias]
    (swap! system-state unsubscribe ledger-alias))

  storage/JsonArchive
  (-read-json [_ address keywordize?]
    (remote-read system-state address keywordize?))

  storage/Identifiable
  (identifiers [_]
    address-identifiers))

(defn initial-state
  [servers]
  {:servers      servers
   :connected-to nil
   :stats        {:connected-at  nil
                  :subscriptions 0}
   :subscription {}})

(defn connect
  [servers identifiers]
  (let [system-state   (-> servers initial-state atom)
        identifier-set (set identifiers)]
    (->RemoteSystem system-state identifier-set)))
