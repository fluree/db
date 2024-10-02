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
  (-> system-state
      (swap! (fn [current-state]
               (if (:connected-to current-state)
                 current-state
                 (let [chosen-server (-> current-state :servers rand-nth)]
                   (assoc current-state
                          :connected-to chosen-server
                          :connected-at (util/current-time-millis)
                          :ssl (str/starts-with? chosen-server "https"))))))
      :connected-to))

(defn close-websocket
  [websocket]
  (xhttp/close-websocket websocket))

(defn launch-subscription-socket
  "Returns channel with websocket or exception."
  [system-state msg-in msg-out]
  (go
    (let [current-server (pick-server system-state)
          url            (-> current-server
                             (str/replace-first "http" "ws")
                             (str "/fluree/subscribe"))
          timeout        10000
          close-fn       (fn []
                           (log/warn "Websocket connection closed!"))]
      (try*
        ;; will return chan with socket object or exception
        (<? (xhttp/try-socket url msg-in msg-out timeout close-fn))
        (catch* e
                (let [msg (ex-message e)]
                  (log/warn "Error establishing web socket:" msg)
                  (clear-connected-server system-state)
                  (ex-info (str "Error establishing websocket connection: " msg)
                           {:status 400
                            :error  :db/websocket-error})))))))

(defn remote-read
  "Returns a core async channel with value of remote resource."
  [system-state commit-key keywordize-keys?]
  (log/debug "Remote read initiated for: " commit-key)
  (let [server-host (pick-server system-state)]
    (xhttp/post-json (str server-host "/fluree/remote/resource")
                     {:resource commit-key}
                     {:keywordize-keys keywordize-keys?})))

(defn remote-lookup
  [system-state ledger-address]
  (go-try
    (let [server-host (pick-server system-state)
          head-commit (<? (xhttp/post-json (str server-host "/fluree/remote/latestCommit")
                                           {:resource ledger-address}
                                           {:keywordize-keys false}))]
      (get head-commit "address"))))

(defn record-subscription
  [current-state ledger-alias sub-ch]
  (if-not (contains? (:subscription current-state) ledger-alias)
    (-> current-state
        (assoc-in [:subscription ledger-alias] sub-ch))
    current-state))

(defn record-unsubscription
  [current-state ledger-alias]
  (if (contains? (:subscription current-state) ledger-alias)
    (-> current-state
        (update :subscription dissoc ledger-alias))
    current-state))

(defrecord RemoteSystem [system-state address-identifiers msg-in pub msg-out]
  storage/JsonArchive
  (-read-json [_ address keywordize?]
    (remote-read system-state address keywordize?))

  storage/Identifiable
  (identifiers [_]
    address-identifiers)

  nameservice/iNameService
  (lookup [_ ledger-address]
    (remote-lookup system-state ledger-address))
  (alias [_ ledger-address]
    ledger-address)
  (address [_ ledger-alias]
    (go ledger-alias))

  nameservice/Publication
  (subscribe [_ ledger-alias]
    (let [sub-ch                  (async/chan)
          [prev-state next-state] (swap-vals! system-state record-subscription ledger-alias sub-ch)]
      (if (not= prev-state next-state)
        (do (async/sub pub ledger-alias sub-ch)
            sub-ch)
        ;; TODO; We currently allow only one subscription per ledger, but we could
        ;; enable multiple subscriptions if necessary with multiple calls to
        ;; `core.async/sub` on the publication
        (do (log/debug "Subscription requested for ledger" ledger-alias "already exists")
            (async/close! sub-ch)))))
  (unsubscribe [_ ledger-alias]
    (if-let [sub-ch (get-in @system-state [:subscription ledger-alias])]
      (do (log/debug "Unsubscribing from updates to ledger:" ledger-alias)
          (async/unsub pub ledger-alias sub-ch)
          (async/close! sub-ch)
          (swap! system-state record-unsubscription ledger-alias)
          :unsubscribed)
      (log/debug "Ledger" ledger-alias "not subscribed"))))

(defn initial-state
  [servers]
  {:servers      servers
   :connected-to nil
   :connected-at nil
   :ssl          nil
   :subscription {}})

(defn parse-message
  [msg]
  (json/parse msg false))

(defn get-ledger-id
  [parsed-msg]
  (get parsed-msg "ledger"))

(defn connect
  [servers identifiers]
  (go-try
    (let [system-state   (-> servers initial-state atom)
          identifier-set (set identifiers)
          msg-in         (async/chan 1 (map parse-message))
          msg-in-pub     (async/pub msg-in get-ledger-id)
          msg-out        (async/chan)
          websocket      (<! (launch-subscription-socket system-state msg-in msg-out))]
      (if (util/exception? websocket)
        (do (async/close! msg-in)
            (async/close! msg-out)
            (throw websocket))
        (->RemoteSystem system-state identifier-set msg-in msg-in-pub msg-out)))))
