(ns fluree.db.nameservice.subscribe
  (:require [clojure.core.async :as async :refer [<! go-loop]]
            [fluree.db.commit.storage :as commit-storage]
            [fluree.db.constants :as const]
            [fluree.db.ledger :as ledger]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util :refer [get-first get-first-value try* catch*]]
            [fluree.db.util.log :as log]))

(defn cached-ledger
  "Returns a cached ledger from the connection if it is cached, else nil"
  [{:keys [state] :as _conn} ledger-alias]
  (get-in @state [:ledger ledger-alias]))

(defn release-ledger
  "Opposite of register-ledger. Removes reference to a ledger from conn"
  [{:keys [state] :as _conn} ledger-alias]
  (swap! state update :ledger dissoc ledger-alias)
  nil)

(defn all-publications
  [{:keys [remote-systems] :as _conn}]
  remote-systems)

(defn subscribe-all
  [publications ledger-alias]
  (->> publications
       (map (fn [pub]
              (nameservice/subscribe pub ledger-alias)))
       async/merge))

(defn subscribed?
  [current-state ledger-alias]
  (contains? (:subscriptions current-state) ledger-alias))

(defn get-subscription
  [current-state ledger-alias]
  (get-in current-state [:subscriptions ledger-alias]))

(defn add-subscription
  [current-state publications ledger-alias]
  (if-not (subscribed? current-state ledger-alias)
    (let [sub-ch (subscribe-all publications ledger-alias)]
      (assoc-in current-state [:subscriptions ledger-alias] sub-ch))
    current-state))

(defn remove-subscription
  [current-state ledger-alias]
  (update current-state :subscriptions dissoc ledger-alias))

(defn notify
  [{:keys [commit-catalog] :as conn} address hash]
  (go-try
    (if-let [expanded-commit (<? (commit-storage/read-commit-jsonld commit-catalog address hash))]
      (if-let [ledger-alias (get-first-value expanded-commit const/iri-alias)]
        (if-let [ledger-ch (cached-ledger conn ledger-alias)]
          (do (log/debug "Notification received for ledger" ledger-alias
                         "of new commit:" expanded-commit)
              (let [ledger        (<? ledger-ch)
                    db-address    (-> expanded-commit
                                      (get-first const/iri-data)
                                      (get-first-value const/iri-address))
                    expanded-data (<? (commit-storage/read-data-jsonld commit-catalog db-address))]
                (case (<? (ledger/notify ledger expanded-commit expanded-data))
                  (::ledger/current ::ledger/newer ::ledger/updated)
                  (do (log/debug "Ledger" ledger-alias "is up to date")
                      true)

                  ::ledger/stale
                  (do (log/debug "Dropping state for stale ledger:" ledger-alias)
                      (release-ledger conn ledger-alias)))))
          (log/debug "No cached ledger found for commit: " expanded-commit))
        (log/warn "Notify called with a data that does not have a ledger alias."
                  "Are you sure it is a commit?: " expanded-commit))
      (log/warn "No commit found for address:" address))))

;; TODO; Were subscribing to every remote system for every ledger we load.
;; Perhaps we should ensure that a remote system manages a particular ledger
;; before subscribing
(defn subscribe-ledger
  "Initiates subscription requests for a ledger into all remote systems on a
  connection."
  [{:keys [state] :as conn} ledger-alias]
  (let [pubs                   (all-publications conn)
        [prev-state new-state] (swap-vals! state add-subscription pubs ledger-alias)]
    (when-not (subscribed? prev-state ledger-alias)
      (let [sub-ch (get-subscription new-state ledger-alias)]
        (go-loop []
          (when-let [msg (<! sub-ch)]
            (log/info "Subscribed ledger:" ledger-alias "received subscription message:" msg)
            (let [action (get msg "action")]
              (if (= "new-commit" action)
                (let [{:keys [address hash]} (get msg "data")]
                  (notify conn address hash))
                (log/info "New subscrition message with action: " action "received, ignored.")))
            (recur)))
        :subscribed))))

(defn unsubscribe-ledger
  "Initiates unsubscription requests for a ledger into all namespaces on a connection."
  [{:keys [state] :as conn} ledger-alias]
  (->> (all-publications conn)
       (map (fn [pub]
              (nameservice/unsubscribe pub ledger-alias)))
       dorun)
  (swap! state remove-subscription ledger-alias))
