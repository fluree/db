(ns fluree.connector.core
  (:refer-clojure :exclude [list load])
  (:require
   [fluree.common.model :as model]
   [fluree.common.protocols :as service-proto]
   [fluree.connector.model :as conn-model]
   [fluree.connector.protocols :as conn-proto]
   [fluree.db.util.log :as log]
   [fluree.indexer.api :as idxr]
   [fluree.publisher.api :as pub]
   [fluree.store.api :as store]
   [fluree.transactor.api :as txr]))

(defn head-db-address
  [conn ledger-address]
  (let [ledger (pub/pull (:publisher conn) ledger-address)]
    (-> ledger :ledger/head :entry/db :db/address)))

(defn head-commit-address
  [conn ledger-address]
  (let [ledger (pub/pull (:publisher conn) ledger-address)]
    (-> ledger :ledger/head :entry/commit :commit/address)))

(defn stop-conn
  [{:keys [transactor indexer publisher enforcer] :as conn}]
  (log/info "Stopping Connection " (service-proto/id conn) ".")
  (when transactor (service-proto/stop transactor))
  (when indexer (service-proto/stop indexer))
  (when publisher (service-proto/stop publisher))
  :stopped)

(defn create-ledger
  [{:keys [indexer publisher]} ledger-name opts]
  (let [db-address     (idxr/init indexer ledger-name opts)
        ledger-address (pub/init publisher ledger-name (assoc opts :db-address db-address))]
    ledger-address))

(defn transact-ledger
  [conn ledger-address tx opts]
  (let [{txr :transactor pub :publisher idxr :indexer} conn

        ledger              (pub/pull pub ledger-address)
        {head :ledger/head} (get ledger :cred/credential-subject ledger)

        db-address     (-> head :entry/db-summary :db/address)
        commit-address (-> head :entry/commit-summary :commit/address)

        {:keys [errors db/address] :as db-summary} (idxr/stage idxr db-address tx)]
    ;; TODO: figure out what auth/schema errors look like
    (if errors
      (do
        (idxr/discard address)
        errors)
      (let [commit-summary (txr/commit txr tx (assoc db-summary
                                                     :commit/prev commit-address
                                                     :ledger/name (:ledger/name ledger)))
            ;; TODO: check that commit t hasn't been pushed already?
            ledger-cred (pub/push pub ledger-address
                                  {:commit-summary commit-summary
                                   :db-summary (select-keys db-summary [:db/address :db/t :db/flakes :db/size
                                                                        :ledger/name])})]
        ledger-cred))))

(defn load-ledger
  [conn ledger-address opts]
  (let [{txr :transactor pub :publisher idxr :indexer} conn

        ledger (pub/pull pub ledger-address)

        _ (when-not ledger
            (throw (ex-info "No ledger found for ledger-address." {:ledger-address ledger-address})))

        {head :ledger/head} (get ledger :cred/credential-subject ledger)

        db-summary     (-> head :entry/db-summary)
        commit-summary (-> head :entry/commit-summary)]
    ;; load db at t
    ;; if it's less than commit-t, stage commits in order starting at (inc db-t)
    (if false
        ;; TODO: attempt to load index first
        :TODO #_(idxr/load idxr db-address)
        ;; fall back to re-staging commits
        (let [commits (loop [ledger-entry head
                             commit-addresses '()]
                        (let [{:keys [entry/previous entry/commit-summary]} ledger-entry
                              {:keys [address]} commit-summary]
                          (if previous
                            (let [prev-ledger (pub/pull pub previous)
                                  {prev-entry :ledger/head} (get prev-ledger :cred/credential-subject prev-ledger)]
                              (recur prev-entry (conj commit-addresses address)))
                            (map (partial txr/resolve txr) commit-addresses))))

              db-summary (reduce (fn [db-summary {:keys [commit/tx]}]
                                   (idxr/stage idxr (:db/address db-summary) tx))
                                 {:db/address (idxr/init idxr {})}
                                 commits)

              ledger-cred (pub/push pub ledger-address
                                    {:commit-summary commit-summary
                                     :db-summary (select-keys db-summary [:db/address :db/t :db/flakes :db/size
                                                                          :ledger/name])})]
          ledger-cred))))

(defn query-ledger
  [conn ledger-address query opts]
  (let [ledger              (pub/pull (:publisher conn) ledger-address)
        _ (def lll ledger)
        {head :ledger/head} (get ledger :cred/credential-subject ledger)
        db-address          (-> head :entry/db-summary :db/address)]
    (idxr/query (:indexer conn) db-address query)))

(defrecord FlureeConnection [id transactor indexer publisher enforcer]
  service-proto/Service
  (id [_] id)
  (stop [conn] (stop-conn conn))

  conn-proto/Connection
  (transact [conn ledger-address tx opts] (transact-ledger conn ledger-address tx opts))
  (create [conn ledger-name opts] (create-ledger conn ledger-name opts))
  (query [conn ledger-address query opts] (query-ledger conn ledger-address query opts))
  (load [conn ledger-address opts] (load-ledger conn ledger-address opts))
  ;; TODO
  #_(subscribe [conn query fn]))

(defn create-conn
  [{:keys [:conn/id :conn/store-config :conn/transactor-config :conn/publisher-config :conn/indexer-config]
    :as config}]
  (let [id (or id (random-uuid))

        store (when store-config
                (store/start store-config))

        idxr  (idxr/start (if store
                            (assoc indexer-config :idxr/store store)
                            indexer-config))

        txr   (txr/start (if store
                           (assoc transactor-config :txr/store store)
                           transactor-config))

        pub   (pub/start (if store
                           (assoc publisher-config :pub/store store)
                           publisher-config))]
    (log/info "Starting Connection " id "." config)
    (map->FlureeConnection
      (cond-> {:id id
               :indexer idxr
               :transactor txr
               :publisher pub}
        store (assoc :store store)))))


(defn connect
  [config]
  (if-let [validation-error (model/explain conn-model/ConnectionConfig config)]
    (throw (ex-info "Invalid connection config." {:errors (model/report validation-error)
                                                  :config config}))
    (create-conn config)))

(defn close
  [conn]
  (service-proto/stop conn))

(defn create
  [conn ledger-name opts]
  (conn-proto/create conn ledger-name opts))

(defn load
  [conn ledger-address opts]
  (conn-proto/load conn ledger-address opts))

(defn transact
  [conn ledger-address tx opts]
  (conn-proto/transact conn ledger-address tx opts))

(defn query
  [conn ledger-address query opts]
  (conn-proto/query conn ledger-address query opts))

;; TODO: make this part of the conn protocol?
(defn list
  [conn]
  (pub/list (:publisher conn)))
