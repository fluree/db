(ns fluree.connector.core
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
  (let [db-address     (idxr/init indexer opts)
        ledger-address (pub/init publisher ledger-name (assoc opts :db-address db-address))]
    ledger-address))

(defn transact-conn
  [conn ledger-address tx opts]
  (let [{txr :transactor pub :publisher idxr :indexer} conn

        ledger              (pub/pull pub ledger-address)
        {head :ledger/head} (get ledger :cred/credential-subject ledger)

        db-address     (-> head :entry/db :db/address)
        commit-address (-> head :entry/commit :commit/address)

        {:keys [errors db/address] :as db-info} (idxr/stage idxr db-address tx)]
    (if errors
      (do
        (idxr/discard address)
        errors)
      (let [commit-info (txr/commit txr tx (assoc db-info
                                                  :commit/prev commit-address
                                                  :ledger/name (:ledger/name ledger)))
            ledger-cred (pub/push pub ledger-address
                                  {:commit-info commit-info
                                   :db-info (select-keys db-info [:db/address :db/t :db/flakes :db/size
                                                                  :ledger/name])})]
        ledger-cred))))

(defn query-conn
  [conn db-address query opts]
  (idxr/query (:indexer conn) db-address query))

(defrecord FlureeConnection [id transactor indexer publisher enforcer]
  service-proto/Service
  (id [_] id)
  (stop [conn] (stop-conn conn))

  conn-proto/Connection
  (transact [conn ledger-address tx opts] (transact-conn conn ledger-address tx opts))
  (create [conn ledger-name opts] (create-ledger conn ledger-name opts))
  (query [conn db-address query opts] (query-conn conn db-address query opts))
  ;; TODO
  #_(load [conn query opts])
  #_(subscribe [conn query fn])
  )

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
    (log/info "Starting FileConnection " id "." config)
    (map->FlureeConnection
      (cond-> {:id id
               :indexer idxr
               :transactor txr
               :publisher pub}
        store (assoc :store store)))))


(defn connect
  [config]
  (if-let [validation-error (model/explain conn-model/ConnectionConfig config)]
    (throw (ex-info "Invalid connection config." {:errors (model/report validation-error)}))
    (create-conn config)))

(defn close
  [conn]
  (service-proto/stop conn))

(defn create
  [conn ledger-name opts]
  (conn-proto/create conn ledger-name opts))

(defn transact
  [conn ledger-address tx opts]
  (conn-proto/transact conn ledger-address tx opts))

(defn query
  [conn db-address query opts]
  (conn-proto/query conn db-address query opts))
