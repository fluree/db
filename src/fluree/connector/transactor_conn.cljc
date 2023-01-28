(ns fluree.connector.transactor-conn
  (:require
   [fluree.common.identity :as ident]
   [fluree.common.protocols :as service-proto]
   [fluree.connector.protocols :as conn-proto]
   [fluree.db.util.log :as log]
   [fluree.store.api :as store]
   [fluree.transactor.api :as txr]))

(defn stop-transactor-conn
  [{:keys [transactor] :as conn}]
  (log/info "Stopping TransactorConnection." (service-proto/id conn))
  (when transactor (service-proto/stop transactor))
  :stopped)

(defn transactor-create
  [{:keys [transactor]} ledger-name opts]
  (let [tx-address (txr/init transactor ledger-name)]
    tx-address))

(defn transactor-load
  [conn ledger-address opts]
  (let [ledger-name (:address/ledger-name (ident/address-parts ledger-address))
        head-tx     (txr/head (:transactor conn) ledger-name)]
    head-tx))

(defn transactor-transact
  [conn ledger-address tx _opts]
  (let [ledger-name (:address/ledger-name (ident/address-parts ledger-address))
        tx-head (txr/transact (:transactor conn) ledger-name tx)]
    tx-head))

(defrecord TransactorConnection [id transactor]
  service-proto/Service
  (id [_] id)
  (stop [conn] (stop-transactor-conn conn))

  conn-proto/Connection
  (create [conn ledger-name opts] (transactor-create conn ledger-name opts))
  (load [conn ledger-address opts] (transactor-load conn ledger-address opts))
  (transact [conn ledger-address tx opts] (transactor-transact conn ledger-address tx opts))

  (list [conn]
    (throw (ex-info "TransactorConnection does not support list." {:error :conn/unsupported-operation-list})))
  (query [conn ledger-address query opts]
    (throw (ex-info "TransactorConnection does not support query." {:error :conn/unsupported-operation-query})))
  (subscribe [idxr ledger-address cb opts]
    (throw (ex-info "TransactorConnection does not support subscribe." {:error :conn/unsupported-operation-subscribe})))
  (unsubscribe [idxr ledger-address subscription-key]
    (throw (ex-info "TransactorConnection does not support unsubscribe." {:error :conn/unsupported-operation-unsubscribe}))))

(defn create-transactor-conn
  [{:keys [:conn/id :conn/did :conn/trust :conn/distrust :conn/store-config :conn/transactor-config ]
    :as   config}]
  (let [id (or id (random-uuid))

        store (when store-config
                (store/start store-config))

        txr (txr/start (cond-> transactor-config
                         did      (assoc :txr/did did)
                         trust    (assoc :txr/trust trust)
                         distrust (assoc :txr/distrust distrust)
                         store    (assoc :txr/store store)))]
    (log/info "Started TransactorConnection." id)
    (map->TransactorConnection {:id id :transactor txr})))
