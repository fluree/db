(ns fluree.transactor.core
  (:refer-clojure :exclude [resolve])
  (:require [fluree.common.identity :as ident]
            [fluree.common.protocols :as service-proto]
            [fluree.common.model :as model]
            [fluree.db.util.log :as log]
            [fluree.store.api :as store]
            [fluree.transactor.commit :as commit]
            [fluree.transactor.model :as txr-model]
            [fluree.transactor.protocols :as txr-proto]))

(defn write-commit
  [txr tx tx-info]
  (let [store       (:store txr)
        commit      (commit/create tx (assoc tx-info :txr/store store))
        commit-info (merge (select-keys commit [:address :hash])
                           (dissoc (:value commit) :commit/assert :commit/retract))

        {commit-path :address/path} (ident/address-parts (:address commit))]
    (store/write store commit-path commit)
    commit-info))

(defn stop-transactor
  [txr]
  (log/info (str "Stopping transactor " (service-proto/id txr) "."))
  (store/stop (:store txr))
  :stopped)

(defn resolve-commit
  [txr commit-address]
  (let [{commit-path :address/path} (ident/address-parts commit-address)]
    (store/read (:store txr) commit-path)))

(defrecord Transactor [id store]
  service-proto/Service
  (id [_] id)
  (stop [txr] (stop-transactor txr))

  txr-proto/Transactor
  (commit [txr tx tx-info] (write-commit txr tx tx-info))
  (resolve [txr commit-address] (resolve-commit txr commit-address)))

(defn create-transactor
  [{:keys [:txr/id :txr/store-config :txr/store] :as config}]
  (let [store (or store (store/start store-config))
        id (or id (random-uuid))]
    (log/info "Starting transactor " id "." config)
    (map->Transactor {:id id :store store})))

(defn start
  [config]
  (if-let [validation-error (model/explain txr-model/TransactorConfig config)]
    (throw (ex-info "Invalid transactor config." {:errors (model/report validation-error)}))
    (create-transactor config)))

(defn stop
  [txr]
  (service-proto/stop txr))

(defn commit
  [txr tx tx-info]
  (txr-proto/commit txr tx tx-info))

(defn resolve
  [txr commit-address]
  (txr-proto/resolve txr commit-address))
