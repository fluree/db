(ns fluree.transactor.core
  (:refer-clojure :exclude [resolve])
  (:require
   [fluree.common.identity :as ident]
   [fluree.common.model :as model]
   [fluree.common.protocols :as service-proto]
   [fluree.db.util.async :refer [<??]]
   [fluree.db.util.log :as log]
   [fluree.store.api :as store]
   [fluree.transactor.model :as txr-model]
   [fluree.transactor.protocols :as txr-proto]
   [fluree.transactor.tx-summary :as tx-summary]))

(defn stop-transactor
  [txr]
  (log/info (str "Stopping transactor " (service-proto/id txr) "."))
  (store/stop (:store txr))
  :stopped)

(defn gen-head-path
  [ledger-name]
  (str (tx-summary/tx-summary-path ledger-name) "HEAD"))

(defn resolve-tx
  [txr tx-address]
  (let [{tx-summary-path :address/path} (ident/address-parts tx-address)]
    (<?? (store/read (:store txr) tx-summary-path))))

(defn init-tx
  [txr ledger-name]
  (let [store              (:store txr)
        init-tx-summary    (tx-summary/create-tx-summary nil nil)
        tx-summary-path    (str (tx-summary/tx-summary-path ledger-name) "init")
        tx-summary-address (tx-summary/create-tx-summary-address store tx-summary-path)

        existing?          (resolve-tx txr tx-summary-address)]
    (when existing? (throw (ex-info "Cannot initialize transactor: " (pr-str ledger-name)
                                          " already exists.")))
    ;; write init summary
    (<?? (store/write store tx-summary-path init-tx-summary))
    ;; initialize head file
    (<?? (store/write store (gen-head-path ledger-name) tx-summary-address))
    tx-summary-address))

(defn head-tx
  [txr ledger-name]
  (let [store     (:store txr)
        head-path (gen-head-path ledger-name)

        head-tx-address (<?? (store/read store head-path))
        head-tx-summary (resolve-tx txr head-tx-address)]
    (tx-summary/create-tx-head head-tx-summary head-tx-address)))

(defn transact-tx
  [txr ledger-name tx]
  (let [store        (:store txr)
        head-path    (gen-head-path ledger-name)
        prev-tx-head (head-tx txr ledger-name)

        tx-summary      (tx-summary/create-tx-summary prev-tx-head tx)
        tx-summary-path (:path (<?? (store/write store (tx-summary/tx-summary-path ledger-name) tx-summary
                                                 {:content-address? true})))

        tx-summary-address (tx-summary/create-tx-summary-address store tx-summary-path)
        tx-head            (tx-summary/create-tx-head tx-summary tx-summary-address)]
    ;; update head
    (<?? (store/write store head-path tx-summary-address))
    tx-head))

(defrecord Transactor [id store]
  service-proto/Service
  (id [_] id)
  (stop [txr] (stop-transactor txr))

  txr-proto/Transactor
  (init [txr ledger-name] (init-tx txr ledger-name))
  (head [txr ledger-name] (head-tx txr ledger-name))
  (resolve [txr tx-address] (resolve-tx txr tx-address))
  (transact [txr ledger-name tx] (transact-tx txr ledger-name tx)))

(defn create-transactor
  [{:keys [:txr/id :txr/store-config :txr/store] :as config}]
  (let [store (or store (store/start store-config))
        id (or id (random-uuid))]
    (log/info "Starting transactor " id "." config)
    (map->Transactor {:id id :store store})))

(defn start
  [config]
  (if-let [validation-error (model/explain txr-model/TransactorConfig config)]
    (throw (ex-info "Invalid transactor config." {:errors (model/report validation-error)
                                                  :config config}))
    (create-transactor config)))

(defn stop
  [txr]
  (service-proto/stop txr))

(defn init
  [txr ledger-name]
  (txr-proto/init txr ledger-name))

(defn head
  [txr ledger-name]
  (txr-proto/head txr ledger-name))

(defn transact
  [txr ledger-name tx]
  (txr-proto/transact txr ledger-name tx))

(defn resolve
  [txr tx-address]
  (txr-proto/resolve txr tx-address))
