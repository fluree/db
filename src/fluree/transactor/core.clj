(ns fluree.transactor.core
  (:refer-clojure :exclude [resolve load])
  (:require
   [fluree.common.identity :as ident]
   [fluree.common.model :as model]
   [fluree.common.protocols :as service-proto]
   [fluree.db.util.async :refer [<??]]
   [fluree.db.util.log :as log]
   [fluree.store.api :as store]
   [fluree.transactor.commit :as commit]
   [fluree.transactor.model :as txr-model]
   [fluree.transactor.protocols :as txr-proto]))

(defn stop-transactor
  [txr]
  (log/info (str "Stopping transactor " (service-proto/id txr) "."))
  (store/stop (:store txr))
  :stopped)

(defn gen-head-path
  [ledger-name]
  (str (commit/commit-path ledger-name) "head"))

(defn head-commit
  [txr ledger-name]
  (let [store          (:store txr)
        init-commit    (commit/create-commit nil nil)
        commit-path      (:path (<?? (store/write store (str (commit/commit-path ledger-name) "init") init-commit)))
        commit-address (commit/create-commit-address store commit-path)
        head-path      (gen-head-path ledger-name)]
    ;; update head
    (<?? (store/write store head-path commit-address))
    commit-address))

(defn resolve-commit
  [txr commit-address]
  (let [{commit-path :address/path} (ident/address-parts commit-address)]
    (<?? (store/read (:store txr) commit-path))))

(defn load-head
  [txr ledger-name]
  (let [store (:store txr)
        head-path (gen-head-path ledger-name)

        head-commit-address (<?? (store/read store head-path))
        head-commit (resolve-commit txr head-commit-address)]
    (commit/create-commit-summary head-commit head-commit-address)))

(defn write-commit
  [txr ledger-name tx]
  (let [store     (:store txr)
        head-path (gen-head-path ledger-name)

        prev-commit-address (<?? (store/read store head-path))
        prev-commit         (resolve-commit txr prev-commit-address)
        prev-commit-summary (commit/create-commit-summary prev-commit prev-commit-address)

        commit    (commit/create-commit prev-commit-summary tx)
        commit-id (:path (<?? (store/write store (commit/commit-path ledger-name) commit
                                           {:content-address? true})))

        commit-address (commit/create-commit-address store commit-id)
        commit-summary (commit/create-commit-summary commit commit-address)]
    ;; update head
    (<?? (store/write store head-path commit-address))
    commit-summary))

(defrecord Transactor [id store]
  service-proto/Service
  (id [_] id)
  (stop [txr] (stop-transactor txr))

  txr-proto/Transactor
  (init [txr ledger-name] (head-commit txr ledger-name))
  (commit [txr ledger-name tx] (write-commit txr ledger-name tx))
  (load [txr ledger-name] (load-head txr ledger-name))
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
    (throw (ex-info "Invalid transactor config." {:errors (model/report validation-error)
                                                  :config config}))
    (create-transactor config)))

(defn stop
  [txr]
  (service-proto/stop txr))

(defn commit
  [txr ledger-name tx]
  (txr-proto/commit txr ledger-name tx))

(defn resolve
  [txr commit-address]
  (txr-proto/resolve txr commit-address))

(defn load
  [txr ledger-name]
  (txr-proto/load txr ledger-name))

(defn init
  [txr ledger-name]
  (txr-proto/init txr ledger-name))
