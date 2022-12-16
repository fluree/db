(ns fluree.indexer.core
  (:require
   [clojure.core.async :as async]
   [fluree.common.model :as model]
   [fluree.common.protocols :as service-proto]
   [fluree.db.api.query :as jld-query]
   [fluree.db.json-ld.transact :as jld-transact]
   [fluree.db.util.log :as log]
   [fluree.indexer.db :as db]
   [fluree.indexer.model :as idxr-model]
   [fluree.indexer.protocols :as idxr-proto]
   [fluree.store.api :as store]))

(defn stop-indexer
  [idxr]
  (log/info "Stopping Indexer " (service-proto/id idxr) ".")
  (store/stop (:store idxr)))

(defn init-indexer
  [{:keys [store] :as idxr} ledger-name opts]
  (let [db (db/create store {:ledger/name ledger-name})
        db-address (db/create-db-address db)]
    (if (store/read store db-address)
      (throw (ex-info "Ledger index already exists." {:ledger/name ledger-name}))
      (do
        (store/write store db-address db)
        db-address))))

(defn stage-indexer
  [{:keys [store] :as idxr} db-address data]
  (if-let [db (store/read store db-address)]
    (let [db0 (db/prepare db)
          db1 (async/<!! (jld-transact/stage db0 data {}))
          db-address (db/create-db-address db1)]
      (store/write store db-address db1)
      ;; return db-info
      {:db/address db-address
       :db/t (- (:t db1))
       :db/flakes (-> db1 :stats :flakes)
       :db/size (-> db1 :stats :size)
       ;; TODO: calculate assert+retract
       :db/assert []
       :db/retract []})
    (throw (ex-info "No such db-address." {:error :stage/no-such-db
                                           :db-address db-address}))))

(defn query-indexer
  [{:keys [store] :as idxr} db-address query]
  (if-let [db (store/read store db-address)]
    (async/<!! (jld-query/query-async db query))
    (throw (ex-info "No such db-address." {:error :query/no-such-db
                                           :db-address db-address}))))

(defrecord Indexer [id]
  service-proto/Service
  (id [_] id)
  (stop [idxr] (stop-indexer idxr))

  idxr-proto/Indexer
  (init [idxr ledger-name opts] (init-indexer idxr ledger-name opts))
  (stage [idxr db-address data] (stage-indexer idxr db-address data))
  (query [idxr db-address query] (query-indexer idxr db-address query))
  (explain [idxr db-address query] (throw (ex-info "TODO"))))

(defn create-indexer
  [{:keys [:idxr/id :idxr/store-config :idxr/store] :as config}]
  (let [store (or store (store/start store-config))
        id (or id (random-uuid))]
    (log/info "Starting Indexer " id "." config)
    (map->Indexer {:id id :store store})))

(defn start
  [config]
  (if-let [validation-error (model/explain idxr-model/IndexerConfig config)]
    (throw (ex-info "Invalid indexer config." {:errors (model/report validation-error)}))
    (create-indexer config)))

(defn stop
  [idxr]
  (service-proto/stop idxr))

(defn init
  [idxr ledger-name opts]
  (idxr-proto/init idxr ledger-name opts))

(defn stage
  [idxr db-address data]
  (idxr-proto/stage idxr db-address data))

(defn query
  [idxr db-address query]
  (idxr-proto/query idxr db-address query))

(defn explain
  [idxr db-address query]
  (idxr-proto/explain idxr db-address query))
