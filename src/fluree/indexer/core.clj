(ns fluree.indexer.core
  (:refer-clojure :exclude [load resolve])
  (:require
   [fluree.common.identity :as ident]
   [fluree.common.iri :as iri]
   [fluree.common.model :as model]
   [fluree.common.protocols :as service-proto]
   [fluree.db.api.query :as jld-query]
   [fluree.db.constants :as const]
   [fluree.db.indexer.proto :as idx-proto]
   [fluree.db.json-ld.bootstrap :as bootstrap]
   [fluree.db.json-ld.commit :as jld-commit]
   [fluree.db.json-ld.reify :as jld-reify]
   [fluree.db.json-ld.transact :as jld-transact]
   [fluree.db.storage.core :as storage]
   [fluree.db.util.async :refer [<??]]
   [fluree.db.util.log :as log]
   [fluree.indexer.db :as db]
   [fluree.indexer.db-block :as block]
   [fluree.indexer.model :as idxr-model]
   [fluree.indexer.protocols :as idxr-proto]
   [fluree.json-ld :as json-ld]
   [fluree.store.api :as store]))

(defn stop-indexer
  [idxr]
  ;; TODO: call idx-proto/-index when stopping to flush novelty to Store
  (log/info "Stopping Indexer " (service-proto/id idxr) ".")
  (store/stop (:store idxr)))

(defn init-db
  [{:keys [store config state] :as idxr} ledger-name opts]
  (let [db         (bootstrap/blank-db (db/create store ledger-name opts))
        db-address (db/create-db-address db (str (block/db-block-path ledger-name) "init"))]
    (if (get @state db-address)
      (throw (ex-info "Ledger db already exists." {:ledger ledger-name}))
      (do
        (swap! state assoc db-address db)
        db-address))))

(defn resolve-db
  [{:keys [store state] :as idxr} db-address]
  (let [db-block-id (:address/path (ident/address-parts db-address))
        db-block    (<?? (store/read store db-block-id))]
    (if db-block
      db-block
      (throw (ex-info (str "Db block with address " db-address " does not exist." {:db-address db-address}))))))

(defn stage-db
  "Index the given data, then store a db-block"
  [{:keys [store state] :as idxr} db-address data {:keys [tx-id] :as _opts}]
  (if-let [db-before (get @state db-address)]
    (let [{ledger-name :ledger/name} (db/db-path-parts db-address)
          db-after                   (<?? (jld-transact/stage (db/prepare db-before) data {}))
          ;; This is a hack to sync t into various places since dbs shouldn't care about branches
          ;; used in do-index > refresh > index-update > empty-novelty
          db-after                   (assoc-in db-after [:commit :data :t] (- (:t db-after)))

          idx-writer (-> db-after :ledger :indexer)

          {:keys [context did private push?] :as _opts} data

          ;; create the information necessary for db-block -
          ;; can this context stuff can be removed if data is expanded?
          context*      (-> (if context
                              (json-ld/parse-context (:context (:schema db-after)) context)
                              (:context (:schema db-after)))
                            (json-ld/parse-context {"f" "https://ns.flur.ee/ledger#"})
                            (jld-commit/stringify-context))
          ctx-used-atom (atom {})
          compact-fn    (json-ld/compact-fn context* ctx-used-atom)
          {:keys [assert retract] :as c}
          (<?? (jld-commit/commit-opts->data db-after {:compact-fn compact-fn :id-key "@id" :type-key "@type"}))

          ;; kick off indexing if necessary
          db-after (if (idx-proto/-index? idx-writer db-after)
                     (<?? (idx-proto/-index idx-writer db-after))
                     db-after)

          ;; create db-block and write it to store
          db-block      (block/create-db-block db-after assert retract tx-id)
          db-block-path (:path (<?? (store/write store (block/db-block-path ledger-name) db-block
                                                 {:content-address? true})))

          ;; save newest db-block so the next stage knows the previous tx
          db-final   (assoc db-after :db-block-id db-block-path)
          db-address (db/create-db-address db-final db-block-path)]
      ;; add an entry of db-address -> db
      (swap! state assoc db-address db-final)

      ;; return db-summary, a truncated db-block
      (block/create-db-summary db-block db-address))
    (throw (ex-info "No such db-address." {:error      :stage/no-such-db
                                           :db-address db-address}))))

(defn load-db-blocks
  "Read each db-block's previous until we reach the stop t or run out of previous to follow."
  [store head-db-block stop-t]
  (loop [{previous iri/DbBlockPrevious} head-db-block
         db-blocks                      (list head-db-block)]
    (if-let [prev-db-block (<?? (store/read store previous))]
      (if (> (get prev-db-block iri/DbBlockT) stop-t)
        db-blocks
        (recur prev-db-block (conj db-blocks prev-db-block)))
      db-blocks)))

(defn merge-db-block
  "Merge the db-block into novelty and return a new Db."
  [db db-block]
  (let [{assert  iri/DbBlockAssert
         retract iri/DbBlockRetract
         t       iri/DbBlockT} db-block

        iris (volatile! {})
        refs (volatile! (-> db :schema :refs))

        expanded-retract         (json-ld/expand retract)
        expanded-assert          (json-ld/expand assert)
        ;; these expect keyword :id :type, not strings, so we expand them
        retract-flakes           (<?? (jld-reify/retract-flakes db expanded-retract t iris))
        {:keys [flakes pid sid]} (<?? (jld-reify/assert-flakes db expanded-assert t iris refs))
        all-flakes               (-> (empty (get-in db [:novelty :spot]))
                                     (into retract-flakes)
                                     (into flakes))
        ecount                   (assoc (:ecount db)
                                        const/$_predicate pid
                                        const/$_default sid)
        db*                      (assoc db :ecount ecount)]
    (jld-reify/merge-flakes db* t @refs all-flakes)))

(defn load-db
  "If given db-address doesn't exist, try to recreate it from index files in store, and
  then rebuild novelty with tx-summaries."
  [{:keys [store state] :as idxr} db-address opts]
  (if-let [db (get @state db-address)]
    ;; already loaded
    (let [db-block (<?? (store/read store (:db-block-id db)))]
      ;; update opts
      (swap! state update db-address db/update-index-writer-opts opts)
      (block/create-db-summary db-block db-address))

    ;; rebuild db from persisted data
    (let [db-block-id (:address/path (ident/address-parts db-address))
          db-block    (<?? (store/read store db-block-id))

          {ledger-name :ledger/name} (db/db-path-parts db-address)
          {root        iri/DbBlockIndexRoot
           previous    iri/DbBlockPrevious
           t           iri/DbBlockT
           tx-id       iri/DbBlockTxId
           reindex-min iri/DbBlockReindexMin
           reindex-max iri/DbBlockReindexMax} db-block

          ;; create a blank db, updating opts with supplied opts
          blank-db   (bootstrap/blank-db (db/create store ledger-name (merge {:reindex-min-bytes reindex-min
                                                                              :reindex-max-bytes reindex-max}
                                                                             opts)))
          ;; load it up with the indexes persisted to disk
          indexed-db (if root
                       (<?? (storage/reify-db store blank-db root))
                       blank-db)
          ;; find all the outstanding tx summaries
          db-blocks  (load-db-blocks store db-block (:t indexed-db))
          ;; merge each db-block into novelty
          loaded-db  (reduce merge-db-block
                             indexed-db
                             db-blocks)

          rebuilt-db-block (block/create-db-block loaded-db
                                                  (get db-block iri/DbBlockAssert)
                                                  (get db-block iri/DbBlockRetract)
                                                  tx-id)]
      ;; fully loaded
      (swap! state assoc db-address loaded-db)
      (block/create-db-summary rebuilt-db-block db-address))))

(defn query-db
  [{:keys [store state] :as idxr} db-address query]
  (if-let [db (get @state db-address)]
    (<?? (jld-query/query-async db query))
    (throw (ex-info "No such db-address." {:error :query/no-such-db
                                           :db-address db-address}))))

(defrecord Indexer [id]
  service-proto/Service
  (id [_] id)
  (stop [idxr] (stop-indexer idxr))

  idxr-proto/Indexer
  (init [idxr ledger-name opts] (init-db idxr ledger-name opts))
  (load [idxr db-address opts] (load-db idxr db-address opts))
  (resolve [idxr db-address] (resolve-db idxr db-address))

  (stage [idxr db-address data opts] (stage-db idxr db-address data opts))
  (query [idxr db-address query] (query-db idxr db-address query))
  (explain [idxr db-address query] (throw (ex-info "TODO" {:todo :explain-not-implemented}))))

(defn create-indexer
  [{:keys [:idxr/id :idxr/store-config :idxr/store] :as config}]
  (let [store (or store (store/start store-config))
        id (or id (random-uuid))
        ;; state is a map of db-address to JsonLdDb
        state (atom {})]
    (log/info "Started Indexer." id)
    (map->Indexer {:id id :store store :config config :state state})))

(defn start
  [config]
  (log/info "Starting Indexer." config)
  (if-let [validation-error (model/explain idxr-model/IndexerConfig config)]
    (throw (ex-info "Invalid indexer config." {:errors (model/report validation-error)
                                               :config config}))
    (create-indexer config)))

(defn stop
  [idxr]
  (service-proto/stop idxr))

(defn init
  [idxr ledger-name opts]
  (idxr-proto/init idxr ledger-name opts))

(defn stage
  [idxr db-address data opts]
  (idxr-proto/stage idxr db-address data opts))

(defn load
  [idxr db-address opts]
  (idxr-proto/load idxr db-address opts))

(defn resolve
  [idxr db-address]
  (idxr-proto/resolve idxr db-address))

(defn query
  [idxr db-address query]
  (idxr-proto/query idxr db-address query))

(defn explain
  [idxr db-address query]
  (idxr-proto/explain idxr db-address query))
