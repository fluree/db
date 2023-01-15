(ns fluree.indexer.core
  (:refer-clojure :exclude [load])
  (:require
   [clojure.core.async :as async]
   [fluree.common.model :as model]
   [fluree.common.protocols :as service-proto]
   [fluree.db.api.query :as jld-query]
   [fluree.db.json-ld.commit :as jld-commit]
   [fluree.db.json-ld.transact :as jld-transact]
   [fluree.db.util.async :refer [<?? go-try]]
   [fluree.db.util.log :as log]
   [fluree.indexer.db :as db]
   [fluree.indexer.tx-summary :as tx-summary]
   [fluree.indexer.model :as idxr-model]
   [fluree.indexer.protocols :as idxr-proto]
   [fluree.store.api :as store]
   [fluree.db.indexer.proto :as idx-proto]
   [fluree.json-ld :as json-ld]
   [fluree.common.identity :as ident]
   [fluree.db.storage.core :as storage]
   [fluree.db.json-ld.reify :as jld-reify]
   [fluree.db.constants :as const]))

(defn stop-indexer
  [idxr]
  ;; TODO: call idx-proto/-index when stopping to flush novelty to Store
  (log/info "Stopping Indexer " (service-proto/id idxr) ".")
  (store/stop (:store idxr)))

(defn init-db
  [{:keys [store config db-map] :as idxr} ledger-name opts]
  (let [db (db/create store ledger-name (merge config opts))
        db-address (db/create-db-address db (str ledger-name "/tx/init"))]
    (if (get @db-map db-address)
      (throw (ex-info "Ledger db already exists." {:ledger ledger-name}))
      (do
        (swap! db-map assoc db-address db)
        db-address))))

(defn stage-db
  "Index the given data, then store a tx-summary."
  [{:keys [store db-map] :as idxr} db-address data]
  (if-let [db-before (get @db-map db-address)]
    (let [{ledger-name :ledger/name} (db/db-path-parts db-address)
          db-after                   (<?? (jld-transact/stage (db/prepare db-before) data {}))
          ;; This is a hack to sync t into various places since dbs shouldn't care about branches
          ;; used in do-index > refresh > index-update > empty-novelty
          db-after                   (assoc-in db-after [:commit :data :t] (- (:t db-after)))

          idx-writer (-> db-after :ledger :indexer)

          {:keys [context did private push?] :as _opts} data

          ;; create the information necessary for tx-summary
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

          ;; create tx-summary and write it to store
          tx-summary    (tx-summary/create-tx-summary db-after @ctx-used-atom assert retract)
          tx-summary-id (:path (<?? (store/write store (tx-summary/tx-path ledger-name) tx-summary
                                                 {:content-address? true})))

          ;; save newest tx-summary so the next stage knows the previous tx
          db-final   (assoc db-after :tx-summary-id tx-summary-id)
          db-address (db/create-db-address db-final tx-summary-id)]
      ;; add an entry of db-address -> db
      (swap! db-map assoc db-address db-final)

      ;; return db-summary, a truncated tx-summary
      (tx-summary/create-db-summary tx-summary db-address))
    (throw (ex-info "No such db-address." {:error      :stage/no-such-db
                                           :db-address db-address}))))

(defn load-tx-summaries
  "Read each tx-summary's previous until we reach the stop t or run out of previous to follow."
  [store head-tx-summary stop-t]
  (loop [{:db/keys [previous]} head-tx-summary
         summaries             (list head-tx-summary)]
    (if-let [prev-summary (<?? (store/read store previous))]
      (if (> (:db/t prev-summary) stop-t)
        summaries
        (recur prev-summary (conj summaries prev-summary)))
      summaries)))

(defn merge-tx-summary
  "Merge the tx summary into novelty."
  [db tx-summary]
  (let [{:db/keys [assert retract context t]} tx-summary

        iris                     (volatile! {})
        refs                     (volatile! (-> db :schema :refs))

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
  [{:keys [store db-map] :as idxr} db-address opts]
  (if-let [db (get @db-map db-address)]
    ;; already loaded
    (let [tx-summary (<?? (store/read store (:tx-summary-id db)))]
      ;; update opts
      (swap! db-map update db-address db/update-index-writer-opts opts)
      (tx-summary/create-db-summary tx-summary db-address))

    ;; rebuild db from persisted data
    (let [tx-summary-id (:address/path (ident/address-parts db-address))
          tx-summary    (<?? (store/read store tx-summary-id))

          {ledger-name :ledger/name}        (db/db-path-parts db-address)
          {:db/keys [root previous t opts]} tx-summary

          ;; create a blank db
          blank-db   (db/create store ledger-name opts)
          ;; load it up with the indexes persisted to disk
          indexed-db (<?? (storage/reify-db store blank-db root))
          ;; find all the outstanding tx summaries
          tx-summaries (load-tx-summaries store tx-summary (:t indexed-db))
          ;; merge each tx-summary into novelty
          loaded-db (reduce merge-tx-summary
                            indexed-db
                            tx-summaries)

          rebuilt-tx-summary (tx-summary/create-tx-summary loaded-db
                                                           (:db/context tx-summary)
                                                           (:db/assert tx-summary)
                                                           (:db/retract tx-summary))]
      ;; fully loaded
      (swap! db-map assoc db-address loaded-db)
      (tx-summary/create-db-summary rebuilt-tx-summary db-address))))

(defn query-db
  [{:keys [store db-map] :as idxr} db-address query]
  (if-let [db (get @db-map db-address)]
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
  (stage [idxr db-address data] (stage-db idxr db-address data))
  (query [idxr db-address query] (query-db idxr db-address query))
  (explain [idxr db-address query] (throw (ex-info "TODO" {:todo :explain-not-implemented}))))

(defn create-indexer
  [{:keys [:idxr/id :idxr/store-config :idxr/store] :as config}]
  (let [store (or store (store/start store-config))
        id (or id (random-uuid))]
    (log/info "Starting Indexer " id "." config)
    (map->Indexer {:id id :store store :config config :db-map (atom {})})))

(defn start
  [config]
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
  [idxr db-address data]
  (idxr-proto/stage idxr db-address data))

(defn load
  [idxr db-address opts]
  (idxr-proto/load idxr db-address opts))

(defn query
  [idxr db-address query]
  (idxr-proto/query idxr db-address query))

(defn explain
  [idxr db-address query]
  (idxr-proto/explain idxr db-address query))
