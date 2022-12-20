(ns fluree.indexer.core
  (:require
   [clojure.core.async :as async]
   [fluree.common.model :as model]
   [fluree.common.protocols :as service-proto]
   [fluree.db.api.query :as jld-query]
   [fluree.db.json-ld.transact :as jld-transact]
   [fluree.db.util.async :refer [<?? go-try]]
   [fluree.db.util.log :as log]
   [fluree.indexer.db :as db]
   [fluree.indexer.model :as idxr-model]
   [fluree.indexer.protocols :as idxr-proto]
   [fluree.store.api :as store]
   [fluree.db.indexer.proto :as idx-proto]))

(defn stop-indexer
  [idxr]
  ;; TODO: call idx-proto/-index when stopping to flush novelty to Store
  (log/info "Stopping Indexer " (service-proto/id idxr) ".")
  (store/stop (:store idxr)))

(defn init-db
  [{:keys [store config] :as idxr} opts]
  (let [db (db/create store (merge config opts))
        db-address (db/create-db-address db)]
    (if (<?? (store/read store db-address))
      db-address
      (do
        (<?? (store/write store db-address db))
        db-address))))

(defn stage-db
  [{:keys [store] :as idxr} db-address data]
  (if-let [db (<?? (store/read store db-address))]
    (let [db0        (db/prepare db)
          db1        (<?? (jld-transact/stage db0 data {}))
          db-address (db/create-db-address db1)
          idx        (-> db1 :ledger :indexer)]
      (<?? (store/write store db-address db1))
      (when (idx-proto/-index? idx db1)
        (idx-proto/-index idx db1))
      ;; return db-info
      {:db/address db-address
       :db/v       0
       :db/t       (- (:t db1))
       :db/flakes  (-> db1 :stats :flakes)
       :db/size    (-> db1 :stats :size)
       ;; TODO: calculate assert+retract
       :db/assert  []
       :db/retract []})
    (throw (ex-info "No such db-address." {:error      :stage/no-such-db
                                           :db-address db-address}))))

(defn discard-db
  [{:keys [store] :as idxr} db-address]
  (store/delete store db-address)
  :idxr/discarded)

(defn query-db
  [{:keys [store] :as idxr} db-address query]
  (if-let [db (<?? (store/read store db-address))]
    (<?? (jld-query/query-async db query))
    (throw (ex-info "No such db-address." {:error :query/no-such-db
                                           :db-address db-address}))))

(defn explain-query
  [idxr db-address query]
  (throw (ex-info "TODO" {:todo :explain-not-implemented})))

(defrecord Indexer [id]
  service-proto/Service
  (id [_] id)
  (stop [idxr] (stop-indexer idxr))

  idxr-proto/Indexer
  (init [idxr opts] (init-db idxr opts))
  (stage [idxr db-address data] (stage-db idxr db-address data))
  (query [idxr db-address query] (query-db idxr db-address query))
  (explain [idxr db-address query] (explain-query idxr db-address query)))

(defn create-indexer
  [{:keys [:idxr/id :idxr/store-config :idxr/store] :as config}]
  (let [store (or store (store/start store-config))
        id (or id (random-uuid))]
    (log/info "Starting Indexer " id "." config)
    (map->Indexer {:id id :store store :config config})))

(defn start
  [config]
  (if-let [validation-error (model/explain idxr-model/IndexerConfig config)]
    (throw (ex-info "Invalid indexer config." {:errors (model/report validation-error)}))
    (create-indexer config)))

(defn stop
  [idxr]
  (service-proto/stop idxr))

(defn init
  [idxr opts]
  (idxr-proto/init idxr opts))

(defn stage
  [idxr db-address data]
  (idxr-proto/stage idxr db-address data))

(defn discard
  [idxr db-address]
  (idxr-proto/discard idxr db-address))

(defn query
  [idxr db-address query]
  (idxr-proto/query idxr db-address query))

(defn explain
  [idxr db-address query]
  (idxr-proto/explain idxr db-address query))


(comment
  (def idxr (start {:idxr/store-config {:store/method :memory}}))

  (def xxx (-> idxr :store :storage-atom deref))

  (init idxr {})
  "fluree:db:memory:init"


  (stage idxr "fluree:db:memory:init" {:context {:ex "http://ex.co/"}
                                       "@id" "ex/dan"
                                       "ex/foo" "bar"})
  #:db{:address "fluree:db:memory:91e45a25-e184-4ed1-8227-5ec16d1c1a01", :t 1, :flakes 6, :size 466, :assert [], :retract []}

  (def xxx (-> idxr :store :storage-atom deref))

  xxx

  (def db (get xxx "fluree:db:memory:91e45a25-e184-4ed1-8227-5ec16d1c1a01"))

  (keys db)
  (:ledger :conn :method :alias :branch :commit :block :t :tt-id :stats :spot :psot :post :opst :tspo :schema :comparators :novelty :permissions :ecount :state)

  (:spot db)
  {:children
   {"[9223372036854775807 0 9223372036854775807 5 0 true nil]"
    {:block 0,
     :ledger-id "",
     :leaf true,
     :size 0,
     :leftmost? true,
     :id :empty,
     :tempid #uuid "14b7520c-ef09-464e-bf45-98621903450a",
     :comparator "[fluree.db.flake/cmp-flakes-spot]",
     :t 0,
     :network nil,
     :first "[9223372036854775807 0 9223372036854775807 5 0 true nil]",
     :tt-id #uuid "91e45a25-e184-4ed1-8227-5ec16d1c1a01",
     :rhs nil}},
   :block 0,
   :ledger-id "",
   :leaf false,
   :size 0,
   :leftmost? true,
   :id :empty,
   :tempid #uuid "540e620b-5657-4302-8182-8f43eb335616",
   :comparator "[fluree.db.flake/cmp-flakes-spot]",
   :t 0,
   :network nil,
   :first "[9223372036854775807 0 9223372036854775807 5 0 true nil]",
   :tt-id #uuid "91e45a25-e184-4ed1-8227-5ec16d1c1a01",
   :rhs nil}

  ,)
