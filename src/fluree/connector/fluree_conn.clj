(ns fluree.connector.fluree-conn
  (:require [fluree.db.util.log :as log]
            [fluree.common.protocols :as service-proto]
            [fluree.indexer.api :as idxr]
            [fluree.publisher.api :as pub]
            [fluree.transactor.api :as txr]
            [fluree.connector.protocols :as conn-proto]
            [fluree.store.api :as store]
            [fluree.common.iri :as iri]))

(defn stop-conn
  [{:keys [transactor indexer publisher enforcer] :as conn}]
  (log/info "Stopping Connection " (service-proto/id conn) ".")
  (when transactor (service-proto/stop transactor))
  (when indexer (service-proto/stop indexer))
  (when publisher (service-proto/stop publisher))
  :stopped)

(defn fluree-create
  [{:keys [indexer publisher transactor]} ledger-name opts]
  (let [tx-address     (txr/init transactor ledger-name)
        db-address     (idxr/init indexer ledger-name opts)
        ledger-address (pub/init publisher ledger-name (assoc opts
                                                              :db-address db-address
                                                              :tx-address tx-address))]
    ledger-address))

(defn fluree-transact
  [conn ledger-address tx opts]
  (let [{txr :transactor pub :publisher idxr :indexer} conn

        ledger (pub/pull pub ledger-address)

        {ledger-name iri/LedgerName} (get ledger :cred/credential-subject ledger)

        ;; lookup latest db
        db-address (-> ledger (get iri/LedgerHead) (get iri/LedgerEntryDb) (get iri/DbBlockAddress))

        ;; save transaction
        tx-head (txr/transact txr ledger-name tx)

        ;; submit tx for indexing
        db-summary (idxr/stage idxr db-address tx {:tx-id (get tx-head iri/TxSummaryTxId)})]
    ;; update db head
    (pub/push pub ledger-address {:db-summary db-summary :commit-summary tx-head})))

(defn load-txs
  "While commit-t is greater than indexed-t, walk back through ledger heads to find commit
  addresses until we find the last indexed-t or the first commit. Then resolve all the
  commits."
  [{:keys [txr pub] :as _conn} ledger-name indexed-tx-id]
  (let [tx-head (txr/head txr ledger-name)]
    (loop [{address  iri/TxHeadAddress
            tx-id    iri/TxSummaryTxId
            previous iri/TxSummaryPrevious
            :as      commit} (txr/resolve txr (get tx-head iri/TxHeadAddress))

           txs '()]
      (if (not= indexed-tx-id tx-id)
        (if previous
          (let [prev-commit (txr/resolve txr previous)]
            (recur prev-commit (conj txs commit)))
          ;; reached first commit
          txs)
        txs))))

(defn fluree-load
  "Load the index and make sure it's up-to-date to ensure it is ready to handle new
  transactions and queries."
  [conn ledger-address opts]
  (let [{txr :transactor pub :publisher idxr :indexer} conn

        ledger (pub/pull pub ledger-address)

        _ (when-not ledger
            (throw (ex-info "No ledger found for ledger-address." {:ledger-address ledger-address})))

        {head iri/LedgerHead} (get ledger :cred/credential-subject ledger)

        {indexed-tx-id iri/DbBlockTxId
         db-address iri/DbBlockAddress} (-> head (get iri/LedgerEntryDb))


        ;; load the un-indexed txs
        txs    (load-txs conn (get ledger iri/LedgerName) indexed-tx-id)
        ;; re-stage the unindexed txs in order
        db-summary (reduce (fn [{db-address iri/DbBlockAddress} {:keys [commit/tx]}]
                             (idxr/stage idxr db-address tx))
                           ;; load the db so it's ready to stage against
                           (idxr/load idxr db-address opts)
                           txs)]
    ledger))

(defn fluree-query
  [conn ledger-address query opts]
  (let [ledger              (pub/pull (:publisher conn) ledger-address)
        ledger              (get ledger :cred/credential-subject ledger)

        db-address          (-> ledger (get iri/LedgerHead) (get iri/LedgerEntryDb) (get iri/DbBlockAddress))]
    (idxr/query (:indexer conn) db-address query)))

(defrecord FlureeConnection [id transactor indexer publisher enforcer]
  service-proto/Service
  (id [_] id)
  (stop [conn] (stop-conn conn))

  conn-proto/Connection
  (create [conn ledger-name opts] (fluree-create conn ledger-name opts))
  (transact [conn ledger-address tx opts] (fluree-transact conn ledger-address tx opts))
  (query [conn ledger-address query opts] (fluree-query conn ledger-address query opts))
  (load [conn ledger-address opts] (fluree-load conn ledger-address opts))
  (list [conn] (pub/list (:publisher conn)))
  ;; TODO
  #_(subscribe [conn query fn]))

(defn create-fluree-conn
  [{:keys [:conn/id :conn/store-config :conn/did :conn/trust :conn/distrust
           :conn/transactor-config :conn/publisher-config :conn/indexer-config]
    :as   config}]
  (let [id (or id (random-uuid))

        store (when store-config
                (store/start store-config))

        idxr (idxr/start (cond-> indexer-config
                           did      (assoc :idxr/did did)
                           trust    (assoc :idxr/trust trust)
                           distrust (assoc :idxr/distrust distrust)
                           store    (assoc :idxr/store store)))

        txr (txr/start (cond-> transactor-config
                         did      (assoc :txr/did did)
                         trust    (assoc :txr/trust trust)
                         distrust (assoc :txr/distrust distrust)
                         store    (assoc :txr/store store)))

        pub (pub/start (cond-> publisher-config
                         did      (assoc :pub/did did)
                         trust    (assoc :pub/trust trust)
                         distrust (assoc :pub/distrust distrust)
                         store    (assoc :pub/store store)))]
    (log/info "Starting FlureeConnection " id "." config)
    (map->FlureeConnection
      (cond-> {:id id
               :indexer idxr
               :transactor txr
               :publisher pub}
        store (assoc :store store)))))
