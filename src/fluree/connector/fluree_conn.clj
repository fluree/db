(ns fluree.connector.fluree-conn
  (:require [fluree.db.util.log :as log]
            [fluree.common.protocols :as service-proto]
            [fluree.indexer.api :as idxr]
            [fluree.publisher.api :as pub]
            [fluree.transactor.api :as txr]
            [fluree.connector.protocols :as conn-proto]
            [fluree.store.api :as store]
            [fluree.common.iri :as iri]))

(defn stop-fluree-conn
  [{:keys [transactor indexer publisher] :as conn}]
  (log/info "Stopping FlureeConnection " (service-proto/id conn) ".")
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

(defn fluree-subscribe
  [{:keys [subscriptions publisher] :as conn} ledger-name cb {:keys [auth-claims] :as opts}]
  (if-let [ledger (pub/resolve publisher ledger-name)]
    (let [subscription-key (str (random-uuid))]
      (swap! subscriptions assoc-in [ledger-name subscription-key] {:subscription/opts opts :subscription/cb cb})
      subscription-key)
    (throw (ex-info "No ledger for ledger-address."
                    {:error :connection/invalid-subscription :ledger-name ledger-name}))))

(defn fluree-unsubscribe
  [{:keys [subscriptions] :as idxr} ledger-name subscription-key]
  (swap! subscriptions update ledger-name dissoc subscription-key)
  :unsubscribed)

(defn fluree-broadcast
  "Broadcast the latest block to all subscribers."
  [{:keys [subscriptions indexer publisher] :as idxr} ledger-name]
  (let [ledger     (pub/resolve publisher ledger-name)
        db-address (-> ledger (get iri/LedgerHead) (get iri/LedgerDbHead) (get iri/DbBlockAddress))
        db-block   (idxr/resolve indexer db-address)]
    (doseq [[subscription-key {:keys [subscription/opts subscription/cb]}] (get @subscriptions ledger-name)]
      (log/info "Broadcasting from db-address to" subscription-key "with opts " opts ".")
      ;; TODO: filter db-block for cb auth (:authClaims opt)
      (cb db-block opts))))

(defn fluree-transact
  [conn ledger-name tx opts]
  (let [{txr :transactor pub :publisher idxr :indexer} conn

        ledger (pub/resolve pub ledger-name)
        ledger (get ledger :cred/credential-subject ledger)

        ;; save transaction
        tx-head (txr/transact txr ledger-name tx)

        ;; submit tx for indexing on top of latest db
        db-address (-> ledger (get iri/LedgerHead) (get iri/LedgerDbHead) (get iri/DbBlockAddress))
        db-summary (idxr/stage idxr db-address tx {:tx-id (get tx-head iri/TxSummaryTxId)})

        ;; update ledger head
        new-ledger (pub/publish pub ledger-name {:db-summary db-summary :tx-summary tx-head})]
    ;; broadcast to subscribers
    (fluree-broadcast conn ledger-name)
    ;; return new ledger
    new-ledger))

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
  [conn ledger-name opts]
  (let [{txr :transactor pub :publisher idxr :indexer} conn

        ledger (pub/resolve pub ledger-name)

        _ (when-not ledger
            (throw (ex-info "No ledger found for ledger-name." {:ledger-name ledger-name})))

        {head iri/LedgerHead} (get ledger :cred/credential-subject ledger)

        {indexed-tx-id iri/DbBlockTxId
         db-address iri/DbBlockAddress} (-> head (get iri/LedgerEntryDb))

        ;; load the un-indexed txs
        txs    (load-txs conn ledger-name indexed-tx-id)
        ;; re-stage the unindexed txs in order
        db-summary (reduce (fn [{db-address iri/DbBlockAddress} {:keys [commit/tx]}]
                             (idxr/stage idxr db-address tx))
                           ;; load the db so it's ready to stage against
                           (idxr/load idxr db-address opts)
                           txs)]
    ledger))

(defn fluree-query
  [conn ledger-name query opts]
  (let [ledger              (pub/resolve (:publisher conn) ledger-name)
        ledger              (get ledger :cred/credential-subject ledger)

        db-address          (-> ledger (get iri/LedgerHead) (get iri/LedgerDbHead) (get iri/DbBlockAddress))]
    (idxr/query (:indexer conn) db-address query)))

(defrecord FlureeConnection [id transactor indexer publisher]
  service-proto/Service
  (id [_] id)
  (stop [conn] (stop-fluree-conn conn))

  conn-proto/Connection
  (create [conn ledger-name opts] (fluree-create conn ledger-name opts))
  (load [conn ledger-name opts] (fluree-load conn ledger-name opts))
  (list [conn] (pub/list (:publisher conn)))

  (transact [conn ledger-name tx opts] (fluree-transact conn ledger-name tx opts))
  (query [conn ledger-name query opts] (fluree-query conn ledger-name query opts))

  (subscribe [idxr ledger-name cb opts] (fluree-subscribe idxr ledger-name cb opts))
  (unsubscribe [idxr ledger-name subscription-key] (fluree-unsubscribe idxr ledger-name subscription-key)))

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
                         store    (assoc :pub/store store)))

        subscriptions (atom {})]
    (log/info "Started FlureeConnection." id)
    (map->FlureeConnection
      (cond-> {:id id
               :indexer idxr
               :transactor txr
               :publisher pub
               :subscriptions subscriptions}
        store (assoc :store store)))))
