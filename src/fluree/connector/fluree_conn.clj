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
  (let [commit-address (txr/init transactor ledger-name)
        db-address     (idxr/init indexer ledger-name opts)
        ledger-address (pub/init publisher ledger-name (assoc opts :db-address db-address))]
    ledger-address))

(defn fluree-transact
  [conn ledger-address tx opts]
  (let [{txr :transactor pub :publisher idxr :indexer} conn

        ledger                (pub/pull pub ledger-address)

        {ledger-name iri/LedgerName} (get ledger :cred/credential-subject ledger)

        ;; lookup latest db
        db-address (-> ledger (get iri/LedgerHead) (get iri/LedgerEntryDb) (get iri/DbBlockAddress))

        ;; write tx in next commit
        commit-summary (txr/commit txr ledger-name tx)

        ;; submit tx for indexing
        db-summary (idxr/stage idxr db-address tx)]
    ;; update db head
    (pub/push pub ledger-address {:db-summary db-summary :commit-summary commit-summary})))

(defn load-commits
  "While commit-t is greater than indexed-t, walk back through ledger heads to find commit
  addresses until we find the last indexed-t or the first commit. Then resolve all the
  commits."
  [{:keys [txr pub] :as _conn} ledger-name indexed-t]
  (let [head-commit-summary (txr/load txr ledger-name)]
    (loop [{address  iri/CommitAddress
            t        iri/CommitT
            previous iri/CommitPrevious
            :as commit} (txr/resolve txr (get head-commit-summary iri/CommitAddress))

           commits '()]
      (if (and t indexed-t (> t indexed-t))
        (if previous
          (let [prev-commit (txr/resolve txr previous)]
            (recur prev-commit (conj commits commit)))
          ;; reached first commit
          commits)))))

(defn fluree-load
  "Load the index and make sure it's up-to-date to ensure it is ready to handle new
  transactions and queries."
  [conn ledger-address opts]
  (let [{txr :transactor pub :publisher idxr :indexer} conn

        ledger (pub/pull pub ledger-address)

        _ (when-not ledger
            (throw (ex-info "No ledger found for ledger-address." {:ledger-address ledger-address})))

        {head iri/LedgerHead} (get ledger :cred/credential-subject ledger)

        {indexed-t iri/DbBlockT
         db-address iri/DbBlockAddress} (-> head (get iri/LedgerEntryDb))


        ;; load the un-indexed commits
        commits    (load-commits conn (get ledger iri/LedgerName) indexed-t)
        ;; re-stage the unindexed commits in order
        db-summary (reduce (fn [{db-address iri/DbBlockAddress} {:keys [commit/tx]}]
                             (idxr/stage idxr db-address tx))
                           ;; load the db so it's ready to stage against
                           (idxr/load idxr db-address opts)
                           commits)]
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
  [{:keys [:conn/id :conn/store-config :conn/transactor-config :conn/publisher-config :conn/indexer-config]
    :as config}]
  (let [id (or id (random-uuid))

        store (when store-config
                (store/start store-config))

        idxr  (idxr/start (if store
                            (assoc indexer-config :idxr/store store)
                            indexer-config))

        txr   (txr/start (if store
                           (assoc transactor-config :txr/store store)
                           transactor-config))

        pub   (pub/start (if store
                           (assoc publisher-config :pub/store store)
                           publisher-config))]
    (log/info "Starting FlureeConnection " id "." config)
    (map->FlureeConnection
      (cond-> {:id id
               :indexer idxr
               :transactor txr
               :publisher pub}
        store (assoc :store store)))))
