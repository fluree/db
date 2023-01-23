(ns fluree.connector.fluree-conn
  (:require [fluree.db.util.log :as log]
            [fluree.common.protocols :as service-proto]
            [fluree.indexer.api :as idxr]
            [fluree.publisher.api :as pub]
            [fluree.transactor.api :as txr]
            [fluree.connector.protocols :as conn-proto]
            [fluree.store.api :as store]))

(defn stop-conn
  [{:keys [transactor indexer publisher enforcer] :as conn}]
  (log/info "Stopping Connection " (service-proto/id conn) ".")
  (when transactor (service-proto/stop transactor))
  (when indexer (service-proto/stop indexer))
  (when publisher (service-proto/stop publisher))
  :stopped)

(defn fluree-create
  [{:keys [indexer publisher]} ledger-name opts]
  (let [db-address     (idxr/init indexer ledger-name opts)
        ledger-address (pub/init publisher ledger-name (assoc opts :db-address db-address))]
    ledger-address))

(defn fluree-transact
  [conn ledger-address tx opts]
  (let [{txr :transactor pub :publisher idxr :indexer} conn

        ledger              (pub/pull pub ledger-address)
        {head :ledger/head} (get ledger :cred/credential-subject ledger)

        ;; lookup latest db
        db-address     (-> head :entry/db-summary :db/address)
        ;; lookup latest commit
        {commit-address :commit/address prev-t :commit/t}
        (-> head :entry/commit-summary)

        ;; write tx in next commit
        commit-summary (txr/commit txr tx {:commit/t (inc (or prev-t 0))
                                           :commit/prev commit-address
                                           :ledger/name (:ledger/name ledger)})

        _ (pub/push pub ledger-address {:commit-summary commit-summary})
        ;; submit tx for indexing
        db-summary (idxr/stage idxr db-address tx)]
    ;; update db head
    (pub/push pub ledger-address {:db-summary db-summary})))

(defn load-commits
  "While commit-t is greater than indexed-t, walk back through ledger heads to find commit
  addresses until we find the last indexed-t or the first commit. Then resolve all the
  commits."
  [{:keys [txr pub] :as _conn} head indexed-t]
  (loop [ledger-entry head
         commit-addresses '()]
    (let [{:keys [entry/previous entry/commit-summary]} ledger-entry
          {:keys [commit/address commit/t]} commit-summary]
      (if (and t indexed-t (> t indexed-t))
        (if previous
          (let [prev-ledger (pub/pull pub previous)
                {prev-entry :ledger/head} (get prev-ledger :cred/credential-subject prev-ledger)]
            (recur prev-entry (conj commit-addresses address)))
          ;; reached first commit
          (map (partial txr/resolve txr) commit-addresses))
        ;; reached indexed commit
        (map (partial txr/resolve txr) commit-addresses)))))

(defn fluree-load
  "Load the index and make sure it's up-to-date to ensure it is ready to handle new
  transactions and queries."
  [conn ledger-address opts]
  (let [{txr :transactor pub :publisher idxr :indexer} conn

        ledger (pub/pull pub ledger-address)

        _ (when-not ledger
            (throw (ex-info "No ledger found for ledger-address." {:ledger-address ledger-address})))

        {head :ledger/head} (get ledger :cred/credential-subject ledger)

        db-summary (-> head :entry/db-summary)
        commit-summary (-> head :entry/commit-summary)

        ;; load the un-indexed commits
        commits (load-commits conn head (- (:db/t db-summary)))
        ;; re-stage the commits in order
        db-summary (reduce (fn [db-summary {:keys [commit/tx]}]
                             (idxr/stage idxr (:db/address db-summary) tx))
                           ;; load the db so it's ready to stage against
                           (:db/address (idxr/load idxr (:db/address db-summary) opts))
                           commits)]
    ledger))

(defn fluree-query
  [conn ledger-address query opts]
  (let [ledger              (pub/pull (:publisher conn) ledger-address)
        _ (def lll ledger)
        {head :ledger/head} (get ledger :cred/credential-subject ledger)
        db-address          (-> head :entry/db-summary :db/address)]
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
