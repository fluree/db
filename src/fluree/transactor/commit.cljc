(ns fluree.transactor.commit
  (:require [fluree.common.identity :as ident]
            [fluree.json-ld :as json-ld]
            [fluree.store.api :as store]))

(defn create-commit-id
  [data]
  (ident/create-id :commit data))

(defn create-commit-address
  [store ledger-name id]
  (store/address store :commit (str ledger-name "/commit/" id)))

(defn create
  [tx db-info]
  (let [{:keys [db/address db/context db/t db/flakes db/size db/assert db/retract
                commit/prev txr/store ledger/name]}
        db-info
        ;; TODO: properly figure out asserts, retracts
        commit-tx (cond-> {:commit/assert assert
                           :commit/retract retract
                           :commit/context context
                           :commit/t t
                           ;; hardcode v to 0 until we need additional versions
                           :commit/v 0}
                    prev (assoc :commit/prev prev))
        data (json-ld/normalize-data commit-tx)
        id (create-commit-id data)
        {hash :id/hash} (ident/id-parts id )]
    {:id id
     :type :commit
     :commit/address (create-commit-address store name hash)
     :db/address address
     :commit/hash hash
     :commit/size size
     :commit/flakes flakes
     :commit/tx commit-tx}))
