(ns fluree.transactor.commit
  (:require [fluree.common.identity :as ident]
            [fluree.json-ld :as json-ld]
            [fluree.store.api :as store]
            [fluree.crypto :as crypto]
            [clojure.string :as str]
            [fluree.db.util.json :as json]))

(defn create-commit-address
  [store path]
  (store/address store :commit path))

(defn create
  [tx db-info]
  (let [{:keys [db/address db/context db/t db/flakes db/size db/assert db/retract
                commit/prev txr/store ledger/name]}
        db-info
        ;; TODO: properly figure out asserts, retracts
        commit-tx      (cond-> {:type :commit
                                :commit/size size
                                :commit/flakes flakes
                                :commit/assert assert
                                :commit/retract retract
                                :commit/tx tx
                                :commit/t t
                                ;; hardcode v to 0 until we need additional versions
                                :commit/v 0}
                         prev (assoc :commit/prev prev))

        commit-data    (json/stringify commit-tx)
        hash           (crypto/sha2-256 commit-data)
        path           (str name "/commit/" hash)
        commit-address (create-commit-address store path)]
    {:address commit-address
     :hash    hash
     :value   commit-tx}))
