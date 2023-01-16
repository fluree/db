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
  [store tx tx-info]
  (let [{:keys [commit/t commit/prev ledger/name]} tx-info

        commit-data    (json/stringify tx)
        size           (count commit-data)
        hash           (crypto/sha2-256 commit-data)
        path           (str name "/commit/" hash)
        commit-address (create-commit-address store path)
        commit (cond-> {:commit/size size
                        :commit/tx tx
                        :commit/t t
                        ;; hardcode v to 0 until we need additional versions
                        :commit/v 0}
                 prev (assoc :commit/prev prev))]
    {:address commit-address
     :hash    hash
     :value   commit}))
