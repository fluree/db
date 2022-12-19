(ns fluree.publisher.ledger
  (:require [fluree.common.identity :as ident]
            [fluree.store.api :as store]))

(defn create-ledger-id
  "Create a stable ledger-id"
  [ledger-name]
  (ident/create-id "ledger" ledger-name))

(defn create-ledger-address
  [store ledger-name]
  (store/address store "ledger" (str "head/" ledger-name)))

(defn create
  "Creates a ledger."
  [store ledger-name {:keys [context head-address db-address] :as opts}]
  (let [id      (create-ledger-id ledger-name)
        address (create-ledger-address store ledger-name)]
    {:id             id
     :type           :ledger
     :ledger/address address
     :ledger/name    ledger-name
     :ledger/v       0}))
