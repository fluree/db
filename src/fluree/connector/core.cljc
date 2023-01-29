(ns fluree.connector.core
  (:refer-clojure :exclude [list load])
  (:require
   [fluree.common.model :as model]
   [fluree.common.protocols :as service-proto]
   [fluree.connector.fluree-conn :as fluree-conn]
   [fluree.connector.model :as conn-model]
   [fluree.connector.protocols :as conn-proto]))

(defn connect
  [{:keys [:conn/mode] :as config}]
  (if-let [validation-error (model/explain conn-model/ConnectionConfig config)]
    (throw (ex-info "Invalid connection config." {:errors (model/report validation-error)
                                                  :config config}))
    (case mode
      :fluree (fluree-conn/create-fluree-conn config))))

(defn close
  [conn]
  (service-proto/stop conn))

(defn list
  [conn]
  (conn-proto/list conn))

(defn create
  [conn ledger-name opts]
  (conn-proto/create conn ledger-name opts))

(defn load
  [conn ledger-address opts]
  (conn-proto/load conn ledger-address opts))

(defn transact
  [conn ledger-address tx opts]
  (conn-proto/transact conn ledger-address tx opts))

(defn query
  [conn ledger-address query opts]
  (conn-proto/query conn ledger-address query opts))

(defn subscribe
  [conn ledger-address cb opts]
  (conn-proto/subscribe conn ledger-address cb opts))

(defn unsubscribe
  [conn ledger-address subscription-key]
  (conn-proto/unsubscribe conn ledger-address subscription-key))
