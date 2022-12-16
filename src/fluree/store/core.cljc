(ns fluree.store.core
  (:refer-clojure :exclude [read])
  (:require [fluree.common.model :as model]
            [fluree.common.protocols :as service-proto]
            [fluree.store.file :as file-store]
            [fluree.store.memory :as memory-store]
            [fluree.store.model :as store-model]
            [fluree.store.protocols :as store-proto]))

(defn start
  "Takes a config and returns a possibly stateful Store."
  [{:keys [:store/method] :as config}]
  (if-let [validation-error (model/explain store-model/StoreConfig config)]
    (throw (ex-info "Invalid store config." (model/report validation-error)))
    (case method
      :file (file-store/create-file-store config)
      :memory (memory-store/create-memory-store config)
      (throw (ex-info (str "No store implementation exists for :store/method: " (pr-str method))
                      config)))))

(defn stop
  "Gracefully shuts down a store."
  [store]
  (service-proto/stop store))

(defn write
  [store k data]
  (store-proto/write store k data))

(defn read
  [store k]
  (store-proto/read store k))

(defn delete
  [store k]
  (store-proto/delete store k))

(defn address
  [store class k]
  (store-proto/address store class k))
