(ns fluree.store.core
  (:refer-clojure :exclude [read list])
  (:require [fluree.common.model :as model]
            [fluree.common.protocols :as service-proto]
            [fluree.store.file :as file-store]
            [fluree.store.memory :as memory-store]
            [fluree.store.model :as store-model]
            [fluree.store.protocols :as store-proto]
            [fluree.db.util.log :as log]))

(defn start
  "Takes a config and returns a possibly stateful Store."
  [{:keys [:store/method] :as config}]
  (log/info "Starting Store." config)
  (if-let [validation-error (model/explain store-model/StoreConfig config)]
    (throw (ex-info "Invalid store config." {:errors (model/report validation-error)
                                             :config config}))
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
  [store path data opts]
  (store-proto/write store path data opts))

(defn read
  [store path opts]
  (store-proto/read store path opts))

(defn list
  [store prefix]
  (store-proto/list store prefix))

(defn delete
  [store path]
  (store-proto/delete store path))

(defn address
  [store class path]
  (store-proto/address store class path))
