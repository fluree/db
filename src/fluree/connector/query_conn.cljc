(ns fluree.connector.query-conn
  (:require [fluree.store.api :as store]))

(defrecord QueryConnection []
    )

(defn create-fluree-conn
  [{:keys [:conn/id :conn/store-config :conn/indexer-config] :as config}]
  (let [id (or id (random-uuid))


        store (when store-config
                (store/start store-config))

        idxr  (idxr/start (if store
                            (assoc indexer-config :idxr/store store)
                            indexer-config))]
    (log/info "Starting QueryConnection " id "." config)
    (map->QueryConnection
      (cond-> {:id id
               :indexer idxr
               :transactor txr
               :publisher pub}
        store (assoc :store store)))))
