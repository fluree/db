(ns fluree.fluree-server.indexer
  (:require [fluree.indexer.api :as idxr]))

(defn indexing-server-routes
  [idxr]
  [["/db" {:post {:summary "Create a new ledger index."
                  :parameters {:body [:map
                                      [:ledger-name :string]
                                      [:opts {:optional true}
                                       [:map
                                        [:reindex-min-bytes {:optional true} :int]
                                        [:reindex-max-bytes {:optional true} :int]]]]}
                  :handler (fn [{{{:keys [ledger-name opts] :as params} :body} :parameters}]
                             (let [res (idxr/init ledger-name opts)]
                               {:status 200 :body res}))}}]
   ["/db"
    ["/load" {:post {:summary "Load an existing ledger index."
                     :parameters {:body [:map
                                         [:db-address :string]
                                         [:opts {:optional true}
                                          [:map
                                           [:reindex-min-bytes {:optional true} :int]
                                           [:reindex-max-bytes {:optional true} :int]]]]}
                     :handler (fn [{{{:keys [db-address opts] :as params} :body} :parameters}]
                                (let [res (idxr/load db-address opts)]
                                  {:status 200 :body res}))}}]
    ["/stage" {:post {:summary "Index some data."
                      :parameters {:body [:map
                                          [:db-address :string]
                                          [:data [:map-of :string :any]]
                                          [:opts {:optional true} :any]]}
                      :handler (fn [{{{:keys [db-address data opts] :as params} :body} :parameters}]
                                 (let [res (idxr/stage db-address data opts)]
                                   {:status 200 :body res}))}}]
    ["/query" {:post {:summary "Query an index."
                      :parameters {:body [:map
                                          [:db-address :string]
                                          [:query [:map-of :string :any]]]}
                      :handler (fn [{{{:keys [db-address query] :as params} :body} :parameters}]
                                 (println "pushing" (pr-str params))
                                 (let [res (idxr/query db-address query)]
                                   {:status 200 :body res}))}}]]])
