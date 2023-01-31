(ns fluree.fluree-server.publisher
  (:require [fluree.publisher.api :as pub]
            [fluree.transactor.api :as txr]
            [fluree.indexer.api :as idxr]))

(defn name-server-routes
  [pub]
  [["/ledger" {:get {:summary "List all ledgers tracked by name server."
                     :handler (fn [_]
                                (let [res (pub/list pub)]
                                  {:status 200 :body res}))}
               :post {:summary "Initialize a new ledger."
                      :parameters {:body [:map
                                          [:name :string]
                                          [:opts {:optional true}
                                           [:map
                                            [:context {:optional true} [:map-of :string :any]]
                                            [:tx-address {:optional true} :string]
                                            [:db-address {:optional true} :string]]]]}
                      :handler (fn [{{{:keys [name opts]} :bod } :parameters }]
                                 (let [res (pub/init pub name opts)]
                                   {:status 200 :body res}))}}]
   ["/ledger/:ledger-name" {:get {:summary "Retrieve ledger summary."
                                   :parameters {:path [:map [:ledger-name :string]]}
                                   :handler (fn [{{{:keys [ledger-name]} :path} :parameters}]
                                              (let [res (pub/resolve pub ledger-name)]
                                                {:status 200 :body res}))}}]
   ["/ledger/:ledger-name"
    ["/publish" {:post {:summary "Publish a new ledger head."
                        :parameters {:path [:map [:ledger-name :string]]
                                     :body [:map
                                            [:summary
                                             [:map
                                              [:tx-summary {:optional true} txr/TxHead]
                                              [:db-summary {:optional true} idxr/DbBlockSummary]]]]}
                        :handler (fn [{{{:keys [ledger-address summary] :as params} :body} :parameters}]
                                   (println "pushing" (pr-str params))
                                   (let [res (pub/push pub ledger-address summary)]
                                     {:status 200 :body res}))}}]]])
