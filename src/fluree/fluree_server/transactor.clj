(ns fluree.fluree-server.transactor
  (:require [fluree.transactor.api :as txr]))

(defn transaction-server-routes
  [txr]
  [["/transactor" {:post {:summary "Iniitialize the transactor to start receiving transactions."
                          :parameters {:body [:map [:name :string]]}
                          :handler (fn [{{{ledger-name :name} :body} :parameters}]
                                     (let [res (txr/init txr ledger-name)]
                                       {:status 200 :body res}))}}]

   ["/transactor/:ledger-name" {:get {:summary "Retrieve the head transaction for the ledger."
                                      :parameters {:path [:map [:ledger-name :string]]}
                                      :handler (fn [{{{ledger-name :ledger-name} :body} :parameters}]
                                                 (let [res (txr/head txr ledger-name)]
                                                   {:status 200 :body res}))}}]
   ["/transactor/:ledger-name"
    ["/transact" {:post {:summary "Commit a transaction."
                         :parameters {:path [:map
                                             [:ledger-name :string]]
                                      :body [:map
                                             [:tx [:map-of :string :any]]
                                             [:opts {:optional true} [:map-of :string :any]]]}
                         :handler (fn [{{{:keys [tx] :as params} :body
                                         {:keys [ledger-name] :as params} :path}
                                        :parameters}]
                                    (let [res (txr/transact txr ledger-name tx)]
                                      {:status 200 :body res}))}}]
    ["/resolve" {:post {:summary "Retrieve a transaction."
                        :parameters {:body [:map [:tx-address :string]]}
                        :handler (fn [{{{:keys [tx-address] :as params} :body} :parameters}]
                                   (let [res (txr/resolve txr tx-address)]
                                     {:status 200 :body res}))}}]]])

(defn start-transaction-server
  [txr]
  (transaction-server-routes txr))
