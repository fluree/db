(ns fluree.fluree-server.fluree
  (:require
   [clojure.walk :as walk]
   [fluree.connector.api :as conn]
   [fluree.http-server.api :as http-server]
   [fluree.db.util.log :as log]))

(defn fluree-server-routes
  [conn]
  [["/ledger" {:get {:summary "List ledgers."
                     :handler (fn [_req]
                                (let [res (conn/list conn)]
                                  (println "list ledgers" res)
                                  {:status 200 :body res}))}
               :post {:summary "Create a ledger."
                      :parameters {:body [:map
                                          [:name :string]
                                          [:opts {:optional true} [:map-of :string :any]]]}
                      :handler (fn [{{{ledger-name :name :as params} :body} :parameters}]
                                 (let [res (conn/create conn ledger-name)]
                                   {:status 200 :body res}))}}]
   ["/ledger/:ledger-name" {:get {:summary "View the ledger summary."
                                  :parameters {:path [:map [:ledger-name :string]]}
                                  :handler (fn [{{{ledger-name :ledger-name} :body} :parameters}]
                                             (let [res (conn/load conn (str "fluree:ledger:ledger/" ledger-name))]
                                               {:status 200 :body res}))}}]
   ["/ledger/:ledger-name"
    ["/transact"
     {:post {:summary "Transact data to the ledger."
             :parameters
             {:path [:map
                     [:ledger-name :string]]
              :body [:map
                     [:ledger :string]
                     [:tx [:map-of :string :any]]
                     [:opts {:optional true} [:map-of :string :any]]]}
             :handler (fn [{{{:keys [ledger tx opts] :as body-params} :body
                             {:keys [ledger-name] :as path-params} :path}
                            :parameters}]
                        (println "transacting" (pr-str body-params) (pr-str path-params))
                        (let [res (conn/transact conn ledger-name tx opts)]
                          {:status 200 :body res}))}}]
    ["/query" {:post {:summary "Query a ledger."
                      :parameters {:path [:map [:ledger-name :string]]
                                   :body [:map
                                          [:ledger :string]
                                          [:query [:map-of :string :any]]
                                          [:opts {:optional true} [:map-of :string :any]]]}
                      :handler (fn [{{{:keys [ledger query opts] :as params} :body
                                      {:keys [ledger-name] :as params} :path} :parameters}]
                                 (println "querying" (pr-str params))
                                 (let [res (conn/query conn ledger-name (walk/keywordize-keys query) opts)]
                                   {:status 200 :body res}))}}]
    ["/subscribe" {:post {:summary "Subscribe to a ledger"
                          :parameters {:path [:map [:ledger-name :string]]
                                       :body [:map
                                              [:ledger :string]
                                              [:authClaims {:optional true} [:map-of :string :any]]]}
                          :handler
                          (fn [{{{:keys [ledger authClaims] :as params} :body
                                 {:keys [ledger-name] :as params} :path} :parameters :as req}]
                            ;; a hack to allow us to use the subscription key in :on-error and :on-close
                            (let [sub-key (atom nil)]
                              (http-server/websocket-handler
                                {:on-connect
                                 (fn [ws]
                                   (let [subscription-key
                                         (conn/subscribe conn
                                                         ledger
                                                         (fn [block opts] (http-server/ws-send! ws block))
                                                         {:authClaims authClaims})]
                                     (reset! sub-key subscription-key)
                                     (log/info "New websocket subscription." subscription-key)
                                     (http-server/ws-send! ws subscription-key)))
                                 :on-error
                                 (fn [ws error] (conn/unsubscribe ledger-name @sub-key))
                                 :on-close
                                 (fn [ws status-code reason] (conn/unsubscribe ledger @sub-key))})))}}]
    ["/unsubscribe" {:post {:summary "Unsubscribe from a ledger"
                            :parameters {:path [:map [:ledger-name :string]]
                                         :body [:map
                                                [:ledger :string]
                                                [:subcription-key :string]]}
                            :handler
                            (fn [{{{:keys [ledger subscription-key] :as params} :body
                                   {:keys [ledger-name] :as params} :path}
                                  :parameters :as req}]
                              (conn/unsubscribe conn ledger-name subscription-key))}}]]
   ["/admin"
    ["ui" {:get {:handler (fn [_] {:status 200 :body "Admin User Interface"})}}]]])

(defn initialize-fluree-server
  "Load all ledgers."
  [conn]
  (doseq [ledger (conn/list conn)]
    (conn/load conn (:ledger/address ledger))))

(defn start-fluree-server
  [conn]
  ;; load the ledgers
  (initialize-fluree-server conn)
  ;; return the routes
  (fluree-server-routes conn))
