(ns fluree.fluree-server.fluree-api
  (:require [fluree.connector.api :as conn]
            [fluree.http-server.api :as http-server]
            [clojure.walk :as walk]
            [reitit.ring :as ring]))

(defn fluree-server-api-routes
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
             :handler
             (fn [{{{:keys [ledger tx opts] :as body-params} :body
                    {:keys [ledger-name] :as path-params} :path}
                   :parameters}]
               (println "transacting" (pr-str body-params) (pr-str path-params))
               (let [res (conn/transact conn ledger tx opts)]
                 {:status 200 :body res}))}}]
    ["/query" {:post {:summary "Query a ledger."
                      :parameters {:body [:map
                                          [:ledger :string]
                                          [:query [:map-of :string :any]]
                                          [:opts {:optional true} [:map-of :string :any]]]}
                      :handler (fn [{{{:keys [ledger query opts] :as params} :body} :parameters :as req}]
                                 (println "querying" (pr-str params))
                                 (let [res (conn/query conn ledger (walk/keywordize-keys query) opts)]
                                   {:status 200 :body res}))}}]
    ["/subscribe" {:post {:summary "Subscribe to a ledger"
                          :parameters {:body [:map
                                              [:ledger :string]
                                              [:authClaims {:optional true} [:map-of :string :any]]]}
                          :handler
                          (fn [{{{:keys [ledger authClaims] :as params} :body} :parameters :as req}]
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
                                     (http-server/ws-send! ws subscription-key)))
                                 :on-error
                                 (fn [ws error] (conn/unsubscribe ledger @sub-key))
                                 :on-close
                                 (fn [ws status-code reason] (conn/unsubscribe ledger @sub-key))})))}}]
    ["/unsubscribe" {:post {:summary "Unsubscribe from a ledger"
                            :parameters {:body [:map
                                                [:ledger :string]
                                                [:subcription-key :string]]}
                            :handler
                            (fn [{{{:keys [ledger subscription-key] :as params} :body} :parameters :as req}]
                              (conn/unsubscribe conn ledger subscription-key))}}]]
   ["/admin"
    ["ui" {:get {:handler (fn [_] {:status 200 :body "Admin User Interface"})}}]]])

(comment
  (require '[reitit.ring :as ring])
  (require '[reitit.core :as r])

  (r/match-by-path
    (r/router [["/ledger" ::ledger]
               ["/ledger/:ledger-name/transact"] ::transact])
    "/ledger")
  (r/match-by-path
    (r/router
      [["/ledger" ::ledger]
       ["/ledger/:id"
        ["/foo" ::foo]
        ["/bar" ::bar]]])
    "/ledger/a/foo")




  )
