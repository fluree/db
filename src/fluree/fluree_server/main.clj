(ns fluree.fluree-server.main
  (:require [fluree.connector.api :as conn]
            [donut.system :as ds]
            [fluree.http-server.api :as http-server]
            [clojure.java.io :as io]
            [clojure.walk :as walk])
  (:gen-class))

(defn app
  [conn]
  ["/ledger"
   ["/list" {:get {:summary "List ledgers."
                   :handler (fn [_req]
                              (let [res (conn/list conn)]
                                {:status 200 :body res}))}}]
   ["/create" {:post {:summary "Create a ledger."
                      :parameters {:body [:map
                                          [:name :string]
                                          [:opts {:optional true} [:map-of :string :any]]]}
                      :handler (fn [{{{ledger-name :name :as params} :body} :parameters}]
                                 (println "creating" (pr-str params))
                                 (let [res (conn/create conn ledger-name)]
                                   {:status 200 :body res}))}}]
   ["/transact" {:post {:summary "Transact data to the ledger."
                        :parameters {:body [:map
                                            [:ledger :string]
                                            [:tx [:map-of :string :any]]
                                            [:opts {:optional true} [:map-of :string :any]]]}
                        :handler (fn [{{{:keys [ledger tx opts] :as params} :body} :parameters}]
                                   (println "transacting" (pr-str params))
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
                                  {:status 200 :body res}))}}]])

(def system
  {::ds/defs
   {:config {:fluree/http-server {:port 58090}
             :fluree/connection
             {:conn/store-config {:store/method :memory}
              :conn/indexer-config {:reindex-min-bytes 10}}}
    :services
    {:http-server #::ds{:start (fn [{{:keys [port conn]} ::ds/config}]
                                 (println "staring fluree-server http-server" (pr-str {:port port :conn conn}))
                                 (http-server/start {:http/port port :http/routes (app conn)}))
                        :stop (fn [{http-server ::ds/instance}]
                                (http-server/stop http-server))
                        :config {:port (ds/ref [:config :fluree/http-server :port])
                                 :conn (ds/ref [:services :conn])}}

     :conn #::ds{:start (fn [{config ::ds/config}]
                          (println "staring fluree-server connection" (pr-str config))
                          (conn/connect config))
                 :stop (fn [{conn ::ds/instance}]
                         (conn/close conn))
                 :config (ds/ref [:config :fluree/connection])}}}})

(defmethod ds/named-system ::ds/repl [_] (ds/system :dev))
(defmethod ds/named-system :dev [_] system)
(defmethod ds/named-system :prod [_] (ds/system :dev))

(defn -main
  [& args]
  (ds/start :dev))


(comment

  (require 'donut.system.repl)

  donut.system.repl.state/system

  ;; start ::ds/repl system
  (donut.system.repl/start)

  ;; stop ::ds/repl system
  (donut.system.repl/stop)

  ;; restart ::ds/repl system
  (donut.system.repl/restart)

  ,)
