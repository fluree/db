(ns fluree.fluree-server.main
  (:require [fluree.connector.api :as conn]
            [donut.system :as ds]
            [fluree.http-server.api :as http-server]
            [clojure.java.io :as io]
            [clojure.walk :as walk]
            [fluree.db.util.log :as log]
            [fluree.transactor.api :as txr]
            [fluree.indexer.api :as idxr]
            [fluree.publisher.api :as pub])
  (:gen-class))

(defn transaction-server-api
  [txr]
  ["/transactor"
   ["/commit" {:post {:summary "Commit a transaction."
                      :parameters {:body [:map
                                          [:tx [:map-of :string :any]]
                                          [:tx-info [:map
                                                     [:ledger-name :string]
                                                     [:commit-t :int]
                                                     [:commit-prev :string]]]
                                          [:opts {:optional true} [:map-of :string :any]]]}
                      :handler (fn [{{{:keys [tx tx-info] :as params} :body} :parameters}]
                                 (println "commiting" (pr-str params))
                                 (let [res (txr/commit txr tx tx-info)]
                                   {:status 200 :body res}))}}]
   ["/resolve" {:post {:summary "Retrieve a commit."
                       :parameters {:body [:map
                                           [:commit-address :string]]}
                       :handler (fn [{{{:keys [commit-address] :as params} :body} :parameters}]
                                  (println "resolving" (pr-str params))
                                  (let [res (txr/resolve txr commit-address)]
                                    {:status 200 :body res}))}}]])

(defn name-server-api
  [pub]
  ["/ledger"
   ["/list" {:get {:summary "List all ledgers tracked by name server."
                   :handler (fn []
                               (println "listing")
                               (let [res (pub/list pub)]
                                 {:status 200 :body res}))}}]
   ["/push" {:post {:summary "Publish a new ledger head."
                    :parameters {:body [:map
                                        [:ledger-address :string]
                                        [:summary
                                         [:map
                                          [:commit-summary {:optional true} txr/CommitSummary]
                                          [:db-summary {:optional true} idxr/DbSummary]]]]}
                    :handler (fn [{{{:keys [ledger-address summary] :as params} :body} :parameters}]
                               (println "pushing" (pr-str params))
                               (let [res (pub/push pub ledger-address summary)]
                                 {:status 200 :body res}))}}]
   ["/pull" {:post {:summary "Retrieve ledger summary."
                    :parameters {:body [:map
                                        [:ledger-address :string]]}
                    :handler (fn [{{{:keys [ledger-address] :as params} :body} :parameters}]
                               (println "pushing" (pr-str params))
                               (let [res (pub/pull pub ledger-address)]
                                 {:status 200 :body res}))}}]])

(defn indexing-server-api
  [idxr]
  ["/index"
   ["/init" {:post {:summary "Create a new ledger index."
                     :parameters {:body [:map
                                         [:ledger-name :string]
                                         [:opts {:optional true}
                                          [:map
                                           [:reindex-min-bytes {:optional true} :int]
                                           [:reindex-max-bytes {:optional true} :int]]]]}
                     :handler (fn [{{{:keys [ledger-name opts] :as params} :body} :parameters}]
                                (println "pushing" (pr-str params))
                                (let [res (idxr/init ledger-name opts)]
                                  {:status 200 :body res}))}}]
   ["/load" {:post {:summary "Load an existing ledger index."
                     :parameters {:body [:map
                                         [:db-address :string]
                                         [:opts {:optional true}
                                          [:map
                                           [:reindex-min-bytes {:optional true} :int]
                                           [:reindex-max-bytes {:optional true} :int]]]]}
                     :handler (fn [{{{:keys [db-address opts] :as params} :body} :parameters}]
                                (println "pushing" (pr-str params))
                                (let [res (idxr/load db-address opts)]
                                  {:status 200 :body res}))}}]
   ["/stage" {:post {:summary "Index some data."
                     :parameters {:body [:map
                                         [:db-address :string]
                                         [:data [:map-of :string :any]]]}
                     :handler (fn [{{{:keys [db-address data] :as params} :body} :parameters}]
                                (println "pushing" (pr-str params))
                                (let [res (idxr/stage db-address data)]
                                  {:status 200 :body res}))}}]
   ["/query" {:post {:summary "Query an index."
                     :parameters {:body [:map
                                         [:db-address :string]
                                         [:query [:map-of :string :any]]]}
                     :handler (fn [{{{:keys [db-address query] :as params} :body} :parameters}]
                                (println "pushing" (pr-str params))
                                (let [res (idxr/query db-address query)]
                                  {:status 200 :body res}))}}]])

(defn fluree-server-api
  [conn]
  [["/admin"
    ["ui" {:get {:handler (fn [] {:status 200 :body "Admin User Interface"})}}]]

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
                                   {:status 200 :body res}))}}]]])

(defn initialize-fluree-server
  "Load all ledgers."
  [conn config]
  (log/info "Initializing fluree-server." {:config config})
  (doseq [ledger (conn/list conn)]
    (conn/load conn (:ledger/address ledger))))

(def system
  {::ds/defs
   {:config {:fluree/http-server {:port 58090}
             :fluree/connection
             {:conn/store-config {:store/method :file
                                  :file-store/storage-path "dev/data"
                                  :file-store/serialize-to :edn}
              :conn/indexer-config {:reindex-min-bytes 10}}}
    :services
    {:http-server #::ds{:start (fn [{{:keys [port conn]} ::ds/config}]
                                 (initialize-fluree-server conn {})
                                 (log/info "Starting fluree-server http-server." {:port port})
                                 (http-server/start {:http/port port :http/routes (fluree-server-api conn)}))
                        :stop (fn [{http-server ::ds/instance}]
                                (http-server/stop http-server))
                        :config {:port (ds/ref [:config :fluree/http-server :port])
                                 :conn (ds/ref [:services :conn])}}

     :conn #::ds{:start (fn [{config ::ds/config}]
                          (log/info "Creating fluree-server connection." config)
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
