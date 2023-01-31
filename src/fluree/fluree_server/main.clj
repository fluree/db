(ns fluree.fluree-server.main
  (:require [fluree.connector.api :as conn]
            [donut.system :as ds]
            [fluree.http-server.api :as http-server]
            [clojure.java.io :as io]
            [clojure.walk :as walk]
            [fluree.fluree-server.fluree :as fluree-api]
            [fluree.fluree-server.transactor :as transactor-api]
            [fluree.db.util.log :as log]
            [fluree.transactor.api :as txr]
            [fluree.indexer.api :as idxr]
            [fluree.publisher.api :as pub]
            [org.httpkit.client :as http])
  (:gen-class))

(def fluree-server-config
  {:conn/mode :fluree
   :conn/did {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6",
              :public "030be728546a7fe37bb527749e19515bd178ba8a5485ebd1c37cdf093cf2c247ca",
              :private "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"}
   :conn/trust :all
   :conn/store-config {:store/method :file
                       :file-store/storage-path "dev/data/fluree"
                       :file-store/serialize-to :edn}})

(def query-server-config
  {:conn/mode :query
   :conn/did {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6",
              :public "030be728546a7fe37bb527749e19515bd178ba8a5485ebd1c37cdf093cf2c247ca",
              :private "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"}
   :conn/trust :all
   :conn/store-config {:store/method :file
                       :file-store/storage-path "dev/data/query"
                       :file-store/serialize-to :edn}})

(def transaction-server-config
  {:conn/mode :transactor
   :txr/did {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6",
             :public "030be728546a7fe37bb527749e19515bd178ba8a5485ebd1c37cdf093cf2c247ca",
             :private "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"}
   :txr/trust :all
   :txr/store-config {:store/method :file
                      :file-store/storage-path "dev/data/txr"
                      :file-store/serialize-to :edn}})

(def name-server-config
  {:conn/mode :publisher
   :pub/did {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6",
             :public "030be728546a7fe37bb527749e19515bd178ba8a5485ebd1c37cdf093cf2c247ca",
             :private "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"}
   :pub/trust :all
   :pub/store-config {:store/method :file
                      :file-store/storage-path "dev/data/pub"
                      :file-store/serialize-to :edn}})

(def indexer-server-config
  {:conn/mode :indexer
   :idxr/did {:id "did:fluree:TfCzWTrXqF16hvKGjcYiLxRoYJ1B8a6UMH6",
              :public "030be728546a7fe37bb527749e19515bd178ba8a5485ebd1c37cdf093cf2c247ca",
              :private "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"}
   :idxr/trust :all
   :idxr/store-config {:store/method :file
                       :file-store/storage-path "dev/data/idxr"
                       :file-store/serialize-to :edn}})

(defn start-api
  [config])

(def system
  {::ds/defs
   {:config {:fluree/http-server {:port 58090}
             :fluree/connection
             {}}
    :services
    {:http-server #::ds{:start (fn [{{:keys [port conn]} ::ds/config}]
                                 (http-server/start {:http/port port
                                                     :http/routes (fluree-api/start-fluree-server)}))
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
