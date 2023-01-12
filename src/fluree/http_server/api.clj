(ns fluree.http-server.api
  (:require
   [fluree.http-server.core :as http-server-impl]
   [fluree.http-server.model :as server-model]))

(defn start
  [config]
  (http-server-impl/start config))

(defn stop
  [http-server]
  (http-server-impl/stop http-server))

(def HttpServerConfig server-model/HttpServerConfig)
