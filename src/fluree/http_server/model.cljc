(ns fluree.http-server.model
  (:require [fluree.store.api :as store]))

(def HttpServerConfig
  [:and
   [:map
    [:http/routes :any]
    [:http/port :int]]
   [:or
    [:map [:http/store {:optional true} store/Store]]
    [:map [:http/store-config {:optional true} store/StoreConfig]]]])
