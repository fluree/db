(ns fluree.db.storage.remote
  (:require [fluree.db.storage :as storage]
            [fluree.db.method.remote :as remote]))

(defrecord RemoteResource [server-state]
  storage/JsonArchive
  (-read-json [_ address keywordize?]
    (remote/remote-read server-state address keywordize?)))

(defn new-state
  [servers]
  (atom {:servers      servers
         :connected-to nil
         :stats        {:connected-at nil}}))

(defn resource
  [servers]
  (-> servers new-state ->RemoteResource))