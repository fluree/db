(ns fluree.db.storage.remote
  (:require [fluree.db.storage :as storage]
            [fluree.db.method.remote :as remote]))

(defrecord RemoteResource [server-state]
  storage/JsonArchive
  (-read-json [_ address keywordize?]
    (remote/remote-read server-state address keywordize?)))

(defn remote-resource
  [server-state]
  (->RemoteResource server-state))
