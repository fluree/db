(ns fluree.db.storage.remote
  (:require [fluree.db.storage :as storage]
            [fluree.db.remote-system :as remote]))

(defrecord RemoteResources [identifier method remote-system]
  storage/JsonArchive
  (-read-json [_ address keywordize?]
    (remote/remote-read remote-system address keywordize?)))

(defn open
  [identifier method remote-system]
  (->RemoteResources identifier method remote-system))
