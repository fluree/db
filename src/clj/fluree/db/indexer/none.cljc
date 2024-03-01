(ns fluree.db.indexer.none
  (:require [fluree.db.indexer :as indexer]))

;; this is an indexer that never returns a new index

#?(:clj (set! *warn-on-reflection* true))

(defn not-supported!
  [ex-message-append]
  (throw (ex-info (str "IndexerNone cannot " ex-message-append)
                  {:status 500 :error :db/unexpected-error})))


(defrecord IndexerNone []
  indexer/iIndex
  (-index? [_ db] false)
  (-halt? [_ db] false)
  (-index [indexer db] (not-supported! "perform index!"))
  (-index [indexer db opts] (not-supported! "perform index!"))
  (-add-watch [_ watch-id callback] (not-supported! "add watches!"))
  (-remove-watch [_ watch-id] (not-supported! "remove watches!"))
  (-push-event [_ event-data] (not-supported! "push events!"))
  (-close [indexer] true)
  (-status [indexer] (not-supported! "index status!"))
  (-reindex [indexer db] (not-supported! "reindex!")))


(defn create
  []
  (map->IndexerNone {}))