(ns fluree.db.indexer
  (:refer-clojure :exclude [-add-watch -remove-watch]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol iIndex
  (-index [indexer db] [indexer db opts]  "Executes index operation, returns a promise chan with indexed db once complete.")
  (-add-watch [indexer id callback]  "Provided callback fn will be executed with new indexing events.")
  (-remove-watch [indexer id]  "Removes watch fn.")
  (-push-event [indexer event-data] "Pushes an index event (map) to all watchers")
  (-close [indexer]  "Shuts down indexer, removes all watches after notification.")
  (-status [indexer]  "Returns current status of reindexing.")
  (-reindex [indexer db]  "Executes a full reindex on db."))


(defprotocol Indexed
  (collect [indexed changes-ch]))
