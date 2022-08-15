(ns fluree.db.indexer.proto)

#?(:clj (set! *warn-on-reflection* true))

(defprotocol iIndex
  (-index? [indexer db] "Returns true if db requires a reindex")
  (-halt? [indexer db] "Returns true if new transactions need to be blocked based on reindex max threshold being hit")
  (-index [indexer db remove-preds]  "Executes index operation, returns an id that can be used to check status.")
  (-add-watch [indexer id callback]  "Provided callback fn will be executed with new indexing events.")
  (-remove-watch [indexer id]  "Removes watch fn.")
  (-push-event [indexer event-data] "Pushes an index event (map) to all watchers")
  (-close [indexer]  "Shuts down indexer, removes all watches after notification.")
  (-status [indexer] [indexer index-id]  "Returns current status of reindexing, or if optional index-id just of that index process.")
  (-reindex [indexer db]  "Executes a full reindex on db."))


