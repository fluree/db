(ns fluree.db.indexer
  (:refer-clojure :exclude [-add-watch -remove-watch]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol Indexable
  (index [indexed changes-ch]))
