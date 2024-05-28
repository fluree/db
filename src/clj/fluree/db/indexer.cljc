(ns fluree.db.indexer
  (:refer-clojure :exclude [-add-watch -remove-watch]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol Indexed
  (collect [indexed changes-ch]))
