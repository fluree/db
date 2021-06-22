(ns fluree.db.dbproto
  (:refer-clojure :exclude [-lookup resolve]))

(defprotocol IFlureeDb
  (-latest-db [db] "Updates a db to the most current version of the db known to this server. Maintains existing permissions")
  (-rootdb [db] "Returns root db version of this db.")
  (-forward-time-travel [db flakes] [db tt-id flakes])
  ;; schema-related
  (-c-prop [db property collection] "Returns schema property for a collection.")
  (-p-prop [db property predicate] "Returns the property specified for the given predicate.")
  ;; following return async chans
  (-tag [db tag-id] [db tag-id pred] "Returns resolved tag, shortens namespace if pred provided.")
  (-tag-id [db tag-name] [db tag-name pred] "Returns the tag sid. If pred provided will namespace tag if not already.")
  (-subid [db ident] [db ident strict?] "Returns subject ID if exists, else nil")
  (-search [db fparts] "Performs a slice, but determines best index to use.")
  (-query [db query] [db query opts] "Performs a query.")
  (-with [db block flakes] [db block flakes opts] "Applies flakes to this db as a new block with possibly multiple 't' transactions.")
  (-with-t [db flakes] [db flakes opts] "Applies flakes to this db as a new 't', but retains current block.")
  (-add-predicate-to-idx [db pred-id] "Adds predicate to idx, return updated db."))

(defn db?
  [db]
  (satisfies? IFlureeDb db))
