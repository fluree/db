(ns fluree.db.dbproto)

(defprotocol IFlureeDb
  (-latest-db [db] "Updates a db to the most current version of the db known to this server. Maintains existing policy.")
  (-rootdb [db] "Returns root db version of this db.")
  (-forward-time-travel [db flakes] [db tt-id flakes])
  ;; schema-related
  (-c-prop [db property collection] "Returns schema property for a collection.")
  (-p-prop [db property predicate] "Returns the property specified for the given predicate.")
  (-class-prop [db property class] "Return class properties")
  (-expand-iri [db iri] [db iri context])
  ;; following return async chans
  (-tag [db tag-id] [db tag-id pred] "Returns resolved tag, shortens namespace if pred provided.")
  (-tag-id [db tag-name] [db tag-name pred] "Returns the tag sid. If pred provided will namespace tag if not already.")
  (-subid [db ident] [db ident strict?] "Returns subject ID if exists, else nil")
  (-class-ids [db subject-id] "For the provided subject-id (long int), returns a list of class subject ids it is a member of (long ints)")
  (-iri [db subject-id] [db ident compact-fn] "Returns the IRI for the requested subject ID (json-ld only)")
  (-search [db fparts] "Performs a slice, but determines best index to use.")
  (-query [db query] [db query opts] "Performs a query.")
  (-with [db block flakes] [db block flakes opts] "Applies flakes to this db as a new block with possibly multiple 't' transactions.")
  (-with-t [db flakes] [db flakes opts] "Applies flakes to this db as a new 't', but retains current block.")
  (-add-predicate-to-idx [db pred-id] "Adds predicate to idx, return updated db.")
  (-db-type [db] "Returns db type, e.g. :json-ld, :json")
  (-stage [db tx] [db tx opts] "Stages a database transaction.")
  (-index-update [db commit-index] "Updates db to reflect a new index point described by commit-index metadata")
  (-context [db] [db context] [db context context-type] "Returns parsed context given supplied context. If no context is supplied, returns default context.")
  (-default-context [db] "Returns the default context the db is configured to use.")
  (-default-context-update [db new-default] "Updates the default context, so it will get written on out the next commit."))

(defn db?
  [db]
  (satisfies? IFlureeDb db))
