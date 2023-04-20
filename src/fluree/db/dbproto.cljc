(ns fluree.db.dbproto)

(defprotocol IFlureeDb
  (-rootdb [db] "Returns root db version of this db.")
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
  (-query [db query] "Performs a query.")
  (-stage [db tx] [db tx opts] "Stages a database transaction.")
  (-index-update [db commit-index] "Updates db to reflect a new index point described by commit-index metadata")
  (-context [db] [db context] "Returns parsed context given supplied context. If no context is supplied, returns default context.")
  (-default-context [db] "Returns the default context the db is configured to use.")
  (-default-context-update [db new-default] "Updates the default context, so it will get written on out the next commit."))

(defn db?
  [db]
  (satisfies? IFlureeDb db))
