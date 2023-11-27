(ns fluree.db.dbproto)

(defprotocol IFlureeDb
  (-rootdb [db] "Returns root db version of this db.")
  ;; schema-related
  (-c-prop [db property collection] "Returns schema property for a collection.")
  (-p-prop [db property predicate] "Returns the property specified for the given predicate.")
  (-class-prop [db property class] "Return class properties")
  (-expand-iri [db iri] [db iri context])
  ;; following return async chans
  (-subid [db ident] [db ident strict?] "Returns subject ID if exists, else nil")
  (-class-ids [db subject-id] "For the provided subject-id (long int), returns a list of class subject ids it is a member of (long ints)")
  (-iri [db subject-id] [db ident compact-fn] "Returns the IRI for the requested subject ID (json-ld only)")
  (-query [db query] [db query opts] "Performs a query.")
  (-stage [db tx] [db tx opts] [db fuel-tracker tx opts] "Stages a database transaction.")
  (-index-update [db commit-index] "Updates db to reflect a new index point described by commit-index metadata")
  (-context [db] [db context] [db context context-type] "Returns parsed context given supplied context. If no context is supplied, returns default context.")
  (-default-context [db] "Returns the default context the db is configured to use.")
  (-default-context-update [db new-default] "Updates the default context, so it will get written on out the next commit.")
  (-context-type [db] "Returns the db's context-type; :keyword or :string"))

(defn db?
  [db]
  (satisfies? IFlureeDb db))
