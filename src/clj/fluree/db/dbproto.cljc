(ns fluree.db.dbproto)

(defprotocol IFlureeDb
  (-rootdb [db] "Returns root db version of this db.")
  ;; schema-related
  (-p-prop [db property predicate] "Returns the property specified for the given predicate.")
  (-class-prop [db property class] "Return class properties")
  ;; following return async chans
  (-class-ids [db subject-id] "For the provided subject-id (long int), returns a list of class subject ids it is a member of (long ints)")
  (-iri [db subject-id] [db ident compact-fn] "Returns the IRI for the requested subject ID (json-ld only)")
  (-query [db query] [db query opts] "Performs a query.")
  (-stage [db tx] [db tx opts] [db fuel-tracker tx opts] "Stages a database transaction.")
  (-index-update [db commit-index] "Updates db to reflect a new index point described by commit-index metadata"))

(defn db?
  [db]
  (satisfies? IFlureeDb db))
