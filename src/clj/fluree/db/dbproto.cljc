(ns fluree.db.dbproto)

(defprotocol IFlureeDb
  (-rootdb [db] "Returns root db version of this db.")
  ;; schema-related
  (-p-prop [db property predicate] "Returns the property specified for the given predicate.")
  ;; following return async chans
  (-class-ids [db subject-id] "For the provided subject-id (long int), returns a list of class subject ids it is a member of (long ints)")
  (-index-update [db commit-index] "Updates db to reflect a new index point described by commit-index metadata"))
