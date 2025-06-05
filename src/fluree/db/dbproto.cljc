(ns fluree.db.dbproto)

(defprotocol IFlureeDb
  (-query [db fuel-tracker query] "Performs a query.")
  (-class-ids [db fuel-tracker subject-id] "For the provided subject-id (long int), returns a list of class subject ids it is a member of (long ints)")
  (-index-update [db commit-index] "Updates db to reflect a new index point described by commit-index metadata"))
