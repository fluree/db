(ns fluree.db.dbproto)

(defprotocol IFlureeDb
  (-query [db tracker query] "Performs a query.")
  (-class-ids [db tracker subject-id] "For the provided subject-id (long int), returns a list of class subject ids it is a member of (long ints)")
  (-index-update [db commit-index]
    "Updates db to reflect a new index point described by commit-index metadata.

    Returns a core.async channel that yields the updated DB when the index has
    been applied. Errors are delivered on the channel (using go-try semantics)."))
