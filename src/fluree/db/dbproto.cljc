(ns fluree.db.dbproto)

(defprotocol IFlureeDb
  (-query [db tracker query] "Performs a query.")
  (-class-ids [db tracker subject-id] "For the provided subject-id (long int), returns a list of class subject ids it is a member of (long ints)")
  (-index-update [db commit-index]
    "Updates db to reflect a new index point described by commit-index metadata.

    Returns a core.async channel that yields the updated DB when the index has
    been applied. Errors are delivered on the channel (using go-try semantics).")
  (-ledger-info [db]
    "Returns ledger metadata needed for the ledger-info API.

    Returns a channel containing a map with:
      :stats - Stats map with :size, :flakes, :properties, :classes
      :namespace-codes - Namespace codes for SID->IRI decoding
      :t - Current transaction number
      :novelty-post - Post novelty for computing current stats
      :commit - Commit metadata
      :index - Index metadata with :id, :t, :address, :flakes, :size

    Both AsyncDB and FlakeDB return a channel for consistency."))
