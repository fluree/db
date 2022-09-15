(ns fluree.db.ledger.proto)

#?(:clj (set! *warn-on-reflection* true))

(defprotocol iCommit
  ;; retrieving/updating DBs
  (-commit! [db] [ledger-or-db db-or-opts] [ledger db opts] "Commits a db to a ledger."))

(defprotocol iLedger
  ;; retrieving/updating DBs
  (-db [ledger] [ledger opts] "Returns queryable db with specified options")
  (-db-update [ledger db] "Updates ledger state with new DB, and optional branch. Returns updated db (which might be modified with newer index).")
  ;; branching
  (-branch [ledger] [ledger branch] "Returns all branch metadata, or metadata for just specified branch. :default branch is always current default.")
  (-branch-checkout [ledger branch] "Checks out (or sets default) the specified branch. If optional 'create?' flag, forks from latest db of current branch")
  (-branch-create [ledger branch opts] "Creates a new branch with specified options map.")
  (-branch-delete [ledger branch] "Deletes specified branch.")
  ;; committing
  (-commit-update [ledger branch db] [ledger branch db force?] "Once a commit completes, update ledger state to reflect. If optional force? flag present, don't validate consistent t sequence")
  (-status [ledger] [ledger branch] "Returns status for branch (default branch if nil)")
  ;; ledger data across time
  (-t-range [ledger from-t to-t] [ledger branch from-t to-t] "Returns list of ledger entries from/to t")
  (-time->t [ledger time] "Returns t value at specified time.")
  (-hash->t [ledger time] "Returns t value at specified ledger hash value.")
  (-tag->t [ledger time] "Returns t value at specified ledger tag value.")
  ;; default did
  (-did [ledger] "Returns default did configuration map")
  ;; alias name for graph
  (-alias [ledger] "Returns the ledger local alias / graph name")
  (-address [ledger] "Returns the permanent ledger address")
  (-close [ledger] "Shuts down ledger processes and clears used resources."))
