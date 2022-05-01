(ns fluree.db.ledger.proto)

#?(:clj (set! *warn-on-reflection* true))

(defprotocol iLedger
  ;; retrieving/updating DBs
  (-stage [ledger tx] [ledger tx opts] "Stage a transaction against a ledger and specific branch (or default)")
  (-db [ledger opts] "Returns queryable db with specified options")
  (-db-latest [ledger] [ledger branch] "Returns the latest DB in the ledger, with optional branch")
  (-db-update [ledger db] "Updates ledger state with new DB, and optional branch")
  ;; branching
  (-branch [ledger] [ledger branch]  "Returns all branch metadata, or metadata for just specified branch. :default branch is always current default.")
  (-branch-checkout [ledger branch]  "Checks out (or sets default) the specified branch. If optional 'create?' flag, forks from latest db of current branch")
  (-branch-create [ledger branch opts]  "Creates a new branch with specified options map.")
  (-branch-delete [ledger branch]  "Deletes specified branch.")
  ;; committing
  (-commit! [ledger] [ledger branch] [ledger branch t] "Commits specified (or :default) branch, optionally only to specified 't' value.")
  (-commit-update [ledger branch commit-meta] "Once a commit completes, update ledger state to reflect")
  (-commit [ledger] [ledger branch] "Returns commit metadata for branch (default branch if nil)" )
  ;; ledger data across time
  (-t-range [ledger from-t to-t] [ledger branch from-t to-t] "Returns list of ledger entries from/to t")
  (-time->t [ledger time]  "Returns t value at specified time.")
  (-hash->t [ledger time]  "Returns t value at specified ledger hash value.")
  (-tag->t [ledger time]  "Returns t value at specified ledger tag value.")
  ;; default did
  (-did [ledger] "Returns default did configuration map")
  )