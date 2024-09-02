(ns fluree.db.ledger)

#?(:clj (set! *warn-on-reflection* true))


(defprotocol iCommit
  ;; retrieving/updating DBs
  (-commit! [ledger db] [ledger db opts] "Commits a db to a ledger.")
  (-notify [ledger commit-notification] "Notifies of an updated commit for a given ledger, will attempt cached ledger."))

(defprotocol iLedger
  ;; retrieving/updating DBs
  (-db [ledger] "Returns queryable db with specified options")
  ;; committing
  (-status [ledger] [ledger branch] "Returns status for branch (default branch if nil)")
  (-close [ledger] "Shuts down ledger processes and clears used resources."))
