(ns fluree.db.json-ld.api)

;; ledger operations
(defn new
  "Creates a new json-ld ledger"
  []
  )

(defn pull
  "Checks name service for ledger and pulls latest version locally."
  []
  )

(defn connect
  "Forms connection to ledger, enabling automatic pulls of new updates, event
  services, index service.

  Multiple connections to same endpoint will share underlying network connectivity."
  []
  )

(defn combine
  "Combines multiple ledgers into a new, read-only ledger."
  []
  )


;; transaction operations
(defn transact
  "Performs a transaction and queues change if valid (does not commit)"
  []
  )


(defn commit
  "Commits one or more transactions that are queued."
  []
  )


(defn push
  "Pushes one or more commits to a naming service, e.g. a Fluree Network, IPNS, DNS, Fluree Nexus.
  Depending on consensus requirements for a Fluree Network, will accept or reject push as newest update."
  []
  )


(defn squash
  "Squashes multiple transactions into a single transaction"
  []
  )


(defn merge
  "Merges changes from one branch into another branch."
  []
  )


(defn branch
  "Creates a new branch of a given ledger"
  []
  )

