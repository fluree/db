(ns fluree.transactor.protocols
  (:refer-clojure :exclude [resolve load]))

(defprotocol Transactor
  (init [txr ledger-name] "Establish a new head for the given ledger.")
  (commit [txr ledger-name tx] "Takes a transaction and persists it as a commit wrapping data.")
  (load [txr ledger-name] "Return the commit address for the most recent commit for the given ledger.")
  (resolve [txr commit-address] "Returns the commit that corresponds to the commit-address."))
