(ns fluree.transactor.protocols
  (:refer-clojure :exclude [read]))

(defprotocol Transactor
  (commit [txr tx tx-info] "Takes a transaction and persists it as a commit wrapping data.")
  (read [txr commit-address] "Returns the commit that corresponds to the commit-address."))
